use std::{sync::Arc, time::Duration};

use chrono::Utc;
use rand::Rng;
use tokio::{
    sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore},
    task::{JoinHandle, JoinSet},
    time::Instant,
};

use crate::{
    AckMode, BatchOutcome, DeliveryControl, DeliveryHandle, EventBusError, Handler, Message,
    MessageId, PublishOptions, Publisher, Subscriber, SubscriptionConfig,
};

use super::{
    ack_flusher::{self, AckRequest},
    backend::{ClaimedMessage, FetchedEntry, SharedBackend, StreamBackend},
    delivery::StreamDelivery,
    observer::{ErrorObserver, ErrorScope},
    subscription::{ConsumerCleanup, StreamSubscription},
};

use crate::HEADER_DEAD_LETTER_REASON;

const DEFAULT_PUBLISH_BATCH_PARALLELISM: usize = 32;
const MAX_BACKOFF_CEILING: Duration = Duration::from_secs(5);
/// Default cap on a single message payload (4 MiB). Prevents an adversarial or
/// runaway producer from blowing past Redis Streams' 512 MiB entry limit and
/// from causing OOM on consumers that allocate before validation.
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

type DeliveryTaskResult = Result<(), EventBusError>;

/// Shared runtime state passed to the consumer loop.
///
/// Unified limiter semantics: every message in flight holds exactly one
/// [`OwnedSemaphorePermit`] from `limiter`, for the full handler + ack
/// round-trip. The permit drops with the `Delivery`, so every termination
/// path (success, panic, cancel, orphan) returns the slot automatically.
#[derive(Clone)]
struct RuntimeState {
    handler: Arc<dyn Handler>,
    config: Arc<SubscriptionConfig>,
    limiter: Arc<Semaphore>,
    ack_tx: mpsc::Sender<AckRequest>,
}

#[derive(Clone)]
pub struct StreamBusOptions {
    pub block_timeout: Duration,
    pub claim_idle_timeout: Duration,
    pub claim_scan_batch_size: usize,
    pub group_start_id: String,
    /// Maximum concurrent backend `publish` calls inside a single
    /// `publish_batch`. Saturating the Redis connection pool gives diminishing
    /// returns; 32 is a sensible default for `MultiplexedConnection`.
    pub publish_batch_parallelism: usize,
    /// Maximum number of ack IDs batched into a single `XACK` command.
    pub ack_batch_size: usize,
    /// Maximum time to wait after the first un-flushed ack before forcing a
    /// flush. Smaller values reduce ack latency; larger values amortize more
    /// round-trips.
    pub ack_flush_interval: Duration,
    /// How often to check for idle messages when handler capacity is available.
    /// Read waits are capped at the next scan so a long `block_timeout` does not
    /// postpone reclaim. No entries are claimed while the subscription is full.
    pub reclaim_interval: Duration,
    /// Hard cap on a single message's payload, in bytes. Messages that exceed
    /// this on publish are rejected with `Validation`; messages that exceed
    /// this on receive are surfaced as `Serialization` and never reach the
    /// handler. Set to `0` to disable the check (not recommended).
    pub max_payload_bytes: usize,
    /// Observer for transient errors raised by the read / reclaim / ack-flush
    /// loops. Without one, errors are silently retried with backoff; with
    /// one, you can route them to metrics, tracing, or alerts. The hook is
    /// invoked from inside the loops and **must not block**.
    pub error_observer: Option<Arc<dyn ErrorObserver>>,
}

impl std::fmt::Debug for StreamBusOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamBusOptions")
            .field("block_timeout", &self.block_timeout)
            .field("claim_idle_timeout", &self.claim_idle_timeout)
            .field("claim_scan_batch_size", &self.claim_scan_batch_size)
            .field("group_start_id", &self.group_start_id)
            .field("publish_batch_parallelism", &self.publish_batch_parallelism)
            .field("ack_batch_size", &self.ack_batch_size)
            .field("ack_flush_interval", &self.ack_flush_interval)
            .field("reclaim_interval", &self.reclaim_interval)
            .field("max_payload_bytes", &self.max_payload_bytes)
            .field(
                "error_observer",
                &self.error_observer.as_ref().map(|_| "<observer>"),
            )
            .finish()
    }
}

impl Default for StreamBusOptions {
    fn default() -> Self {
        Self {
            block_timeout: Duration::from_secs(2),
            claim_idle_timeout: Duration::from_secs(60),
            claim_scan_batch_size: 64,
            group_start_id: "$".to_string(),
            publish_batch_parallelism: DEFAULT_PUBLISH_BATCH_PARALLELISM,
            ack_batch_size: 64,
            ack_flush_interval: Duration::from_millis(2),
            reclaim_interval: Duration::from_millis(500),
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            error_observer: None,
        }
    }
}

impl StreamBusOptions {
    /// Constructs options with all defaults. Equivalent to [`Default::default`],
    /// provided so callers can chain `with_*` methods without importing `Default`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the `XREADGROUP BLOCK` timeout.
    #[must_use]
    pub fn with_block_timeout(mut self, v: Duration) -> Self {
        self.block_timeout = v;
        self
    }

    /// Sets how long a pending entry must sit before `XAUTOCLAIM` reclaims it.
    #[must_use]
    pub fn with_claim_idle_timeout(mut self, v: Duration) -> Self {
        self.claim_idle_timeout = v;
        self
    }

    /// Sets the `XAUTOCLAIM COUNT` per scan.
    #[must_use]
    pub fn with_claim_scan_batch_size(mut self, v: usize) -> Self {
        self.claim_scan_batch_size = v;
        self
    }

    /// Sets the consumer-group start id (`$` = new only, `0` = from history).
    #[must_use]
    pub fn with_group_start_id(mut self, v: impl Into<String>) -> Self {
        self.group_start_id = v.into();
        self
    }

    /// Sets the cap on concurrent backend `publish` calls per `publish_batch`.
    #[must_use]
    pub fn with_publish_batch_parallelism(mut self, v: usize) -> Self {
        self.publish_batch_parallelism = v;
        self
    }

    /// Sets the maximum number of ack ids per `XACK` command.
    #[must_use]
    pub fn with_ack_batch_size(mut self, v: usize) -> Self {
        self.ack_batch_size = v;
        self
    }

    /// Sets the maximum delay before forcing a partial ack batch flush.
    #[must_use]
    pub fn with_ack_flush_interval(mut self, v: Duration) -> Self {
        self.ack_flush_interval = v;
        self
    }

    /// Sets how often the reclaim task scans for idle pending entries.
    #[must_use]
    pub fn with_reclaim_interval(mut self, v: Duration) -> Self {
        self.reclaim_interval = v;
        self
    }

    /// Sets the maximum payload size accepted by `publish`/`publish_batch`
    /// and surfaced from incoming messages. `0` disables the check.
    #[must_use]
    pub fn with_max_payload_bytes(mut self, v: usize) -> Self {
        self.max_payload_bytes = v;
        self
    }

    /// Installs an [`ErrorObserver`] for transient runtime errors.
    #[must_use]
    pub fn with_error_observer(mut self, observer: Arc<dyn ErrorObserver>) -> Self {
        self.error_observer = Some(observer);
        self
    }

    fn normalize(mut self) -> Result<Self, EventBusError> {
        let defaults = Self::default();
        if self.block_timeout.is_zero() {
            self.block_timeout = defaults.block_timeout;
        }

        if self.claim_idle_timeout.is_zero() {
            self.claim_idle_timeout = defaults.claim_idle_timeout;
        }

        if self.claim_scan_batch_size == 0 {
            self.claim_scan_batch_size = defaults.claim_scan_batch_size;
        }

        if self.group_start_id.trim().is_empty() {
            self.group_start_id = defaults.group_start_id;
        }

        if self.publish_batch_parallelism == 0 {
            self.publish_batch_parallelism = defaults.publish_batch_parallelism;
        }

        if self.ack_batch_size == 0 {
            self.ack_batch_size = defaults.ack_batch_size;
        }

        if self.ack_flush_interval.is_zero() {
            self.ack_flush_interval = defaults.ack_flush_interval;
        }

        if self.reclaim_interval.is_zero() {
            self.reclaim_interval = defaults.reclaim_interval;
        }

        Ok(self)
    }
}

pub struct StreamBus<B: StreamBackend> {
    backend: SharedBackend<B>,
    options: StreamBusOptions,
}

impl<B: StreamBackend> Clone for StreamBus<B> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            options: self.options.clone(),
        }
    }
}

impl<B: StreamBackend> StreamBus<B> {
    pub fn new(
        backend: SharedBackend<B>,
        options: StreamBusOptions,
    ) -> Result<Self, EventBusError> {
        Ok(Self {
            backend,
            options: options.normalize()?,
        })
    }

    pub async fn publish(
        &self,
        msg: Message,
        opts: PublishOptions,
    ) -> Result<MessageId, EventBusError> {
        <Self as Publisher>::publish(self, msg, opts).await
    }

    pub async fn publish_batch(
        &self,
        msgs: Vec<Message>,
        opts: PublishOptions,
    ) -> Result<BatchOutcome, EventBusError> {
        <Self as Publisher>::publish_batch(self, msgs, opts).await
    }

    /// Convenience: subscribe with a concrete handler value, returning a
    /// concrete [`StreamSubscription`] (not the trait object). For dyn
    /// dispatch use the [`Subscriber`] trait directly.
    pub async fn subscribe<H>(
        &self,
        cfg: SubscriptionConfig,
        handler: H,
    ) -> Result<StreamSubscription, EventBusError>
    where
        H: Handler + 'static,
    {
        self.subscribe_inner(cfg, Arc::new(handler)).await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, message, options), fields(topic = %message.topic)))]
    async fn publish_inner(
        &self,
        message: Message,
        options: &PublishOptions,
    ) -> Result<MessageId, EventBusError> {
        options.validate()?;

        if let Some(delay) = options.delay {
            tokio::time::sleep(delay).await;
        }

        let message = Self::prepare_message(message, options, self.options.max_payload_bytes)?;

        let topic = message.topic.clone();
        let id = self.backend.publish(topic.as_str(), message).await?;
        Ok(MessageId::new(id))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, msgs, opts), fields(count = msgs.len())))]
    async fn publish_batch_impl(
        &self,
        msgs: Vec<Message>,
        opts: PublishOptions,
    ) -> Result<BatchOutcome, EventBusError> {
        opts.validate()?;

        if let Some(delay) = opts.delay {
            tokio::time::sleep(delay).await;
        }

        let total = msgs.len();
        let parallelism = total.clamp(1, self.options.publish_batch_parallelism);
        let mut iter = msgs.into_iter().enumerate();
        let mut tasks: JoinSet<(usize, Result<MessageId, EventBusError>)> = JoinSet::new();
        let mut results: Vec<Option<Result<MessageId, EventBusError>>> =
            std::iter::repeat_with(|| None).take(total).collect();

        // Pre-fill each spawned slot with a placeholder so a task panic
        // (which loses its captured `idx` inside JoinError) still leaves a
        // correctly-attributed error in the right slot.
        loop {
            while tasks.len() < parallelism {
                let Some((idx, message)) = iter.next() else {
                    break;
                };
                let prep = Self::prepare_message(message, &opts, self.options.max_payload_bytes);
                results[idx] = Some(Err(EventBusError::Internal(
                    "publish task did not complete".into(),
                )));
                let backend = Arc::clone(&self.backend);
                tasks.spawn(async move {
                    let r = match prep {
                        Err(e) => Err(e),
                        Ok(m) => {
                            let topic = m.topic.clone();
                            backend.publish(topic.as_str(), m).await.map(MessageId::new)
                        }
                    };
                    (idx, r)
                });
            }
            let Some(joined) = tasks.join_next().await else {
                break;
            };
            match joined {
                Ok((idx, r)) => {
                    results[idx] = Some(r);
                }
                Err(je) => {
                    // The placeholder stays in place at the panicking task's
                    // slot; we surface the panic via the observer rather than
                    // guessing the slot.
                    if let Some(obs) = self.options.error_observer.as_ref() {
                        obs.on_panic(ErrorScope::HandlerPanic, &je.to_string());
                    }
                }
            }
        }

        Ok(BatchOutcome {
            results: results
                .into_iter()
                .map(|o| {
                    o.unwrap_or_else(|| {
                        Err(EventBusError::Internal(
                            "publish_batch slot never filled".into(),
                        ))
                    })
                })
                .collect(),
        })
    }

    fn prepare_message(
        mut message: Message,
        options: &PublishOptions,
        max_payload_bytes: usize,
    ) -> Result<Message, EventBusError> {
        // Topic was validated at `Topic::new` construction; nothing to do here.
        if max_payload_bytes > 0 && message.payload.len() > max_payload_bytes {
            return Err(EventBusError::Validation(format!(
                "message payload {} bytes exceeds max_payload_bytes {}",
                message.payload.len(),
                max_payload_bytes,
            )));
        }

        for (key, value) in &options.metadata {
            message.headers.insert(key.clone(), value.clone());
        }

        if let Some(idempotency_key) = options.idempotency_key.as_deref() {
            message.set_idempotency_key(idempotency_key);
        }

        Ok(message)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            skip_all,
            fields(
                topic = %runtime.config.topic.as_str(),
                group = %runtime.config.consumer_group.as_str()
            )
        )
    )]
    async fn consume_loop(
        self,
        mut close_rx: watch::Receiver<bool>,
        runtime: RuntimeState,
        flusher_handle: JoinHandle<()>,
    ) -> Result<(), EventBusError> {
        let mut tasks = JoinSet::new();
        let mut first_delivery_error: Option<EventBusError> = None;
        let mut backoff = BackoffState::new(runtime.config.retry_backoff);
        let mut reclaim_backoff = BackoffState::new(Duration::from_millis(100));
        let mut next_reclaim = Instant::now() + self.options.reclaim_interval;
        let observer = self.options.error_observer.clone();

        loop {
            if *close_rx.borrow() {
                break;
            }

            drain_completed_tasks(&mut tasks, observer.as_ref(), &mut first_delivery_error)?;

            // Reserve capacity before either read or reclaim, including the ACK
            // round-trip. Reclaim must not take ownership of undispatchable work.
            let max_batch = runtime
                .config
                .backpressure
                .as_ref()
                .map_or(usize::MAX, |p| p.max_batch_size.max(1));
            let mut permits: Vec<OwnedSemaphorePermit> = Vec::new();
            while permits.len() < max_batch {
                match Arc::clone(&runtime.limiter).try_acquire_owned() {
                    Ok(p) => permits.push(p),
                    Err(_) => break,
                }
            }

            if permits.is_empty() {
                if !wait_for_task_or_close(
                    &mut tasks,
                    &mut close_rx,
                    backoff.peek(),
                    observer.as_ref(),
                    &mut first_delivery_error,
                )
                .await
                {
                    break;
                }
                continue;
            }

            let read_limit = permits.len();
            if Instant::now() >= next_reclaim {
                let result = tokio::select! {
                    _ = close_rx.changed() => break,
                    result = self.backend.reclaim_idle(
                        runtime.config.topic.as_str(),
                        runtime.config.consumer_group.as_str(),
                        runtime.config.consumer_name.as_str(),
                        self.options.claim_idle_timeout,
                        read_limit.min(self.options.claim_scan_batch_size),
                    ) => result,
                };
                next_reclaim = Instant::now() + self.options.reclaim_interval;
                match result {
                    Ok(messages) => {
                        reclaim_backoff.reset();
                        if !messages.is_empty() {
                            self.spawn_messages(&mut tasks, messages, &mut permits, &runtime)
                                .await?;
                            continue;
                        }
                    }
                    Err(err) => {
                        if let Some(obs) = observer.as_ref() {
                            obs.on_error(ErrorScope::Reclaim, &err);
                        }
                        next_reclaim += reclaim_backoff.next();
                    }
                }
            }
            let read_timeout = self.options.block_timeout.min(
                next_reclaim
                    .saturating_duration_since(Instant::now())
                    .max(Duration::from_millis(1)),
            );
            let read_future = self.backend.read_new(
                runtime.config.topic.as_str(),
                runtime.config.consumer_group.as_str(),
                runtime.config.consumer_name.as_str(),
                read_limit,
                read_timeout,
            );
            tokio::pin!(read_future);

            let mut any_work = false;

            // Only shutdown can cancel a read. Racing it against reclaim could
            // discard a response after the backend already moved entries to PEL.
            tokio::select! {
                biased;
                changed = close_rx.changed() => {
                    if changed.is_ok() && *close_rx.borrow() {
                        break;
                    }
                    // permits drop here, returning slots to the limiter
                    continue;
                }
                result = &mut read_future => {
                    match result {
                        Ok(messages) if !messages.is_empty() => {
                            any_work = true;
                            self.spawn_messages(&mut tasks, messages, &mut permits, &runtime).await?;
                        }
                        Ok(_) => {}
                        Err(err) => {
                            if let Some(obs) = observer.as_ref() {
                                obs.on_error(ErrorScope::Read, &err);
                            }
                            let sleep_dur = backoff.next();
                            // permits drop at end of iteration
                            if !sleep_or_close(&mut close_rx, sleep_dur).await {
                                break;
                            }
                            continue;
                        }
                    }
                }
            }

            // Any permits left in `permits` drop here — slots return to the limiter.

            if any_work {
                backoff.reset();
            }
        }

        // Graceful drain: wait for in-flight tasks to finish.
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    first_delivery_error.get_or_insert(err);
                }
                Err(err) => {
                    if let Some(obs) = observer.as_ref() {
                        obs.on_panic(ErrorScope::HandlerPanic, &err.to_string());
                    }
                    first_delivery_error.get_or_insert_with(|| {
                        EventBusError::source("delivery task panicked", err)
                    });
                }
            }
        }

        // Drop all senders so the flusher drains its remaining buffer and exits.
        drop(runtime);
        let _ = flusher_handle.await;

        if let Some(err) = first_delivery_error {
            return Err(err);
        }

        Ok(())
    }

    async fn spawn_messages(
        &self,
        tasks: &mut JoinSet<DeliveryTaskResult>,
        entries: Vec<FetchedEntry>,
        permits: &mut Vec<OwnedSemaphorePermit>,
        runtime: &RuntimeState,
    ) -> Result<(), EventBusError> {
        // Both backend paths were given a count bounded by these permits.
        for entry in entries {
            match entry {
                FetchedEntry::Decoded(claimed) => {
                    let Some(permit) = permits.pop() else {
                        return Err(EventBusError::Internal(
                            "backend returned more messages than requested".into(),
                        ));
                    };
                    let bus = self.clone();
                    let config = Arc::clone(&runtime.config);
                    let handler = Arc::clone(&runtime.handler);
                    let ack_tx = runtime.ack_tx.clone();
                    tasks.spawn(async move {
                        bus.process_single_message(config, handler, claimed, permit, ack_tx)
                            .await
                    });
                }
                FetchedEntry::Malformed { id, error } => {
                    self.handle_malformed_entry(&runtime.config, id, error)
                        .await;
                }
            }
        }
        Ok(())
    }

    /// Surface a malformed PEL entry: observe, route to dead-letter when one
    /// is configured, and ack so the entry leaves Redis' pending list. Without
    /// this, `XREADGROUP` having already moved the entry into PEL means
    /// reclaim would re-decode + re-fail forever (poison pill).
    async fn handle_malformed_entry(
        &self,
        config: &SubscriptionConfig,
        id: String,
        error: EventBusError,
    ) {
        if let Some(obs) = self.options.error_observer.as_ref() {
            obs.on_error(ErrorScope::Read, &error);
        }

        if let Some(dlq) = config.dead_letter_topic.as_ref() {
            let mut headers = std::collections::HashMap::new();
            headers.insert(HEADER_DEAD_LETTER_REASON.to_string(), error.to_string());
            let envelope = Message {
                uid: format!("malformed-{id}"),
                topic: dlq.clone(),
                key: id.clone(),
                kind: "eventbus.malformed".into(),
                source: config.topic.as_str().to_string(),
                occurred_at: Utc::now(),
                headers,
                payload: bytes::Bytes::new(),
                content_type: None,
                event_version: None,
                idempotency_key: None,
                expires_at: None,
                trace_uid: None,
                correlation_uid: None,
            };
            if let Err(err) = self.backend.publish(dlq.as_str(), envelope).await {
                if let Some(obs) = self.options.error_observer.as_ref() {
                    obs.on_error(ErrorScope::Read, &err);
                }
                // If the DLQ publish fails we still want the original entry
                // out of the PEL — fall through to ack.
            }
        }

        if let Err(err) = self
            .backend
            .ack(config.topic.as_str(), config.consumer_group.as_str(), &id)
            .await
        {
            if let Some(obs) = self.options.error_observer.as_ref() {
                obs.on_error(ErrorScope::AckFlush, &err);
            }
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            skip(self, config, handler, claimed, permit, ack_tx),
            fields(message_id = %claimed.id)
        )
    )]
    async fn process_single_message(
        &self,
        config: Arc<SubscriptionConfig>,
        handler: Arc<dyn Handler>,
        claimed: ClaimedMessage,
        permit: OwnedSemaphorePermit,
        ack_tx: mpsc::Sender<AckRequest>,
    ) -> Result<(), EventBusError> {
        use super::auto_finalize::AutoFinalizeTracker;

        // `max_retry` is the number of *retries*; the initial delivery
        // doesn't count, so the attempt budget is `max_retry + 1`. With
        // `max_retry = 0`, the first failure goes straight to DLQ.
        let max_attempt = (config.max_retry as u32).saturating_add(1);
        let state = claimed.state.with_max_attempt(max_attempt);
        let max_payload_bytes = self.options.max_payload_bytes;
        let ack_mode = config.ack_mode;

        // Post-decode safety net: oversized payloads are routed straight to
        // dead-letter (when configured) so the user handler never sees them.
        if max_payload_bytes > 0 && claimed.message.payload.len() > max_payload_bytes {
            let oversize_err = EventBusError::Validation(format!(
                "received payload {} bytes exceeds max_payload_bytes {}",
                claimed.message.payload.len(),
                max_payload_bytes,
            ));
            let delivery = Box::new(StreamDelivery::new(
                Arc::clone(&self.backend),
                ack_tx,
                claimed.id,
                claimed.message,
                state,
                Arc::clone(&config),
                permit,
            ));
            if config.dead_letter_topic.is_some() {
                return delivery.nack(Box::new(oversize_err)).await;
            }
            return Err(oversize_err);
        }

        let delivery = Box::new(StreamDelivery::new(
            Arc::clone(&self.backend),
            ack_tx,
            claimed.id,
            claimed.message,
            state,
            Arc::clone(&config),
            permit,
        ));

        match ack_mode {
            AckMode::AutoOnReceive => {
                // Pre-ack via the internal seam, then deliver. The handler still
                // gets a fully-functional `DeliveryHandle`; if it calls `ack()`
                // again it just re-enqueues a redundant XACK which the backend
                // treats as idempotent (Redis silently ignores unknown ids;
                // the in-memory backend likewise no-ops on missing ids).
                delivery.pre_ack().await?;
                let boxed: Box<dyn DeliveryHandle> = delivery;
                // Handler errors in Auto/Manual modes are the handler's concern;
                // they don't drive any bus-level retry decision (the message is
                // already finalized for AutoOnReceive, and Manual leaves the
                // ack decision entirely to the handler).
                let _ = handler.handle(boxed).await;
                Ok(())
            }
            AckMode::Manual => {
                let boxed: Box<dyn DeliveryHandle> = delivery;
                let _ = handler.handle(boxed).await;
                Ok(())
            }
            AckMode::AutoOnHandlerSuccess => {
                let (tracker, proxy) = AutoFinalizeTracker::new(delivery);
                let proxy_boxed: Box<dyn DeliveryHandle> = Box::new(proxy);
                let result = handler.handle(proxy_boxed).await;
                // If the handler did not finalize via the proxy, do it for them.
                if let Some(remaining) = tracker.take_remaining() {
                    match result {
                        Ok(()) => remaining.ack().await?,
                        Err(err) => {
                            remaining.retry(Box::new(err)).await?;
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

impl<B: StreamBackend> Publisher for StreamBus<B> {
    fn publish(
        &self,
        msg: Message,
        opts: PublishOptions,
    ) -> crate::BoxFuture<'_, Result<MessageId, EventBusError>> {
        Box::pin(async move { self.publish_inner(msg, &opts).await })
    }

    fn publish_batch(
        &self,
        msgs: Vec<Message>,
        opts: PublishOptions,
    ) -> crate::BoxFuture<'_, Result<BatchOutcome, EventBusError>> {
        Box::pin(async move { self.publish_batch_impl(msgs, opts).await })
    }
}

impl<B: StreamBackend> StreamBus<B> {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            skip(self, cfg, handler),
            fields(topic = %cfg.topic, group = %cfg.consumer_group)
        )
    )]
    async fn subscribe_inner(
        &self,
        mut cfg: SubscriptionConfig,
        handler: Arc<dyn Handler>,
    ) -> Result<StreamSubscription, EventBusError> {
        // Topic / group / name are newtypes (validated at construction).
        // Defensive normalize_and_validate in case the caller hand-built the
        // config bypassing the builder.
        if cfg.consumer_name.as_str().trim().is_empty() {
            // Replace blank consumer-name with a unique auto-generated one.
            cfg.consumer_name = crate::ConsumerName::auto();
        }

        cfg.normalize_and_validate()?;

        if cfg.max_retry > 0 && cfg.retry_backoff >= self.options.claim_idle_timeout {
            return Err(EventBusError::Validation(
                "retry backoff must be shorter than claim idle timeout".into(),
            ));
        }

        if cfg.balance_mode == Some(crate::ConsumerBalanceMode::FanOut) {
            return Err(EventBusError::Validation(
                "FanOut balance mode is not yet supported by StreamBus".into(),
            ));
        }

        if cfg.ordering_mode == Some(crate::OrderingMode::Key) {
            return Err(EventBusError::Validation(
                "Key ordering is not supported by StreamBus".into(),
            ));
        }

        self.backend
            .create_group(
                cfg.topic.as_str(),
                cfg.consumer_group.as_str(),
                &self.options.group_start_id,
            )
            .await?;

        let (close_tx, close_rx) = watch::channel(false);
        // The handler-concurrency limiter is sized by `max_in_flight` *only*.
        // `max_pending_acks` previously inflated this, allowing the loop to
        // read up to 2× the documented in-flight cap (default
        // `max_pending_acks = 2 * max_in_flight`). It is still validated by
        // `BackpressurePolicy` as an advisory upper bound, but does not
        // control the limiter — every in-flight handler holds its permit
        // through ack flush, so `max_in_flight` already bounds the
        // handler+pending-ack pipeline.
        let limit = cfg.max_in_flight.max(1);
        let consumer_name = cfg.consumer_name.as_str().to_string();

        let stream = cfg.topic.as_str().to_string();
        let group = cfg.consumer_group.as_str().to_string();
        let (ack_tx, flusher_handle) = ack_flusher::spawn(
            Arc::clone(&self.backend),
            stream,
            group,
            self.options.ack_batch_size,
            self.options.ack_flush_interval,
            self.options.error_observer.clone(),
        );

        let limiter = Arc::new(Semaphore::new(limit));

        let (cleanup, cleaned) = ConsumerCleanup::new(
            Arc::clone(&self.backend),
            cfg.topic.as_str().to_owned(),
            cfg.consumer_group.as_str().to_owned(),
            cfg.consumer_name.as_str().to_owned(),
        );
        let runtime = RuntimeState {
            handler,
            config: Arc::new(cfg),
            limiter,
            ack_tx,
        };

        let task = tokio::spawn({
            let bus = self.clone();
            let runtime = runtime.clone();
            async move {
                let _cleanup = cleanup;
                bus.consume_loop(close_rx, runtime, flusher_handle).await
            }
        });

        drop(runtime);

        Ok(StreamSubscription::new(
            consumer_name,
            close_tx,
            task,
            cleaned,
            self.options.error_observer.clone(),
        ))
    }
}

impl<B: StreamBackend> Subscriber for StreamBus<B> {
    fn subscribe(
        &self,
        cfg: SubscriptionConfig,
        handler: Arc<dyn Handler>,
    ) -> crate::BoxFuture<'_, Result<Arc<dyn crate::Subscription>, EventBusError>> {
        Box::pin(async move {
            let sub = self.subscribe_inner(cfg, handler).await?;
            Ok(Arc::new(sub) as Arc<dyn crate::Subscription>)
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Drain synchronously-ready completed tasks without an executor roundtrip.
fn drain_completed_tasks(
    tasks: &mut JoinSet<DeliveryTaskResult>,
    observer: Option<&Arc<dyn ErrorObserver>>,
    first_delivery_error: &mut Option<EventBusError>,
) -> Result<(), EventBusError> {
    while let Some(result) = tasks.try_join_next() {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                first_delivery_error.get_or_insert(err);
            }
            Err(err) => {
                if let Some(obs) = observer {
                    obs.on_panic(ErrorScope::HandlerPanic, &err.to_string());
                }
                return Err(EventBusError::source("delivery task panicked", err));
            }
        }
    }
    Ok(())
}

/// Exponential backoff with full jitter, capped at [`MAX_BACKOFF_CEILING`].
struct BackoffState {
    base: Duration,
    current: Duration,
}

impl BackoffState {
    fn new(base: Duration) -> Self {
        let base = if base.is_zero() {
            Duration::from_millis(100)
        } else {
            base
        };
        Self {
            base,
            current: base,
        }
    }

    fn peek(&self) -> Duration {
        self.base
    }

    fn next(&mut self) -> Duration {
        let dur = self.current;
        let next_raw = dur.saturating_mul(2).min(MAX_BACKOFF_CEILING);
        self.current = next_raw;

        let jitter_nanos = rand::thread_rng().gen_range(0..=dur.as_nanos() as u64);
        Duration::from_nanos(jitter_nanos)
            .saturating_add(dur / 2)
            .min(MAX_BACKOFF_CEILING)
    }

    fn reset(&mut self) {
        self.current = self.base;
    }
}

async fn sleep_or_close(close_rx: &mut watch::Receiver<bool>, duration: Duration) -> bool {
    tokio::select! {
        changed = close_rx.changed() => {
            if changed.is_err() {
                false
            } else {
                !*close_rx.borrow()
            }
        }
        _ = tokio::time::sleep(duration) => true,
    }
}

async fn wait_for_task_or_close(
    tasks: &mut JoinSet<DeliveryTaskResult>,
    close_rx: &mut watch::Receiver<bool>,
    duration: Duration,
    observer: Option<&Arc<dyn ErrorObserver>>,
    first_delivery_error: &mut Option<EventBusError>,
) -> bool {
    if tasks.is_empty() {
        return sleep_or_close(close_rx, duration).await;
    }

    tokio::select! {
        changed = close_rx.changed() => {
            if changed.is_err() {
                false
            } else {
                !*close_rx.borrow()
            }
        }
        result = tasks.join_next() => match result {
            Some(Ok(Ok(()))) | None => true,
            Some(Ok(Err(err))) => {
                first_delivery_error.get_or_insert(err);
                true
            }
            Some(Err(err)) => {
                if let Some(obs) = observer {
                    obs.on_panic(ErrorScope::HandlerPanic, &err.to_string());
                }
                first_delivery_error.get_or_insert_with(|| {
                    EventBusError::source("delivery task failed", err)
                });
                true
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{BackoffState, StreamBusOptions, MAX_BACKOFF_CEILING};

    #[test]
    fn zero_duration_options_normalize_to_defaults() {
        let normalized = StreamBusOptions {
            block_timeout: Duration::ZERO,
            claim_idle_timeout: Duration::ZERO,
            claim_scan_batch_size: 0,
            group_start_id: String::new(),
            publish_batch_parallelism: 0,
            ack_batch_size: 0,
            ack_flush_interval: Duration::ZERO,
            reclaim_interval: Duration::ZERO,
            max_payload_bytes: 0,
            error_observer: None,
        }
        .normalize()
        .expect("normalize options");

        assert_eq!(normalized.block_timeout, Duration::from_secs(2));
        assert_eq!(normalized.claim_idle_timeout, Duration::from_secs(60));
        assert_eq!(normalized.claim_scan_batch_size, 64);
        assert_eq!(normalized.group_start_id, "$".to_string());
        assert_eq!(normalized.publish_batch_parallelism, 32);
        assert_eq!(normalized.ack_batch_size, 64);
        assert_eq!(normalized.ack_flush_interval, Duration::from_millis(2));
        assert_eq!(normalized.reclaim_interval, Duration::from_millis(500));
    }

    #[test]
    fn backoff_grows_exponentially_and_caps() {
        let mut backoff = BackoffState::new(Duration::from_millis(100));
        for _ in 0..20 {
            let dur = backoff.next();
            assert!(dur <= MAX_BACKOFF_CEILING);
        }
        backoff.reset();
        let first = backoff.next();
        assert!(first <= MAX_BACKOFF_CEILING);
    }
}
