use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::contract::delivery::DeliveryInspector;
use crate::contract::{
    AckMode, BackpressurePolicy, ConsumerBalanceMode, DeliveryGuarantee, OrderingMode,
    OverflowStrategy, PublishConfirmation,
};
use crate::error::EventBusError;

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

pub type Headers = HashMap<String, String>;

// ---------------------------------------------------------------------------
// Newtypes
// ---------------------------------------------------------------------------

/// Stream / queue identifier. Validated via [`Topic::new`] at construction:
/// non-empty, ≤ [`Topic::MAX_LEN`] bytes, no ASCII control characters.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
#[repr(transparent)]
pub struct Topic(String);

impl Topic {
    /// Maximum allowed topic length (bytes).
    pub const MAX_LEN: usize = 1024;

    /// Construct a [`Topic`], validating it at the boundary.
    pub fn new(s: impl Into<String>) -> Result<Self, EventBusError> {
        let s = s.into();
        if s.trim().is_empty() {
            return Err(EventBusError::Validation("topic is required".into()));
        }
        if s.len() > Self::MAX_LEN {
            return Err(EventBusError::Validation(format!(
                "topic length {} exceeds limit of {}",
                s.len(),
                Self::MAX_LEN,
            )));
        }
        if s.chars().any(|c| c.is_control()) {
            return Err(EventBusError::Validation(
                "topic must not contain control characters".into(),
            ));
        }
        Ok(Self(s))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl std::ops::Deref for Topic {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for Topic {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for Topic {
    type Error = EventBusError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Topic::new(s)
    }
}

impl TryFrom<String> for Topic {
    type Error = EventBusError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Topic::new(s)
    }
}

/// Consumer group name. Validated: non-empty, ≤ [`ConsumerGroup::MAX_LEN`] bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
#[repr(transparent)]
pub struct ConsumerGroup(String);

impl ConsumerGroup {
    pub const MAX_LEN: usize = 256;

    pub fn new(s: impl Into<String>) -> Result<Self, EventBusError> {
        let s = s.into();
        if s.trim().is_empty() {
            return Err(EventBusError::Validation(
                "consumer group is required".into(),
            ));
        }
        if s.len() > Self::MAX_LEN {
            return Err(EventBusError::Validation(format!(
                "consumer group length {} exceeds limit of {}",
                s.len(),
                Self::MAX_LEN,
            )));
        }
        Ok(Self(s))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl std::ops::Deref for ConsumerGroup {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ConsumerGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for ConsumerGroup {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for ConsumerGroup {
    type Error = EventBusError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl TryFrom<String> for ConsumerGroup {
    type Error = EventBusError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

/// Per-process consumer name. Validated: non-empty, ≤ [`ConsumerName::MAX_LEN`].
/// Use [`ConsumerName::auto`] to obtain a unique auto-generated name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
#[repr(transparent)]
pub struct ConsumerName(String);

impl ConsumerName {
    pub const MAX_LEN: usize = 256;

    pub fn new(s: impl Into<String>) -> Result<Self, EventBusError> {
        let s = s.into();
        if s.trim().is_empty() {
            return Err(EventBusError::Validation(
                "consumer name is required".into(),
            ));
        }
        if s.len() > Self::MAX_LEN {
            return Err(EventBusError::Validation(format!(
                "consumer name length {} exceeds limit of {}",
                s.len(),
                Self::MAX_LEN,
            )));
        }
        Ok(Self(s))
    }

    /// Auto-generate a unique consumer name of the form
    /// `consumer-<nanos>-<entropy>`. Bypasses validation since the format is
    /// known-good.
    #[must_use]
    pub fn auto() -> Self {
        let nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let entropy: u64 = rand::Rng::gen(&mut rand::thread_rng());
        Self(format!("consumer-{nanos}-{entropy:016x}"))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl std::ops::Deref for ConsumerName {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ConsumerName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for ConsumerName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for ConsumerName {
    type Error = EventBusError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl TryFrom<String> for ConsumerName {
    type Error = EventBusError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

// ---------------------------------------------------------------------------
// Message
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub uid: String,
    pub topic: Topic,
    pub key: String,
    pub kind: String,
    pub source: String,
    pub occurred_at: DateTime<Utc>,
    pub headers: Headers,
    #[serde(with = "crate::serde_bytes")]
    pub payload: bytes::Bytes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_uid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_uid: Option<String>,
}

// ---------------------------------------------------------------------------
// Publish options (builder pattern)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
#[must_use = "PublishOptions is inert until passed to publish/publish_batch"]
pub struct PublishOptions {
    /// Blocks the calling task for the specified duration before publishing.
    /// For high-throughput scenarios, consider handling delays externally.
    pub delay: Option<Duration>,
    pub ordered_key: Option<String>,
    pub metadata: HashMap<String, String>,
    pub require_ordered_key: bool,
    pub guarantee: Option<DeliveryGuarantee>,
    pub confirmation: Option<PublishConfirmation>,
    pub idempotency_key: Option<String>,
    pub backpressure: Option<BackpressurePolicy>,
    pub topic_ttl: Option<Duration>,
    pub expected_content_type: Option<String>,
    pub expected_event_version: Option<String>,
}

impl PublishOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    pub fn with_ordered_key(mut self, key: impl Into<String>) -> Self {
        self.ordered_key = Some(key.into());
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn with_require_ordered_key(mut self) -> Self {
        self.require_ordered_key = true;
        self
    }

    pub fn with_guarantee(mut self, g: DeliveryGuarantee) -> Self {
        self.guarantee = Some(g);
        self
    }

    pub fn with_confirmation(mut self, c: PublishConfirmation) -> Self {
        self.confirmation = Some(c);
        self
    }

    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    pub fn with_backpressure(mut self, bp: BackpressurePolicy) -> Self {
        self.backpressure = Some(bp);
        self
    }

    pub fn with_topic_ttl(mut self, ttl: Duration) -> Self {
        self.topic_ttl = Some(ttl);
        self
    }

    pub fn with_expected_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.expected_content_type = Some(content_type.into());
        self
    }

    pub fn with_expected_event_version(mut self, version: impl Into<String>) -> Self {
        self.expected_event_version = Some(version.into());
        self
    }

    pub fn validate(&self) -> Result<(), EventBusError> {
        if self.require_ordered_key
            && self
                .ordered_key
                .as_ref()
                .is_none_or(|k| k.trim().is_empty())
        {
            return Err(EventBusError::Validation("ordered key is required".into()));
        }

        if self.guarantee == Some(DeliveryGuarantee::ExactlyOnce)
            && self.confirmation != Some(PublishConfirmation::Persisted)
        {
            return Err(EventBusError::Validation(format!(
                "exactly-once requires {:?} confirmation",
                PublishConfirmation::Persisted,
            )));
        }

        if let Some(ref bp) = self.backpressure {
            bp.validate()?;
        }

        if let Some(ttl) = self.topic_ttl {
            if ttl.is_zero() {
                return Err(EventBusError::Validation("topic ttl must be > 0".into()));
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Subscription config
// ---------------------------------------------------------------------------

/// Subscription configuration.
///
/// ## In-flight sizing precedence
///
/// During [`SubscriptionConfig::apply_defaults`] the bus reconciles three
/// related inputs:
///
/// 1. If `backpressure` is set, its `max_in_flight` / `max_pending_acks` seed
///    the matching fields when those are still `0`. Setting both
///    `backpressure.max_in_flight` and `max_in_flight` to **different**
///    non-zero values is a configuration error surfaced by `validate`.
/// 2. `max_in_flight` falls back to `1` if still unset.
/// 3. `max_pending_acks` falls back to `2 * max_in_flight`.
/// 4. If `backpressure` was unset, one is synthesized from the resolved
///    `max_in_flight` / `max_pending_acks`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SubscriptionConfig {
    pub(crate) topic: Topic,
    pub(crate) consumer_group: ConsumerGroup,
    pub(crate) consumer_name: ConsumerName,
    pub(crate) max_retry: usize,
    pub(crate) retry_backoff: Duration,
    pub(crate) dead_letter_topic: Option<Topic>,
    pub(crate) ack_mode: AckMode,
    pub(crate) ordering_mode: Option<OrderingMode>,
    pub(crate) balance_mode: Option<ConsumerBalanceMode>,
    pub(crate) guarantee: Option<DeliveryGuarantee>,
    pub(crate) max_in_flight: usize,
    pub(crate) max_pending_acks: usize,
    pub(crate) wildcard_topic: bool,
    pub(crate) backpressure: Option<BackpressurePolicy>,
}

impl SubscriptionConfig {
    /// Begin building a [`SubscriptionConfig`] for the given topic + group.
    pub fn builder(topic: Topic, consumer_group: ConsumerGroup) -> SubscriptionConfigBuilder {
        SubscriptionConfigBuilder::new(topic, consumer_group)
    }

    #[must_use]
    pub fn topic(&self) -> &Topic {
        &self.topic
    }
    #[must_use]
    pub fn consumer_group(&self) -> &ConsumerGroup {
        &self.consumer_group
    }
    #[must_use]
    pub fn consumer_name(&self) -> &ConsumerName {
        &self.consumer_name
    }
    #[must_use]
    pub fn ack_mode(&self) -> AckMode {
        self.ack_mode
    }
    #[must_use]
    pub fn ordering_mode(&self) -> Option<OrderingMode> {
        self.ordering_mode
    }
    #[must_use]
    pub fn balance_mode(&self) -> Option<ConsumerBalanceMode> {
        self.balance_mode
    }
    #[must_use]
    pub fn guarantee(&self) -> Option<DeliveryGuarantee> {
        self.guarantee
    }
    #[must_use]
    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }
    #[must_use]
    pub fn max_pending_acks(&self) -> usize {
        self.max_pending_acks
    }
    #[must_use]
    pub fn max_retry(&self) -> usize {
        self.max_retry
    }
    #[must_use]
    pub fn retry_backoff(&self) -> Duration {
        self.retry_backoff
    }
    #[must_use]
    pub fn dead_letter_topic(&self) -> Option<&Topic> {
        self.dead_letter_topic.as_ref()
    }
    #[must_use]
    pub fn backpressure(&self) -> Option<&BackpressurePolicy> {
        self.backpressure.as_ref()
    }
    #[must_use]
    pub fn wildcard_topic(&self) -> bool {
        self.wildcard_topic
    }
}

/// Builder for [`SubscriptionConfig`]. Construct via
/// [`SubscriptionConfig::builder`].
#[must_use = "build the config and pass it to subscribe()"]
#[derive(Debug, Clone)]
pub struct SubscriptionConfigBuilder {
    cfg: SubscriptionConfig,
}

impl SubscriptionConfigBuilder {
    fn new(topic: Topic, consumer_group: ConsumerGroup) -> Self {
        Self {
            cfg: SubscriptionConfig {
                topic,
                consumer_group,
                consumer_name: ConsumerName::auto(),
                max_retry: 0,
                retry_backoff: Duration::ZERO,
                dead_letter_topic: None,
                ack_mode: AckMode::Manual,
                ordering_mode: None,
                balance_mode: None,
                guarantee: None,
                max_in_flight: 0,
                max_pending_acks: 0,
                wildcard_topic: false,
                backpressure: None,
            },
        }
    }

    pub fn consumer_name(mut self, n: ConsumerName) -> Self {
        self.cfg.consumer_name = n;
        self
    }
    pub fn max_retry(mut self, n: usize) -> Self {
        self.cfg.max_retry = n;
        self
    }
    pub fn retry_backoff(mut self, d: Duration) -> Self {
        self.cfg.retry_backoff = d;
        self
    }
    pub fn dead_letter_topic(mut self, t: Topic) -> Self {
        self.cfg.dead_letter_topic = Some(t);
        self
    }
    pub fn ack_mode(mut self, m: AckMode) -> Self {
        self.cfg.ack_mode = m;
        self
    }
    pub fn ordering(mut self, o: OrderingMode) -> Self {
        self.cfg.ordering_mode = Some(o);
        self
    }
    pub fn balance(mut self, b: ConsumerBalanceMode) -> Self {
        self.cfg.balance_mode = Some(b);
        self
    }
    pub fn guarantee(mut self, g: DeliveryGuarantee) -> Self {
        self.cfg.guarantee = Some(g);
        self
    }
    pub fn max_in_flight(mut self, n: usize) -> Self {
        self.cfg.max_in_flight = n;
        self
    }
    pub fn max_pending_acks(mut self, n: usize) -> Self {
        self.cfg.max_pending_acks = n;
        self
    }
    pub fn backpressure(mut self, p: BackpressurePolicy) -> Self {
        self.cfg.backpressure = Some(p);
        self
    }
    pub fn wildcard_topic(mut self) -> Self {
        self.cfg.wildcard_topic = true;
        self
    }

    /// Apply defaults, validate, and return the finished config.
    pub fn build(mut self) -> Result<SubscriptionConfig, EventBusError> {
        self.cfg.normalize_and_validate()?;
        Ok(self.cfg)
    }
}

impl SubscriptionConfig {
    /// Fill in zero-value fields with sensible defaults.
    pub fn apply_defaults(&mut self) {
        if self.ordering_mode.is_none() {
            self.ordering_mode = Some(OrderingMode::None);
        }

        if self.balance_mode.is_none() {
            self.balance_mode = Some(ConsumerBalanceMode::Competing);
        }

        if self.guarantee.is_none() {
            self.guarantee = Some(DeliveryGuarantee::AtLeastOnce);
        }

        if let Some(ref bp) = self.backpressure {
            if self.max_in_flight == 0 {
                self.max_in_flight = bp.max_in_flight;
            }
            if self.max_pending_acks == 0 {
                self.max_pending_acks = bp.max_pending_acks;
            }
        }

        if self.max_in_flight == 0 {
            self.max_in_flight = 1;
        }
        if self.max_pending_acks == 0 {
            self.max_pending_acks = self.max_in_flight * 2;
        }
        if self.retry_backoff.is_zero() {
            self.retry_backoff = Duration::from_millis(100);
        }

        if self.backpressure.is_none() {
            self.backpressure = Some(BackpressurePolicy {
                max_in_flight: self.max_in_flight,
                max_pending_acks: self.max_pending_acks,
                max_batch_size: self.max_in_flight,
                overflow_strategy: OverflowStrategy::Reject,
            });
        }
    }

    /// Check config consistency without mutating fields.
    /// Call `apply_defaults` first, or use `normalize_and_validate`.
    pub fn validate(&self) -> Result<(), EventBusError> {
        if self.ordering_mode.is_none() {
            return Err(EventBusError::Validation(
                "ordering mode is required".into(),
            ));
        }

        if self.balance_mode.is_none() {
            return Err(EventBusError::Validation("balance mode is required".into()));
        }

        if self.guarantee.is_none() {
            return Err(EventBusError::Validation(
                "delivery guarantee is required".into(),
            ));
        }

        if let Some(ref bp) = self.backpressure {
            bp.validate()?;
            if self.max_in_flight > 0 && self.max_in_flight != bp.max_in_flight {
                return Err(EventBusError::Validation(
                    "max in flight conflicts with backpressure policy".into(),
                ));
            }
            if self.max_pending_acks > 0 && self.max_pending_acks != bp.max_pending_acks {
                return Err(EventBusError::Validation(
                    "max pending acks conflicts with backpressure policy".into(),
                ));
            }
        }

        if self.max_pending_acks < self.max_in_flight {
            return Err(EventBusError::Validation(
                "max pending acks must be >= max in flight".into(),
            ));
        }

        Ok(())
    }

    /// Apply defaults then validate. Recommended single-call entrypoint.
    pub fn normalize_and_validate(&mut self) -> Result<(), EventBusError> {
        self.apply_defaults();
        self.validate()
    }
}

// ---------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------

/// Read-only view of a message in flight.
///
/// Backends supply this to handlers as part of [`DeliveryHandle`]. The handler
/// can inspect the message and delivery state, then call methods on
/// [`DeliveryControl`] to finalize.
pub trait Delivery: DeliveryInspector + Send + Sync {
    fn message(&self) -> &Message;
}

/// Finalize a delivery exactly once.
///
/// Each method consumes `Box<Self>`, so the compiler guarantees a delivery is
/// finalized at most once: a handler that calls `ack()` cannot call `nack()`
/// or `retry()` after — the box is already moved.
///
/// Dropping the box without calling any of these is also valid: the message
/// is left un-acked and will be reclaimed by the backend after the
/// configured idle timeout.
pub trait DeliveryControl: Send {
    fn ack(self: Box<Self>) -> crate::BoxFuture<'static, Result<(), EventBusError>>;
    fn nack(
        self: Box<Self>,
        reason: crate::BoxedError,
    ) -> crate::BoxFuture<'static, Result<(), EventBusError>>;
    fn retry(
        self: Box<Self>,
        reason: crate::BoxedError,
    ) -> crate::BoxFuture<'static, Result<(), EventBusError>>;
}

/// Composite trait — a handler-facing delivery. Anything that is both
/// [`Delivery`] (read) and [`DeliveryControl`] (finalize) qualifies.
pub trait DeliveryHandle: Delivery + DeliveryControl {}
impl<T: Delivery + DeliveryControl + ?Sized> DeliveryHandle for T {}

pub trait Handler: Send + Sync {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> crate::BoxFuture<'_, Result<(), EventBusError>>;
}

pub trait Subscription: Send + Sync {
    fn name(&self) -> &str;
    fn close(self: Arc<Self>) -> crate::BoxFuture<'static, Result<(), EventBusError>>;
}

/// Backend-assigned identifier for a published message. Unvalidated — backends
/// produce these strings, the bus only forwards them.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct MessageId(String);

impl MessageId {
    #[must_use]
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl std::ops::Deref for MessageId {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for MessageId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<&str> for MessageId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for MessageId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

/// Per-message result for `publish_batch`. PR 5 will populate `results`
/// during a full re-implementation that no longer fails fast.
#[derive(Debug)]
#[non_exhaustive]
pub struct BatchOutcome {
    pub results: Vec<Result<MessageId, EventBusError>>,
}

impl BatchOutcome {
    #[must_use]
    pub fn all_ok(&self) -> bool {
        self.results.iter().all(Result::is_ok)
    }
    #[must_use]
    pub fn ok_count(&self) -> usize {
        self.results.iter().filter(|r| r.is_ok()).count()
    }
    #[must_use]
    pub fn err_count(&self) -> usize {
        self.results.len() - self.ok_count()
    }
    pub fn errors(&self) -> impl Iterator<Item = &EventBusError> {
        self.results.iter().filter_map(|r| r.as_ref().err())
    }

    /// Convert to `Ok(Vec<MessageId>)` when every slot succeeded, or
    /// `Err(BatchError { outcome: self })` otherwise. The error variant
    /// preserves the full outcome so callers can still inspect partial
    /// successes.
    pub fn into_result(self) -> Result<Vec<MessageId>, BatchError> {
        if self.all_ok() {
            Ok(self.results.into_iter().filter_map(Result::ok).collect())
        } else {
            Err(BatchError { outcome: self })
        }
    }
}

/// Wraps a partially-failed [`BatchOutcome`] for callers that want a single
/// `Result`. Carries the full outcome so partial successes remain inspectable.
#[derive(Debug)]
pub struct BatchError {
    pub outcome: BatchOutcome,
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "publish_batch had {} failure(s) out of {} messages",
            self.outcome.err_count(),
            self.outcome.results.len()
        )
    }
}

impl std::error::Error for BatchError {}

pub trait Publisher: Send + Sync {
    fn publish(
        &self,
        msg: Message,
        opts: PublishOptions,
    ) -> crate::BoxFuture<'_, Result<MessageId, EventBusError>>;

    fn publish_batch(
        &self,
        msgs: Vec<Message>,
        opts: PublishOptions,
    ) -> crate::BoxFuture<'_, Result<BatchOutcome, EventBusError>>;
}

/// Convenience layer over [`Publisher`]. Generic methods that monomorphize
/// for callers who don't need dynamic dispatch.
pub trait PublisherExt: Publisher {
    /// Publish an iterator of messages. Collects into `Vec` and delegates
    /// to [`Publisher::publish_batch`]. The collect is mandatory because the
    /// dyn-safe `publish_batch` cannot accept an opaque iterator.
    fn publish_iter<I>(
        &self,
        msgs: I,
        opts: PublishOptions,
    ) -> crate::BoxFuture<'_, Result<BatchOutcome, EventBusError>>
    where
        I: IntoIterator<Item = Message> + Send + 'static,
        I::IntoIter: Send,
    {
        let collected: Vec<Message> = msgs.into_iter().collect();
        self.publish_batch(collected, opts)
    }
}

impl<P: Publisher + ?Sized> PublisherExt for P {}

pub trait Subscriber: Send + Sync {
    fn subscribe(
        &self,
        cfg: SubscriptionConfig,
        handler: Arc<dyn Handler>,
    ) -> crate::BoxFuture<'_, Result<Arc<dyn Subscription>, EventBusError>>;
}

/// Convenience layer over [`Subscriber`]. Generic methods that monomorphize
/// for callers who don't need dynamic dispatch.
pub trait SubscriberExt: Subscriber {
    fn subscribe_with<H>(
        &self,
        cfg: SubscriptionConfig,
        handler: H,
    ) -> crate::BoxFuture<'_, Result<Arc<dyn Subscription>, EventBusError>>
    where
        H: Handler + 'static,
    {
        self.subscribe(cfg, Arc::new(handler))
    }
}

impl<S: Subscriber + ?Sized> SubscriberExt for S {}

pub trait Bus: Publisher + Subscriber + Send + Sync {}

impl<T> Bus for T where T: Publisher + Subscriber + Send + Sync {}

/// Pluggable wire-format encoder for [`Message`].
///
/// Object-safe: backends store an `Arc<dyn Codec>` and dispatch through it.
/// Implementations own the full envelope so swapping codecs is a binary
/// decision (e.g. JSON for cross-language compat vs. binary for throughput).
pub trait Codec: Send + Sync {
    fn name(&self) -> &str;
    fn encode(&self, msg: &Message) -> Result<Vec<u8>, EventBusError>;
    fn decode(&self, bytes: &[u8]) -> Result<Message, EventBusError>;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_options_accepts_valid() {
        let opts = PublishOptions::new()
            .with_ordered_key("user-1")
            .with_require_ordered_key()
            .with_guarantee(DeliveryGuarantee::AtLeastOnce)
            .with_confirmation(PublishConfirmation::Accepted)
            .with_backpressure(BackpressurePolicy {
                max_in_flight: 10,
                max_pending_acks: 20,
                max_batch_size: 10,
                overflow_strategy: OverflowStrategy::Reject,
            });
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn publish_options_rejects_missing_ordered_key() {
        let opts = PublishOptions::new().with_require_ordered_key();
        assert!(opts.validate().is_err());
    }

    #[test]
    fn publish_options_rejects_exactly_once_without_persisted() {
        let opts = PublishOptions::new()
            .with_guarantee(DeliveryGuarantee::ExactlyOnce)
            .with_confirmation(PublishConfirmation::Accepted);
        assert!(opts.validate().is_err());
    }

    #[test]
    fn publish_options_rejects_exactly_once_without_confirmation() {
        let opts = PublishOptions::new().with_guarantee(DeliveryGuarantee::ExactlyOnce);
        assert!(opts.validate().is_err());
    }

    #[test]
    fn publish_options_rejects_zero_ttl() {
        let opts = PublishOptions::new().with_topic_ttl(Duration::ZERO);
        assert!(opts.validate().is_err());
    }

    #[test]
    fn batch_outcome_into_result_ok_when_all_succeed() {
        let outcome = BatchOutcome {
            results: vec![Ok(MessageId::new("a")), Ok(MessageId::new("b"))],
        };
        let res = outcome.into_result().expect("should be ok");
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].as_str(), "a");
    }

    #[test]
    fn batch_outcome_into_result_preserves_outcome_on_failure() {
        let outcome = BatchOutcome {
            results: vec![
                Ok(MessageId::new("a")),
                Err(EventBusError::Validation("bad".into())),
            ],
        };
        let err = outcome.into_result().expect_err("should be err");
        assert_eq!(err.outcome.results.len(), 2);
        assert_eq!(err.outcome.ok_count(), 1);
        assert_eq!(err.outcome.err_count(), 1);
    }

    fn topic() -> Topic {
        Topic::new("evt.test").expect("topic")
    }
    fn group() -> ConsumerGroup {
        ConsumerGroup::new("cg.test").expect("group")
    }

    #[test]
    fn subscription_config_preserves_explicit_ack_mode() {
        let cfg = SubscriptionConfig::builder(topic(), group())
            .ack_mode(AckMode::AutoOnHandlerSuccess)
            .max_in_flight(8)
            .build()
            .expect("build");
        assert_eq!(cfg.ack_mode(), AckMode::AutoOnHandlerSuccess);
    }

    #[test]
    fn subscription_config_defaults_to_manual_ack() {
        let cfg = SubscriptionConfig::builder(topic(), group())
            .max_in_flight(8)
            .build()
            .expect("build");
        assert_eq!(cfg.ack_mode(), AckMode::Manual);
    }

    #[test]
    fn subscription_config_rejects_conflicting_backpressure() {
        let res = SubscriptionConfig::builder(topic(), group())
            .ack_mode(AckMode::Manual)
            .max_in_flight(8)
            .backpressure(BackpressurePolicy {
                max_in_flight: 16,
                max_pending_acks: 32,
                max_batch_size: 8,
                overflow_strategy: OverflowStrategy::Reject,
            })
            .build();
        assert!(res.is_err());
    }
}

#[cfg(test)]
mod topic_tests {
    use super::*;

    #[test]
    fn topic_rejects_empty() {
        assert!(Topic::new("").is_err());
    }
    #[test]
    fn topic_rejects_only_whitespace() {
        assert!(Topic::new("   ").is_err());
    }
    #[test]
    fn topic_rejects_oversize() {
        let s = "a".repeat(Topic::MAX_LEN + 1);
        assert!(Topic::new(s).is_err());
    }
    #[test]
    fn topic_rejects_control_chars() {
        assert!(Topic::new("foo\x07bar").is_err());
    }
    #[test]
    fn topic_accepts_normal() {
        assert!(Topic::new("orders.created").is_ok());
    }

    #[test]
    fn consumer_group_rejects_empty() {
        assert!(ConsumerGroup::new("").is_err());
    }
    #[test]
    fn consumer_group_accepts_normal() {
        assert!(ConsumerGroup::new("cg.x").is_ok());
    }

    #[test]
    fn consumer_name_auto_is_unique() {
        let a = ConsumerName::auto();
        let b = ConsumerName::auto();
        assert_ne!(a.as_str(), b.as_str());
        assert!(a.as_str().starts_with("consumer-"));
    }

    #[test]
    fn message_id_is_unvalidated() {
        let id = MessageId::new("");
        assert_eq!(id.as_str(), "");
    }
}
