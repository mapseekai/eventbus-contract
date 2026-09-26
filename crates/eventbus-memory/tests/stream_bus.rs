use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use std::{collections::VecDeque, future::Future, pin::Pin};

use chrono::Utc;
use eventbus_core::stream::{
    ClaimedMessage, ErrorObserver, ErrorScope, FetchedEntry, StreamBackend, StreamBus,
    StreamBusOptions,
};
use eventbus_core::{
    AckMode, DeliveryHandle, EventBusError, Handler, Headers, Message, PartialDeliveryState,
    PublishOptions,
};
use eventbus_memory::MemoryStreamBackend;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::time::{sleep, timeout};

fn message(topic: &str, uid: &str) -> Message {
    Message {
        uid: uid.to_string(),
        topic: eventbus_core::Topic::new(topic).expect("topic"),
        key: String::new(),
        kind: "test.message".to_string(),
        source: "test".to_string(),
        occurred_at: Utc::now(),
        headers: Headers::new(),
        payload: bytes::Bytes::from_static(br#"{"ok":true}"#),
        content_type: None,
        event_version: None,
        idempotency_key: None,
        expires_at: None,
        trace_uid: None,
        correlation_uid: None,
    }
}

struct AutoAckHandler {
    tx: mpsc::Sender<Message>,
}

impl Handler for AutoAckHandler {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> eventbus_core::BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            self.tx
                .send(delivery.message().clone())
                .await
                .map_err(|err| EventBusError::Internal(err.to_string()))
        })
    }
}

struct ManualAckHandler {
    tx: mpsc::Sender<()>,
}

impl Handler for ManualAckHandler {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> eventbus_core::BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            delivery.ack().await?;
            self.tx
                .send(())
                .await
                .map_err(|err| EventBusError::Internal(err.to_string()))
        })
    }
}

struct RetryOnceHandler {
    attempts: Arc<AtomicUsize>,
    tx: mpsc::Sender<usize>,
}

impl Handler for RetryOnceHandler {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> eventbus_core::BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt == 1 {
                delivery
                    .retry(Box::new(std::io::Error::other("retry-later")))
                    .await?;
            } else {
                delivery.ack().await?;
            }

            self.tx
                .send(attempt)
                .await
                .map_err(|err| EventBusError::Internal(err.to_string()))
        })
    }
}

struct ReceiveOnlyHandler {
    tx: mpsc::Sender<()>,
}

impl Handler for ReceiveOnlyHandler {
    fn handle(
        &self,
        _delivery: Box<dyn DeliveryHandle>,
    ) -> eventbus_core::BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            self.tx
                .send(())
                .await
                .map_err(|err| EventBusError::Internal(err.to_string()))
        })
    }
}

struct AckAndSignalHandler {
    tx: mpsc::Sender<()>,
}

impl Handler for AckAndSignalHandler {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> eventbus_core::BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            delivery.ack().await?;
            self.tx
                .send(())
                .await
                .map_err(|err| EventBusError::Internal(err.to_string()))
        })
    }
}

struct NackHandler {
    tx: mpsc::Sender<()>,
}

impl Handler for NackHandler {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> eventbus_core::BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            delivery
                .nack(Box::new(std::io::Error::other("poison-message")))
                .await?;
            self.tx
                .send(())
                .await
                .map_err(|err| EventBusError::Internal(err.to_string()))
        })
    }
}

struct ErrorHandler {
    attempts: Arc<AtomicUsize>,
}

impl Handler for ErrorHandler {
    fn handle(
        &self,
        _delivery: Box<dyn DeliveryHandle>,
    ) -> eventbus_core::BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Err(EventBusError::Internal("handler failed".into()))
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_subscribe_auto_ack_drains_pending() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend.clone(), StreamBusOptions::default()).expect("construct bus");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.user").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.auto-ack").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::AutoOnHandlerSuccess)
            .max_in_flight(1)
            .build()
            .expect("build subscription config"),
            AutoAckHandler { tx },
        )
        .await
        .expect("subscribe");

    bus.publish(
        message("evt.user", "uid-auto-ack"),
        PublishOptions::new()
            .with_idempotency_key("idem-auto-ack")
            .with_metadata("meta-key", "meta-val"),
    )
    .await
    .expect("publish");

    let got = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("receive in time")
        .expect("message delivered");
    assert_eq!(got.uid, "uid-auto-ack");
    assert_eq!(got.idempotency_key.as_deref(), Some("idem-auto-ack"));
    assert_eq!(
        got.headers.get("meta-key").map(String::as_str),
        Some("meta-val")
    );

    sub.close().await.expect("close sub");
    assert_eq!(backend.pending_count("evt.user", "cg.auto-ack").await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_ack_drains_pending() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend.clone(), StreamBusOptions::default()).expect("construct bus");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.manual").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.manual").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::Manual)
            .max_in_flight(1)
            .build()
            .expect("build subscription config"),
            ManualAckHandler { tx },
        )
        .await
        .expect("subscribe");

    bus.publish(
        message("evt.manual", "uid-manual"),
        PublishOptions::default(),
    )
    .await
    .expect("publish");

    timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("ack in time")
        .expect("ack signal");
    assert_eq!(backend.pending_count("evt.manual", "cg.manual").await, 0);
    sub.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_redelivers_message_and_then_drains_pending() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend.clone(), StreamBusOptions::default()).expect("construct bus");

    let attempts = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel(4);
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.retry").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.retry").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::Manual)
            .max_in_flight(1)
            .max_retry(5)
            .build()
            .expect("build subscription config"),
            RetryOnceHandler {
                attempts: attempts.clone(),
                tx,
            },
        )
        .await
        .expect("subscribe");

    bus.publish(message("evt.retry", "uid-retry"), PublishOptions::default())
        .await
        .expect("publish");

    timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("first attempt")
        .expect("signal");
    timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("second attempt")
        .expect("signal");

    assert!(attempts.load(Ordering::SeqCst) >= 2);
    assert_eq!(backend.pending_count("evt.retry", "cg.retry").await, 0);
    sub.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaims_pending_from_inactive_consumer() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(
        backend.clone(),
        StreamBusOptions {
            claim_idle_timeout: Duration::from_millis(20),
            ..Default::default()
        },
    )
    .expect("construct bus");

    let (first_tx, mut first_rx) = mpsc::channel(1);
    let first_sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.reclaim").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.reclaim").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::Manual)
            .max_in_flight(1)
            .build()
            .expect("build subscription config"),
            ReceiveOnlyHandler { tx: first_tx },
        )
        .await
        .expect("first subscribe");

    bus.publish(
        message("evt.reclaim", "uid-reclaim"),
        PublishOptions::default(),
    )
    .await
    .expect("publish");

    timeout(Duration::from_secs(2), first_rx.recv())
        .await
        .expect("first receive")
        .expect("signal");
    first_sub.close().await.expect("close first sub");

    sleep(Duration::from_millis(50)).await;

    let (second_tx, mut second_rx) = mpsc::channel(1);
    let second_sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.reclaim").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.reclaim").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-2").expect("consumer name"))
            .ack_mode(AckMode::Manual)
            .max_in_flight(1)
            .build()
            .expect("build subscription config"),
            AckAndSignalHandler { tx: second_tx },
        )
        .await
        .expect("second subscribe");

    timeout(Duration::from_secs(2), second_rx.recv())
        .await
        .expect("second receive")
        .expect("signal");

    assert_eq!(backend.pending_count("evt.reclaim", "cg.reclaim").await, 0);
    second_sub.close().await.expect("close second sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_routes_to_dead_letter_stream() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend.clone(), StreamBusOptions::default()).expect("construct bus");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.nack").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.nack").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::Manual)
            .dead_letter_topic(eventbus_core::Topic::new("evt.nack.dlq").expect("dlq topic"))
            .max_in_flight(1)
            .build()
            .expect("build subscription config"),
            NackHandler { tx },
        )
        .await
        .expect("subscribe");

    bus.publish(message("evt.nack", "uid-nack"), PublishOptions::default())
        .await
        .expect("publish");

    timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("nack handled")
        .expect("signal");

    assert_eq!(backend.pending_count("evt.nack", "cg.nack").await, 0);
    assert_eq!(backend.stream_len("evt.nack.dlq").await, 1);
    sub.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_max_routes_to_dead_letter_stream() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend.clone(), StreamBusOptions::default()).expect("construct bus");

    let attempts = Arc::new(AtomicUsize::new(0));
    let result = Arc::new(Mutex::new(Vec::new()));

    struct RetryToDeadLetterHandler {
        attempts: Arc<AtomicUsize>,
        result: Arc<Mutex<Vec<usize>>>,
    }

    impl Handler for RetryToDeadLetterHandler {
        fn handle(
            &self,
            delivery: Box<dyn DeliveryHandle>,
        ) -> eventbus_core::BoxFuture<'_, Result<(), EventBusError>> {
            Box::pin(async move {
                let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
                delivery
                    .retry(Box::new(std::io::Error::other("retry-limit")))
                    .await?;
                self.result.lock().await.push(attempt);
                Ok(())
            })
        }
    }

    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.retry.max").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.retry.max").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::Manual)
            .dead_letter_topic(eventbus_core::Topic::new("evt.retry.max.dlq").expect("dlq topic"))
            .max_in_flight(1)
            .max_retry(1)
            .build()
            .expect("build subscription config"),
            RetryToDeadLetterHandler {
                attempts: attempts.clone(),
                result: result.clone(),
            },
        )
        .await
        .expect("subscribe");

    bus.publish(
        message("evt.retry.max", "uid-retry-max"),
        PublishOptions::default(),
    )
    .await
    .expect("publish");

    timeout(Duration::from_secs(2), async {
        loop {
            if backend.stream_len("evt.retry.max.dlq").await >= 1 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("dead letter written");

    // Publishing to the DLQ precedes the ACK round-trip and handler completion.
    sub.close().await.expect("close sub");
    // `max_retry: 1` means one *retry* on top of the initial delivery, so the
    // handler runs twice before the message ends up in the DLQ.
    assert_eq!(backend.stream_len("evt.retry.max.dlq").await, 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(result.lock().await.len(), 2);
    assert_eq!(
        backend.pending_count("evt.retry.max", "cg.retry.max").await,
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_ack_handler_error_does_not_auto_retry() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend.clone(), StreamBusOptions::default()).expect("construct bus");

    let attempts = Arc::new(AtomicUsize::new(0));
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.manual.error").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.manual.error").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::Manual)
            .max_in_flight(1)
            .max_retry(5)
            .build()
            .expect("build subscription config"),
            ErrorHandler {
                attempts: attempts.clone(),
            },
        )
        .await
        .expect("subscribe");

    bus.publish(
        message("evt.manual.error", "uid-manual-error"),
        PublishOptions::default(),
    )
    .await
    .expect("publish");

    timeout(Duration::from_secs(2), async {
        loop {
            if attempts.load(Ordering::SeqCst) >= 1 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("handler invoked");

    sleep(Duration::from_millis(100)).await;

    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        backend
            .pending_count("evt.manual.error", "cg.manual.error")
            .await,
        1
    );
    assert_eq!(backend.stream_len("evt.manual.error").await, 1);

    sub.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_generates_consumer_name_when_empty() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend.clone(), StreamBusOptions::default()).expect("construct bus");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.generated-consumer").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.generated-consumer").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::auto())
            .ack_mode(AckMode::AutoOnHandlerSuccess)
            .max_in_flight(1)
            .build()
            .expect("build subscription config"),
            AutoAckHandler { tx },
        )
        .await
        .expect("subscribe with generated consumer name");

    assert!(!sub.name().trim().is_empty());

    bus.publish(
        message("evt.generated-consumer", "uid-generated-consumer"),
        PublishOptions::default(),
    )
    .await
    .expect("publish");

    timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("message received")
        .expect("delivered");

    sub.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_subscription_stops_background_workers() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend, StreamBusOptions::default()).expect("construct bus");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.drop").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.drop").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::AutoOnHandlerSuccess)
            .max_in_flight(1)
            .build()
            .expect("build subscription config"),
            AutoAckHandler { tx },
        )
        .await
        .expect("subscribe");

    drop(sub);
    sleep(Duration::from_millis(50)).await;

    bus.publish(message("evt.drop", "uid-drop"), PublishOptions::default())
        .await
        .expect("publish");

    match timeout(Duration::from_millis(200), rx.recv()).await {
        Err(_) | Ok(None) => {}
        Ok(Some(_)) => panic!("dropped subscription should not deliver new messages"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscription_respects_max_in_flight_limit() {
    struct BlockingHandler {
        started_tx: mpsc::Sender<String>,
        release: Arc<tokio::sync::Semaphore>,
    }

    impl Handler for BlockingHandler {
        fn handle(
            &self,
            delivery: Box<dyn DeliveryHandle>,
        ) -> eventbus_core::BoxFuture<'_, Result<(), EventBusError>> {
            Box::pin(async move {
                self.started_tx
                    .send(delivery.message().uid.clone())
                    .await
                    .map_err(|err| EventBusError::Internal(err.to_string()))?;
                let _permit = self
                    .release
                    .acquire()
                    .await
                    .map_err(|err| EventBusError::Internal(err.to_string()))?;
                delivery.ack().await
            })
        }
    }

    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend, StreamBusOptions::default()).expect("construct bus");

    let (started_tx, mut started_rx) = mpsc::channel(4);
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.max-in-flight").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.max-in-flight").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer").expect("consumer name"))
            .ack_mode(AckMode::Manual)
            .max_in_flight(1)
            .max_pending_acks(1)
            .build()
            .expect("build subscription config"),
            BlockingHandler {
                started_tx,
                release: release.clone(),
            },
        )
        .await
        .expect("subscribe");

    bus.publish(
        message("evt.max-in-flight", "uid-1"),
        PublishOptions::default(),
    )
    .await
    .expect("publish first");
    bus.publish(
        message("evt.max-in-flight", "uid-2"),
        PublishOptions::default(),
    )
    .await
    .expect("publish second");

    let first = timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .expect("first delivery")
        .expect("first uid");
    assert_eq!(first, "uid-1");
    assert!(timeout(Duration::from_millis(200), started_rx.recv())
        .await
        .is_err());

    release.add_permits(2);
    timeout(Duration::from_secs(2), started_rx.recv())
        .await
        .expect("second delivery")
        .expect("second uid");

    sub.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_start_id_zero_reads_existing_messages() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(
        backend.clone(),
        StreamBusOptions {
            group_start_id: "0".to_string(),
            ..Default::default()
        },
    )
    .expect("construct bus");

    bus.publish(
        message("evt.from-zero", "uid-existing"),
        PublishOptions::default(),
    )
    .await
    .expect("publish existing before subscribe");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.from-zero").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.from-zero").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::AutoOnHandlerSuccess)
            .max_in_flight(1)
            .build()
            .expect("build subscription config"),
            AutoAckHandler { tx },
        )
        .await
        .expect("subscribe");

    let got = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("existing message received")
        .expect("delivered");
    assert_eq!(got.uid, "uid-existing");

    sub.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_batch_rejects_oversize_payload_before_publishing_any_message() {
    // Topic invalidity is unreachable post-PR4 (Topic::new validates at
    // construction). The remaining publish-time validation path is the
    // payload-size check, exercised here.
    //
    // Post-PR5 semantics: `publish_batch` returns Ok(BatchOutcome) with one
    // Result per input — a per-message validation failure is recorded in the
    // outcome rather than aborting the whole batch.
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(
        backend.clone(),
        StreamBusOptions::default().with_max_payload_bytes(8),
    )
    .expect("construct bus");

    // Construct a small valid payload that fits inside the 8-byte cap.
    let mut small = message("evt.batch.atomic", "uid-valid");
    small.payload = bytes::Bytes::from_static(b"ok");
    let mut oversize = message("evt.batch.atomic", "uid-invalid");
    oversize.payload = bytes::Bytes::from_static(b"this payload is too large");
    let messages = vec![small, oversize];

    let outcome = bus
        .publish_batch(messages, PublishOptions::default())
        .await
        .expect("publish_batch returns BatchOutcome");

    assert_eq!(outcome.results.len(), 2);
    assert!(outcome.results[0].is_ok(), "small message should succeed");
    assert!(matches!(
        outcome.results[1].as_ref().err(),
        Some(EventBusError::Validation(_))
    ));
    assert_eq!(backend.stream_len("evt.batch.atomic").await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_allows_empty_uid_to_match_go_parity() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend.clone(), StreamBusOptions::default()).expect("construct bus");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.empty-uid").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.empty-uid").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::AutoOnHandlerSuccess)
            .max_in_flight(1)
            .build()
            .expect("build subscription config"),
            AutoAckHandler { tx },
        )
        .await
        .expect("subscribe");

    bus.publish(message("evt.empty-uid", ""), PublishOptions::default())
        .await
        .expect("publish with empty uid");

    let got = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("message received")
        .expect("delivered");
    assert!(got.uid.is_empty());

    sub.close().await.expect("close sub");
}

/// Unified in-flight limiter: a running handler must hold its permit for the
/// full duration of `handle()`. A slot-1 consumer that is still inside a
/// blocked handler must not read another message until the handler returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaimed_messages_consume_in_flight_budget() {
    struct BlockingHandler {
        started_tx: mpsc::Sender<String>,
        release: Arc<Notify>,
    }

    impl Handler for BlockingHandler {
        fn handle(
            &self,
            delivery: Box<dyn DeliveryHandle>,
        ) -> eventbus_core::BoxFuture<'_, Result<(), EventBusError>> {
            Box::pin(async move {
                self.started_tx
                    .send(delivery.message().uid.clone())
                    .await
                    .map_err(|err| EventBusError::Internal(err.to_string()))?;
                self.release.notified().await;
                delivery.ack().await
            })
        }
    }

    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(
        backend.clone(),
        StreamBusOptions {
            claim_idle_timeout: Duration::from_millis(20),
            ..Default::default()
        },
    )
    .expect("construct bus");

    let (started_tx, mut started_rx) = mpsc::channel(2);
    let release = Arc::new(Notify::new());
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.inflight-budget").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.inflight-budget").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::Manual)
            .max_in_flight(1)
            .max_pending_acks(1)
            .build()
            .expect("build subscription config"),
            BlockingHandler {
                started_tx,
                release: Arc::clone(&release),
            },
        )
        .await
        .expect("subscribe");

    bus.publish(
        message("evt.inflight-budget", "uid-first"),
        PublishOptions::default(),
    )
    .await
    .expect("publish first");
    bus.publish(
        message("evt.inflight-budget", "uid-second"),
        PublishOptions::default(),
    )
    .await
    .expect("publish second");

    // First message enters the handler.
    let first = timeout(Duration::from_secs(2), started_rx.recv())
        .await
        .expect("first delivery")
        .expect("uid");
    assert_eq!(first, "uid-first");
    // Second message must NOT be delivered while the handler still holds the
    // slot (permit not yet released).
    assert!(timeout(Duration::from_millis(200), started_rx.recv())
        .await
        .is_err());

    // Release the handler; second message must now be delivered.
    release.notify_one();
    let second = timeout(Duration::from_secs(2), started_rx.recv())
        .await
        .expect("second delivery")
        .expect("uid");
    assert_eq!(second, "uid-second");
    release.notify_one();

    sub.close().await.expect("close sub");
}

#[derive(Default)]
struct FailingAckBackend {
    next_id: AtomicUsize,
    queue: Mutex<VecDeque<ClaimedMessage>>,
    notify: Notify,
}

impl FailingAckBackend {
    fn read_new_impl<'a>(
        &'a self,
        count: usize,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<FetchedEntry>, EventBusError>> + Send + 'a>> {
        Box::pin(async move {
            if count == 0 {
                return Ok(Vec::new());
            }

            {
                let mut queue = self.queue.lock().await;
                if !queue.is_empty() {
                    let take = count.min(queue.len());
                    return Ok(queue.drain(..take).map(FetchedEntry::Decoded).collect());
                }
            }

            if timeout.is_zero() {
                return Ok(Vec::new());
            }

            let notified = self.notify.notified();
            let _ = tokio::time::timeout(timeout, notified).await;
            let mut queue = self.queue.lock().await;
            let take = count.min(queue.len());
            Ok(queue.drain(..take).map(FetchedEntry::Decoded).collect())
        })
    }
}

impl StreamBackend for FailingAckBackend {
    async fn create_group(
        &self,
        _stream: &str,
        _group: &str,
        _start_id: &str,
    ) -> Result<(), EventBusError> {
        Ok(())
    }

    async fn publish(&self, _stream: &str, message: Message) -> Result<String, EventBusError> {
        let id = format!("{}-0", self.next_id.fetch_add(1, Ordering::SeqCst));
        {
            let mut queue = self.queue.lock().await;
            queue.push_back(ClaimedMessage {
                id: id.clone(),
                message: Arc::new(message),
                state: PartialDeliveryState {
                    attempt: 1,
                    first_received: Utc::now(),
                    last_received: Utc::now(),
                    redelivered: false,
                },
            });
        }
        self.notify.notify_waiters();
        Ok(id)
    }

    async fn reclaim_idle(
        &self,
        _stream: &str,
        _group: &str,
        _consumer: &str,
        _min_idle: Duration,
        _count: usize,
    ) -> Result<Vec<FetchedEntry>, EventBusError> {
        Ok(Vec::new())
    }

    fn read_new(
        &self,
        _stream: &str,
        _group: &str,
        _consumer: &str,
        count: usize,
        timeout: Duration,
    ) -> impl Future<Output = Result<Vec<FetchedEntry>, EventBusError>> + Send {
        self.read_new_impl(count, timeout)
    }

    async fn ack(
        &self,
        _stream: &str,
        _group: &str,
        _message_id: &str,
    ) -> Result<(), EventBusError> {
        Err(EventBusError::Connection("ack failed".into()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscription_close_surfaces_background_delivery_errors() {
    let backend = Arc::new(FailingAckBackend::default());
    let bus = StreamBus::new(backend, StreamBusOptions::default()).expect("construct bus");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.failing-ack").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.failing-ack").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::AutoOnHandlerSuccess)
            .max_in_flight(1)
            .build()
            .expect("build subscription config"),
            AutoAckHandler { tx },
        )
        .await
        .expect("subscribe");

    bus.publish(
        message("evt.failing-ack", "uid-1"),
        PublishOptions::default(),
    )
    .await
    .expect("publish");

    timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("handler ran")
        .expect("message");

    let err = sub
        .close()
        .await
        .expect_err("close should surface task error");
    assert!(matches!(err, EventBusError::Connection(_)));
}

#[derive(Default)]
struct ParallelPublishBackend {
    next_id: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
}

impl StreamBackend for ParallelPublishBackend {
    async fn create_group(
        &self,
        _stream: &str,
        _group: &str,
        _start_id: &str,
    ) -> Result<(), EventBusError> {
        Ok(())
    }

    async fn publish(&self, _stream: &str, _message: Message) -> Result<String, EventBusError> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self
            .max_active
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                Some(current.max(active))
            });
        sleep(Duration::from_millis(25)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(format!("{}-0", self.next_id.fetch_add(1, Ordering::SeqCst)))
    }

    async fn reclaim_idle(
        &self,
        _stream: &str,
        _group: &str,
        _consumer: &str,
        _min_idle: Duration,
        _count: usize,
    ) -> Result<Vec<FetchedEntry>, EventBusError> {
        Ok(Vec::new())
    }

    async fn read_new(
        &self,
        _stream: &str,
        _group: &str,
        _consumer: &str,
        _count: usize,
        _timeout: Duration,
    ) -> Result<Vec<FetchedEntry>, EventBusError> {
        Ok(Vec::new())
    }

    async fn ack(
        &self,
        _stream: &str,
        _group: &str,
        _message_id: &str,
    ) -> Result<(), EventBusError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_batch_uses_parallel_publishes() {
    let backend = Arc::new(ParallelPublishBackend::default());
    let bus = StreamBus::new(backend.clone(), StreamBusOptions::default()).expect("construct bus");

    let messages = vec![
        message("evt.parallel", "uid-1"),
        message("evt.parallel", "uid-2"),
        message("evt.parallel", "uid-3"),
        message("evt.parallel", "uid-4"),
    ];

    bus.publish_batch(messages, PublishOptions::default())
        .await
        .expect("publish batch");

    assert!(backend.max_active.load(Ordering::SeqCst) > 1);
}

/// Tracks whether ack comes through as batched `ack_many` or per-id `ack`.
/// Used to prove the flusher coalesces concurrent handler-success acks into
/// a single XACK command instead of N round-trips.
#[derive(Default)]
struct BatchAckBackend {
    next_id: AtomicUsize,
    queue: Mutex<VecDeque<ClaimedMessage>>,
    notify: Notify,
    ack_calls: AtomicUsize,
    ack_many_calls: AtomicUsize,
    ack_many_total_ids: AtomicUsize,
    ack_many_max_batch: AtomicUsize,
}

impl StreamBackend for BatchAckBackend {
    async fn create_group(
        &self,
        _stream: &str,
        _group: &str,
        _start_id: &str,
    ) -> Result<(), EventBusError> {
        Ok(())
    }

    async fn publish(&self, _stream: &str, message: Message) -> Result<String, EventBusError> {
        let id = format!("{}-0", self.next_id.fetch_add(1, Ordering::SeqCst));
        {
            let mut queue = self.queue.lock().await;
            queue.push_back(ClaimedMessage {
                id: id.clone(),
                message: Arc::new(message),
                state: PartialDeliveryState {
                    attempt: 1,
                    first_received: Utc::now(),
                    last_received: Utc::now(),
                    redelivered: false,
                },
            });
        }
        self.notify.notify_waiters();
        Ok(id)
    }

    async fn reclaim_idle(
        &self,
        _stream: &str,
        _group: &str,
        _consumer: &str,
        _min_idle: Duration,
        _count: usize,
    ) -> Result<Vec<FetchedEntry>, EventBusError> {
        Ok(Vec::new())
    }

    async fn read_new(
        &self,
        _stream: &str,
        _group: &str,
        _consumer: &str,
        count: usize,
        wait: Duration,
    ) -> Result<Vec<FetchedEntry>, EventBusError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        {
            let mut queue = self.queue.lock().await;
            if !queue.is_empty() {
                let take = count.min(queue.len());
                return Ok(queue.drain(..take).map(FetchedEntry::Decoded).collect());
            }
        }
        if wait.is_zero() {
            return Ok(Vec::new());
        }
        let notified = self.notify.notified();
        let _ = tokio::time::timeout(wait, notified).await;
        let mut queue = self.queue.lock().await;
        let take = count.min(queue.len());
        Ok(queue.drain(..take).map(FetchedEntry::Decoded).collect())
    }

    fn ack(
        &self,
        _stream: &str,
        _group: &str,
        _message_id: &str,
    ) -> impl Future<Output = Result<(), EventBusError>> + Send {
        self.ack_calls.fetch_add(1, Ordering::SeqCst);
        async { Ok(()) }
    }

    fn ack_many(
        &self,
        _stream: &str,
        _group: &str,
        message_ids: &[String],
    ) -> impl Future<Output = Result<(), EventBusError>> + Send {
        let n = message_ids.len();
        self.ack_many_calls.fetch_add(1, Ordering::SeqCst);
        self.ack_many_total_ids.fetch_add(n, Ordering::SeqCst);
        let _ = self
            .ack_many_max_batch
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |cur| Some(cur.max(n)));
        async { Ok(()) }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_ack_uses_batched_xack() {
    const TOTAL: usize = 16;

    let backend = Arc::new(BatchAckBackend::default());
    let options = StreamBusOptions {
        // Widen the flush window so bursty handler-success acks coalesce into
        // one batch instead of racing the 2ms default.
        ack_flush_interval: Duration::from_millis(50),
        ack_batch_size: 64,
        ..Default::default()
    };
    let bus = StreamBus::new(backend.clone(), options).expect("construct bus");

    let (tx, mut rx) = mpsc::channel(TOTAL);
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.batch-ack").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.batch-ack").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::AutoOnHandlerSuccess)
            .max_in_flight(TOTAL)
            .build()
            .expect("build subscription config"),
            AutoAckHandler { tx },
        )
        .await
        .expect("subscribe");

    let msgs: Vec<Message> = (0..TOTAL)
        .map(|i| message("evt.batch-ack", &format!("uid-{i}")))
        .collect();
    bus.publish_batch(msgs, PublishOptions::default())
        .await
        .expect("publish batch");

    for _ in 0..TOTAL {
        timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("handler ran")
            .expect("message");
    }

    sub.close().await.expect("close sub");

    let ack_calls = backend.ack_calls.load(Ordering::SeqCst);
    let ack_many_calls = backend.ack_many_calls.load(Ordering::SeqCst);
    let total_ids = backend.ack_many_total_ids.load(Ordering::SeqCst);
    let max_batch = backend.ack_many_max_batch.load(Ordering::SeqCst);

    assert_eq!(
        ack_calls, 0,
        "flusher must never fall back to per-id ack; saw {ack_calls}"
    );
    assert!(
        ack_many_calls >= 1,
        "expected at least one ack_many call, saw {ack_many_calls}"
    );
    assert_eq!(
        total_ids, TOTAL,
        "every delivery must be acked exactly once"
    );
    assert!(
        max_batch > 1,
        "expected at least one batched ack (>1 id), largest batch was {max_batch}"
    );
    assert!(
        ack_many_calls < TOTAL,
        "flusher should coalesce: {ack_many_calls} calls for {TOTAL} messages is not batching"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_rejects_oversize_payload() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(
        backend.clone(),
        StreamBusOptions {
            max_payload_bytes: 16,
            ..Default::default()
        },
    )
    .expect("construct bus");

    let mut msg = message("evt.size", "uid-too-big");
    msg.payload = bytes::Bytes::from(vec![0u8; 32]);

    let err = bus
        .publish(msg, PublishOptions::default())
        .await
        .expect_err("should reject oversize payload");
    assert!(matches!(err, EventBusError::Validation(_)));
    assert_eq!(backend.stream_len("evt.size").await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_exhausted_without_dlq_returns_error() {
    struct AlwaysRetryHandler {
        attempts: Arc<AtomicUsize>,
        last_err: Arc<Mutex<Option<String>>>,
        done: Arc<Notify>,
    }

    impl Handler for AlwaysRetryHandler {
        fn handle(
            &self,
            delivery: Box<dyn DeliveryHandle>,
        ) -> eventbus_core::BoxFuture<'_, Result<(), EventBusError>> {
            Box::pin(async move {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                let res = delivery
                    .retry(Box::new(std::io::Error::other("force-retry")))
                    .await;
                if let Err(e) = res {
                    *self.last_err.lock().await = Some(e.to_string());
                    self.done.notify_one();
                    return Err(e);
                }
                Ok(())
            })
        }
    }

    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend.clone(), StreamBusOptions::default()).expect("construct bus");

    let attempts = Arc::new(AtomicUsize::new(0));
    let last_err = Arc::new(Mutex::new(None::<String>));
    let done = Arc::new(Notify::new());
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.no-dlq").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.no-dlq").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::Manual)
            .max_in_flight(1)
            .max_retry(1)
            .build()
            .expect("build subscription config"),
            AlwaysRetryHandler {
                attempts: attempts.clone(),
                last_err: last_err.clone(),
                done: Arc::clone(&done),
            },
        )
        .await
        .expect("subscribe");

    bus.publish(
        message("evt.no-dlq", "uid-no-dlq"),
        PublishOptions::default(),
    )
    .await
    .expect("publish");

    timeout(Duration::from_secs(2), done.notified())
        .await
        .expect("retry rejection surfaced");

    let err = last_err.lock().await.clone();
    assert!(
        err.as_deref()
            .unwrap_or_default()
            .contains("dead_letter_topic"),
        "expected dead_letter_topic error, got: {err:?}"
    );

    sub.close().await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_generated_consumer_name_has_random_suffix() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend, StreamBusOptions::default()).expect("construct bus");

    let cfg = || {
        eventbus_core::SubscriptionConfig::builder(
            eventbus_core::Topic::new("evt.cn-rand").expect("topic"),
            eventbus_core::ConsumerGroup::new("cg.cn-rand").expect("group"),
        )
        .consumer_name(eventbus_core::ConsumerName::auto())
        .ack_mode(AckMode::AutoOnHandlerSuccess)
        .max_in_flight(1)
        .build()
        .expect("build subscription config")
    };

    let (tx_a, _rx_a) = mpsc::channel(1);
    let (tx_b, _rx_b) = mpsc::channel(1);
    let sub_a = bus
        .subscribe(cfg(), AutoAckHandler { tx: tx_a })
        .await
        .expect("subscribe a");
    let sub_b = bus
        .subscribe(cfg(), AutoAckHandler { tx: tx_b })
        .await
        .expect("subscribe b");

    assert_ne!(
        sub_a.name(),
        sub_b.name(),
        "auto-generated consumer names must be unique even at high spawn rate"
    );

    sub_a.close().await.ok();
    sub_b.close().await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscription_is_running_flips_on_close() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend, StreamBusOptions::default()).expect("construct bus");

    let (tx, _rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.is-running").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.is-running").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::AutoOnHandlerSuccess)
            .max_in_flight(1)
            .build()
            .expect("build subscription config"),
            AutoAckHandler { tx },
        )
        .await
        .expect("subscribe");

    assert!(sub.is_running());
    sub.close().await.expect("close");
    assert!(!sub.is_running());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_new_rejects_control_char() {
    // Topic is validated at construction; the bus boundary trusts the type.
    let err = eventbus_core::Topic::new("evt.\u{0007}bell")
        .expect_err("control char in topic must be rejected");
    assert!(matches!(err, EventBusError::Validation(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn error_observer_receives_ack_flush_failures() {
    #[derive(Default)]
    struct ScopeRecorder {
        scopes: Mutex<Vec<ErrorScope>>,
    }

    impl ErrorObserver for ScopeRecorder {
        fn on_error(&self, scope: ErrorScope, _err: &EventBusError) {
            // The observer hook is sync, but in a test we want to record into
            // an async-aware structure — `try_lock` keeps the hook non-blocking.
            if let Ok(mut g) = self.scopes.try_lock() {
                g.push(scope);
            }
        }
    }

    let recorder = Arc::new(ScopeRecorder::default());
    let backend = Arc::new(FailingAckBackend::default());
    let bus = StreamBus::new(
        backend,
        StreamBusOptions::default().with_error_observer(recorder.clone()),
    )
    .expect("construct bus");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.observer-ack").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.observer-ack").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::AutoOnHandlerSuccess)
            .max_in_flight(1)
            .build()
            .expect("build subscription config"),
            AutoAckHandler { tx },
        )
        .await
        .expect("subscribe");

    bus.publish(
        message("evt.observer-ack", "uid-obs"),
        PublishOptions::default(),
    )
    .await
    .expect("publish");

    timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("handler ran")
        .expect("message");

    let _ = sub.close().await; // returns Err because of the failing ack
    let scopes = recorder.scopes.lock().await.clone();
    assert!(
        scopes.contains(&ErrorScope::AckFlush),
        "expected AckFlush observation, saw: {scopes:?}"
    );
}

/// Regression for the limiter-cap bug: with `max_in_flight: 1` and the
/// **default** `max_pending_acks` (= 0, normalized to 2), the bus must never
/// run two handlers concurrently. Earlier code sized the limiter as
/// `max(max_pending_acks, max_in_flight)` and would silently allow 2 concurrent
/// handlers under the documented configuration.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_max_pending_acks_does_not_inflate_handler_concurrency() {
    struct H {
        in_flight: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        release: Arc<Notify>,
        started_tx: mpsc::Sender<()>,
    }
    impl Handler for H {
        fn handle(
            &self,
            delivery: Box<dyn DeliveryHandle>,
        ) -> eventbus_core::BoxFuture<'_, Result<(), EventBusError>> {
            Box::pin(async move {
                let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                let _ = self
                    .peak
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |p| Some(p.max(now)));
                self.started_tx.send(()).await.ok();
                self.release.notified().await;
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                delivery.ack().await
            })
        }
    }

    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend, StreamBusOptions::default()).expect("construct bus");

    let release = Arc::new(Notify::new());
    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let (started_tx, mut started_rx) = mpsc::channel(8);

    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.cap").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.cap").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::Manual)
            .max_in_flight(1)
            .build()
            .expect("build subscription config"),
            H {
                in_flight: Arc::clone(&in_flight),
                peak: Arc::clone(&peak),
                release: Arc::clone(&release),
                started_tx,
            },
        )
        .await
        .expect("subscribe");

    for i in 0..4 {
        bus.publish(
            message("evt.cap", &format!("uid-{i}")),
            PublishOptions::default(),
        )
        .await
        .expect("publish");
    }

    // Drain handlers one at a time.
    for _ in 0..4 {
        timeout(Duration::from_secs(2), started_rx.recv())
            .await
            .expect("handler started")
            .expect("signal");
        // While this handler is parked, no other handler must be running.
        assert_eq!(
            in_flight.load(Ordering::SeqCst),
            1,
            "max_in_flight=1 must mean exactly 1 concurrent handler",
        );
        release.notify_one();
    }

    assert_eq!(peak.load(Ordering::SeqCst), 1);
    sub.close().await.expect("close sub");
}

/// Regression for the poison-pill bug: a single corrupt entry must not stall
/// the rest of the batch. The bus is expected to ack the malformed entry,
/// route a synthetic envelope to the configured DLQ, fire the error
/// observer, and continue delivering well-formed entries normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_entry_is_acked_dlqd_and_does_not_stall_batch() {
    use std::sync::Mutex as StdMutex;

    #[derive(Default)]
    struct PoisonBackend {
        next_id: AtomicUsize,
        // queue of (id, entry) pairs the consumer should see
        queue: StdMutex<VecDeque<FetchedEntry>>,
        notify: Notify,
        dlq: StdMutex<Vec<Message>>,
        acks: StdMutex<Vec<String>>,
    }

    impl StreamBackend for PoisonBackend {
        async fn create_group(
            &self,
            _stream: &str,
            _group: &str,
            _start_id: &str,
        ) -> Result<(), EventBusError> {
            Ok(())
        }

        async fn publish(&self, stream: &str, message: Message) -> Result<String, EventBusError> {
            let id = format!("{}-0", self.next_id.fetch_add(1, Ordering::SeqCst));
            if stream == "evt.poison.dlq" {
                self.dlq.lock().unwrap().push(message);
            }
            Ok(id)
        }

        async fn reclaim_idle(
            &self,
            _stream: &str,
            _group: &str,
            _consumer: &str,
            _min_idle: Duration,
            _count: usize,
        ) -> Result<Vec<FetchedEntry>, EventBusError> {
            Ok(Vec::new())
        }

        async fn read_new(
            &self,
            _stream: &str,
            _group: &str,
            _consumer: &str,
            count: usize,
            wait: Duration,
        ) -> Result<Vec<FetchedEntry>, EventBusError> {
            if count == 0 {
                return Ok(Vec::new());
            }
            {
                let mut q = self.queue.lock().unwrap();
                if !q.is_empty() {
                    let take = count.min(q.len());
                    return Ok(q.drain(..take).collect());
                }
            }
            if wait.is_zero() {
                return Ok(Vec::new());
            }
            let _ = tokio::time::timeout(wait, self.notify.notified()).await;
            let mut q = self.queue.lock().unwrap();
            let take = count.min(q.len());
            Ok(q.drain(..take).collect())
        }

        async fn ack(
            &self,
            _stream: &str,
            _group: &str,
            message_id: &str,
        ) -> Result<(), EventBusError> {
            self.acks.lock().unwrap().push(message_id.to_string());
            Ok(())
        }
    }

    let backend = Arc::new(PoisonBackend::default());

    // Pre-load a malformed entry then a healthy one. The malformed should be
    // routed to DLQ + acked; the healthy one must reach the handler.
    {
        let mut q = backend.queue.lock().unwrap();
        q.push_back(FetchedEntry::Malformed {
            id: "1-0".into(),
            error: EventBusError::Serialization("bad payload".into()),
        });
        q.push_back(FetchedEntry::Decoded(ClaimedMessage {
            id: "2-0".into(),
            message: Arc::new(message("evt.poison", "uid-good")),
            state: PartialDeliveryState {
                attempt: 1,
                first_received: Utc::now(),
                last_received: Utc::now(),
                redelivered: false,
            },
        }));
    }

    let bus =
        StreamBus::new(Arc::clone(&backend), StreamBusOptions::default()).expect("construct bus");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("evt.poison").expect("topic"),
                eventbus_core::ConsumerGroup::new("cg.poison").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("consumer-1").expect("consumer name"))
            .ack_mode(AckMode::AutoOnHandlerSuccess)
            .max_in_flight(1)
            .dead_letter_topic(eventbus_core::Topic::new("evt.poison.dlq").expect("dlq topic"))
            .build()
            .expect("build subscription config"),
            AutoAckHandler { tx },
        )
        .await
        .expect("subscribe");

    let got = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("healthy entry was delivered despite the poison entry")
        .expect("message");
    assert_eq!(got.uid, "uid-good");

    // Wait briefly for the malformed-entry side effects (DLQ + ack) to flush.
    sleep(Duration::from_millis(50)).await;

    {
        let dlq = backend.dlq.lock().unwrap();
        assert_eq!(dlq.len(), 1, "malformed entry should land in DLQ");
        assert_eq!(dlq[0].kind, "eventbus.malformed");
        assert_eq!(dlq[0].key, "1-0");
    }
    {
        let acks = backend.acks.lock().unwrap();
        assert!(
            acks.contains(&"1-0".to_string()),
            "malformed entry must be acked so it leaves the PEL, saw: {acks:?}"
        );
    }

    sub.close().await.expect("close sub");
}
