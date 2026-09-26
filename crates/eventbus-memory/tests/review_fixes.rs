use std::{sync::Arc, time::Duration};

use eventbus_core::{
    stream::{FetchedEntry, StreamBackend},
    BoxFuture, ConsumerGroup, DeliveryHandle, EventBusError, Handler, Headers, Message,
    OrderingMode, PublishOptions, StreamBus, StreamBusOptions, SubscriptionConfig, Topic,
    HEADER_RETRY_ATTEMPT,
};
use eventbus_memory::MemoryStreamBackend;
use tokio::{sync::Notify, time::timeout};

fn message(topic: &str, uid: &str) -> Message {
    Message {
        uid: uid.into(),
        topic: Topic::new(topic).unwrap(),
        key: "same-key".into(),
        kind: "test".into(),
        source: "review".into(),
        occurred_at: chrono::Utc::now(),
        headers: Headers::new(),
        payload: bytes::Bytes::from_static(b"{}"),
        content_type: None,
        event_version: None,
        idempotency_key: None,
        expires_at: None,
        trace_uid: None,
        correlation_uid: None,
    }
}

struct Blocking {
    started: Arc<Notify>,
    release: Arc<Notify>,
    finished: Arc<Notify>,
}

struct FinishOnDrop(Arc<Notify>);
impl Drop for FinishOnDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

impl Handler for Blocking {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            let _finished = FinishOnDrop(self.finished.clone());
            self.started.notify_one();
            self.release.notified().await;
            delivery.ack().await
        })
    }
}

#[tokio::test]
async fn cancelled_close_can_resume_or_abort_the_running_handler() {
    for abort in [false, true] {
        let backend = Arc::new(MemoryStreamBackend::default());
        let bus = StreamBus::new(backend, StreamBusOptions::default()).unwrap();
        let config = SubscriptionConfig::builder(
            Topic::new("close").unwrap(),
            ConsumerGroup::new("g").unwrap(),
        )
        .build()
        .unwrap();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let finished = Arc::new(Notify::new());
        let sub = bus
            .subscribe(
                config,
                Blocking {
                    started: started.clone(),
                    release: release.clone(),
                    finished: finished.clone(),
                },
            )
            .await
            .unwrap();
        bus.publish(message("close", "one"), PublishOptions::new())
            .await
            .unwrap();
        timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();
        assert!(timeout(Duration::from_millis(20), sub.close())
            .await
            .is_err());
        assert!(
            sub.is_running(),
            "a cancelled close must not report the live task as stopped"
        );
        if abort {
            timeout(Duration::from_secs(1), sub.abort())
                .await
                .unwrap()
                .unwrap();
        } else {
            release.notify_one();
            timeout(Duration::from_secs(1), sub.close())
                .await
                .unwrap()
                .unwrap();
        }
        timeout(Duration::from_secs(1), finished.notified())
            .await
            .unwrap();
        assert!(!sub.is_running());
        sub.close().await.unwrap();
    }
}

#[tokio::test]
async fn abort_interrupts_a_concurrent_graceful_close() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend, StreamBusOptions::default()).unwrap();
    let config = SubscriptionConfig::builder(
        Topic::new("close").unwrap(),
        ConsumerGroup::new("g").unwrap(),
    )
    .build()
    .unwrap();
    let started = Arc::new(Notify::new());
    let finished = Arc::new(Notify::new());
    let sub = Arc::new(
        bus.subscribe(
            config,
            Blocking {
                started: started.clone(),
                release: Arc::new(Notify::new()),
                finished: finished.clone(),
            },
        )
        .await
        .unwrap(),
    );
    bus.publish(message("close", "one"), PublishOptions::new())
        .await
        .unwrap();
    timeout(Duration::from_secs(1), started.notified())
        .await
        .unwrap();
    let closing = tokio::spawn({
        let sub = sub.clone();
        async move { sub.close().await }
    });
    tokio::task::yield_now().await;
    timeout(Duration::from_secs(1), sub.abort())
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(1), closing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(1), finished.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn key_ordering_is_rejected_before_starting_a_subscription() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend, StreamBusOptions::default()).unwrap();
    for limit in [1, 2] {
        let config = SubscriptionConfig::builder(
            Topic::new("ordered").unwrap(),
            ConsumerGroup::new("g").unwrap(),
        )
        .ordering(OrderingMode::Key)
        .max_in_flight(limit)
        .build()
        .unwrap();
        let result = bus
            .subscribe(
                config,
                Blocking {
                    started: Arc::new(Notify::new()),
                    release: Arc::new(Notify::new()),
                    finished: Arc::new(Notify::new()),
                },
            )
            .await;
        assert!(matches!(result, Err(EventBusError::Validation(_))));
    }
}

#[tokio::test]
async fn overflowing_retry_entry_is_malformed_and_does_not_hide_the_next_message() {
    let backend = MemoryStreamBackend::default();
    backend.create_group("overflow", "g", "0").await.unwrap();
    let mut invalid = message("overflow", "invalid");
    invalid
        .headers
        .insert(HEADER_RETRY_ATTEMPT.into(), u32::MAX.to_string());
    let bad_id = backend.publish("overflow", invalid).await.unwrap();
    let good_id = backend
        .publish("overflow", message("overflow", "valid"))
        .await
        .unwrap();
    let entries = backend
        .read_new("overflow", "g", "c", 2, Duration::ZERO)
        .await
        .unwrap();
    assert!(matches!(&entries[0], FetchedEntry::Malformed { id, .. } if id == &bad_id));
    assert!(matches!(&entries[1], FetchedEntry::Decoded(entry) if entry.id == good_id));
    backend.ack("overflow", "g", &good_id).await.unwrap();
    let entries = backend
        .reclaim_idle("overflow", "g", "c", Duration::ZERO, 1)
        .await
        .unwrap();
    assert!(matches!(&entries[0], FetchedEntry::Malformed { id, .. } if id == &bad_id));
    backend.ack("overflow", "g", &bad_id).await.unwrap();
    assert_eq!(backend.pending_count("overflow", "g").await, 0);
}
