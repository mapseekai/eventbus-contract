use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use tokio::sync::{Mutex, Notify};

use eventbus_core::stream::{ClaimedMessage, FetchedEntry, StreamBackend};
use eventbus_core::{EventBusError, Message, PartialDeliveryState};

/// In-process stream backend.
///
/// ⚠️ **For functional tests and examples only.**
///
/// All state lives behind a single `tokio::sync::Mutex`, so every backend
/// operation (publish / read / ack / reclaim) is globally serialized. This is
/// deliberate: it makes the memory backend a deterministic reference for
/// correctness tests, but it is **not representative of production
/// throughput** and must not be used for benchmarking the `StreamBus`
/// concurrency model. Use `eventbus_redis::RedisBackend` (behind the
/// `redis` feature on the facade) for any performance-oriented workload.
#[derive(Debug, Default)]
pub struct MemoryStreamBackend {
    state: Mutex<MemoryState>,
    notify: Notify,
}

impl MemoryStreamBackend {
    pub async fn pending_count(&self, stream: &str, group: &str) -> usize {
        let state = self.state.lock().await;
        state
            .streams
            .get(stream)
            .and_then(|stream_state| stream_state.groups.get(group))
            .map_or(0, |group_state| group_state.pending.len())
    }

    pub async fn stream_len(&self, stream: &str) -> usize {
        let state = self.state.lock().await;
        state
            .streams
            .get(stream)
            .map_or(0, |stream_state| stream_state.entries.len())
    }
}

impl StreamBackend for MemoryStreamBackend {
    async fn create_group(
        &self,
        stream: &str,
        group: &str,
        start_id: &str,
    ) -> Result<(), EventBusError> {
        let mut state = self.state.lock().await;
        let stream_state = state.streams.entry(stream.to_string()).or_default();
        let cursor = if matches!(start_id.trim(), "0" | "0-0") {
            0
        } else {
            stream_state.entries.len()
        };
        stream_state
            .groups
            .entry(group.to_string())
            .or_insert_with(|| GroupState {
                cursor,
                pending: HashMap::new(),
            });
        Ok(())
    }

    async fn publish(&self, stream: &str, message: Message) -> Result<String, EventBusError> {
        let id = {
            let mut state = self.state.lock().await;
            let next = state.next_id;
            state.next_id += 1;
            let id = format!("{next}-0");
            state
                .streams
                .entry(stream.to_string())
                .or_default()
                .entries
                .push(StreamEntry {
                    id: id.clone(),
                    message,
                });
            id
        };

        self.notify.notify_waiters();
        Ok(id)
    }

    async fn reclaim_idle(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        min_idle: Duration,
        count: usize,
    ) -> Result<Vec<FetchedEntry>, EventBusError> {
        let now = Utc::now();
        let instant = Instant::now();
        let mut state = self.state.lock().await;
        let Some(stream_state) = state.streams.get_mut(stream) else {
            return Ok(Vec::new());
        };
        let Some(group_state) = stream_state.groups.get_mut(group) else {
            return Ok(Vec::new());
        };

        let ids: Vec<String> = group_state
            .pending
            .iter()
            .filter(|(_, pending)| pending.last_delivered_at.elapsed() >= min_idle)
            .map(|(id, _)| id.clone())
            .take(count)
            .collect();

        let mut claimed = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(pending) = group_state.pending.get_mut(&id) {
                pending.owner = consumer.to_string();
                let attempt = pending.delivery_count.checked_add(1);
                pending.delivery_count = attempt.unwrap_or(u32::MAX);
                pending.last_received = now;
                pending.last_delivered_at = instant;
                pending.redelivered = true;

                let Some(attempt) = attempt else {
                    claimed.push(FetchedEntry::Malformed {
                        id,
                        error: EventBusError::Serialization("delivery attempt overflow".into()),
                    });
                    continue;
                };
                claimed.push(FetchedEntry::Decoded(ClaimedMessage {
                    id,
                    message: Arc::clone(&pending.message),
                    state: PartialDeliveryState {
                        attempt,
                        first_received: pending.first_received,
                        last_received: pending.last_received,
                        redelivered: pending.redelivered,
                    },
                }));
            }
        }

        Ok(claimed)
    }

    async fn read_new(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        count: usize,
        timeout: Duration,
    ) -> Result<Vec<FetchedEntry>, EventBusError> {
        if count == 0 {
            return Ok(Vec::new());
        }

        if timeout.is_zero() {
            return self
                .read_new_available(stream, group, consumer, count)
                .await;
        }

        let messages = self
            .read_new_available(stream, group, consumer, count)
            .await?;
        if !messages.is_empty() {
            return Ok(messages);
        }

        let notified = self.notify.notified();
        let _ = tokio::time::timeout(timeout, notified).await;
        self.read_new_available(stream, group, consumer, count)
            .await
    }

    async fn ack(&self, stream: &str, group: &str, message_id: &str) -> Result<(), EventBusError> {
        let mut state = self.state.lock().await;
        if let Some(stream_state) = state.streams.get_mut(stream) {
            if let Some(group_state) = stream_state.groups.get_mut(group) {
                group_state.pending.remove(message_id);
            }
        }

        Ok(())
    }

    async fn ack_many(
        &self,
        stream: &str,
        group: &str,
        message_ids: &[String],
    ) -> Result<(), EventBusError> {
        let mut state = self.state.lock().await;
        if let Some(stream_state) = state.streams.get_mut(stream) {
            if let Some(group_state) = stream_state.groups.get_mut(group) {
                for id in message_ids {
                    group_state.pending.remove(id);
                }
            }
        }
        Ok(())
    }
}

impl MemoryStreamBackend {
    async fn read_new_available(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        count: usize,
    ) -> Result<Vec<FetchedEntry>, EventBusError> {
        let now = Utc::now();
        let instant = Instant::now();
        let mut state = self.state.lock().await;
        let Some(stream_state) = state.streams.get_mut(stream) else {
            return Ok(Vec::new());
        };
        let Some(group_state) = stream_state.groups.get_mut(group) else {
            return Ok(Vec::new());
        };

        let end = (group_state.cursor + count).min(stream_state.entries.len());
        if end == group_state.cursor {
            return Ok(Vec::new());
        }

        let entries = stream_state.entries[group_state.cursor..end].to_vec();
        group_state.cursor = end;

        let mut claimed = Vec::with_capacity(entries.len());
        for mut entry in entries {
            // Match the wire-boundary normalization that real backends do.
            entry.message.normalize();
            let attempt = retry_attempt(&entry.message).checked_add(1);
            let message = Arc::new(entry.message);
            group_state.pending.insert(
                entry.id.clone(),
                PendingEntry {
                    message: Arc::clone(&message),
                    owner: consumer.to_string(),
                    delivery_count: attempt.unwrap_or(u32::MAX),
                    first_received: now,
                    last_received: now,
                    last_delivered_at: instant,
                    redelivered: false,
                },
            );

            let Some(attempt) = attempt else {
                claimed.push(FetchedEntry::Malformed {
                    id: entry.id,
                    error: EventBusError::Serialization("delivery attempt overflow".into()),
                });
                continue;
            };
            claimed.push(FetchedEntry::Decoded(ClaimedMessage {
                id: entry.id,
                message,
                state: PartialDeliveryState {
                    attempt,
                    first_received: now,
                    last_received: now,
                    redelivered: false,
                },
            }));
        }

        Ok(claimed)
    }
}

#[derive(Debug, Default)]
struct MemoryState {
    next_id: u64,
    streams: HashMap<String, StreamState>,
}

#[derive(Debug, Default)]
struct StreamState {
    entries: Vec<StreamEntry>,
    groups: HashMap<String, GroupState>,
}

#[derive(Debug, Clone)]
struct StreamEntry {
    id: String,
    message: Message,
}

#[derive(Debug)]
struct GroupState {
    cursor: usize,
    pending: HashMap<String, PendingEntry>,
}

#[derive(Debug, Clone)]
struct PendingEntry {
    message: Arc<Message>,
    owner: String,
    delivery_count: u32,
    first_received: DateTime<Utc>,
    last_received: DateTime<Utc>,
    last_delivered_at: Instant,
    redelivered: bool,
}

fn retry_attempt(message: &Message) -> u32 {
    message
        .headers
        .get("retry-attempt")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0)
}
