use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::EventBusError;

use super::backend::StreamBackend;
use super::observer::{ErrorObserver, ErrorScope};

pub(super) struct AckRequest {
    pub id: String,
    pub done: oneshot::Sender<Result<(), EventBusError>>,
}

/// Spawns a background task that collects individual ack requests and flushes
/// them to the backend in batches via [`StreamBackend::ack_many`].
///
/// Batching triggers on whichever comes first:
/// - `batch_size` requests accumulated, or
/// - `flush_interval` elapsed since the first un-flushed request.
///
/// Returns the sender half (cloneable, held by each `StreamDelivery`)
/// and the task handle (awaited by the subscription on shutdown).
///
/// **Shutdown**: when all senders drop, the flusher drains remaining requests,
/// flushes a final batch, and exits.
pub(super) fn spawn<B: StreamBackend>(
    backend: Arc<B>,
    stream: String,
    group: String,
    batch_size: usize,
    flush_interval: Duration,
    error_observer: Option<Arc<dyn ErrorObserver>>,
) -> (mpsc::Sender<AckRequest>, JoinHandle<()>) {
    // StreamBusOptions normalizes these once at construction.
    debug_assert!(batch_size > 0);
    debug_assert!(!flush_interval.is_zero());

    let channel_cap = batch_size.saturating_mul(4).max(64);
    let (tx, mut rx) = mpsc::channel::<AckRequest>(channel_cap);

    let handle = tokio::spawn(async move {
        let mut buf: Vec<AckRequest> = Vec::with_capacity(batch_size);

        loop {
            let first = match rx.recv().await {
                Some(req) => req,
                None => break,
            };
            buf.push(first);

            // Greedy synchronous drain — grab everything already queued.
            while buf.len() < batch_size {
                match rx.try_recv() {
                    Ok(req) => buf.push(req),
                    Err(_) => break,
                }
            }

            // If batch full, flush immediately; otherwise wait for more.
            if buf.len() < batch_size {
                let deadline = tokio::time::sleep(flush_interval);
                tokio::pin!(deadline);
                loop {
                    tokio::select! {
                        biased;
                        maybe = rx.recv() => match maybe {
                            Some(req) => {
                                buf.push(req);
                                if buf.len() >= batch_size {
                                    break;
                                }
                            }
                            None => break,
                        },
                        _ = &mut deadline => break,
                    }
                }
            }

            flush_batch(
                &backend,
                &stream,
                &group,
                &mut buf,
                error_observer.as_deref(),
            )
            .await;
        }
    });

    (tx, handle)
}

async fn flush_batch<B: StreamBackend>(
    backend: &Arc<B>,
    stream: &str,
    group: &str,
    buf: &mut Vec<AckRequest>,
    error_observer: Option<&dyn ErrorObserver>,
) {
    if buf.is_empty() {
        return;
    }

    // Move IDs out of each request to avoid the per-ack `String` clone — the
    // request is consumed in the same batch anyway, and we still have the
    // oneshot `done` channel to reply on.
    let mut ids: Vec<String> = Vec::with_capacity(buf.len());
    let mut dones = Vec::with_capacity(buf.len());
    for req in buf.drain(..) {
        ids.push(req.id);
        dones.push(req.done);
    }

    match backend.ack_many(stream, group, &ids).await {
        Ok(()) => {
            for done in dones {
                let _ = done.send(Ok(()));
            }
        }
        Err(err) => {
            if let Some(obs) = error_observer {
                obs.on_error(ErrorScope::AckFlush, &err);
            }
            let msg = err.to_string();
            for done in dones {
                let _ = done.send(Err(EventBusError::Connection(format!(
                    "batched xack on {stream}: {msg}"
                ))));
            }
        }
    }
}
