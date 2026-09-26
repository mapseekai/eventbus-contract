use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use tokio::sync::{oneshot, watch, Mutex};
use tokio::task::{AbortHandle, JoinHandle};

use super::backend::StreamBackend;
use super::observer::{ErrorObserver, ErrorScope};
use crate::{EventBusError, Subscription};

/// Capture before spawning so abort-before-first-poll also cleans up. The
/// completion notification lets close/abort await asynchronous backend cleanup.
pub(super) struct ConsumerCleanup<B: StreamBackend> {
    backend: Arc<B>,
    stream: String,
    group: String,
    consumer: String,
    completed: Option<oneshot::Sender<()>>,
}

impl<B: StreamBackend> ConsumerCleanup<B> {
    pub(super) fn new(
        backend: Arc<B>,
        stream: String,
        group: String,
        consumer: String,
    ) -> (Self, oneshot::Receiver<()>) {
        let (completed, cleaned) = oneshot::channel();
        (
            Self {
                backend,
                stream,
                group,
                consumer,
                completed: Some(completed),
            },
            cleaned,
        )
    }
}

impl<B: StreamBackend> Drop for ConsumerCleanup<B> {
    fn drop(&mut self) {
        let Some(completed) = self.completed.take() else {
            return;
        };
        let backend = Arc::clone(&self.backend);
        let stream = std::mem::take(&mut self.stream);
        let group = std::mem::take(&mut self.group);
        let consumer = std::mem::take(&mut self.consumer);
        // Async cleanup requires a live runtime. Explicit close/abort should
        // finish before shutting down that runtime.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                backend.forget_consumer(&stream, &group, &consumer).await;
                let _ = completed.send(());
            });
        }
    }
}

struct SubscriptionTask {
    handle: JoinHandle<Result<(), EventBusError>>,
    cleaned: oneshot::Receiver<()>,
    result: Option<Result<(), EventBusError>>,
}

#[must_use = "subscription is idle until bound; call `.close().await` for graceful shutdown"]
pub struct StreamSubscription {
    name: String,
    closed: AtomicBool,
    close_tx: watch::Sender<bool>,
    task: Mutex<Option<SubscriptionTask>>,
    abort_handle: AbortHandle,
    observer: Option<Arc<dyn ErrorObserver>>,
}

impl StreamSubscription {
    pub(crate) fn new(
        name: String,
        close_tx: watch::Sender<bool>,
        task: JoinHandle<Result<(), EventBusError>>,
        cleaned: oneshot::Receiver<()>,
        observer: Option<Arc<dyn ErrorObserver>>,
    ) -> Self {
        let abort_handle = task.abort_handle();
        Self {
            name,
            closed: AtomicBool::new(false),
            close_tx,
            task: Mutex::new(Some(SubscriptionTask {
                handle: task,
                cleaned,
                result: None,
            })),
            abort_handle,
            observer,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Whether the background consumer task is still running, including
    /// graceful drain after a close request.
    pub fn is_running(&self) -> bool {
        !self.abort_handle.is_finished()
    }

    fn begin_shutdown(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            let _ = self.close_tx.send(true);
        }
    }

    /// Request graceful shutdown and wait for delivery tasks and cleanup.
    /// Cancelling this future leaves the task available to a later close/abort.
    pub async fn close(&self) -> Result<(), EventBusError> {
        self.begin_shutdown();
        self.wait_for_shutdown().await
    }

    /// Abort the background task without waiting for graceful drain. Returns
    /// `Ok(())` if the abort was acknowledged or the task was already done;
    /// surfaces the task's last error if it had one.
    pub async fn abort(&self) -> Result<(), EventBusError> {
        self.begin_shutdown();
        // Interrupt before taking the join lock: another caller may be
        // waiting there for a handler that never completes.
        self.abort_handle.abort();
        self.wait_for_shutdown().await
    }

    async fn wait_for_shutdown(&self) -> Result<(), EventBusError> {
        // Serialize joins, but keep ownership here when a waiter is cancelled.
        let mut guard = self.task.lock().await;
        let Some(task) = guard.as_mut() else {
            return Ok(());
        };
        if task.result.is_none() {
            task.result = Some(match (&mut task.handle).await {
                Ok(result) => result,
                Err(err) if err.is_cancelled() => Ok(()),
                Err(err) => Err(EventBusError::source("subscription task failed", err)),
            });
        }
        let cleanup = (&mut task.cleaned)
            .await
            .map_err(|_| EventBusError::Internal("consumer cleanup did not complete".into()));
        let result = task.result.take().expect("joined subscription task");
        *guard = None;
        result.and(cleanup)
    }
}

impl Subscription for StreamSubscription {
    fn name(&self) -> &str {
        StreamSubscription::name(self)
    }

    fn close(self: std::sync::Arc<Self>) -> crate::BoxFuture<'static, Result<(), EventBusError>> {
        Box::pin(async move {
            // Deref the Arc to call the inherent &self method, which already
            // handles the close handshake (begin_shutdown -> JoinHandle::await).
            // The Arc keeps the subscription alive until close completes.
            (*self).close().await
        })
    }
}

/// Dropping a [`StreamSubscription`] is fire-and-forget: it signals the
/// background task to exit but does not await it, and **delivery errors
/// raised after the close signal are silently discarded**. To surface those
/// errors, call [`StreamSubscription::close`] explicitly and await the
/// returned `Result`.
///
/// When the subscription is dropped without `close()` having been called,
/// the configured [`ErrorObserver`] (if any) is notified via
/// [`ErrorScope::Drop`] so leaked subscriptions are observable.
impl Drop for StreamSubscription {
    fn drop(&mut self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }

        let _ = self.close_tx.send(true);
        if let Some(obs) = self.observer.as_ref() {
            obs.on_error(
                ErrorScope::Drop,
                &EventBusError::Internal(format!(
                    "subscription `{}` dropped without close()",
                    self.name
                )),
            );
        }
        self.task.get_mut().take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn close_cancelled_during_cleanup_does_not_repoll_the_join_handle() {
        let (close_tx, _close_rx) = watch::channel(false);
        let (cleaned_tx, cleaned) = oneshot::channel();
        let sub = StreamSubscription::new(
            "cleanup".into(),
            close_tx,
            tokio::spawn(async { Ok(()) }),
            cleaned,
            None,
        );
        assert!(tokio::time::timeout(Duration::from_millis(20), sub.close())
            .await
            .is_err());
        cleaned_tx.send(()).unwrap();
        sub.close().await.unwrap();
        sub.abort().await.unwrap();
    }
}
