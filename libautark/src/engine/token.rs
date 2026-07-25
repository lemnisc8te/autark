use std::sync::Arc;
use tokio::sync::{Notify, mpsc, oneshot};

/// A type-safe handle representing an entity or state that will exist in the future.
/// This acts as a compile-time proof of dependency.
#[derive(Clone)]
pub struct Entity<T> {
    pub inner: T,
    // A shared primitive that allows downstream commands to wait for this asset to be ready
    pub ready_gate: Arc<Notify>,
}

impl<T> Entity<T> {
    /// Create a new asset handle and its associated synchronization gate
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            ready_gate: Arc::new(Notify::new()),
        }
    }

    /// Blocks until this specific asset has been processed by the engine
    pub async fn wait_until_ready(&self) {
        self.ready_gate.notified().await;
    }

    /// Signals to all dependents that this asset is ready
    pub fn mark_as_ready(&self) {
        self.ready_gate.notify_waiters();
    }
}
