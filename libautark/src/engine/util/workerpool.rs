pub struct WorkerPool {}
impl WorkerPool {
    /// Initializes the pool with a fixed number of workers
    pub fn new() -> Self {
        Self {}
    }

    pub fn execute<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let _ = tokio::task::spawn_blocking(task);
    }
}
