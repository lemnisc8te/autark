use std::sync::Arc;

use futures::future::BoxFuture;
use tokio::sync::{mpsc, oneshot};

type Task<O: Send> = Box<dyn FnOnce() -> BoxFuture<'static, O> + Send + 'static>;

pub struct WorkerPool<O: Send> {
    tx: mpsc::Sender<(Task<O>, oneshot::Sender<O>)>,
}
impl<O: Send + 'static> WorkerPool<O> {
    /// Initializes the pool with a fixed number of workers
    pub fn new(num_workers: usize) -> Self {
        let (tx, rx) = mpsc::channel::<(Task<O>, oneshot::Sender<O>)>(100);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));

        for _ in 0..num_workers {
            let rx = Arc::clone(&rx);
            tokio::task::spawn_blocking(async move || {
                while let Ok(job) = {
                    let mut lock = rx.lock().await;
                    lock.recv().await.ok_or(())
                } {
                    let (task, responder) = job;
                    // Execute the arbitrary task
                    let result = task().await;
                    // Send result back to the requester
                    let _ = responder.send(result);
                }
            });
        }

        Self { tx }
    }

    pub async fn execute<F, Fut>(&self, task: F) -> O
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = O> + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let boxed_task = Box::new(move || Box::pin(task()) as BoxFuture<'static, O>);
        let _ = self.tx.send((boxed_task, tx)).await;
        rx.await.expect("Task failed")
    }
}
