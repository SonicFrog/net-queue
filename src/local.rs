use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::prelude::*;

use pin_project_lite::pin_project;

use postage::dispatch;

use tokio::sync::RwLock;

use tracing::*;

use url::Url;

use super::{Consumer, JobHandle, JobQueue, JobResult, MakeJobQueue};

#[derive(snafu::Snafu, Debug)]
pub struct LocalError;

/// A local job queue used for testing
#[derive(Clone)]
pub struct LocalJobQueue {
    in_queue: dispatch::Receiver<Vec<u8>>,
    out_queue: Arc<RwLock<dispatch::Sender<Vec<u8>>>>,
}

impl Default for LocalJobQueue {
    fn default() -> LocalJobQueue {
        let (out_queue, in_queue) = dispatch::channel(64);

        Self {
            in_queue,
            out_queue: Arc::new(RwLock::new(out_queue)),
        }
    }
}

#[async_trait::async_trait]
impl JobQueue for LocalJobQueue {
    type Err = LocalError;

    type Handle = ();

    type Consumer = LocalConsumer;

    async fn put_job<D>(&self, data: D) -> Result<(), Self::Err>
    where
        D: AsRef<[u8]> + Send,
    {
        let data = data.as_ref().to_vec();

        self.out_queue
            .read()
            .await
            .clone()
            .send(data)
            .await
            .map_err(|_| LocalError)?;

        debug!("registered new job");

        Ok(())
    }

    async fn get_job(&self) -> Result<JobResult<Self::Handle>, Self::Err> {
        loop {
            if let Some(job) = self.in_queue.clone().next().await {
                debug!("removed job from queue");

                break Ok(JobResult::new(job, ()));
            }

            tokio::task::yield_now().await;
        }
    }

    async fn consumer(&self) -> Self::Consumer {
        LocalConsumer::new(self.in_queue.clone())
    }

    async fn close(&self) -> Result<(), Self::Err> {
        trace!("closing queue");

        self.out_queue
            .write()
            .await
            .close()
            .await
            .map_err(|_| LocalError)
    }
}

#[async_trait::async_trait]
impl JobHandle for () {
    type Err = LocalError;

    async fn ack_job(&self) -> Result<(), Self::Err> {
        Ok(())
    }

    async fn nack_job(&self) -> Result<(), Self::Err> {
        Ok(())
    }
}

/// Local queue maker
#[derive(Clone, Default)]
pub struct MakeLocalQueue;

#[async_trait::async_trait]
impl MakeJobQueue for MakeLocalQueue {
    type Queue = LocalJobQueue;

    type Err = LocalError;

    async fn make_job_queue(&self, _: &str, _: Url) -> Result<Self::Queue, Self::Err> {
        debug!("creating new local test queue");

        Ok(Self::Queue::default())
    }
}

pin_project! {
    /// A queue [`Consumer`] for [`LocalJobQueue`]
    ///
    /// [`Consumer`]: crate::Consumer
    /// [`LocalJobQueue`]: LocalJobQueue
    pub struct LocalConsumer {
        #[pin]
        consumer: dispatch::Receiver<Vec<u8>>,
    }
}

impl LocalConsumer {
    pub(self) fn new(consumer: dispatch::Receiver<Vec<u8>>) -> Self {
        LocalConsumer { consumer }
    }
}

impl Consumer for LocalConsumer {
    type Handle = ();

    type Err = LocalError;
}

impl Stream for LocalConsumer {
    type Item = Result<JobResult<<Self as Consumer>::Handle>, <Self as Consumer>::Err>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project()
            .consumer
            .poll_next(cx)
            .map(|x| x.map(Ok::<_, LocalError>))
            .map_ok(|data| JobResult::new(data, ()))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use test_log::test;

    #[test(tokio::test)]
    async fn create() {
        let _queue = MakeLocalQueue
            .make_job_queue("dummy", "file://test".parse().unwrap())
            .await
            .expect("failed to open queue");
    }

    #[test(tokio::test)]
    async fn job_queue() {
        let queue = LocalJobQueue::default();
        let job = Vec::new();

        queue.put_job(&job).await.expect("failed to put job");

        let actual = queue.get_job().await.expect("failed to get job");

        assert_eq!(&job, &*actual, "wrong job returned");
    }

    #[test(tokio::test)]
    async fn consumer() {
        let queue = LocalJobQueue::default();
        let mut consumer = queue.consumer().await;
        let expected = Vec::new();

        queue.put_job(&expected).await.expect("put_job failed");

        let actual = consumer
            .next()
            .await
            .expect("no job")
            .expect("failed to get job");

        assert_eq!(&expected, &*actual);
    }

    #[test(tokio::test)]
    async fn close() {
        let mut consumer = {
            let queue = LocalJobQueue::default();

            let consumer = queue.consumer().await;

            queue.close().await.expect("failed to close queue");

            consumer
        };

        let item = consumer.next().await;

        assert!(item.is_none(), "got item from closed queue");
    }
}
