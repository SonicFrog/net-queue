use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::prelude::*;

use pin_project_lite::pin_project;

use postage::dispatch;
use postage::prelude::Stream as _;

use tokio::sync::Mutex;
use tokio::sync::RwLock;

use tracing::*;

use url::Url;

use super::{InputQueue, JobHandle, JobResult, MakeQueue, OutputQueue};

#[derive(snafu::Snafu, Debug)]
pub struct LocalError;

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
pub struct MakeLocalQueue {
    queues: Arc<RwLock<HashMap<String, (LocalOutputQueue, LocalInputQueue)>>>,
}

impl MakeLocalQueue {
    async fn get_or_insert(&self, name: impl Into<String>) -> (LocalOutputQueue, LocalInputQueue) {
        self.queues
            .write()
            .await
            .entry(name.into())
            .or_insert_with(|| {
                let (tx, rx) = dispatch::channel(64);

                let output = LocalOutputQueue {
                    queue: Arc::new(Mutex::new(tx)),
                };

                let input = LocalInputQueue { queue: rx };

                (output, input)
            })
            .clone()
    }
}

#[async_trait::async_trait]
impl MakeQueue for MakeLocalQueue {
    type InputQueue = LocalInputQueue;

    type OutputQueue = LocalOutputQueue;

    type Err = LocalError;

    async fn input_queue(&self, name: &str, _: Url) -> Result<Self::InputQueue, Self::Err> {
        debug!("creating new local test input queue with name {}", name);

        let (_, input) = self.get_or_insert(name).await;

        Ok(input)
    }

    async fn output_queue(&self, name: &str, _: Url) -> Result<Self::OutputQueue, Self::Err> {
        debug!("creating new local test output queue with name {}", name);

        let (output, _) = self.get_or_insert(name).await;

        Ok(output)
    }
}

/// The receiving end for local queues
#[derive(Clone)]
pub struct LocalInputQueue {
    queue: dispatch::Receiver<Vec<u8>>,
}

#[async_trait::async_trait]
impl InputQueue for LocalInputQueue {
    type Stream = LocalConsumer;

    type Handle = ();

    type Err = LocalError;

    async fn get(&self) -> Result<JobResult<Self::Handle>, Self::Err> {
        self.queue
            .clone()
            .recv()
            .await
            .map(|x| JobResult::new(x, ()))
            .ok_or(LocalError)
    }

    async fn into_stream(self) -> Self::Stream {
        let consumer = self.queue;

        LocalConsumer { consumer }
    }
}

/// The sending end for local queues
#[derive(Clone)]
pub struct LocalOutputQueue {
    queue: Arc<Mutex<dispatch::Sender<Vec<u8>>>>,
}

#[async_trait::async_trait]
impl OutputQueue for LocalOutputQueue {
    type Err = LocalError;

    async fn put<D>(&self, data: D) -> Result<(), Self::Err>
    where
        D: AsRef<[u8]> + Send,
    {
        let data = data.as_ref().to_vec();

        self.queue
            .lock()
            .await
            .send(data)
            .await
            .map_err(|_| LocalError)
    }

    async fn close(&self) -> Result<(), Self::Err> {
        self.queue
            .lock()
            .await
            .close()
            .await
            .map_err(|_| LocalError)
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

impl Stream for LocalConsumer {
    type Item = Result<JobResult<()>, LocalError>;

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

    async fn make_pair(name: &str) -> (impl OutputQueue, impl InputQueue) {
        let maker = MakeLocalQueue::default();
        let input = maker
            .input_queue(name, "dummy://".parse().unwrap())
            .await
            .unwrap();

        let output = maker
            .output_queue(name, "dummy://".parse().unwrap())
            .await
            .unwrap();

        (output, input)
    }

    #[test(tokio::test)]
    async fn create() {
        let _queue = MakeLocalQueue::default()
            .input_queue("dummy", "file://test".parse().unwrap())
            .await
            .expect("failed to open queue");
    }

    #[test(tokio::test)]
    async fn job_queue() {
        let (output, input) = make_pair("job_queue").await;

        let job = Vec::new();

        output.put(&job).await.expect("failed to put job");

        let actual = input.get().await.expect("failed to get job");

        assert_eq!(&job, &*actual, "wrong job returned");
    }

    #[test(tokio::test)]
    async fn consumer() {
        let (output, input) = make_pair("consumer").await;

        let mut consumer = input.into_stream().await;
        let expected = Vec::new();

        output.put(&expected).await.expect("put_job failed");

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
            let (output, input) = make_pair("close").await;

            output.close().await.expect("failed to close queue");

            input.into_stream().await
        };

        let item = consumer.next().await;

        assert!(item.is_none(), "got item from closed queue");
    }
}
