use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync::RwLock;

use tracing::*;

use url::Url;

use super::{JobHandle, JobQueue, JobResult, MakeJobQueue};

#[derive(snafu::Snafu, Debug)]
pub struct LocalError;

/// A local job queue used for testing
#[derive(Clone, Default)]
pub struct LocalJobQueue {
    queue: Arc<RwLock<VecDeque<Vec<u8>>>>,
}

#[async_trait::async_trait]
impl JobQueue for LocalJobQueue {
    type Err = LocalError;

    type Handle = ();

    async fn put_job<D>(&self, data: D) -> Result<(), Self::Err>
    where
        D: AsRef<[u8]> + Send,
    {
        self.queue.write().await.push_back(data.as_ref().to_vec());

        debug!("registered new job");

        Ok(())
    }

    async fn get_job(&self) -> Result<JobResult<Self::Handle>, Self::Err> {
        loop {
            if let Some(job) = self.queue.write().await.pop_front() {
                debug!("removed job from queue");

                break Ok(JobResult::new(job, ()));
            }

            tokio::task::yield_now().await;
        }
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
pub struct MakeLocalQueue;

#[async_trait::async_trait]
impl MakeJobQueue for MakeLocalQueue {
    type Queue = LocalJobQueue;

    type Err = LocalError;

    async fn make_job_queue(&self, _: &str, _: Url) -> Result<Self::Queue, Self::Err> {
        debug!("creating new local test queue");

        Ok(Default::default())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn job_queue() {
        let queue = LocalJobQueue::default();
        let job = Vec::new();

        queue.put_job(&job).await.expect("failed to put job");

        let actual = queue.get_job().await.expect("failed to get job");

        assert_eq!(&job, &*actual, "wrong job returned");
    }
}
