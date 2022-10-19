use std::error::Error;
use std::fmt::Debug;
use std::future::Future;
use std::ops::Deref;

use futures::Stream;

use url::Url;

#[cfg(feature = "rabbitmq")]
mod amqp;
#[cfg(feature = "rabbitmq")]
pub use amqp::*;

#[cfg(feature = "local")]
mod local;
#[cfg(feature = "local")]
pub use local::*;

/// An abstract queue that handles reliable delivery through job acknowledgment and
/// optionally persistence
#[async_trait::async_trait]
pub trait JobQueue: Send + Sync {
    /// The type of error that can occur when getting/putting a job
    type Err: Debug;

    /// The type of handle returned by this JobQueue
    type Handle: JobHandle<Err = Self::Err>;

    /// The type of `Consumer` `Stream` this `JobQueue` produces
    type Consumer: Consumer<Err = Self::Err>;

    /// Put a job in the queue
    async fn put_job<D>(&self, job: D) -> Result<(), Self::Err>
    where
        D: AsRef<[u8]> + Send;

    /// Get a job from this queue
    async fn get_job(&self) -> Result<JobResult<Self::Handle>, Self::Err>;

    /// Get a [`Consumer`] for this [`JobQueue`] to allow [`Stream`] usage
    ///
    /// [`Consumer`]: self::Consumer
    /// [`JobQueue`]: self::JobQueue
    /// [`Stream`]: self::Stream
    async fn consumer(&self) -> Self::Consumer;
}

#[async_trait::async_trait]
/// The queue factory trait that takes care of creating queues
pub trait MakeJobQueue: Send + Sync {
    /// The type of job queue returned by this factory
    type Queue: JobQueue<Err = Self::Err>;

    /// The type of error that can occur when creating a job queue
    type Err: Error + Send + Sync;

    /// Create a new job queue using this factory
    async fn make_job_queue(&self, name: &str, url: Url) -> Result<Self::Queue, Self::Err>;
}

/// A trait to manager job timeouts and (n)acks
#[async_trait::async_trait]
pub trait JobHandle: Send + Sync + 'static {
    /// Type of errors that can occur
    type Err: Debug;

    /// Ack the job referred by this `JobHandle`
    async fn ack_job(&self) -> Result<(), Self::Err>;

    /// N-ack the job referred by this [`JobHandle`], this must trigger a requeue if the
    /// amount of tries has not exceeded the maximum amount
    async fn nack_job(&self) -> Result<(), Self::Err>;
}

/// A [`Consumer`] for a [`JobQueue`]
///
/// [`Consumer`]: self::Consumer
/// [`JobQueue`]: self::JobQueue
pub trait Consumer: Stream<Item = Result<JobResult<Self::Handle>, Self::Err>> {
    /// Type of error that can occur while fetching jobs
    type Err: Debug;
    /// Type of `JobHandle` used to acknowledge jobs in this `Consumer`
    type Handle: JobHandle<Err = Self::Err>;
}

/// A struct that holds both the job data and a JobHandle used to acknowledge jobs completion
pub struct JobResult<H>
where
    H: JobHandle + 'static,
{
    handle: Option<H>,
    job: Vec<u8>,
}

impl<H> JobResult<H>
where
    H: JobHandle,
{
    /// Create a new JobResult from a job and a JobHandle to acknowledge job completion
    pub fn new(job: Vec<u8>, handle: H) -> Self {
        Self {
            handle: handle.into(),
            job,
        }
    }

    async fn run_with_handle<F>(&mut self, f: impl FnOnce(H) -> F) -> Result<(), H::Err>
    where
        F: Future<Output = Result<(), H::Err>>,
    {
        if let Some(handle) = self.handle.take() {
            (f)(handle).await
        } else {
            Ok(())
        }
    }

    /// Get a reference to the job contained in this `JobResult`
    pub fn job(&self) -> &Vec<u8> {
        &self.job
    }

    /// Split this result into its handle if it has not been already used and the actual job content
    pub fn split(self) -> (Option<H>, Vec<u8>) {
        (self.handle, self.job)
    }

    /// Nack the job associated with this `JobResult`
    pub async fn nack_job(&mut self) -> Result<(), H::Err> {
        self.run_with_handle(|h| async move { h.nack_job().await })
            .await
    }

    /// Ack the job associated with this `JobResult`
    pub async fn ack_job(&mut self) -> Result<(), H::Err> {
        self.run_with_handle(|h| async move { h.ack_job().await })
            .await
    }
}

impl<H> PartialEq for JobResult<H>
where
    H: JobHandle,
{
    fn eq(&self, other: &Self) -> bool {
        self.job == other.job
    }
}

impl<H> Deref for JobResult<H>
where
    H: JobHandle + Send + Sync + 'static,
{
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.job
    }
}
