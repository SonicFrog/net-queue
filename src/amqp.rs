use futures::prelude::*;

use lapin::acker::Acker;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
    QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};

use snafu::prelude::*;

use super::*;

type Result<T, E = RabbitError> = std::result::Result<T, E>;

#[derive(snafu::Snafu, Debug)]
/// Possible errors when using RabbitMQ as queue backend
pub enum RabbitError {
    #[snafu(display("rabbitmq connection error: {}", source))]
    Connection { source: lapin::Error },
    #[snafu(display("channel creation error: {}", source))]
    Channel { source: lapin::Error },
    #[snafu(display("could not create channel {}: {}", name, source))]
    Queue { name: String, source: lapin::Error },
    #[snafu(display("no job available"))]
    NoJob,
}

#[derive(Clone)]
/// A job queue that uses a rabbit mq server both for all functionnality
pub struct AmqpJobQueue {
    queue_name: String,
    channel: lapin::Channel,
    out_queue: lapin::Consumer,
}

impl AmqpJobQueue {
    /// Create a new `JobQueue` backed by the given RabbitMQ connection
    pub async fn new(queue_name: impl Into<String>, channel: Channel) -> Result<Self> {
        let queue_name = queue_name.into();

        channel
            .queue_declare(
                &queue_name,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .context(QueueSnafu {
                name: queue_name.to_string(),
            })?;

        let out_queue = channel
            .basic_consume(
                &queue_name,
                "spooler",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .context(QueueSnafu {
                name: queue_name.to_string(),
            })?;

        Ok(Self {
            out_queue,
            channel,
            queue_name,
        })
    }
}

#[async_trait::async_trait]
impl JobQueue for AmqpJobQueue {
    type Err = RabbitError;

    type Handle = AckManager;

    /// Put a job in the job queue that will be forwarded to a client once there
    /// is a get job request
    async fn put_job<D>(&self, job: D) -> Result<(), Self::Err>
    where
        D: AsRef<[u8]> + Send,
    {
        let data = job.as_ref();

        self.channel
            .basic_publish(
                "",
                &self.queue_name,
                BasicPublishOptions::default(),
                data,
                BasicProperties::default(),
            )
            .await
            .map(|_| ())
            .context(ConnectionSnafu)
    }

    /// Get a job through a [`Future`] that will resolve to a [`JobData`] once
    /// a new job is available or immediately if there are jobs pending
    ///
    /// [`Future`]: self::Future
    /// [`JobData`]: self::JobData
    async fn get_job(&self) -> Result<JobResult<Self::Handle>> {
        let delivery = self
            .out_queue
            .clone()
            .next()
            .await
            .context(NoJobSnafu)?
            .context(ConnectionSnafu)?;

        Ok(JobResult::new(
            delivery.data,
            Self::Handle::new(delivery.acker),
        ))
    }
}

/// A factory for rabbit mq job queues
pub struct MakeRabbitJobQueue;

#[async_trait::async_trait]
impl MakeJobQueue for MakeRabbitJobQueue {
    type Err = RabbitError;

    type Queue = AmqpJobQueue;

    async fn make_job_queue(&self, name: &str, url: Url) -> Result<Self::Queue, Self::Err> {
        let connection = Connection::connect(url.as_str(), ConnectionProperties::default())
            .await
            .context(ConnectionSnafu)?;

        let channel = connection.create_channel().await.context(ConnectionSnafu)?;

        AmqpJobQueue::new(name, channel).await
    }
}

impl Eq for AmqpJobQueue {}

impl PartialEq for AmqpJobQueue {
    fn eq(&self, other: &Self) -> bool {
        self.queue_name == other.queue_name
    }
}

/// A rabbitmq acknowledgement manager
pub struct AckManager {
    acker: Acker,
}

impl AckManager {
    fn new(acker: Acker) -> Self {
        Self { acker }
    }
}

#[async_trait::async_trait]
impl JobHandle for AckManager {
    type Err = RabbitError;

    async fn ack_job(&self) -> Result<()> {
        self.acker
            .ack(BasicAckOptions::default())
            .await
            .context(ConnectionSnafu)
    }

    async fn nack_job(&self) -> Result<()> {
        self.acker
            .nack(BasicNackOptions::default())
            .await
            .context(ConnectionSnafu)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use lapin::{Channel, Connection, ConnectionProperties};

    fn make_job_data() -> Vec<u8> {
        let mut vec = Vec::new();

        vec.resize(100, 1);

        vec
    }

    /// Get a rabbit mq connection to some server for integration tests
    pub async fn rabbit_mq_connection() -> Channel {
        let addr = env!("AMQP_ADDR");

        let conn = Connection::connect(addr, ConnectionProperties::default())
            .await
            .expect("failed to connect to rabbitmq");

        conn.create_channel()
            .await
            .expect("could not create rabbitmq channel")
    }

    #[tokio::test]
    async fn job_queue() {
        use std::time::Duration;

        let channel = rabbit_mq_connection().await;
        let queue = AmqpJobQueue::new("test_job_queue", channel)
            .await
            .expect("could not create job queue");
        let posted = make_job_data();

        queue
            .put_job(posted.clone())
            .await
            .expect("failed to put job");

        let future = queue.get_job();

        let mut gotten = tokio::time::timeout(Duration::from_secs(1), future)
            .await
            .expect("timed out waiting for job")
            .expect("failed to get job");

        assert_eq!(&posted, &*gotten, "job differs");

        gotten.ack_job().await.expect("failed to ack job");
    }
}
