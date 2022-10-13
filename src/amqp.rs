use futures::prelude::*;

use lapin::acker::Acker;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
    QueueDeclareOptions,
};
use lapin::types::{DeliveryTag, FieldTable};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};

use snafu::prelude::*;

use tracing::*;

use uuid::Uuid;

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
        let consumer_tag = Uuid::new_v4();

        trace!(queue_name = %queue_name, "declaring new queue");

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

        trace!(queue_name = %queue_name, "creating consumer");

        let out_queue = channel
            .basic_consume(
                &queue_name,
                consumer_tag.to_string().as_str(),
                BasicConsumeOptions {
                    no_ack: false,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .context(QueueSnafu {
                name: queue_name.to_string(),
            })?;

        trace!(queue_name = % queue_name, "done initializing");

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

        trace!(queue_name = % self.queue_name, "posting data");

        self.channel
            .basic_publish(
                "",
                &self.queue_name,
                BasicPublishOptions::default(),
                data,
                BasicProperties::default().with_delivery_mode(1),
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
        trace!("attempting get job on queue {}", self.queue_name);

        let delivery = self
            .out_queue
            .clone()
            .next()
            .await
            .context(NoJobSnafu)?
            .context(ConnectionSnafu)?;

        trace!(tag = delivery.delivery_tag, "new job fetched from rabbitmq");

        Ok(JobResult::new(
            delivery.data,
            Self::Handle::new(delivery.acker, delivery.delivery_tag),
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
        trace!(url = %url, "connecting to rabbitmq at {}", url);

        let connection = Connection::connect(url.as_str(), ConnectionProperties::default())
            .await
            .context(ConnectionSnafu)?;

        let channel = connection.create_channel().await.context(ConnectionSnafu)?;

        trace!(url = %url, "connection and channel created");

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
    tag: DeliveryTag,
    acker: Acker,
}

impl AckManager {
    fn new(acker: Acker, tag: DeliveryTag) -> Self {
        Self { acker, tag }
    }
}

#[async_trait::async_trait]
impl JobHandle for AckManager {
    type Err = RabbitError;

    async fn ack_job(&self) -> Result<()> {
        trace!(tag = self.tag, "acking job");

        self.acker
            .ack(BasicAckOptions::default())
            .await
            .context(ConnectionSnafu)
    }

    async fn nack_job(&self) -> Result<()> {
        trace!(tag = self.tag, "n-acking job");

        self.acker
            .nack(BasicNackOptions {
                requeue: true,
                ..Default::default()
            })
            .await
            .context(ConnectionSnafu)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::time::Duration;

    use lapin::{Channel, Connection, ConnectionProperties};

    fn make_job_data() -> Vec<u8> {
        String::from("test-message").into_bytes()
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

    /// Create a queue with a given name
    pub async fn create_queue(name: &str) -> impl JobQueue {
        let channel = rabbit_mq_connection().await;

        AmqpJobQueue::new(name, channel)
            .await
            .expect("could not create job queue")
    }

    #[test_log::test(tokio::test)]
    async fn job_queue() {
        let posted = make_job_data();
        let queue = create_queue("test-job-queu").await;

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

    #[test_log::test(tokio::test)]
    async fn nack_request() {
        static NAME: &str = "nack_job_queue";

        let queue = create_queue(NAME).await;

        let posted = make_job_data();

        queue.put_job(&posted).await.expect("failed to put job");

        let mut handle = queue.get_job().await.expect("failed to get job");

        handle.nack_job().await.expect("failed to n-ack job");

        let mut gotten = queue.get_job().await.expect("failed to get job");

        assert_eq!(&posted, &*gotten, "different job after nack");

        gotten
            .ack_job()
            .await
            .expect("could not ack job after nack");
    }
}
