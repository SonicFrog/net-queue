use std::pin::Pin;
use std::task::{Context, Poll};

use futures::prelude::*;

use lapin::acker::Acker;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
    QueueDeclareOptions, QueueDeleteOptions,
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
    #[snafu(display("failed to get job from queue: {}", source))]
    Channel { source: lapin::Error },
    #[snafu(display("failed to create queue  {}: {}", name, source))]
    Queue { name: String, source: lapin::Error },
    #[snafu(display("no job available"))]
    NoJob,
}

/// The receiving end of a rabbitmq message queue
pub struct RabbitInputQueue {
    queue_name: String,
    queue: lapin::Consumer,
}

#[async_trait::async_trait]
impl InputQueue for RabbitInputQueue {
    type Err = RabbitError;

    type Handle = RabbitHandle;

    type Stream = RabbitConsumer;

    /// Get a job through a [`Future`] that will resolve to a [`JobData`] once
    /// a new job is available or immediately if there are jobs pending
    ///
    /// [`Future`]: self::Future
    /// [`JobData`]: self::JobData
    async fn get(&self) -> Result<JobResult<Self::Handle>> {
        trace!(queue_name = % self.queue_name, "attempting get job");

        let delivery = self
            .queue
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

    async fn into_stream(self) -> Self::Stream {
        self.queue.into()
    }
}

#[derive(Clone)]
pub struct RabbitOutputQueue {
    queue_name: String,
    channel: Channel,
}

#[async_trait::async_trait]
impl OutputQueue for RabbitOutputQueue {
    type Err = RabbitError;

    /// Put a job in the job queue that will be forwarded to a client once there
    /// is a get job request
    async fn put<D>(&self, job: D) -> Result<(), Self::Err>
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

    async fn close(&self) -> Result<(), Self::Err> {
        trace!(queue_name = % self.queue_name, "closing queue");

        self.channel
            .queue_delete(
                &self.queue_name,
                QueueDeleteOptions {
                    if_empty: true,
                    ..Default::default()
                },
            )
            .await
            .map(|_| ())
            .context(ChannelSnafu)
    }
}

/// A factory for rabbit mq job queues
#[derive(Clone, Default)]
pub struct MakeRabbitQueue;

impl MakeRabbitQueue {
    async fn connect(&self, url: Url) -> Result<lapin::Channel, RabbitError> {
        trace!(url = %url, "connecting to rabbitmq at {}", url);

        let connection = Connection::connect(url.as_str(), ConnectionProperties::default())
            .await
            .context(ConnectionSnafu)?;

        let channel = connection.create_channel().await.context(ConnectionSnafu)?;

        trace!(url = %url, "connection and channel created");

        Ok(channel)
    }
}

#[async_trait::async_trait]
impl MakeQueue for MakeRabbitQueue {
    type Err = RabbitError;

    type InputQueue = RabbitInputQueue;

    type OutputQueue = RabbitOutputQueue;

    async fn input_queue(&self, name: &str, url: Url) -> Result<Self::InputQueue, Self::Err> {
        let consumer_tag = Uuid::new_v4().to_string();
        let channel = self.connect(url).await?;

        trace!(
            queue_name = name,
            tag = consumer_tag,
            "opening queue for reading"
        );

        let queue = channel
            .basic_consume(
                name,
                consumer_tag.as_str(),
                BasicConsumeOptions {
                    no_ack: false,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .context(QueueSnafu {
                name: name.to_string(),
            })?;

        trace!(
            queue_name = name,
            tag = consumer_tag,
            "opened queue for reading"
        );

        Ok(RabbitInputQueue {
            queue_name: name.into(),
            queue,
        })
    }

    async fn output_queue(&self, name: &str, url: Url) -> Result<Self::OutputQueue, Self::Err> {
        trace!(queue_name = name, "declaring new queue for sending");

        let channel = self.connect(url).await?;

        channel
            .queue_declare(
                name,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .context(QueueSnafu {
                name: name.to_string(),
            })?;

        trace!(
            queue_name = name,
            "successfully declared new queue for sending"
        );

        Ok(Self::OutputQueue {
            queue_name: name.into(),
            channel,
        })
    }
}

impl PartialEq for RabbitOutputQueue {
    fn eq(&self, other: &Self) -> bool {
        self.queue_name == other.queue_name
    }
}

/// A rabbitmq acknowledgement manager
pub struct RabbitHandle {
    tag: DeliveryTag,
    acker: Acker,
}

impl RabbitHandle {
    fn new(acker: Acker, tag: DeliveryTag) -> Self {
        Self { acker, tag }
    }
}

#[async_trait::async_trait]
impl JobHandle for RabbitHandle {
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

pin_project_lite::pin_project! {
    /// A [`Consumer`] for an [`AmqpJobQueue`]
    ///
    /// [`Consumer`]: crate::Consumer
    /// [`AmqpJobQueue`]: self::AmqpJobQueue
    pub struct RabbitConsumer {
        #[pin]
        consumer: lapin::Consumer,
    }
}

impl From<lapin::Consumer> for RabbitConsumer {
    fn from(consumer: lapin::Consumer) -> Self {
        Self { consumer }
    }
}

impl Stream for RabbitConsumer {
    type Item = Result<JobResult<RabbitHandle>, RabbitError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project()
            .consumer
            .poll_next(cx)
            .map_ok(|value| {
                let handle = RabbitHandle::new(value.acker, value.delivery_tag);

                JobResult::new(value.data, handle)
            })
            .map_err(|e| RabbitError::Channel { source: e })
    }
}

#[cfg(test)]
mod test {
    use test_log::test;

    use super::*;

    use std::time::Duration;

    fn make_job_data() -> Vec<u8> {
        String::from("test-message").into_bytes()
    }

    async fn create_pair(name: &str) -> (impl InputQueue, impl OutputQueue) {
        let addr = option_env!("AMQP_ADDR").unwrap_or("amqp://localhost:5672");

        let maker = MakeRabbitQueue::default();

        let output = maker
            .output_queue(name, addr.parse().unwrap())
            .await
            .expect("could not create output queue");

        let input = maker
            .input_queue(name, addr.parse().unwrap())
            .await
            .expect("could not create input queue");

        (input, output)
    }

    #[test(tokio::test)]
    async fn job_queue() {
        let posted = make_job_data();
        let (input, output) = create_pair("test-job-queu").await;

        output.put(posted.clone()).await.expect("failed to put job");

        let future = input.get();

        let mut gotten = tokio::time::timeout(Duration::from_secs(1), future)
            .await
            .expect("timed out waiting for job")
            .expect("failed to get job");

        assert_eq!(&posted, &*gotten, "job differs");

        gotten.ack_job().await.expect("failed to ack job");
    }

    #[test(tokio::test)]
    async fn nack_request() {
        static NAME: &str = "nack_job_queue";

        let (input, output) = create_pair(NAME).await;

        let posted = make_job_data();

        output.put(&posted).await.expect("failed to put job");

        let mut handle = input.get().await.expect("failed to get job");

        handle.nack_job().await.expect("failed to n-ack job");

        let mut gotten = input.get().await.expect("failed to get job");

        assert_eq!(&posted, &*gotten, "different job after nack");

        gotten
            .ack_job()
            .await
            .expect("could not ack job after nack");
    }

    #[test(tokio::test)]
    async fn consumer() {
        static NAME: &str = "consumer_queue";

        let (input, output) = create_pair(NAME).await;

        let posted = make_job_data();

        output.put(&posted).await.expect("failed to put job");

        let consumer = input.into_stream().await;

        futures::pin_mut!(consumer);

        let mut gotten = consumer
            .next()
            .await
            .expect("no job to get")
            .expect("unable to get job");

        assert_eq!(&posted, &*gotten, "message differs");

        gotten.ack_job().await.expect("failed to ack job");
    }

    #[test(tokio::test)]
    async fn close() {
        static NAME: &str = "closed_queue";

        let (input, output) = create_pair(NAME).await;
        let consumer = input.into_stream().await;

        futures::pin_mut!(consumer);

        output.close().await.expect("failed to close queue");

        let item = consumer.next().await;

        assert!(item.is_none(), "received item from closed queue");
    }
}
