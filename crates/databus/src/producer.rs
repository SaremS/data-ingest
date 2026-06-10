use std::future::Future;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use arrayvec::ArrayString;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{
    databus::DataBus, message::Message, runnable::Runnable, send_receive_handles::SendHandle,
};

pub enum Schedule {
    Once,
    Interval(u64),
}

impl Schedule {
    pub fn next_run(&self) -> Option<std::time::Duration> {
        match self {
            Schedule::Once => None,
            Schedule::Interval(millis) => Some(std::time::Duration::from_millis(*millis)),
        }
    }
}

#[derive(Error, Debug)]
pub enum ProducerError {
    #[error("Failed to publish message to topic: {0}")]
    PublishError(String),

    #[error("Error Creating Producer: {0}")]
    CreationError(String),
}

pub trait Producer<T: Clone + Send + Sync, S: Clone + Send + Sync, const STR_CAP: usize>:
    Send + Sync
{
    fn produce(
        &self,
        topic: ArrayString<STR_CAP>,
        old_state: Arc<Mutex<S>>,
    ) -> impl Future<Output = T> + Send;
}

pub struct ScheduledProducer<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    const STR_CAP: usize,
    V: Producer<T, S, STR_CAP>,
> {
    producer: V,
    producer_state: Arc<Mutex<S>>,
    topic: ArrayString<STR_CAP>,
    sender: SendHandle<T>,

    schedule: Schedule,
    _marker: PhantomData<S>,
}

impl<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    const STR_CAP: usize,
    V: Producer<T, S, STR_CAP>,
> ScheduledProducer<T, S, STR_CAP, V>
{
    pub fn new(
        producer: V,
        producer_state: S,
        bus: Arc<DataBus<T, STR_CAP>>,
        topic: ArrayString<STR_CAP>,
        schedule: Schedule,
    ) -> Result<Self, ProducerError> {
        if topic.is_empty() {
            return Err(ProducerError::CreationError("Topic cannot be empty".into()));
        }

        let sender = bus.get_sender(&topic);
        if sender.is_err() {
            return Err(ProducerError::CreationError(format!(
                "Failed to get sender for topic: {}",
                topic
            )));
        }

        Ok(Self {
            producer,
            producer_state: Arc::new(Mutex::new(producer_state)),
            topic,
            sender: sender.unwrap(),

            schedule,
            _marker: PhantomData,
        })
    }

    pub fn producer_state(&self) -> S {
        self.producer_state.lock().unwrap().clone()
    }
}

impl<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    const STR_CAP: usize,
    V: Producer<T, S, STR_CAP>,
> Runnable for ScheduledProducer<T, S, STR_CAP, V>
{
    async fn run(&mut self) {
        loop {
            let message = self
                .producer
                .produce(self.topic, self.producer_state.clone())
                .await;

            if self.sender.send(message).await.is_err() {
                break;
            }

            let sleep_duration = match self.schedule.next_run() {
                Some(duration) => duration,
                None => break,
            };

            tokio::time::sleep(sleep_duration).await;
        }
    }
}

pub trait RunnableProducer {
    fn run_producer(&mut self) -> impl Future<Output = ()> + Send;
}

impl<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    const STR_CAP: usize,
    V: Producer<T, S, STR_CAP> + Send,
> RunnableProducer for ScheduledProducer<T, S, STR_CAP, V>
{
    #[inline(always)]
    fn run_producer(&mut self) -> impl Future<Output = ()> + Send {
        self.run()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runnable::Runnable;
    use std::sync::Arc;
    use std::time::Duration;

    struct TestProducer;

    impl Producer<Arc<Message<String>>, i32, 20> for TestProducer {
        async fn produce(
            &self,
            _topic: ArrayString<20>,
            old_state: Arc<std::sync::Mutex<i32>>,
        ) -> Arc<Message<String>> {
            let mut state_guard = old_state.lock().unwrap();
            let next_state = *state_guard + 1;
            *state_guard = next_state;

            Arc::new(Message::new_data(format!("test data {}", next_state)))
        }
    }

    fn topic(s: &str) -> ArrayString<20> {
        ArrayString::from(s).unwrap()
    }

    #[tokio::test]
    async fn test_scheduled_producer_rejects_empty_topic() {
        let err = ProducerError::CreationError("Topic cannot be empty".into());
        assert!(matches!(err, ProducerError::CreationError(_)));
        assert_eq!(
            err.to_string(),
            "Error Creating Producer: Topic cannot be empty"
        );
    }

    #[tokio::test]
    async fn test_scheduled_produce_once() {
        let bus = Arc::new(DataBus::<Arc<Message<String>>, 20>::new(10));
        let state = 0;
        let t = topic("testtopic");
        bus.add_topic(t);
        let mut scheduled_producer =
            ScheduledProducer::new(TestProducer, state, bus.clone(), t, Schedule::Once).unwrap();

        let mut rx = bus.subscribe(&t).unwrap();

        scheduled_producer.run().await;

        let received = rx.receive().await.expect("Failed to receive message");
        assert_eq!(received.payload(), "test data 1");
        assert_eq!(scheduled_producer.producer_state(), 1);
    }

    #[tokio::test]
    async fn test_interval_produce() {
        let bus = Arc::new(DataBus::<Arc<Message<String>>, 20>::new(10));
        let state = 0;
        let t = topic("testtopic");
        bus.add_topic(t);

        let mut scheduled_producer =
            ScheduledProducer::new(TestProducer, state, bus.clone(), t, Schedule::Interval(10))
                .unwrap();

        let mut rx = bus.subscribe(&t).unwrap();
        let worker = tokio::spawn(async move {
            scheduled_producer.run().await;
            scheduled_producer
        });

        let msg1 = rx.receive().await.expect("Failed to receive message 1");
        assert_eq!(msg1.payload(), "test data 1");

        let msg2 = rx.receive().await.expect("Failed to receive message 2");
        assert_eq!(msg2.payload(), "test data 2");

        let msg3 = rx.receive().await.expect("Failed to receive message 3");
        assert_eq!(msg3.payload(), "test data 3");

        drop(rx);
        let scheduled_producer = worker.await.unwrap();

        assert!(scheduled_producer.producer_state() >= 3);
    }

    #[test]
    fn test_schedule_next_run() {
        assert_eq!(Schedule::Once.next_run(), None);
        assert_eq!(
            Schedule::Interval(25).next_run(),
            Some(Duration::from_millis(25))
        );
    }
}
