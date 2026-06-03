use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use arrayvec::ArrayString;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{
    databus::DataBus, message::Message, runnable::Runnable, send_receive_handles::SendHandle,
    state::State,
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
        old_state: S,
    ) -> impl Future<Output = (T, S)> + Send;
}

pub struct ScheduledProducer<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    U: State<S>,
    const STR_CAP: usize,
    V: Producer<T, S, STR_CAP>,
> {
    producer: V,
    producer_state: U,
    topic: ArrayString<STR_CAP>,
    sender: SendHandle<T>,

    schedule: Schedule,
    cancellation_token: CancellationToken,
    _marker: PhantomData<S>,
}

impl<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    U: State<S>,
    const STR_CAP: usize,
    V: Producer<T, S, STR_CAP>,
> ScheduledProducer<T, S, U, STR_CAP, V>
{
    pub fn new(
        producer: V,
        producer_state: U,
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
            producer_state,
            topic,
            sender: sender.unwrap(),

            schedule,
            cancellation_token: CancellationToken::new(),
            _marker: PhantomData,
        })
    }
}

impl<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    U: State<S>,
    const STR_CAP: usize,
    V: Producer<T, S, STR_CAP>,
> Runnable for ScheduledProducer<T, S, U, STR_CAP, V>
{
    async fn run(&mut self) {
        loop {
            let old_state = self.producer_state.get_state().await;
            let (message, new_state) = self.producer.produce(self.topic, old_state).await;

            if self.sender.send(message).await.is_err() {
                break; 
            }

            self.producer_state.set_state(new_state).await;

            let sleep_duration = match self.schedule.next_run() {
                Some(duration) => duration,
                None => break, 
            };

            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    break;
                }
                _ = tokio::time::sleep(sleep_duration) => {}
            }
        }
    }

    async fn stop(&self) {
        self.cancellation_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runnable::Runnable;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    struct TestProducer;

    #[derive(Clone)]
    struct TestState {
        value: Arc<Mutex<i32>>,
    }

    impl TestState {
        fn new(initial: i32) -> Self {
            Self {
                value: Arc::new(Mutex::new(initial)),
            }
        }
    }

    impl State<i32> for TestState {
        async fn get_state(&self) -> i32 {
            *self.value.lock().await
        }

        async fn set_state(&self, state: i32) {
            *self.value.lock().await = state;
        }
    }

    impl Producer<Arc<Message<String>>, i32, 20> for TestProducer {
        async fn produce(
            &self,
            _topic: ArrayString<20>,
            old_state: i32,
        ) -> (Arc<Message<String>>, i32) {
            (
                Arc::new(Message::new_data(format!("test data {}", old_state + 1))),
                old_state + 1,
            )
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
        let state = TestState::new(0);
        let state_checker = state.clone();
        let t = topic("testtopic");
        bus.add_topic(t);
        let mut scheduled_producer =
            ScheduledProducer::new(TestProducer, state, bus.clone(), t, Schedule::Once).unwrap();

        let mut rx = bus.subscribe(&t).unwrap();

        scheduled_producer.run().await;

        let received = rx.receive().await.expect("Failed to receive message");
        assert_eq!(received.payload(), "test data 1");
        assert_eq!(state_checker.get_state().await, 1);
    }

    #[tokio::test]
    async fn test_interval_produce() {
        let bus = Arc::new(DataBus::<Arc<Message<String>>, 20>::new(10));
        let state = TestState::new(0);
        let state_checker = state.clone();
        let t = topic("testtopic");
        bus.add_topic(t);

        let mut scheduled_producer =
            ScheduledProducer::new(TestProducer, state, bus.clone(), t, Schedule::Interval(10))
                .unwrap();

        let mut rx = bus.subscribe(&t).unwrap();
        let cancellation_token = scheduled_producer.cancellation_token.clone();
        let worker = tokio::spawn(async move {
            scheduled_producer.run().await;
        });

        let msg1 = rx.receive().await.expect("Failed to receive message 1");
        assert_eq!(msg1.payload(), "test data 1");

        let msg2 = rx.receive().await.expect("Failed to receive message 2");
        assert_eq!(msg2.payload(), "test data 2");

        let msg3 = rx.receive().await.expect("Failed to receive message 3");
        assert_eq!(msg3.payload(), "test data 3");

        cancellation_token.cancel();
        worker.await.unwrap();

        assert_eq!(state_checker.get_state().await, 3);
    }

    #[test]
    fn test_schedule_next_run() {
        assert_eq!(Schedule::Once.next_run(), None);
        assert_eq!(
            Schedule::Interval(25).next_run(),
            Some(Duration::from_millis(25))
        );
    }

    #[tokio::test]
    async fn test_scheduled_producer_stops_when_bus_is_closed() {
        let bus = Arc::new(DataBus::<Arc<Message<String>>, 20>::new(10));
        let state = TestState::new(0);
        let test_topic = topic("testtopic");
        bus.add_topic(test_topic);

        let mut scheduled_producer =
            ScheduledProducer::new(TestProducer, state, bus.clone(), test_topic, Schedule::Once)
                .unwrap();

        bus.shutdown();

        scheduled_producer.run().await;
    }

    #[tokio::test]
    async fn test_scheduled_producer_stop_cancels_run_loop() {
        let bus = Arc::new(DataBus::<Arc<Message<String>>, 20>::new(10));
        let state = TestState::new(0);
        let t = topic("testtopic");
        bus.add_topic(t);

        let mut scheduled_producer =
            ScheduledProducer::new(TestProducer, state, bus, t, Schedule::Interval(10)).unwrap();

        scheduled_producer.stop().await;
        scheduled_producer.run().await;
    }
}
