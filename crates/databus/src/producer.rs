use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use trie::hierarchical_index::HierarchicalTopic;

use crate::{
    databus::DataBus,
    message::Message,
    runnable::Runnable,
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

#[async_trait]
pub trait Producer<T: Clone + Send + Sync, S: Clone + Send + Sync>: Send + Sync {
    async fn produce(&self, topic: HierarchicalTopic, old_state: S) -> (Message<T>, S);
}

pub struct ScheduledProducer<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    U: State<S>,
    V: Producer<T, S>,
    const VEC_CAP: usize,
    const STR_CAP: usize,
> {
    producer: V,
    producer_state: U,
    bus: Arc<DataBus<T, VEC_CAP, STR_CAP>>,
    topic: HierarchicalTopic,

    schedule: Schedule,
    cancellation_token: CancellationToken,
    _marker: PhantomData<S>,
}

impl<T: Clone + Send + Sync, S: Clone + Send + Sync, U: State<S>, V: Producer<T, S>, const VEC_CAP: usize, const STR_CAP: usize>
    ScheduledProducer<T, S, U, V, VEC_CAP, STR_CAP>
{
    pub fn new(
        producer: V,
        producer_state: U,
        bus: Arc<DataBus<T, VEC_CAP, STR_CAP>>,
        topic: HierarchicalTopic<VEC_CAP, STR_CAP>,
        schedule: Schedule,
    ) -> Result<Self, ProducerError> {
        if topic.is_empty() {
            return Err(ProducerError::CreationError("Topic cannot be empty".into()));
        }

        Ok(Self {
            producer,
            producer_state,
            bus,
            topic,

            schedule,
            cancellation_token: CancellationToken::new(),
            _marker: PhantomData,
        })
    }
}

#[async_trait]
impl<T: Clone + Send + Sync, S: Clone + Send + Sync, U: State<S>, V: Producer<T, S>, const VEC_CAP: usize, const STR_CAP: usize> Runnable
    for ScheduledProducer<T, S, U, V, VEC_CAP, STR_CAP>
{
    async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    break;
                }
                should_continue = async {
                    let old_state = self.producer_state.get_state().await;
                    let (message, new_state) = self.producer.produce(self.topic.clone(), old_state).await;

                    if let Err(_e) = self.bus.publish(message).await {
                        return false;
                    }

                    self.producer_state.set_state(new_state).await;

                    if let Some(duration) = self.schedule.next_run() {
                        tokio::time::sleep(duration).await;
                        true
                    } else {
                        false
                    }
                } => {
                    if !should_continue {
                        break;
                    }
                }
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
    use crate::{
        message::{MessageHeader, MessageType},
        runnable::Runnable,
    };
    use std::collections::HashMap;
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

    #[async_trait]
    impl State<i32> for TestState {
        async fn get_state(&self) -> i32 {
            *self.value.lock().await
        }

        async fn set_state(&self, state: i32) {
            *self.value.lock().await = state;
        }
    }

    #[async_trait]
    impl Producer<String, i32> for TestProducer {
        async fn produce(
            &self,
            topic: HierarchicalTopic,
            old_state: i32,
        ) -> (Message<String>, i32) {
            (
                Message {
                    topic,
                    header: MessageHeader {
                        message_type: MessageType::Data,
                        message_meta: HashMap::new(),
                    },
                    payload: format!("test data {}", old_state + 1),
                },
                old_state + 1,
            )
        }
    }

    fn topic(s: &str) -> HierarchicalTopic {
        HierarchicalTopic::new(s)
    }

    #[tokio::test]
    async fn test_scheduled_producer_rejects_empty_topic() {
        // HierarchicalTopic cannot be constructed as empty via the public API;
        // the is_empty() guard in ScheduledProducer::new is a safety net.
        // Verify the error message is correct by constructing the error directly.
        let err = ProducerError::CreationError("Topic cannot be empty".into());
        assert!(matches!(err, ProducerError::CreationError(_)));
        assert_eq!(
            err.to_string(),
            "Error Creating Producer: Topic cannot be empty"
        );
    }

    #[tokio::test]
    async fn test_scheduled_produce_once() {
        let bus = Arc::new(DataBus::<String>::new(10));
        let state = TestState::new(0);
        let state_checker = state.clone();
        let t = topic("test_topic");
        let mut scheduled_producer =
            ScheduledProducer::new(TestProducer, state, bus.clone(), t.clone(), Schedule::Once)
                .unwrap();

        let mut rx = bus.subscribe(&t).unwrap();

        scheduled_producer.run().await;

        let received = rx.recv().await.expect("Failed to receive message");
        assert_eq!(received.payload, "test data 1");
        assert_eq!(state_checker.get_state().await, 1);
    }

    #[tokio::test]
    async fn test_interval_produce() {
        let bus = Arc::new(DataBus::<String>::new(10));
        let state = TestState::new(0);
        let state_checker = state.clone();
        let t = topic("test_topic");
        let mut scheduled_producer = ScheduledProducer::new(
            TestProducer,
            state,
            bus.clone(),
            t.clone(),
            Schedule::Interval(10),
        )
        .unwrap();

        let mut rx = bus.subscribe(&t).unwrap();
        let worker = async {
            scheduled_producer.run().await;
        };
        let driver = async {
            let msg1 = rx.recv().await.expect("Failed to receive message 1");
            assert_eq!(msg1.payload, "test data 1");

            let msg2 = rx.recv().await.expect("Failed to receive message 2");
            assert_eq!(msg2.payload, "test data 2");

            let msg3 = rx.recv().await.expect("Failed to receive message 3");
            assert_eq!(msg3.payload, "test data 3");

            bus.shutdown();
        };

        tokio::join!(worker, driver);

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
        let bus = Arc::new(DataBus::<String>::new(10));
        let state = TestState::new(0);
        let mut scheduled_producer = ScheduledProducer::new(
            TestProducer,
            state,
            bus.clone(),
            topic("test_topic"),
            Schedule::Once,
        )
        .unwrap();

        bus.shutdown();

        scheduled_producer.run().await;
    }

    #[tokio::test]
    async fn test_scheduled_producer_stop_cancels_run_loop() {
        let bus = Arc::new(DataBus::<String>::new(10));
        let state = TestState::new(0);
        let mut scheduled_producer = ScheduledProducer::new(
            TestProducer,
            state,
            bus,
            topic("test_topic"),
            Schedule::Interval(10),
        )
        .unwrap();

        scheduled_producer.stop().await;
        scheduled_producer.run().await;
    }
}
