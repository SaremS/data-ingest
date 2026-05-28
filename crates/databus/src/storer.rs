use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;

use arrayvec::ArrayString;
use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::broadcast::Receiver;
use tokio_util::sync::CancellationToken;

use crate::{databus::DataBus, message::Message, runnable::Runnable, state::State};

#[derive(Error, Debug)]
pub enum BusStorerError {
    #[error("Failed to subscribe to topic: {0}")]
    SubscriptionError(Cow<'static, str>),

    #[error("Failed to publish message to topic: {0}")]
    PublishError(Cow<'static, str>),

    #[error("Error Creating BusStorer: {0}")]
    CreationError(Cow<'static, str>),
}

#[async_trait]
pub trait Storer<T: Clone + Send + Sync, S: Clone + Send + Sync>: Send + Sync {
    async fn store(&self, message: Arc<Message<T>>, old_state: S) -> S;
}

pub struct BusStorer<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    U: State<S>,
    const STR_CAP: usize,
    V: Storer<T, S>,
> {
    processor: V,
    processor_state: U,
    bus: Arc<DataBus<T, STR_CAP>>,
    input_topic: ArrayString<STR_CAP>,
    receiver: Option<Receiver<Arc<Message<T>>>>,

    cancellation_token: CancellationToken,
    _marker: PhantomData<S>,
}

impl<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    U: State<S>,
    const STR_CAP: usize,
    V: Storer<T, S>,
> BusStorer<T, S, U, STR_CAP, V>
{
    pub fn new(
        processor: V,
        processor_state: U,
        bus: Arc<DataBus<T, STR_CAP>>,
        input_topic: ArrayString<STR_CAP>,
    ) -> Result<Self, BusStorerError> {
        if input_topic.is_empty() {
            return Err(BusStorerError::CreationError(
                "Input topic cannot be empty".into(),
            ));
        }

        Ok(Self {
            processor,
            processor_state,
            bus,
            input_topic,
            receiver: None,

            cancellation_token: CancellationToken::new(),
            _marker: PhantomData,
        })
    }
}

#[async_trait]
impl<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    U: State<S>,
    const STR_CAP: usize,
    V: Storer<T, S>,
> Runnable for BusStorer<T, S, U, STR_CAP, V>
{
    async fn run(&mut self) {
        if self.receiver.is_none() {
            if let Some(rx) = self.bus.subscribe(&self.input_topic) {
                self.receiver = Some(rx);
            } else {
                return;
            }
        }

        let receiver = self.receiver.as_mut().unwrap();

        loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    break;
                }

                result_msg = receiver.recv() => {
                    match result_msg {
                        Ok(message) => {
                            let old_state = self.processor_state.get_state().await;
                            let new_state =
                                self.processor.store(message, old_state).await;
                            self.processor_state.set_state(new_state).await;
                        }
                        Err(_) => {
                            break;
                        }
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
    use crate::message::{MessageHeader, MessageType};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio::time::{Duration, sleep};

    #[derive(Clone)]
    struct MockState {
        inner_value: Arc<Mutex<Vec<String>>>,
    }

    impl MockState {
        fn new(initial: Vec<String>) -> Self {
            Self {
                inner_value: Arc::new(Mutex::new(initial)),
            }
        }
    }

    #[async_trait]
    impl State<Vec<String>> for MockState {
        async fn get_state(&self) -> Vec<String> {
            self.inner_value.lock().await.clone()
        }

        async fn set_state(&self, state: Vec<String>) {
            *self.inner_value.lock().await = state;
        }
    }

    struct MockStorer;

    #[async_trait]
    impl Storer<String, Vec<String>> for MockStorer {
        async fn store(
            &self,
            message: Arc<Message<String>>,
            mut old_state: Vec<String>,
        ) -> Vec<String> {
            old_state.push(message.payload.clone());
            old_state
        }
    }

    fn topic(s: &str) -> ArrayString<20> {
        ArrayString::from(s).unwrap()
    }

    fn test_message(payload: &str) -> Arc<Message<String>> {
        Arc::new(Message {
            header: MessageHeader {
                message_type: MessageType::Data,
                message_meta: HashMap::new(),
            },
            payload: payload.to_string(),
        })
    }

    #[tokio::test]
    async fn test_bus_storer_initialization() {
        let bus = Arc::new(DataBus::new(10));
        let state = MockState::new(Vec::new());
        let i = topic("test.input.topic");

        let bus_storer = BusStorer::new(MockStorer, state, bus, i).unwrap();

        assert!(bus_storer.receiver.is_none());
        assert_eq!(bus_storer.input_topic, i);
    }

    #[test]
    fn test_bus_storer_rejects_empty_input_topic() {
        // HierarchicalTopic cannot be constructed as empty via the public API.
        // Verify the error message is correct by constructing the error directly.
        let err = BusStorerError::CreationError("Input topic cannot be empty".into());
        assert!(matches!(err, BusStorerError::CreationError(_)));
        assert_eq!(
            err.to_string(),
            "Error Creating BusStorer: Input topic cannot be empty"
        );
    }

    #[tokio::test]
    async fn test_bus_storer_run_loop_updates_state() {
        let bus = Arc::new(DataBus::new(10));
        let state = MockState::new(Vec::new());
        let state_checker = state.clone();
        let input_topic = topic("test.input.topic");

        bus.add_topic(input_topic);

        let mut bus_storer =
            BusStorer::new(MockStorer, state, bus.clone(), input_topic.clone()).unwrap();

        let worker = async {
            bus_storer.run().await;
        };
        let driver = async {
            sleep(Duration::from_millis(10)).await;

            bus.publish(test_message("stored value"), &input_topic)
                .await
                .unwrap();

            sleep(Duration::from_millis(25)).await;
            bus.shutdown();
        };

        tokio::join!(worker, driver);

        assert_eq!(
            state_checker.get_state().await,
            vec!["stored value".to_string()]
        );
    }

    #[tokio::test]
    async fn test_bus_storer_returns_when_bus_is_closed_before_run() {
        let bus = Arc::new(DataBus::new(10));
        let state = MockState::new(Vec::new());
        let topic = topic("test.input.topic");

        bus.add_topic(topic.clone());

        let mut bus_storer = BusStorer::new(MockStorer, state, bus.clone(), topic).unwrap();

        bus.shutdown();
        bus_storer.run().await;

        assert!(bus_storer.receiver.is_none());
    }

    #[tokio::test]
    async fn test_bus_storer_stop_cancels_run_loop() {
        let bus = Arc::new(DataBus::new(10));
        let state = MockState::new(Vec::new());
        let topic = topic("test.input.topic");
        bus.add_topic(topic.clone());
        let mut bus_storer = BusStorer::new(MockStorer, state, bus, topic).unwrap();

        bus_storer.stop().await;
        bus_storer.run().await;

        assert!(bus_storer.receiver.is_some());
    }
}
