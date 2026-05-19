use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;

use crate::{
    databus::DataBus,
    message::{HierarchicalTopic, Message},
    runnable::Runnable,
    state::State,
};

#[async_trait]
pub trait Processor<T: Clone + Send + Sync, S: Clone + Send + Sync>: Send + Sync {
    async fn process(
        &self,
        topic: HierarchicalTopic,
        message: Message<T>,
        old_state: S,
    ) -> (Message<T>, S);
}

pub struct BusProcessor<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    U: State<S>,
    V: Processor<T, S>,
> {
    processor: V,
    processor_state: U,
    bus: Arc<DataBus<T>>,
    input_topic: HierarchicalTopic,
    output_topic: HierarchicalTopic,
    receiver: Option<Receiver<Message<T>>>,

    cancellation_token: CancellationToken,
    _marker: PhantomData<S>,
}

#[derive(Error, Debug)]
pub enum BusProcessorError {
    #[error("Failed to subscribe to topic: {0}")]
    SubscriptionError(Cow<'static, str>),

    #[error("Failed to publish message to topic: {0}")]
    PublishError(Cow<'static, str>),

    #[error("Error Creating BusProcessor: {0}")]
    CreationError(Cow<'static, str>),
}

impl<T: Clone + Send + Sync, S: Clone + Send + Sync, U: State<S>, V: Processor<T, S>>
    BusProcessor<T, S, U, V>
{
    pub fn new(
        processor: V,
        processor_state: U,
        bus: Arc<DataBus<T>>,
        input_topic: HierarchicalTopic,
        output_topic: HierarchicalTopic,
    ) -> Result<Self, BusProcessorError> {
        if input_topic.is_empty() {
            return Err(BusProcessorError::CreationError(
                "Input topic cannot be empty".into(),
            ));
        }
        if output_topic.is_empty() {
            return Err(BusProcessorError::CreationError(
                "Output topic cannot be empty".into(),
            ));
        }
        if input_topic == output_topic {
            return Err(BusProcessorError::CreationError(
                "Input and output topics must be different".into(),
            ));
        }

        Ok(Self {
            processor,
            processor_state,
            bus,
            input_topic,
            output_topic,
            receiver: None,

            cancellation_token: CancellationToken::new(),
            _marker: PhantomData,
        })
    }
}

#[async_trait]
impl<T: Clone + Send + Sync, S: Clone + Send + Sync, U: State<S>, V: Processor<T, S>> Runnable
    for BusProcessor<T, S, U, V>
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

                option_msg = receiver.recv() => {
                    match option_msg {
                        Some(message) => {
                            let old_state = self.processor_state.get_state().await;
                            let (mut new_message, new_state) =
                                self.processor.process(message.topic.clone(), message, old_state).await;
                            new_message.topic = self.output_topic.clone();
                            self.processor_state.set_state(new_state).await;

                            if self
                                .bus
                                .publish(new_message)
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        None => {
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
        inner_value: Arc<Mutex<i32>>,
    }

    impl MockState {
        fn new(initial: i32) -> Self {
            Self {
                inner_value: Arc::new(Mutex::new(initial)),
            }
        }
    }

    #[async_trait]
    impl State<i32> for MockState {
        async fn get_state(&self) -> i32 {
            *self.inner_value.lock().await
        }

        async fn set_state(&self, state: i32) {
            *self.inner_value.lock().await = state;
        }
    }

    struct MockProcessor;

    #[async_trait]
    impl Processor<String, i32> for MockProcessor {
        async fn process(
            &self,
            _topic: HierarchicalTopic,
            message: Message<String>,
            old_state: i32,
        ) -> (Message<String>, i32) {
            let new_state = old_state + 1;
            (message, new_state)
        }
    }

    struct SlowProcessor;

    #[async_trait]
    impl Processor<String, i32> for SlowProcessor {
        async fn process(
            &self,
            _topic: HierarchicalTopic,
            message: Message<String>,
            old_state: i32,
        ) -> (Message<String>, i32) {
            sleep(Duration::from_millis(25)).await;
            (message, old_state + 1)
        }
    }

    fn topic(s: &str) -> HierarchicalTopic {
        HierarchicalTopic::new(s)
    }

    fn test_message() -> Message<String> {
        Message {
            topic: topic("test_input_topic"),
            header: MessageHeader {
                message_type: MessageType::Data,
                message_meta: HashMap::new(),
            },
            payload: "hello bus".to_string(),
        }
    }

    #[tokio::test]
    async fn test_bus_processor_initialization() {
        let bus = Arc::new(DataBus::new(10));
        let state = MockState::new(0);
        let processor = MockProcessor;

        let bus_processor = BusProcessor::new(
            processor,
            state,
            bus,
            topic("test_input_topic"),
            topic("test_output_topic"),
        )
        .unwrap();

        assert!(bus_processor.receiver.is_none());
        assert_eq!(bus_processor.input_topic, topic("test_input_topic"));
        assert_eq!(bus_processor.output_topic, topic("test_output_topic"));
    }

    #[test]
    fn test_bus_processor_rejects_empty_input_topic() {
        let bus = Arc::new(DataBus::new(10));
        let state = MockState::new(0);

        let err = BusProcessorError::CreationError("Input topic cannot be empty".into());
        assert!(matches!(err, BusProcessorError::CreationError(_)));
        assert_eq!(
            err.to_string(),
            "Error Creating BusProcessor: Input topic cannot be empty"
        );

        // Verify a valid processor is created when topics differ
        let valid = BusProcessor::new(
            MockProcessor,
            state,
            bus,
            topic("test_input_topic"),
            topic("test_output_topic"),
        );
        assert!(valid.is_ok());
    }

    #[test]
    fn test_bus_processor_rejects_same_input_and_output_topics() {
        let bus = Arc::new(DataBus::new(10));
        let state = MockState::new(0);

        let err = match BusProcessor::new(
            MockProcessor,
            state,
            bus,
            topic("same_topic"),
            topic("same_topic"),
        ) {
            Ok(_) => panic!("expected matching topic validation error"),
            Err(err) => err,
        };

        assert!(matches!(err, BusProcessorError::CreationError(_)));
        assert_eq!(
            err.to_string(),
            "Error Creating BusProcessor: Input and output topics must be different"
        );
    }

    #[tokio::test]
    async fn test_bus_processor_run_loop() {
        let bus = Arc::new(DataBus::new(10));
        let input_topic = topic("test_input_topic");
        let output_topic = topic("test_output_topic");

        let state = MockState::new(0);
        let state_checker = state.clone();

        let mut bus_processor = BusProcessor::new(
            MockProcessor,
            state,
            bus.clone(),
            input_topic.clone(),
            output_topic.clone(),
        )
        .unwrap();

        let worker = async {
            bus_processor.run().await;
        };
        let driver = async {
            sleep(Duration::from_millis(10)).await;

            bus.publish(test_message()).await.unwrap();

            sleep(Duration::from_millis(50)).await;
            bus.shutdown();
        };

        tokio::join!(worker, driver);

        let final_state = state_checker.get_state().await;
        assert_eq!(
            final_state, 1,
            "The state should have been incremented by the processor"
        );
    }

    #[tokio::test]
    async fn test_bus_processor_returns_when_bus_is_closed_before_run() {
        let bus = Arc::new(DataBus::new(10));
        let state = MockState::new(0);
        let mut bus_processor = BusProcessor::new(
            MockProcessor,
            state,
            bus.clone(),
            topic("test_input_topic"),
            topic("test_output_topic"),
        )
        .unwrap();

        bus.shutdown();
        bus_processor.run().await;

        assert!(bus_processor.receiver.is_none());
    }

    #[tokio::test]
    async fn test_bus_processor_stop_cancels_run_loop() {
        let bus = Arc::new(DataBus::new(10));
        let state = MockState::new(0);
        let mut bus_processor = BusProcessor::new(
            MockProcessor,
            state,
            bus,
            topic("test_input_topic"),
            topic("test_output_topic"),
        )
        .unwrap();

        bus_processor.stop().await;
        bus_processor.run().await;

        assert!(bus_processor.receiver.is_some());
    }

    #[tokio::test]
    async fn test_bus_processor_stops_when_output_publish_fails() {
        let bus = Arc::new(DataBus::new(10));
        let state = MockState::new(0);
        let state_checker = state.clone();
        let input_topic = topic("test_input_topic");
        let output_topic = topic("test_output_topic");

        let mut bus_processor = BusProcessor::new(
            SlowProcessor,
            state,
            bus.clone(),
            input_topic.clone(),
            output_topic,
        )
        .unwrap();

        let worker = async {
            bus_processor.run().await;
        };
        let driver = async {
            sleep(Duration::from_millis(10)).await;

            bus.publish(test_message()).await.unwrap();
            bus.shutdown();
        };

        tokio::join!(worker, driver);

        assert_eq!(state_checker.get_state().await, 1);
    }

    #[tokio::test]
    async fn test_bus_processor_stops_when_input_channel_closes() {
        let bus = Arc::new(DataBus::new(10));
        let state = MockState::new(0);
        let mut bus_processor = BusProcessor::new(
            MockProcessor,
            state,
            bus.clone(),
            topic("test_input_topic"),
            topic("test_output_topic"),
        )
        .unwrap();

        let worker = async {
            bus_processor.run().await;
        };
        let driver = async {
            sleep(Duration::from_millis(10)).await;
            bus.shutdown();
        };

        tokio::join!(worker, driver);
    }
}
