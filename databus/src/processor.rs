use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;

use crate::{DataBus, Message, runnable::Runnable, state::State};

#[async_trait]
pub trait Processor<T: Clone + Send + Sync, S: Send + Sync>: Send + Sync {
    async fn process(&self, message: &Message<T>, old_state: &S) -> (Message<T>, S);
}

pub struct BusProcessor<T: Clone + Send + Sync, S: Send + Sync, U: State<S>, V: Processor<T, S>> {
    processor: V,
    processor_state: U,
    bus: Arc<DataBus<T>>,
    input_topic: String,
    output_topic: String,
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

impl<T: Clone + Send + Sync, S: Send + Sync, U: State<S>, V: Processor<T, S>>
    BusProcessor<T, S, U, V>
{
    pub fn new(
        processor: V,
        processor_state: U,
        bus: Arc<DataBus<T>>,
        input_topic: String,
        output_topic: String,
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
impl<T: Clone + Send + Sync, S: Send + Sync, U: State<S>, V: Processor<T, S>> Runnable
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
                            let (new_message, new_state) =
                                self.processor.process(&message, &old_state).await;
                            self.processor_state.set_state(new_state).await;

                            if self
                                .bus
                                .publish(&self.output_topic, new_message)
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
    use crate::{MessageHeader, MessageType};
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
            message: &Message<String>,
            old_state: &i32,
        ) -> (Message<String>, i32) {
            let new_state = old_state + 1;

            let new_msg = message.clone();

            (new_msg, new_state)
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
            "test_input_topic".to_string(),
            "test_output_topic".to_string(),
        )
        .unwrap();

        assert!(bus_processor.receiver.is_none());
        assert_eq!(bus_processor.input_topic, "test_input_topic");
        assert_eq!(bus_processor.output_topic, "test_output_topic");
    }

    #[tokio::test]
    async fn test_bus_processor_run_loop() {
        let bus = Arc::new(DataBus::new(10));
        let input_topic = "test_input_topic".to_string();
        let output_topic = "test_output_topic".to_string();

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

        let handle = tokio::spawn(async move {
            bus_processor.run().await;
        });

        sleep(Duration::from_millis(10)).await;

        let dummy_message = Message {
            header: MessageHeader {
                topic: "test_input_topic".to_string(),
                message_type: MessageType::Data,
            },
            payload: "hello bus".to_string(),
        };

        bus.publish(&input_topic, dummy_message).await.unwrap();

        sleep(Duration::from_millis(50)).await;

        let final_state = state_checker.get_state().await;
        assert_eq!(
            final_state, 1,
            "The state should have been incremented by the processor"
        );

        bus.shutdown();

        handle
            .await
            .expect("Processor task panicked or failed to cleanly exit");
    }
}
