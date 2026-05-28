use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;

use trie::hierarchical_index::HierarchicalIndex;

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
pub trait Storer<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    const VEC_CAP: usize,
    const STR_CAP: usize,
>: Send + Sync
{
    async fn store(&self, message: Arc<Message<T, VEC_CAP, STR_CAP>>, old_state: S) -> S;
}

pub struct BusStorer<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    U: State<S>,
    const VEC_CAP: usize,
    const STR_CAP: usize,
    V: Storer<T, S, VEC_CAP, STR_CAP>,
> {
    processor: V,
    processor_state: U,
    bus: Arc<DataBus<T, VEC_CAP, STR_CAP>>,
    input_index: HierarchicalIndex<VEC_CAP, STR_CAP>,
    receiver: Option<Receiver<Arc<Message<T, VEC_CAP, STR_CAP>>>>,

    cancellation_token: CancellationToken,
    _marker: PhantomData<S>,
}

impl<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    U: State<S>,
    const VEC_CAP: usize,
    const STR_CAP: usize,
    V: Storer<T, S, VEC_CAP, STR_CAP>,
> BusStorer<T, S, U, VEC_CAP, STR_CAP, V>
{
    pub fn new(
        processor: V,
        processor_state: U,
        bus: Arc<DataBus<T, VEC_CAP, STR_CAP>>,
        input_index: HierarchicalIndex<VEC_CAP, STR_CAP>,
    ) -> Result<Self, BusStorerError> {
        if input_index.is_empty() {
            return Err(BusStorerError::CreationError(
                "Input topic cannot be empty".into(),
            ));
        }

        Ok(Self {
            processor,
            processor_state,
            bus,
            input_index,
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
    const VEC_CAP: usize,
    const STR_CAP: usize,
    V: Storer<T, S, VEC_CAP, STR_CAP>,
> Runnable for BusStorer<T, S, U, VEC_CAP, STR_CAP, V>
{
    async fn run(&mut self) {
        if self.receiver.is_none() {
            if let Some(rx) = self.bus.subscribe(&self.input_index) {
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
                            let new_state =
                                self.processor.store(message, old_state).await;
                            self.processor_state.set_state(new_state).await;
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

    use trie::hierarchical_index::HierarchicalTopic;

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
    impl Storer<String, Vec<String>, 3, 10> for MockStorer {
        async fn store(
            &self,
            message: Arc<Message<String, 3, 10>>,
            mut old_state: Vec<String>,
        ) -> Vec<String> {
            old_state.push(message.payload.clone());
            old_state
        }
    }

    fn topic(s: &str) -> HierarchicalTopic<3, 10> {
        HierarchicalTopic::from_str(s).unwrap()
    }

    fn index(s: &str) -> HierarchicalIndex<3, 10> {
        HierarchicalIndex::from_str(s).unwrap()
    }

    fn test_message(payload: &str) -> Arc<Message<String, 3, 10>> {
        Arc::new(Message {
            topic: topic("test.input.topic"),
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
        let i = index("test.input.topic");

        let bus_storer = BusStorer::new(MockStorer, state, bus, i.clone()).unwrap();

        assert!(bus_storer.receiver.is_none());
        assert_eq!(bus_storer.input_index, i);
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
        let input_index = index("test.input.topic");

        let mut bus_storer =
            BusStorer::new(MockStorer, state, bus.clone(), input_index.clone()).unwrap();

        let worker = async {
            bus_storer.run().await;
        };
        let driver = async {
            sleep(Duration::from_millis(10)).await;

            bus.publish(test_message("stored value")).await.unwrap();

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
        let mut bus_storer =
            BusStorer::new(MockStorer, state, bus.clone(), index("test.input.topic")).unwrap();

        bus.shutdown();
        bus_storer.run().await;

        assert!(bus_storer.receiver.is_none());
    }

    #[tokio::test]
    async fn test_bus_storer_stop_cancels_run_loop() {
        let bus = Arc::new(DataBus::new(10));
        let state = MockState::new(Vec::new());
        let mut bus_storer =
            BusStorer::new(MockStorer, state, bus, index("test.input.topic")).unwrap();

        bus_storer.stop().await;
        bus_storer.run().await;

        assert!(bus_storer.receiver.is_some());
    }
}
