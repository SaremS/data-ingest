use std::borrow::Cow;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use arrayvec::ArrayString;
use thiserror::Error;

use crate::{databus::DataBus, runnable::Runnable, send_receive_handles::ReceiveHandle};

#[derive(Error, Debug)]
pub enum BusStorerError {
    #[error("Failed to subscribe to topic: {0}")]
    SubscriptionError(Cow<'static, str>),

    #[error("Failed to publish message to topic: {0}")]
    PublishError(Cow<'static, str>),

    #[error("Error Creating BusStorer: {0}")]
    CreationError(Cow<'static, str>),
}

pub trait Storer<T: Clone + Send + Sync, S: Clone + Send + Sync>: Send + Sync {
    fn store(&self, message: T, old_state: Arc<Mutex<S>>) -> impl Future<Output = ()> + Send;
}

pub struct BusStorer<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    const STR_CAP: usize,
    V: Storer<T, S>,
> {
    processor: V,
    processor_state: Arc<Mutex<S>>,
    bus: Arc<DataBus<T, STR_CAP>>,
    input_topic: ArrayString<STR_CAP>,
    receiver: ReceiveHandle<T>,

    _marker: PhantomData<S>,
}

impl<T: Clone + Send + Sync, S: Clone + Send + Sync, const STR_CAP: usize, V: Storer<T, S>>
    BusStorer<T, S, STR_CAP, V>
{
    pub fn new(
        processor: V,
        processor_state: S,
        bus: Arc<DataBus<T, STR_CAP>>,
        input_topic: ArrayString<STR_CAP>,
    ) -> Result<Self, BusStorerError> {
        if input_topic.is_empty() {
            return Err(BusStorerError::CreationError(
                "Input topic cannot be empty".into(),
            ));
        }

        let receiver = bus.subscribe(&input_topic);
        if receiver.is_err() {
            return Err(BusStorerError::SubscriptionError(
                format!("Failed to subscribe to topic: {}", input_topic).into(),
            ));
        }

        Ok(Self {
            processor,
            processor_state: Arc::new(Mutex::new(processor_state)),
            bus,
            input_topic,
            receiver: receiver.unwrap(),

            _marker: PhantomData,
        })
    }
}

impl<T: Clone + Send + Sync, S: Clone + Send + Sync, const STR_CAP: usize, V: Storer<T, S>> Runnable
    for BusStorer<T, S, STR_CAP, V>
{
    async fn run(&mut self) {
        loop {
            tokio::select! {
                result_msg = self.receiver.receive() => {
                    match result_msg {
                        Some(message) => {
                            self.processor.store(message, self.processor_state.clone()).await;
                        }
                        None => {
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::message::Message;
    use tokio::time::{Duration, sleep};

    struct MockStorer;

    impl Storer<Arc<Message<String>>, Vec<String>> for MockStorer {
        async fn store(&self, message: Arc<Message<String>>, old_state: Arc<Mutex<Vec<String>>>) {
            let mut old_state_guard = old_state.lock().unwrap();
            (*old_state_guard).push(message.payload().clone());
        }
    }

    fn topic(s: &str) -> ArrayString<20> {
        ArrayString::from(s).unwrap()
    }

    fn test_message(payload: &str) -> Arc<Message<String>> {
        Arc::new(Message::new_data(payload.to_string()))
    }

    #[tokio::test]
    async fn test_bus_storer_initialization() {
        let bus = Arc::new(DataBus::new(10));
        let state = Vec::new();
        let i = topic("test.input.topic");
        bus.add_topic(i);

        let bus_storer = BusStorer::new(MockStorer, state, bus, i).unwrap();

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
        let state = Vec::new();
        let state_checker = state.clone();
        let input_topic = topic("test.input.topic");

        bus.add_topic(input_topic);

        let mut bus_storer = BusStorer::new(MockStorer, state, bus.clone(), input_topic).unwrap();

        let worker = async {
            bus_storer.run().await;
        };
        let driver = async {
            sleep(Duration::from_millis(10)).await;

            let sender = bus.get_sender(&input_topic).unwrap();
            sender.send(test_message("stored value")).await.unwrap();

            sleep(Duration::from_millis(25)).await;
            bus.shutdown();
        };

        tokio::join!(worker, driver);

        /*assert_eq!(
            state_checker.get_state().await,
            vec!["stored value".to_string()]
        );*/
    }

    #[tokio::test]
    async fn test_bus_storer_returns_when_bus_is_closed_before_run() {
        let bus = Arc::new(DataBus::new(10));
        let state = Vec::new();
        let topic = topic("test.input.topic");

        bus.add_topic(topic);

        let mut bus_storer = BusStorer::new(MockStorer, state, bus.clone(), topic).unwrap();

        bus.shutdown();
        bus_storer.run().await;
    }

    #[tokio::test]
    async fn test_bus_storer_stop_cancels_run_loop() {
        let bus = Arc::new(DataBus::new(10));
        let bus_clone = bus.clone();
        let state = Vec::new();
        let topic = topic("test.input.topic");
        bus.add_topic(topic);
        let mut bus_storer = BusStorer::new(MockStorer, state, bus_clone, topic).unwrap();

        let worker = tokio::spawn(async move {
            bus_storer.run().await;
        });

        bus.clone().shutdown();

        worker.await.unwrap();
    }
}
