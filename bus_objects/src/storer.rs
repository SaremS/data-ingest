use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;

use crate::{runnable::Runnable, state::State};
use databus::{DataBus, Message};

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
pub trait Storer<T: Clone + Send + Sync, S: Send + Sync>: Send + Sync {
    async fn store(&self, message: &Message<T>, old_state: &S) -> S;
}

pub struct BusStorer<T: Clone + Send + Sync, S: Send + Sync, U: State<S>, V: Storer<T, S>> {
    processor: V,
    processor_state: U,
    bus: Arc<DataBus<T>>,
    input_topic: String,
    receiver: Option<Receiver<Message<T>>>,

    cancellation_token: CancellationToken,
    _marker: PhantomData<S>,
}

impl<T: Clone + Send + Sync, S: Send + Sync, U: State<S>, V: Storer<T, S>> BusStorer<T, S, U, V> {
    pub fn new(
        processor: V,
        processor_state: U,
        bus: Arc<DataBus<T>>,
        input_topic: String,
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
impl<T: Clone + Send + Sync, S: Send + Sync, U: State<S>, V: Storer<T, S>> Runnable
    for BusStorer<T, S, U, V>
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
                            let new_state =
                                self.processor.store(&message, &old_state).await;
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
