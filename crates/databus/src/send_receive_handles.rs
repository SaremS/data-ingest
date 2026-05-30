use std::borrow::Cow;

use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Error, Debug)]
pub enum SendError {
    #[error("Failed to send message: {0}")]
    SendError(Cow<'static, str>),
}

#[derive(Debug, Clone)]
pub struct SendHandle<T: Clone + Send + Sync> {
    sender: Sender<T>,
}

pub struct ReceiveHandle<T: Clone + Send + Sync> {
    receiver: Receiver<T>,
}

impl<T: Clone + Send + Sync> SendHandle<T> {
    pub fn new(sender: Sender<T>) -> Self {
        SendHandle { sender }
    }

    pub async fn send(&self, item: T) -> Result<(), SendError> {
        self.sender
            .send(item)
            .await
            .map_err(|e| SendError::SendError(Cow::Owned(format!("Failed to send message: {}", e))))
    }
}

impl<T: Clone + Send + Sync> ReceiveHandle<T> {
    pub fn new(receiver: Receiver<T>) -> Self {
        ReceiveHandle { receiver }
    }

    pub async fn receive(&mut self) -> Option<T> {
        self.receiver.recv().await
    }
}

pub fn create_send_receive_handles<T: Clone + Send + Sync>(
    buffer_size: usize,
) -> (SendHandle<T>, ReceiveHandle<T>) {
    let (sender, receiver) = tokio::sync::mpsc::channel(buffer_size);
    (SendHandle::new(sender), ReceiveHandle::new(receiver))
}
