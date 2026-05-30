use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use arrayvec::ArrayString;
use thiserror::Error;

use crate::message::Message;
use crate::send_receive_handles::{ReceiveHandle, SendHandle, create_send_receive_handles};

pub struct DataBus<T: Clone + Send + Sync, const STR_CAP: usize> {
    senders: Mutex<HashMap<ArrayString<STR_CAP>, SendHandle<Arc<Message<T>>>>>,
    receivers: Mutex<HashMap<ArrayString<STR_CAP>, Option<ReceiveHandle<Arc<Message<T>>>>>>,
    channel_capacity: usize,
    is_closed: AtomicBool,
}

#[derive(Error, Debug)]
pub enum SubscribeError {
    #[error("All available receivers are leased")]
    OutOfReceivers,
}

impl<T: Clone + Send + Sync, const STR_CAP: usize> DataBus<T, STR_CAP> {
    pub fn new(channel_capacity: usize) -> Self {
        DataBus {
            senders: Mutex::new(HashMap::new()),
            receivers: Mutex::new(HashMap::new()),
            channel_capacity,
            is_closed: AtomicBool::new(false),
        }
    }

    pub fn shutdown(&self) {
        self.is_closed.store(true, Ordering::Release);
        let mut sender_guard = self.senders.lock().unwrap();
        *sender_guard = HashMap::new();

        let mut receiver_guard = self.receivers.lock().unwrap();
        *receiver_guard = HashMap::new();
    }

    pub fn add_topic(&self, topic_index: ArrayString<STR_CAP>) {
        if self.is_closed.load(Ordering::Acquire) {
            return;
        }

        let (sender, receiver) = create_send_receive_handles(self.channel_capacity);

        let mut sender_guard = self.senders.lock().unwrap();
        sender_guard.insert(topic_index, sender);

        let mut receiver_guard = self.receivers.lock().unwrap();
        receiver_guard.insert(topic_index, Some(receiver));
    }

    pub fn subscribe(&self, topic: &str) -> Option<ReceiveHandle<Arc<Message<T>>>> {
        if self.is_closed.load(Ordering::Acquire) {
            return None;
        }

        let mut receiver_guard = self.receivers.lock().unwrap();

        receiver_guard
            .get_mut(topic)
            .and_then(|receiver| std::mem::take(receiver))
    }

    pub fn get_sender(&self, topic: &str) -> Option<SendHandle<Arc<Message<T>>>> {
        if self.is_closed.load(Ordering::Acquire) {
            return None;
        }

        let sender_guard = self.senders.lock().unwrap();

        sender_guard.get(topic).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::message::{MessageHeader, MessageType};

    fn test_message(payload: impl Into<String>) -> Arc<Message<String>> {
        Arc::new(Message {
            header: MessageHeader {
                message_type: MessageType::Data,
                message_meta: None,
            },
            payload: payload.into(),
        })
    }

    #[tokio::test]
    async fn test_data_bus() {
        let bus = DataBus::<String, 20>::new(10);

        let topic = "test_topic";
        bus.add_topic(ArrayString::from(topic).unwrap());

        let mut rx = bus.subscribe(&topic).unwrap();

        let tx = bus.get_sender(&topic).unwrap();

        tx.send(test_message("Hello, DataBus!")).await.unwrap();

        let received = rx.receive().await.expect("Failed to receive message");
        assert_eq!(received.payload, "Hello, DataBus!");
    }

    #[tokio::test]
    async fn test_responding_listener() {
        let bus = Arc::new(DataBus::<String, 20>::new(10));
        let request_topic = "request";
        let response_topic = "response";

        bus.add_topic(ArrayString::from(request_topic).unwrap());
        bus.add_topic(ArrayString::from(response_topic).unwrap());

        let mut request_rx = bus.subscribe(&request_topic).unwrap();
        let mut response_rx = bus.subscribe(&response_topic).unwrap();

        let bus_clone = bus.clone();
        tokio::spawn(async move {
            while let Some(request) = request_rx.receive().await {
                assert_eq!(request.payload, "Request to listener".to_string());

                let tx = bus.get_sender(&response_topic).unwrap();
                tx.send(test_message("Response from listener"))
                    .await
                    .unwrap();
            }
        });

        let tx = bus_clone.get_sender(&request_topic).unwrap();
        tx.send(test_message("Request to listener")).await.unwrap();

        let received = response_rx
            .receive()
            .await
            .expect("Failed to receive response");
        assert_eq!(received.payload, "Response from listener".to_string());
    }

    #[tokio::test]
    async fn test_multiple_listeners() {
        let bus = Arc::new(DataBus::<String, 20>::new(10));

        let topic = "test";
        bus.add_topic(ArrayString::from(topic).unwrap());

        let mut rx1 = bus.subscribe(&topic).unwrap();

        let tx = bus.get_sender(&topic).unwrap();
        tx.send(test_message("test")).await.unwrap();

        assert_eq!(rx1.receive().await.unwrap().payload, "test");
    }

    #[tokio::test]
    async fn test_graceful_shutdown() {
        let bus = DataBus::<String, 20>::new(10);

        let shutdown_topic = "shutdown";
        let another_topic = "another";
        bus.add_topic(ArrayString::from(shutdown_topic).unwrap());
        bus.add_topic(ArrayString::from(another_topic).unwrap());

        let mut rx = bus.subscribe(&shutdown_topic).unwrap();

        bus.shutdown();

        assert!(bus.subscribe(&another_topic).is_none());
        assert!(bus.get_sender(&another_topic).is_none());

        assert!(rx.receive().await.is_none());
    }
}
