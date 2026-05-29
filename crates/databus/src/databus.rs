use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use arc_swap::ArcSwap;
use arrayvec::ArrayString;
use tokio::sync::{broadcast, mpsc};

use crate::message::Message;

pub struct DataBus<T: Clone + Send + Sync, const STR_CAP: usize> {
    topics: ArcSwap<HashMap<ArrayString<STR_CAP>, broadcast::Sender<Arc<Message<T>>>>>,
    channel_capacity: usize,
    is_closed: AtomicBool,
}

impl<T: Clone + Send + Sync, const STR_CAP: usize> DataBus<T, STR_CAP> {
    pub fn new(channel_capacity: usize) -> Self {
        DataBus {
            topics: ArcSwap::from_pointee(HashMap::new()),
            channel_capacity,
            is_closed: AtomicBool::new(false),
        }
    }

    pub fn shutdown(&self) {
        self.is_closed.store(true, Ordering::Release);
        self.topics.store(Arc::new(HashMap::new()));
    }

    pub fn add_topic(&self, topic_index: ArrayString<STR_CAP>) {
        if self.is_closed.load(Ordering::Acquire) {
            return;
        }

        let mut new_topics: HashMap<ArrayString<STR_CAP>, _> = self.topics.load().as_ref().clone();
        new_topics.insert(topic_index, broadcast::channel(self.channel_capacity).0);

        self.topics.store(Arc::new(new_topics));
    }

    pub fn subscribe(&self, topic: &str) -> Option<broadcast::Receiver<Arc<Message<T>>>> {
        if self.is_closed.load(Ordering::Acquire) {
            return None;
        }

        self.topics
            .load()
            .get(topic)
            .map(|sender| sender.subscribe())
    }

    pub fn get_sender(&self, topic: &str) -> Option<broadcast::Sender<Arc<Message<T>>>> {
        if self.is_closed.load(Ordering::Acquire) {
            return None;
        }

        self.topics.load().get(topic).cloned()
    }

    pub fn publish(&self, message: Arc<Message<T>>, topic: &str) -> Result<(), &'static str> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Err("Bus is closed");
        }

        for tx in self.topics.load().get(topic).into_iter() {
            let send_result = tx.send(message.clone());
            if send_result.is_err() {
                // If the channel is closed, we ignore it since it means there are no active subscribers
                continue;
            }
        }

        Ok(())
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
                message_meta: HashMap::new(),
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

        bus.publish(test_message("Hello, DataBus!"), &topic)
            .unwrap();

        let received = rx.recv().await.expect("Failed to receive message");
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
            while let Ok(request) = request_rx.recv().await {
                assert_eq!(request.payload, "Request to listener".to_string());

                bus_clone
                    .publish(test_message("Response from listener"), &response_topic)
                    .unwrap();
            }
        });

        bus.publish(test_message("Request to listener"), &request_topic)
            .unwrap();

        let received = response_rx
            .recv()
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
        let mut rx2 = bus.subscribe(&topic).unwrap();
        let mut rx3 = bus.subscribe(&topic).unwrap();

        bus.publish(test_message("test"), &topic).unwrap();

        assert_eq!(rx1.recv().await.unwrap().payload, "test");
        assert_eq!(rx2.recv().await.unwrap().payload, "test");
        assert_eq!(rx3.recv().await.unwrap().payload, "test");
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

        assert!(bus.publish(test_message("fail"), &another_topic).is_err());

        assert!(rx.recv().await.is_err());
    }
}
