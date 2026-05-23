use std::sync::atomic::{AtomicBool, Ordering};

use dashmap::DashMap;
use tokio::sync::mpsc;

use trie::{
    hierarchical_index::{HierarchicalIndex, HierarchicalTopic},
    trie_index::TrieIndex,
};

use crate::message::{Message};


pub struct DataBus<T: Clone + Send + Sync, const VEC_CAP: usize, const STR_CAP: usize> {
    topics: TrieIndex<Vec<mpsc::Sender<Message<T>>>, VEC_CAP, STR_CAP>,
    channel_capacity: usize,
    is_closed: AtomicBool,
}

impl<T: Clone + Send + Sync, const VEC_CAP: usize, const STR_CAP: usize> DataBus<T, VEC_CAP, STR_CAP> {
    pub fn new(channel_capacity: usize) -> Self {
        DataBus {
            topics: TrieIndex::new(),
            channel_capacity,
            is_closed: AtomicBool::new(false),
        }
    }

    pub fn shutdown(&self) {
        self.is_closed.store(true, Ordering::SeqCst);
        self.topics.clear();
    }

    pub fn subscribe(&self, topic: &HierarchicalTopic) -> Option<mpsc::Receiver<Message<T>>> {
        if self.is_closed.load(Ordering::SeqCst) {
            return None;
        }

        let (tx, rx) = mpsc::channel(self.channel_capacity);

        self.topics.entry(topic.clone()).or_default().push(tx);

        Some(rx)
    }

    pub async fn publish(&self, message: Message<T>) -> Result<(), &'static str> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Err("Bus is closed");
        }

        let topic = message.topic.clone();

        let senders = {
            let mut active_senders = Vec::new();

            if let Some(mut subscribers) = self.topics.get_mut(&topic) {
                subscribers.retain(|tx| !tx.is_closed());
                active_senders = subscribers.clone();
            }

            active_senders
        };

        self.topics
            .remove_if(&topic, |_, subscribers| subscribers.is_empty());

        for tx in senders {
            let _ = tx.send(message.clone()).await;
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

    fn topic(s: &str) -> HierarchicalTopic {
        HierarchicalTopic::new(s)
    }

    fn test_message(payload: impl Into<String>, t: &str) -> Message<String> {
        Message {
            topic: topic(t),
            header: MessageHeader {
                message_type: MessageType::Data,
                message_meta: HashMap::new(),
            },
            payload: payload.into(),
        }
    }

    #[tokio::test]
    async fn test_data_bus() {
        let bus = DataBus::<String>::new(10);
        let t = topic("test_topic");

        let mut rx = bus.subscribe(&t).unwrap();

        bus.publish(test_message("Hello, DataBus!", "test_topic"))
            .await
            .unwrap();

        let received = rx.recv().await.expect("Failed to receive message");
        assert_eq!(received.payload, "Hello, DataBus!");
    }

    #[tokio::test]
    async fn test_responding_listener() {
        let bus = Arc::new(DataBus::<String>::new(10));
        let request_topic = topic("request_topic");
        let response_topic = topic("response_topic");

        let mut request_rx = bus.subscribe(&request_topic).unwrap();
        let mut response_rx = bus.subscribe(&response_topic).unwrap();

        let bus_clone = bus.clone();
        tokio::spawn(async move {
            if let Some(_req) = request_rx.recv().await {
                bus_clone
                    .publish(test_message("Response from listener", "response_topic"))
                    .await
                    .unwrap();
            }
        });

        bus.publish(test_message("Request to listener", "request_topic"))
            .await
            .unwrap();

        let received = response_rx
            .recv()
            .await
            .expect("Failed to receive response");
        assert_eq!(received.payload, "Response from listener".to_string());
    }

    #[tokio::test]
    async fn test_multiple_listeners() {
        let bus = Arc::new(DataBus::<String>::new(10));
        let t = topic("global_events");

        let mut rx1 = bus.subscribe(&t).unwrap();
        let mut rx2 = bus.subscribe(&t).unwrap();
        let mut rx3 = bus.subscribe(&t).unwrap();

        bus.publish(test_message("test", "global_events"))
            .await
            .unwrap();

        assert_eq!(rx1.recv().await.unwrap().payload, "test");
        assert_eq!(rx2.recv().await.unwrap().payload, "test");
        assert_eq!(rx3.recv().await.unwrap().payload, "test");
    }

    #[tokio::test]
    async fn test_topic_pruning() {
        let bus = DataBus::<String>::new(10);
        let t = topic("temporary_topic");

        {
            let _rx = bus.subscribe(&t).unwrap();
            assert!(bus.topics.contains_key(&t));
        }

        bus.publish(test_message("trigger pruning", "temporary_topic"))
            .await
            .unwrap();

        assert!(
            !bus.topics.contains_key(&t),
            "Topic should have been pruned"
        );
    }

    #[tokio::test]
    async fn test_graceful_shutdown() {
        let bus = DataBus::<String>::new(10);
        let shutdown_topic = topic("shutdown_topic");
        let another_topic = topic("another_topic");
        let mut rx = bus.subscribe(&shutdown_topic).unwrap();

        bus.shutdown();

        assert!(bus.subscribe(&another_topic).is_none());

        assert!(
            bus.publish(test_message("fail", "shutdown_topic"))
                .await
                .is_err()
        );

        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_publish_without_subscribers_is_ok() {
        let bus = DataBus::<String>::new(10);
        let missing_topic = topic("missing_topic");

        assert!(
            bus.publish(test_message("no listeners", "missing_topic"))
                .await
                .is_ok()
        );
        assert!(!bus.topics.contains_key(&missing_topic));
    }
}
