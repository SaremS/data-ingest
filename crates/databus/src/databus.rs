use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering}
};

use tokio::sync::mpsc;

use trie::{
    hierarchical_index::{HierarchicalIndex, HierarchicalTopic},
    trie_index::TrieIndex,
};

use crate::message::Message;

pub struct DataBus<T: Clone + Send + Sync, const VEC_CAP: usize, const STR_CAP: usize> {
    topics: TrieIndex<mpsc::Sender<Arc<Message<T, VEC_CAP, STR_CAP>>>, VEC_CAP, STR_CAP>,
    channel_capacity: usize,
    is_closed: AtomicBool,
}

impl<T: Clone + Send + Sync, const VEC_CAP: usize, const STR_CAP: usize>
    DataBus<T, VEC_CAP, STR_CAP>
{
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

    pub fn subscribe(
        &self,
        topic_index: &HierarchicalIndex<VEC_CAP, STR_CAP>,
    ) -> Option<mpsc::Receiver<Arc<Message<T, VEC_CAP, STR_CAP>>>> {
        if self.is_closed.load(Ordering::SeqCst) {
            return None;
        }

        let (tx, rx) = mpsc::channel(self.channel_capacity);

        self.topics.insert_and_set_at_index(topic_index, tx);

        Some(rx)
    }

    pub async fn publish(&self, message: Arc<Message<T, VEC_CAP, STR_CAP>>) -> Result<(), &'static str> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Err("Bus is closed");
        }

        let topic = message.topic.clone();

        let senders = {
            let mut active_senders = Vec::new();

            for tx in self.topics.get_at_index(&topic) {
                if !tx.is_closed() {
                    active_senders.push(tx.clone());
                }
            }

            active_senders
        };

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

    fn topic(s: &str) -> HierarchicalTopic<3, 10> {
        HierarchicalTopic::from_str(s).unwrap()
    }

    fn index(s: &str) -> HierarchicalIndex<3, 10> {
        HierarchicalIndex::from_str(s).unwrap()
    }

    fn test_message(payload: impl Into<String>, t: &str) -> Arc<Message<String, 3, 10>> {
        Arc::new(
        Message {
            topic: topic(t),
            header: MessageHeader {
                message_type: MessageType::Data,
                message_meta: HashMap::new(),
            },
            payload: payload.into(),
        }
        )
    }

    #[tokio::test]
    async fn test_data_bus() {
        let bus = DataBus::<String, 3, 10>::new(10);
        let t = index("testtopic");

        let mut rx = bus.subscribe(&t).unwrap();

        bus.publish(test_message("Hello, DataBus!", "testtopic"))
            .await
            .unwrap();

        let received = rx.recv().await.expect("Failed to receive message");
        assert_eq!(received.payload, "Hello, DataBus!");
    }

    #[tokio::test]
    async fn test_responding_listener() {
        let bus = Arc::new(DataBus::<String, 3, 10>::new(10));
        let request_index = index("request");
        let response_index = index("response");

        let mut request_rx = bus.subscribe(&request_index).unwrap();
        let mut response_rx = bus.subscribe(&response_index).unwrap();

        let bus_clone = bus.clone();
        tokio::spawn(async move {
            if let Some(_req) = request_rx.recv().await {
                bus_clone
                    .publish(test_message("Response from listener", "response"))
                    .await
                    .unwrap();
            }
        });

        bus.publish(test_message("Request to listener", "request"))
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
        let bus = Arc::new(DataBus::<String, 3, 10>::new(10));
        let mut rx1 = bus.subscribe(&index("global.one")).unwrap();
        let mut rx2 = bus.subscribe(&index("global.*")).unwrap();
        let mut rx3 = bus.subscribe(&index("*.*")).unwrap();

        bus.publish(test_message("test", "global.one"))
            .await
            .unwrap();

        assert_eq!(rx1.recv().await.unwrap().payload, "test");
        assert_eq!(rx2.recv().await.unwrap().payload, "test");
        assert_eq!(rx3.recv().await.unwrap().payload, "test");
    }

    #[tokio::test]
    async fn test_graceful_shutdown() {
        let bus = DataBus::<String, 3, 10>::new(10);
        let shutdown_topic = index("shutdown");
        let another_topic = index("another");
        let mut rx = bus.subscribe(&shutdown_topic).unwrap();

        bus.shutdown();

        assert!(bus.subscribe(&another_topic).is_none());

        assert!(bus.publish(test_message("fail", "shutdown")).await.is_err());

        assert!(rx.recv().await.is_none());
    }
}
