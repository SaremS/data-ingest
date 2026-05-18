use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageType {
    Data,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageHeader {
    pub topic: String,
    pub message_type: MessageType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message<T: Clone + Send + Sync> {
    pub header: MessageHeader,
    pub payload: T,
}

pub struct DataBus<T: Clone + Send + Sync> {
    topics: DashMap<String, Vec<mpsc::Sender<Message<T>>>>,
    channel_capacity: usize,
    is_closed: AtomicBool,
}

impl<T: Clone + Send + Sync> DataBus<T> {
    pub fn new(channel_capacity: usize) -> Self {
        DataBus {
            topics: DashMap::new(),
            channel_capacity,
            is_closed: AtomicBool::new(false),
        }
    }

    pub fn shutdown(&self) {
        self.is_closed.store(true, Ordering::SeqCst);
        self.topics.clear();
    }

    pub fn subscribe(&self, topic: &str) -> Option<mpsc::Receiver<Message<T>>> {
        if self.is_closed.load(Ordering::SeqCst) {
            return None;
        }

        let (tx, rx) = mpsc::channel(self.channel_capacity);

        self.topics.entry(topic.to_string()).or_default().push(tx);

        Some(rx)
    }

    pub async fn publish(&self, topic: &str, message: Message<T>) -> Result<(), &'static str> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Err("Bus is closed");
        }

        let senders = {
            let mut active_senders = Vec::new();

            if let Some(mut subscribers) = self.topics.get_mut(topic) {
                subscribers.retain(|tx| !tx.is_closed());
                active_senders = subscribers.clone();
            }

            active_senders
        };

        self.topics
            .remove_if(topic, |_, subscribers| subscribers.is_empty());

        for tx in senders {
            let _ = tx.send(message.clone()).await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_data_bus() {
        let bus = DataBus::<String>::new(10);

        let mut rx = bus.subscribe("test_topic").unwrap();

        let message = Message {
            header: MessageHeader {
                topic: "test_topic".to_string(),
                message_type: MessageType::Data,
            },
            payload: "Hello, DataBus!".into(),
        };

        bus.publish("test_topic", message).await.unwrap();

        let received = rx.recv().await.expect("Failed to receive message");
        assert_eq!(received.payload, "Hello, DataBus!");
    }

    #[tokio::test]
    async fn test_responding_listener() {
        let bus = Arc::new(DataBus::<String>::new(10));

        let mut request_rx = bus.subscribe("request_topic").unwrap();
        let mut response_rx = bus.subscribe("response_topic").unwrap();

        let bus_clone = bus.clone();
        tokio::spawn(async move {
            if let Some(_req) = request_rx.recv().await {
                let response_data = Message {
                    header: MessageHeader {
                        topic: "response_topic".to_string(),
                        message_type: MessageType::Data,
                    },
                    payload: "Response from listener".into(),
                };

                bus_clone
                    .publish("response_topic", response_data)
                    .await
                    .unwrap();
            }
        });

        let request_message = Message {
            header: MessageHeader {
                topic: "request_topic".to_string(),
                message_type: MessageType::Data,
            },
            payload: "Request to listener".into(),
        };
        bus.publish("request_topic", request_message).await.unwrap();

        let received = response_rx
            .recv()
            .await
            .expect("Failed to receive response");
        assert_eq!(received.payload, "Response from listener".to_string());
    }

    #[tokio::test]
    async fn test_multiple_listeners() {
        let bus = Arc::new(DataBus::<String>::new(10));

        let mut rx1 = bus.subscribe("global_events").unwrap();
        let mut rx2 = bus.subscribe("global_events").unwrap();
        let mut rx3 = bus.subscribe("global_events").unwrap();

        let msg = Message {
            header: MessageHeader {
                topic: "global_events".to_string(),
                message_type: MessageType::Data,
            },
            payload: "test".to_string(),
        };
        bus.publish("global_events", msg).await.unwrap();

        assert_eq!(rx1.recv().await.unwrap().payload, "test");
        assert_eq!(rx2.recv().await.unwrap().payload, "test");
        assert_eq!(rx3.recv().await.unwrap().payload, "test");
    }

    #[tokio::test]
    async fn test_topic_pruning() {
        let bus = DataBus::<String>::new(10);

        {
            let _rx = bus.subscribe("temporary_topic").unwrap();
            assert!(bus.topics.contains_key("temporary_topic"));
        }

        let msg = Message {
            header: MessageHeader {
                topic: "temporary_topic".to_string(),
                message_type: MessageType::Data,
            },
            payload: "trigger pruning".to_string(),
        };

        bus.publish("temporary_topic", msg).await.unwrap();

        assert!(
            !bus.topics.contains_key("temporary_topic"),
            "Topic should have been pruned"
        );
    }

    #[tokio::test]
    async fn test_graceful_shutdown() {
        let bus = DataBus::<String>::new(10);
        let mut rx = bus.subscribe("shutdown_topic").unwrap();

        bus.shutdown();

        assert!(bus.subscribe("another_topic").is_none());

        let msg = Message {
            header: MessageHeader {
                topic: "shutdown_topic".to_string(),
                message_type: MessageType::Data,
            },
            payload: "fail".to_string(),
        };
        assert!(bus.publish("shutdown_topic", msg).await.is_err());

        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_publish_without_subscribers_is_ok() {
        let bus = DataBus::<String>::new(10);

        let msg = Message {
            header: MessageHeader {
                topic: "missing_topic".to_string(),
                message_type: MessageType::Data,
            },
            payload: "no listeners".to_string(),
        };

        assert!(bus.publish("missing_topic", msg).await.is_ok());
        assert!(!bus.topics.contains_key("missing_topic"));
    }
}
