use async_trait::async_trait;
use std::sync::Arc;

use crate::{DataBus, Message};

pub enum Schedule {
    Once,
    Interval(u64),
}

impl Schedule {
    pub fn next_run(&self) -> Option<std::time::Duration> {
        match self {
            Schedule::Once => None,
            Schedule::Interval(millis) => Some(std::time::Duration::from_millis(*millis)),
        }
    }
}

#[async_trait]
pub trait Producer<T: Clone + Send + Sync>: Send + Sync {
    async fn produce(&self) -> Message<T>;
}

pub struct ScheduledProducer<T: Clone + Send + Sync, S: Producer<T>> {
    schedule: Schedule,
    bus: Arc<DataBus<T>>,
    topic: String,
    producer: S,
}

impl<T: Clone + Send + Sync, S: Producer<T>> ScheduledProducer<T, S> {
    pub fn new(schedule: Schedule, bus: Arc<DataBus<T>>, topic: String, producer: S) -> Self {
        ScheduledProducer {
            schedule,
            bus,
            topic,
            producer,
        }
    }

    pub async fn start(&self) {
        loop {
            let message = self.producer.produce().await;
            if let Err(_e) = self.bus.publish(&self.topic, message).await {
                break;
            }

            if let Some(duration) = self.schedule.next_run() {
                tokio::time::sleep(duration).await;
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MessageHeader, MessageType};
    use std::sync::Arc;

    struct TestProducer;

    #[async_trait]
    impl Producer<String> for TestProducer {
        async fn produce(&self) -> Message<String> {
            Message {
                header: MessageHeader {
                    topic: "request".to_string(),
                    message_type: MessageType::Data,
                },
                payload: "test data".into(),
            }
        }
    }

    #[tokio::test]
    async fn test_scheduled_produce() {
        let bus = Arc::new(DataBus::<String>::new(10));
        let producer = TestProducer;
        let scheduled_producer = ScheduledProducer::new(
            Schedule::Once,
            bus.clone(),
            "test_topic".to_string(),
            producer,
        );

        let mut rx = bus.subscribe("test_topic").unwrap();

        scheduled_producer.start().await;

        let received = rx.recv().await.expect("Failed to receive message");
        assert_eq!(received.payload, "test data");
    }

    #[tokio::test]
    async fn test_interval_produce() {
        let bus = Arc::new(DataBus::<String>::new(10));
        let producer = TestProducer;

        let scheduled_producer = ScheduledProducer::new(
            Schedule::Interval(10),
            bus.clone(),
            "test_topic".to_string(),
            producer,
        );

        let mut rx = bus.subscribe("test_topic").unwrap();

        let shutdown_bus = bus.clone();

        tokio::spawn(async move {
            scheduled_producer.start().await;
        });

        let msg1 = rx.recv().await.expect("Failed to receive message 1");
        assert_eq!(msg1.payload, "test data");

        let msg2 = rx.recv().await.expect("Failed to receive message 2");
        assert_eq!(msg2.payload, "test data");

        let msg3 = rx.recv().await.expect("Failed to receive message 3");
        assert_eq!(msg3.payload, "test data");

        shutdown_bus.shutdown();
    }
}
