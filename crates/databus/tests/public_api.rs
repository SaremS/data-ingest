use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use databus::{
    databus::DataBus,
    message::{Message, MessageHeader, MessageType},
    processor::{BusProcessor, BusProcessorError, Processor},
    producer::{Producer, ProducerError, Schedule, ScheduledProducer},
    state::State,
    storer::{BusStorer, BusStorerError, Storer},
};
use tokio::sync::Mutex;

#[derive(Clone)]
struct CounterState {
    value: Arc<Mutex<i32>>,
}

impl CounterState {
    fn new(initial: i32) -> Self {
        Self {
            value: Arc::new(Mutex::new(initial)),
        }
    }
}

#[async_trait]
impl State<i32> for CounterState {
    async fn get_state(&self) -> i32 {
        *self.value.lock().await
    }

    async fn set_state(&self, state: i32) {
        *self.value.lock().await = state;
    }
}

#[derive(Clone)]
struct StringListState {
    values: Arc<Mutex<Vec<String>>>,
}

impl StringListState {
    fn new(initial: Vec<String>) -> Self {
        Self {
            values: Arc::new(Mutex::new(initial)),
        }
    }
}

#[async_trait]
impl State<Vec<String>> for StringListState {
    async fn get_state(&self) -> Vec<String> {
        self.values.lock().await.clone()
    }

    async fn set_state(&self, state: Vec<String>) {
        *self.values.lock().await = state;
    }
}

struct TestProducer;

#[async_trait]
impl Producer<String, i32> for TestProducer {
    async fn produce(&self, old_state: &i32) -> (Message<String>, i32) {
        (
            Message {
                header: MessageHeader {
                    message_type: MessageType::Data,
                    message_meta: HashMap::new(),
                },
                payload: format!("value-{}", old_state + 1),
            },
            old_state + 1,
        )
    }
}

struct TestProcessor;

#[async_trait]
impl Processor<String, i32> for TestProcessor {
    async fn process(&self, message: &Message<String>, old_state: &i32) -> (Message<String>, i32) {
        (
            Message {
                header: MessageHeader {
                    message_type: MessageType::Data,
                    message_meta: HashMap::new(),
                },
                payload: format!("{}-processed", message.payload),
            },
            old_state + 1,
        )
    }
}

struct TestStorer;

#[async_trait]
impl Storer<String, Vec<String>> for TestStorer {
    async fn store(&self, message: &Message<String>, old_state: &Vec<String>) -> Vec<String> {
        let mut new_state = old_state.clone();
        new_state.push(message.payload.clone());
        new_state
    }
}

#[tokio::test]
async fn root_exports_support_external_publish_and_subscribe() {
    let bus = DataBus::<String>::new(4);
    let mut rx = bus.subscribe("public-topic").unwrap();

    bus.publish(
        "public-topic",
        Message {
            header: MessageHeader {
                message_type: MessageType::Error,
                message_meta: HashMap::new(),
            },
            payload: "from integration test".to_string(),
        },
    )
    .await
    .unwrap();

    let received = rx.recv().await.expect("message should be received");
    assert_eq!(received.header.message_type, MessageType::Error);
    assert_eq!(received.payload, "from integration test");
}

#[test]
fn public_constructor_validation_errors_are_exposed() {
    let bus = Arc::new(DataBus::<String>::new(4));

    let producer_error = match ScheduledProducer::new(
        TestProducer,
        CounterState::new(0),
        bus.clone(),
        String::new(),
        Schedule::Once,
    ) {
        Ok(_) => panic!("expected producer creation to fail"),
        Err(err) => err,
    };

    let processor_error = match BusProcessor::new(
        TestProcessor,
        CounterState::new(0),
        bus.clone(),
        String::new(),
        "output".to_string(),
    ) {
        Ok(_) => panic!("expected processor creation to fail"),
        Err(err) => err,
    };

    let storer_error = match BusStorer::new(
        TestStorer,
        StringListState::new(Vec::new()),
        bus,
        String::new(),
    ) {
        Ok(_) => panic!("expected storer creation to fail"),
        Err(err) => err,
    };

    assert!(matches!(producer_error, ProducerError::CreationError(_)));
    assert_eq!(
        producer_error.to_string(),
        "Error Creating Producer: Topic cannot be empty"
    );

    assert!(matches!(
        processor_error,
        BusProcessorError::CreationError(_)
    ));
    assert_eq!(
        processor_error.to_string(),
        "Error Creating BusProcessor: Input topic cannot be empty"
    );

    assert!(matches!(storer_error, BusStorerError::CreationError(_)));
    assert_eq!(
        storer_error.to_string(),
        "Error Creating BusStorer: Input topic cannot be empty"
    );
}
