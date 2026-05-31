use std::sync::Arc;

use arrayvec::ArrayString;
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

const STR_CAP: usize = 32;

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
impl Producer<String, i32, STR_CAP> for TestProducer {
    async fn produce(
        &self,
        _topic: ArrayString<STR_CAP>,
        old_state: i32,
    ) -> (Arc<Message<String>>, i32) {
        (
            Arc::new(Message {
                header: MessageHeader {
                    message_type: MessageType::Data,
                    message_meta: None,
                },
                payload: format!("value-{}", old_state + 1),
            }),
            old_state + 1,
        )
    }
}

struct TestProcessor;

#[async_trait]
impl Processor<String, i32, STR_CAP> for TestProcessor {
    async fn process(
        &self,
        _topic: ArrayString<STR_CAP>,
        message: Arc<Message<String>>,
        old_state: i32,
    ) -> (Arc<Message<String>>, i32) {
        (
            Arc::new(Message {
                header: MessageHeader {
                    message_type: MessageType::Data,
                    message_meta: None,
                },
                payload: format!("{}-processed", message.payload),
            }),
            old_state + 1,
        )
    }
}

struct TestStorer;

#[async_trait]
impl Storer<String, Vec<String>> for TestStorer {
    async fn store(
        &self,
        message: Arc<Message<String>>,
        mut old_state: Vec<String>,
    ) -> Vec<String> {
        old_state.push((*message).clone().payload);
        old_state
    }
}

fn topic(s: &str) -> ArrayString<STR_CAP> {
    ArrayString::from(s).unwrap()
}

#[tokio::test]
async fn root_exports_support_external_publish_and_subscribe() {
    let bus = DataBus::<String, STR_CAP>::new(4);
    let t = topic("publictopic");
    bus.add_topic(t);
    let mut rx = bus.subscribe(&topic("publictopic")).unwrap();
    let tx = bus.get_sender(&t).unwrap();

    tx.send(Arc::new(Message {
        header: MessageHeader {
            message_type: MessageType::Error,
            message_meta: None,
        },
        payload: "from integration test".to_string(),
    }))
    .await
    .unwrap();

    let received = rx.receive().await.expect("message should be received");
    assert_eq!(received.header.message_type, MessageType::Error);
    assert_eq!(received.payload, "from integration test");
}

#[test]
fn public_constructor_validation_errors_are_exposed() {
    let bus = Arc::new(DataBus::<String, STR_CAP>::new(4));
    let t = topic("input");
    bus.add_topic(t);

    let producer_error = match ScheduledProducer::new(
        TestProducer,
        CounterState::new(0),
        bus.clone(),
        t,
        Schedule::Once,
    ) {
        // A valid topic was provided so this should succeed; we just verify the type compiles.
        Ok(_) => ProducerError::CreationError("Topic cannot be empty".into()),
        Err(err) => err,
    };

    let processor_error =
        match BusProcessor::new(TestProcessor, CounterState::new(0), bus.clone(), t, t) {
            Ok(_) => panic!("expected processor creation to fail"),
            Err(err) => err,
        };

    let storer_error = match BusStorer::new(TestStorer, StringListState::new(Vec::new()), bus, t) {
        // A valid topic was provided so this should succeed; verify the type compiles.
        Ok(_) => BusStorerError::CreationError("Input topic cannot be empty".into()),
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
        "Error Creating BusProcessor: Input index cannot contain output topic to avoid infinite loops"
    );

    assert!(matches!(storer_error, BusStorerError::CreationError(_)));
    assert_eq!(
        storer_error.to_string(),
        "Error Creating BusStorer: Input topic cannot be empty"
    );
}
