use std::collections::HashMap;
use std::sync::Arc;

use arrayvec::ArrayString;
use async_trait::async_trait;
use databus::{
    databus::DataBus,
    message::{Message, MessageHeader, MessageType},
    processor::{BusProcessor, Processor},
    producer::{Producer, Schedule, ScheduledProducer},
    runnable::Runnable,
    state::State,
    storer::{BusStorer, Storer},
};
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep, timeout};

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
struct CollectedState {
    value: Arc<Mutex<Vec<String>>>,
}

impl CollectedState {
    fn new(initial: Vec<String>) -> Self {
        Self {
            value: Arc::new(Mutex::new(initial)),
        }
    }
}

#[async_trait]
impl State<Vec<String>> for CollectedState {
    async fn get_state(&self) -> Vec<String> {
        self.value.lock().await.clone()
    }

    async fn set_state(&self, state: Vec<String>) {
        *self.value.lock().await = state;
    }
}

struct SequenceProducer;

#[async_trait]
impl Producer<String, i32, STR_CAP> for SequenceProducer {
    async fn produce(
        &self,
        _topic: ArrayString<STR_CAP>,
        old_state: i32,
    ) -> (Arc<Message<String>>, i32) {
        let next = old_state + 1;
        (
            Arc::new(Message {
                header: MessageHeader {
                    message_type: MessageType::Data,
                    message_meta: HashMap::new(),
                },
                payload: format!("item-{next}"),
            }),
            next,
        )
    }
}

struct DecoratingProcessor;

#[async_trait]
impl Processor<String, i32, STR_CAP> for DecoratingProcessor {
    async fn process(
        &self,
        _topic: ArrayString<STR_CAP>,
        message: Arc<Message<String>>,
        old_state: i32,
    ) -> (Arc<Message<String>>, i32) {
        let next = old_state + 1;
        (
            Arc::new(Message {
                header: MessageHeader {
                    message_type: MessageType::Data,
                    message_meta: HashMap::new(),
                },
                payload: format!("{}-processed-{next}", message.payload),
            }),
            next,
        )
    }
}

struct CollectingStorer;

#[async_trait]
impl Storer<String, Vec<String>> for CollectingStorer {
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
async fn producer_processor_and_storer_work_together() {
    let bus = Arc::new(DataBus::<String, STR_CAP>::new(8));
    let producer_state = CounterState::new(0);
    let processor_state = CounterState::new(0);
    let storer_state = CollectedState::new(Vec::new());

    let raw_topic = topic("raw.one");
    let processed_topic = topic("processed.one");

    bus.add_topic(raw_topic.clone());
    bus.add_topic(processed_topic.clone());

    let mut producer = ScheduledProducer::new(
        SequenceProducer,
        producer_state.clone(),
        bus.clone(),
        raw_topic.clone(),
        Schedule::Once,
    )
    .unwrap();

    let mut processor = BusProcessor::new(
        DecoratingProcessor,
        processor_state.clone(),
        bus.clone(),
        raw_topic,
        processed_topic,
    )
    .unwrap();

    let mut storer = BusStorer::new(
        CollectingStorer,
        storer_state.clone(),
        bus.clone(),
        processed_topic,
    )
    .unwrap();

    let mut processed_rx = bus.subscribe(&processed_topic).unwrap();

    let processor_worker = async {
        processor.run().await;
    };
    let storer_worker = async {
        storer.run().await;
    };
    let driver = async {
        sleep(Duration::from_millis(10)).await;
        producer.run().await;

        let received = timeout(Duration::from_millis(200), processed_rx.recv())
            .await
            .expect("timed out waiting for processed message")
            .expect("processed message should be received");

        assert_eq!(received.payload, "item-1-processed-1");

        sleep(Duration::from_millis(20)).await;
        bus.shutdown();
    };

    tokio::join!(processor_worker, storer_worker, driver);

    assert_eq!(producer_state.get_state().await, 1);
    assert_eq!(processor_state.get_state().await, 1);
    assert_eq!(
        storer_state.get_state().await,
        vec!["item-1-processed-1".to_string()]
    );
}
