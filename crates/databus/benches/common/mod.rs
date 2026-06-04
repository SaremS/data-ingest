#![allow(dead_code)]

use std::hint::black_box;
use std::sync::Arc;

use arrayvec::ArrayString;
use bytes::Bytes;
use databus::{
    databus::DataBus,
    message::Message,
    processor::{BusProcessor, Processor},
    producer::{Producer, Schedule, ScheduledProducer},
    send_receive_handles::{ReceiveHandle, SendHandle},
    state::State,
    storer::{BusStorer, Storer},
};
use tokio::sync::{
    Mutex,
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};

pub const STR_CAP: usize = 32;
pub const CHANNEL_CAPACITY: usize = 1024;
pub const TOPIC: &str = "feed.nasdaq";
pub const INPUT_TOPIC: &str = "feed.raw";
pub const OUTPUT_TOPIC: &str = "feed.processed";

pub type BenchMessage = Message<Bytes>;
pub type BenchBus = Arc<DataBus<Arc<BenchMessage>, STR_CAP>>;
pub type BenchReceiver = ReceiveHandle<Arc<BenchMessage>>;
pub type BenchSender = SendHandle<Arc<BenchMessage>>;
pub type BenchProducerRunner =
    ScheduledProducer<Arc<Message<Bytes>>, usize, STR_CAP, BenchProducer>;
pub type BenchProcessorRunner = BusProcessor<Arc<Message<Bytes>>, usize, STR_CAP, BenchProcessor>;
pub type BenchStorerRunner = BusStorer<Bytes, usize, BenchState, STR_CAP, BenchStorer>;

pub fn topic(t: &str) -> ArrayString<STR_CAP> {
    ArrayString::from(t).unwrap()
}

pub fn message() -> BenchMessage {
    Message::new_data(Bytes::from_static(b"benchmark-payload"))
}

pub fn setup_publish_case() -> (BenchReceiver, Arc<BenchMessage>, BenchSender) {
    let bus = DataBus::new(CHANNEL_CAPACITY);

    let t = topic(TOPIC);
    bus.add_topic(t);

    let receiver = bus.subscribe(&t).expect("open bus");
    let sender = bus.get_sender(&t).expect("get sender");

    (receiver, Arc::new(message()), sender)
}

pub async fn drain(receiver: &mut BenchReceiver, publish_count: usize) {
    for _ in 0..publish_count {
        black_box(
            receiver
                .receive()
                .await
                .expect("message should be delivered"),
        );
    }
}

#[derive(Clone)]
pub struct BenchState {
    value: Arc<Mutex<usize>>,
}

impl BenchState {
    pub fn new(initial: usize) -> Self {
        Self {
            value: Arc::new(Mutex::new(initial)),
        }
    }
}

impl State<usize> for BenchState {
    async fn get_state(&self) -> usize {
        *self.value.lock().await
    }

    async fn set_state(&self, state: usize) {
        *self.value.lock().await = state;
    }
}

pub struct BenchProducer;

impl Producer<Arc<Message<Bytes>>, usize, STR_CAP> for BenchProducer {
    async fn produce(
        &self,
        _topic: ArrayString<STR_CAP>,
        old_state: Arc<std::sync::Mutex<usize>>,
    ) -> Arc<Message<Bytes>> {
        let mut state_guard = old_state.lock().unwrap();
        let next_state = *state_guard + 1;
        *state_guard = next_state;

        Arc::new(message())
    }
}

pub struct BenchProcessor;

impl Processor<Arc<Message<Bytes>>, usize, STR_CAP> for BenchProcessor {
    async fn process(
        &self,
        _topic: ArrayString<STR_CAP>,
        message: Arc<Message<Bytes>>,
        old_state: Arc<std::sync::Mutex<usize>>,
    ) -> Arc<Message<Bytes>> {
        let mut old_state_guard = old_state.lock().unwrap();
        *old_state_guard += 1;

        message
    }
}

pub struct BenchStorer {
    completions: UnboundedSender<()>,
}

impl Storer<Bytes, usize> for BenchStorer {
    async fn store(&self, _message: Arc<Message<Bytes>>, old_state: usize) -> usize {
        self.completions.send(()).expect("record store completion");
        old_state + 1
    }
}

pub fn setup_producer_case() -> (BenchProducerRunner, BenchReceiver) {
    let bus: BenchBus = Arc::new(DataBus::new(CHANNEL_CAPACITY));
    let output_topic = topic(TOPIC);
    bus.add_topic(output_topic);

    let receiver = bus.subscribe(&output_topic).expect("subscribe producer");
    let producer = ScheduledProducer::new(BenchProducer, 0, bus, output_topic, Schedule::Once)
        .expect("create producer");

    (producer, receiver)
}

pub fn setup_processor_case() -> (
    BenchBus,
    BenchProcessorRunner,
    BenchSender,
    BenchReceiver,
    Arc<BenchMessage>,
) {
    let bus: BenchBus = Arc::new(DataBus::new(CHANNEL_CAPACITY));
    let input_topic = topic(INPUT_TOPIC);
    let output_topic = topic(OUTPUT_TOPIC);

    bus.add_topic(input_topic);
    bus.add_topic(output_topic);

    let input_sender = bus.get_sender(&input_topic).expect("get input sender");
    let output_receiver = bus.subscribe(&output_topic).expect("subscribe output");
    let processor = BusProcessor::new(BenchProcessor, 0, bus.clone(), input_topic, output_topic)
        .expect("create processor");

    (
        bus,
        processor,
        input_sender,
        output_receiver,
        Arc::new(message()),
    )
}

pub fn setup_storer_case() -> (
    BenchBus,
    BenchStorerRunner,
    BenchSender,
    UnboundedReceiver<()>,
    Arc<BenchMessage>,
) {
    let bus: BenchBus = Arc::new(DataBus::new(CHANNEL_CAPACITY));
    let input_topic = topic(INPUT_TOPIC);

    bus.add_topic(input_topic);

    let input_sender = bus.get_sender(&input_topic).expect("get input sender");
    let (completion_tx, completion_rx) = unbounded_channel();
    let storer = BusStorer::new(
        BenchStorer {
            completions: completion_tx,
        },
        BenchState::new(0),
        bus.clone(),
        input_topic,
    )
    .expect("create storer");

    (
        bus,
        storer,
        input_sender,
        completion_rx,
        Arc::new(message()),
    )
}

pub async fn await_stores(receiver: &mut UnboundedReceiver<()>, store_count: usize) {
    for _ in 0..store_count {
        black_box(receiver.recv().await.expect("store should complete"));
    }
}
