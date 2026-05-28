use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use databus::{
    databus::DataBus,
    message::{Message, MessageHeader, MessageType},
};
use tokio::{runtime::Runtime, sync::mpsc::Receiver};
use trie::hierarchical_index::{HierarchicalIndex, HierarchicalTopic};

const VEC_CAP: usize = 8;
const STR_CAP: usize = 32;
const CHANNEL_CAPACITY: usize = 1024;
const TOPIC_PARTS: [&str; 5] = ["feed", "nasdaq", "equities", "aapl", "quote"];

type BenchMessage = Message<Bytes, VEC_CAP, STR_CAP>;
type BenchBus = DataBus<Bytes, VEC_CAP, STR_CAP>;
type BenchReceiver = Receiver<Arc<BenchMessage>>;

fn topic(parts: &[&str]) -> HierarchicalTopic<VEC_CAP, STR_CAP> {
    HierarchicalTopic::from_str(&parts.join(".")).expect("valid topic")
}

fn wildcard_index(mask: usize) -> HierarchicalIndex<VEC_CAP, STR_CAP> {
    let pattern = TOPIC_PARTS
        .iter()
        .enumerate()
        .map(|(position, part)| {
            if (mask & (1 << position)) == 0 {
                "*"
            } else {
                part
            }
        })
        .collect::<Vec<_>>()
        .join(".");

    HierarchicalIndex::from_str(&pattern).expect("valid index")
}

fn message() -> BenchMessage {
    Message {
        topic: topic(&TOPIC_PARTS),
        header: MessageHeader {
            message_type: MessageType::Data,
            message_meta: HashMap::new(),
        },
        payload: Bytes::from_static(b"benchmark-payload"),
    }
}

fn setup_publish_case(subscriber_count: usize) -> (BenchBus, Vec<BenchReceiver>, Arc<BenchMessage>) {
    let bus = BenchBus::new(CHANNEL_CAPACITY);
    let mut receivers = Vec::with_capacity(subscriber_count);

    for mask in 1..=subscriber_count {
        let receiver = bus.subscribe(&wildcard_index(mask)).expect("open bus");
        receivers.push(receiver);
    }

    (bus, receivers, Arc::new(message()))
}

async fn drain(receivers: &mut [BenchReceiver]) {
    for receiver in receivers {
        black_box(receiver.recv().await.expect("message should be delivered"));
    }
}

fn publish_benches(c: &mut Criterion) {
    let runtime = Runtime::new().expect("criterion tokio runtime");
    let mut group = c.benchmark_group("databus_publish");

    for subscribers in [1_usize, 4, 8, 16, 32] {
        group.throughput(Throughput::Elements(subscribers as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(subscribers),
            &subscribers,
            |b, &subscriber_count| {
                b.to_async(&runtime).iter_batched(
                    || setup_publish_case(subscriber_count),
                    |(bus, mut receivers, msg)| async move {
                        bus.publish(msg.clone()).await.expect("publish");
                        drain(&mut receivers).await;
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(latency, publish_benches);
criterion_main!(latency);
