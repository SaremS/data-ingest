use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use arrayvec::ArrayString;
use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use databus::{
    databus::DataBus,
    message::{Message, MessageHeader, MessageType},
};
use tokio::{runtime::Runtime, sync::broadcast::Receiver};

const VEC_CAP: usize = 8;
const STR_CAP: usize = 32;
const CHANNEL_CAPACITY: usize = 1024;
const TOPIC: &str = "feed.nasdaq";

type BenchMessage = Message<Bytes>;
type BenchBus = DataBus<Bytes, STR_CAP>;
type BenchReceiver = Receiver<Arc<BenchMessage>>;

fn topic(t: &str) -> ArrayString<STR_CAP> {
    ArrayString::from(t).unwrap()
}

fn message() -> BenchMessage {
    Message {
        header: MessageHeader {
            message_type: MessageType::Data,
            message_meta: HashMap::new(),
        },
        payload: Bytes::from_static(b"benchmark-payload"),
    }
}

fn setup_publish_case(
    subscriber_count: usize,
) -> (BenchBus, Vec<BenchReceiver>, Arc<BenchMessage>) {
    let bus = BenchBus::new(CHANNEL_CAPACITY);
    let mut receivers = Vec::with_capacity(subscriber_count);

    let t = topic(TOPIC);
    bus.add_topic(t);

    for _mask in 1..=subscriber_count {
        let receiver = bus.subscribe(&t).expect("open bus");
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
    let t = topic(TOPIC);

    for subscribers in [1_usize, 4, 8, 16, 32] {
        group.throughput(Throughput::Elements(subscribers as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(subscribers),
            &subscribers,
            |b, &subscriber_count| {
                b.to_async(&runtime).iter_batched(
                    || setup_publish_case(subscriber_count),
                    |(bus, mut receivers, msg)| async move {
                        bus.publish(msg, &t).await.expect("publish");
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
