use std::hint::black_box;
use std::sync::Arc;

use arrayvec::ArrayString;
use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use databus::{
    databus::DataBus,
    message::Message,
    send_receive_handles::{ReceiveHandle, SendHandle},
};
use tokio::runtime::Runtime;

const STR_CAP: usize = 16;
const CHANNEL_CAPACITY: usize = 1024;
const TOPIC: &str = "feed.nasdaq";

type BenchMessage = Arc<[u8; 1024]>;
type BenchBus = DataBus<Arc<[u8; 1024]>, STR_CAP>;
type BenchReceiver = ReceiveHandle<BenchMessage>;
type BenchSender = SendHandle<BenchMessage>;

fn topic(t: &str) -> ArrayString<STR_CAP> {
    ArrayString::from(t).unwrap()
}

fn message() -> BenchMessage {
    Arc::new([b'a'; 1024])
}

fn setup_publish_case() -> (BenchReceiver, BenchMessage, BenchSender) {
    let bus = BenchBus::new(CHANNEL_CAPACITY);

    let t = topic(TOPIC);
    bus.add_topic(t);

    let receiver = bus.subscribe(&t).expect("open bus");
    let sender = bus.get_sender(&t).expect("get sender");

    (receiver, message(), sender)
}

async fn drain(receiver: &mut BenchReceiver, publish_count: usize) {
    for _ in 0..publish_count {
        black_box(
            receiver
                .receive()
                .await
                .expect("message should be delivered"),
        );
    }
}

fn publish_benches(c: &mut Criterion) {
    let runtime = Runtime::new().expect("criterion tokio runtime");
    let mut group = c.benchmark_group("databus_publish");

    for publish_count in [1_usize, 4, 8, 16, 32] {
        group.throughput(Throughput::Elements(publish_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(publish_count),
            &publish_count,
            |b, &publish_count| {
                b.to_async(&runtime).iter_batched(
                    setup_publish_case,
                    |(mut receiver, msg, sender)| async move {
                        for _ in 0..publish_count {
                            sender.send(msg.clone()).await.expect("publish");
                        }
                        drain(&mut receiver, publish_count).await;
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(latency_with_buffer, publish_benches);
criterion_main!(latency_with_buffer);
