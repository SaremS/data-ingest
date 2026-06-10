mod common;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use common::{drain, setup_publish_case};

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

criterion_group!(databus, publish_benches);
criterion_main!(databus);
