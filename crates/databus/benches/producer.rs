mod common;

use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use databus::runnable::Runnable;
use tokio::runtime::Runtime;

use common::setup_producer_case;

fn producer_benches(c: &mut Criterion) {
    let runtime = Runtime::new().expect("criterion tokio runtime");
    let mut group = c.benchmark_group("databus_producer");

    for produce_count in [1_usize, 4, 8, 16, 32] {
        group.throughput(Throughput::Elements(produce_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(produce_count),
            &produce_count,
            |b, &produce_count| {
                b.to_async(&runtime).iter_batched(
                    || {
                        (0..produce_count)
                            .map(|_| setup_producer_case())
                            .collect::<Vec<_>>()
                    },
                    |cases| async move {
                        for (mut producer, mut receiver) in cases {
                            producer.run().await;
                            black_box(
                                receiver
                                    .receive()
                                    .await
                                    .expect("producer should publish a message"),
                            );
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(producer, producer_benches);
criterion_main!(producer);
