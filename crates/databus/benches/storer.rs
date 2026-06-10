mod common;

use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use common::{await_stores, setup_running_storer_case};

fn storer_benches(c: &mut Criterion) {
    let runtime = Runtime::new().expect("criterion tokio runtime");
    let mut group = c.benchmark_group("databus_storer");

    for message_count in [1_usize, 4, 8, 16, 32] {
        group.throughput(Throughput::Elements(message_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(message_count),
            &message_count,
            |b, &message_count| {
                b.iter_custom(|iters| {
                    runtime.block_on(async move {
                        let mut elapsed = Duration::ZERO;

                        for _ in 0..iters {
                            let mut case = setup_running_storer_case();
                            let start = Instant::now();

                            for _ in 0..message_count {
                                case.input_sender
                                    .send(case.msg.clone())
                                    .await
                                    .expect("send to storer");
                            }

                            await_stores(&mut case.completions, message_count).await;
                            elapsed += start.elapsed();
                            case.finish().await;
                        }

                        elapsed
                    })
                });
            },
        );
    }

    group.finish();
}

criterion_group!(storer, storer_benches);
criterion_main!(storer);
