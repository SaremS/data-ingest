mod common;

use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use common::{drain, setup_running_processor_case};

fn processor_benches(c: &mut Criterion) {
    let runtime = Runtime::new().expect("criterion tokio runtime");
    let mut group = c.benchmark_group("databus_processor");

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
                            let mut case = setup_running_processor_case();
                            let start = Instant::now();

                            for _ in 0..message_count {
                                case.input_sender
                                    .send(case.msg.clone())
                                    .await
                                    .expect("send to processor");
                            }

                            drain(&mut case.output_receiver, message_count).await;
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

criterion_group!(processor, processor_benches);
criterion_main!(processor);
