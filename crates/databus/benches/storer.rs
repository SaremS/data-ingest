mod common;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use databus::runnable::Runnable;
use tokio::runtime::Runtime;

use common::{await_stores, setup_storer_case};

fn storer_benches(c: &mut Criterion) {
    let runtime = Runtime::new().expect("criterion tokio runtime");
    let mut group = c.benchmark_group("databus_storer");

    for message_count in [1_usize, 4, 8, 16, 32] {
        group.throughput(Throughput::Elements(message_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(message_count),
            &message_count,
            |b, &message_count| {
                b.to_async(&runtime).iter_batched(
                    setup_storer_case,
                    |(bus, mut storer, input_sender, mut completions, msg)| async move {
                        let worker = tokio::spawn(async move {
                            storer.run().await;
                        });

                        for _ in 0..message_count {
                            input_sender
                                .send(msg.clone())
                                .await
                                .expect("send to storer");
                        }
                        drop(input_sender);

                        await_stores(&mut completions, message_count).await;
                        bus.shutdown();
                        worker.await.expect("storer task should finish");
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(storer, storer_benches);
criterion_main!(storer);
