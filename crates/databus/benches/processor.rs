mod common;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use databus::runnable::Runnable;
use tokio::runtime::Runtime;

use common::{drain, setup_processor_case};

fn processor_benches(c: &mut Criterion) {
    let runtime = Runtime::new().expect("criterion tokio runtime");
    let mut group = c.benchmark_group("databus_processor");

    for message_count in [1_usize, 4, 8, 16, 32] {
        group.throughput(Throughput::Elements(message_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(message_count),
            &message_count,
            |b, &message_count| {
                b.to_async(&runtime).iter_batched(
                    setup_processor_case,
                    |(bus, mut processor, input_sender, mut output_receiver, msg)| async move {
                        let worker = tokio::spawn(async move {
                            processor.run().await;
                        });

                        for _ in 0..message_count {
                            input_sender
                                .send(msg.clone())
                                .await
                                .expect("send to processor");
                        }
                        drop(input_sender);

                        drain(&mut output_receiver, message_count).await;
                        bus.shutdown();
                        worker.await.expect("processor task should finish");
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(processor, processor_benches);
criterion_main!(processor);
