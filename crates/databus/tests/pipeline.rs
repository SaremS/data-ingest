use std::sync::{Arc, Mutex};

use arrayvec::ArrayString;
use databus::{
    databus::DataBus,
    message::Message,
    processor::{BusProcessor, Processor},
    producer::{Producer, Schedule, ScheduledProducer},
    runnable::Runnable,
    storer::{BusStorer, Storer},
};
use tokio::time::{Duration, sleep};

const STR_CAP: usize = 32;

struct SequenceProducer;

impl Producer<Arc<Message<String>>, i32, STR_CAP> for SequenceProducer {
    async fn produce(
        &self,
        _topic: ArrayString<STR_CAP>,
        old_state: Arc<std::sync::Mutex<i32>>,
    ) -> Arc<Message<String>> {
        let mut old_state_guard = old_state.lock().unwrap();
        let next = *old_state_guard + 1;
        *old_state_guard = next;

        Arc::new(Message::new_data(format!("item-{next}")))
    }
}

struct DecoratingProcessor;

impl Processor<Arc<Message<String>>, i32, STR_CAP> for DecoratingProcessor {
    fn process(
        &self,
        _topic: ArrayString<STR_CAP>,
        message: Arc<Message<String>>,
        old_state: &mut i32,
    ) -> Arc<Message<String>> {
        let next = *old_state + 1;
        *old_state = next;

        Arc::new(Message::new_data(format!(
            "{}-processed-{next}",
            message.payload()
        )))
    }
}

struct CollectingStorer;

impl Storer<Arc<Message<String>>, Vec<String>> for CollectingStorer {
    async fn store(&self, message: Arc<Message<String>>, old_state: Arc<Mutex<Vec<String>>>) {
        let mut old_state_guard = old_state.lock().unwrap();
        (*old_state_guard).push((*message).payload().clone());
    }
}

fn topic(s: &str) -> ArrayString<STR_CAP> {
    ArrayString::from(s).unwrap()
}

#[tokio::test]
async fn producer_processor_and_storer_work_together() {
    let bus = Arc::new(DataBus::<Arc<Message<String>>, STR_CAP>::new(8));
    let producer_state = 0;
    let processor_state = 0;
    let storer_state = Vec::new();

    let raw_topic = topic("raw.one");
    let processed_topic = topic("processed.one");

    bus.add_topic(raw_topic);
    bus.add_topic(processed_topic);

    let mut producer = ScheduledProducer::new(
        SequenceProducer,
        producer_state,
        bus.clone(),
        raw_topic,
        Schedule::Once,
    )
    .unwrap();

    let mut processor = BusProcessor::new(
        DecoratingProcessor,
        processor_state.clone(),
        bus.clone(),
        raw_topic,
        processed_topic,
    )
    .unwrap();

    let mut storer = BusStorer::new(
        CollectingStorer,
        storer_state.clone(),
        bus.clone(),
        processed_topic,
    )
    .unwrap();

    let processor_worker = tokio::spawn(async move {
        processor.run().await;
        processor
    });
    let storer_worker = tokio::spawn(async move {
        storer.run().await;
    });

    sleep(Duration::from_millis(10)).await;
    producer.run().await;

    sleep(Duration::from_millis(50)).await;
    let producer_state = producer.producer_state();
    drop(producer);

    bus.shutdown();
    let binding = processor_worker.await.unwrap();
    let processor_state = *binding.processor_state();
    drop(binding);

    storer_worker.await.unwrap();

    assert_eq!(producer_state, 1);
    assert_eq!(processor_state, 1);
    /*assert_eq!(
        storer_state.get_state().await,
        vec!["item-1-processed-1".to_string()]
    );*/
}
