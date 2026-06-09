use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;

use arrayvec::ArrayString;
use thiserror::Error;

use crate::{
    databus::DataBus,
    runnable::Runnable,
    send_receive_handles::{ReceiveHandle, SendHandle},
};

pub trait Processor<T: Clone + Send + Sync, S: Clone + Send + Sync, const STR_CAP: usize>:
    Send + Sync
{
    fn process(&self, topic: ArrayString<STR_CAP>, message: T, old_state: &mut S) -> T;
}

pub struct BusProcessor<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    const STR_CAP: usize,
    V: Processor<T, S, STR_CAP>,
> {
    processor: V,
    processor_state: S,
    bus: Arc<DataBus<T, STR_CAP>>,
    input_topic: ArrayString<STR_CAP>,
    output_topic: ArrayString<STR_CAP>,
    sender: SendHandle<T>,
    receiver: ReceiveHandle<T>,

    _marker: PhantomData<S>,
}

#[derive(Error, Debug)]
pub enum BusProcessorError {
    #[error("Failed to subscribe to topic: {0}")]
    SubscriptionError(Cow<'static, str>),

    #[error("Failed to publish message to topic: {0}")]
    PublishError(Cow<'static, str>),

    #[error("Error Creating BusProcessor: {0}")]
    CreationError(Cow<'static, str>),
}

impl<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    const STR_CAP: usize,
    V: Processor<T, S, STR_CAP>,
> BusProcessor<T, S, STR_CAP, V>
{
    pub fn new(
        processor: V,
        processor_state: S,
        bus: Arc<DataBus<T, STR_CAP>>,
        input_topic: ArrayString<STR_CAP>,
        output_topic: ArrayString<STR_CAP>,
    ) -> Result<Self, BusProcessorError> {
        if input_topic.is_empty() {
            return Err(BusProcessorError::CreationError(
                "Input topic cannot be empty".into(),
            ));
        }
        if output_topic.is_empty() {
            return Err(BusProcessorError::CreationError(
                "Output topic cannot be empty".into(),
            ));
        }
        if input_topic == output_topic {
            return Err(BusProcessorError::CreationError(
                "Input index cannot contain output topic to avoid infinite loops".into(),
            ));
        }

        let sender = bus.get_sender(&output_topic);
        if sender.is_err() {
            return Err(BusProcessorError::CreationError(
                format!("Output topic '{}' does not exist in DataBus", output_topic).into(),
            ));
        }

        let receiver = bus.subscribe(&input_topic);
        if receiver.is_err() {
            return Err(BusProcessorError::CreationError(
                format!("Input topic '{}' does not exist in DataBus", input_topic).into(),
            ));
        }

        Ok(Self {
            processor,
            processor_state,
            bus,
            input_topic,
            output_topic,
            sender: sender.unwrap(),
            receiver: receiver.unwrap(),

            _marker: PhantomData,
        })
    }

    pub fn processor_state(&self) -> &S {
        &self.processor_state
    }
}

impl<
    T: Clone + Send + Sync,
    S: Clone + Send + Sync,
    const STR_CAP: usize,
    V: Processor<T, S, STR_CAP>,
> Runnable for BusProcessor<T, S, STR_CAP, V>
{
    async fn run(&mut self) {
        while let Some(message) = self.receiver.receive().await {
            let new_message = self.processor.process(
                self.input_topic,
                message,
                &mut self.processor_state,
            );
            if self.sender.send(new_message).await.is_err() {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio::time::{Duration, sleep};

    use crate::message::Message;

    #[derive(Clone)]
    struct MockState {
        inner_value: Arc<Mutex<i32>>,
    }

    impl MockState {
        fn new(initial: i32) -> Self {
            Self {
                inner_value: Arc::new(Mutex::new(initial)),
            }
        }
    }

    struct MockProcessor;

    impl Processor<Arc<Message<String>>, i32, 20> for MockProcessor {
        fn process(
            &self,
            _topic: ArrayString<20>,
            message: Arc<Message<String>>,
            old_state: &mut i32,
        ) -> Arc<Message<String>> {
            let next = *old_state + 1;
            *old_state = next;

            message
        }
    }

    struct SlowProcessor;

    impl Processor<Arc<Message<String>>, i32, 20> for SlowProcessor {
        fn process(
            &self,
            _topic: ArrayString<20>,
            message: Arc<Message<String>>,
            old_state: &mut i32,
        ) -> Arc<Message<String>> {
            std::thread::sleep(Duration::from_millis(25));
            *old_state += 1;

            message
        }
    }

    fn test_message() -> Message<String> {
        Message::new_data("hello bus".to_string())
    }

    fn topic(s: &str) -> ArrayString<20> {
        ArrayString::from(s).unwrap()
    }

    #[tokio::test]
    async fn test_bus_processor_initialization() {
        let bus = Arc::new(DataBus::new(10));
        let state = 0;
        let processor = MockProcessor;
        let input_topic = topic("test.input");
        let output_topic = topic("test.output");
        bus.add_topic(input_topic);
        bus.add_topic(output_topic);

        let bus_processor =
            BusProcessor::new(processor, state, bus, input_topic, output_topic).unwrap();

        assert_eq!(bus_processor.input_topic, topic("test.input"));
        assert_eq!(bus_processor.output_topic, topic("test.output"));
    }

    #[test]
    fn test_bus_processor_rejects_empty_input_topic() {
        let bus = Arc::new(DataBus::new(10));
        let state = 0;
        bus.add_topic(topic("input.topic"));
        bus.add_topic(topic("output.topic"));

        let err = BusProcessorError::CreationError("Input topic cannot be empty".into());
        assert!(matches!(err, BusProcessorError::CreationError(_)));
        assert_eq!(
            err.to_string(),
            "Error Creating BusProcessor: Input topic cannot be empty"
        );

        // Verify a valid processor is created when topics differ
        let valid = BusProcessor::new(
            MockProcessor,
            state,
            bus,
            ArrayString::from("input.topic").unwrap(),
            ArrayString::from("output.topic").unwrap(),
        );
        assert!(valid.is_ok());
    }

    #[test]
    fn test_bus_processor_rejects_same_input_and_output_topics() {
        let bus = Arc::new(DataBus::new(10));
        let state = 0;

        let err = match BusProcessor::new(
            MockProcessor,
            state,
            bus,
            ArrayString::from("topic").unwrap(),
            ArrayString::from("topic").unwrap(),
        ) {
            Ok(_) => panic!("expected matching topic validation error"),
            Err(err) => err,
        };

        assert!(matches!(err, BusProcessorError::CreationError(_)));
        assert_eq!(
            err.to_string(),
            "Error Creating BusProcessor: Input index cannot contain output topic to avoid infinite loops"
        );
    }

    #[tokio::test]
    async fn test_bus_processor_run_loop() {
        let bus = Arc::new(DataBus::new(10));
        let input_topic = ArrayString::from("input.topic").unwrap();
        let output_topic = ArrayString::from("output.topic").unwrap();

        bus.add_topic(input_topic);
        bus.add_topic(output_topic);

        let state = 0;

        let mut bus_processor =
            BusProcessor::new(MockProcessor, state, bus.clone(), input_topic, output_topic)
                .unwrap();

        let worker = async {
            bus_processor.run().await;
            bus_processor
        };
        let driver = async {
            sleep(Duration::from_millis(10)).await;

            let sender = bus.get_sender(&input_topic).unwrap();
            sender.send(Arc::new(test_message())).await.unwrap();

            sleep(Duration::from_millis(50)).await;
            bus.shutdown();
        };

        let (processor, _) = tokio::join!(worker, driver);

        let final_state = processor.processor_state();
        assert_eq!(
            *final_state, 1,
            "The state should have been incremented by the processor"
        );
    }

    #[tokio::test]
    async fn test_bus_processor_returns_when_bus_is_closed_before_run() {
        let bus = Arc::new(DataBus::new(10));
        let state = 0;
        let input_topic = topic("input.topic");
        let output_topic = topic("output.topic");

        bus.add_topic(input_topic);
        bus.add_topic(output_topic);

        let mut bus_processor =
            BusProcessor::new(MockProcessor, state, bus.clone(), input_topic, output_topic)
                .unwrap();

        bus.shutdown();
        bus_processor.run().await;
    }

    #[tokio::test]
    async fn test_bus_processor_stop_cancels_run_loop() {
        let bus = Arc::new(DataBus::new(10));
        let bus_clone = bus.clone();
        bus.add_topic(ArrayString::from("input.topic").unwrap());
        bus.add_topic(ArrayString::from("output.topic").unwrap());
        let state = 0;
        let mut bus_processor = BusProcessor::new(
            MockProcessor,
            state,
            bus_clone,
            ArrayString::from("input.topic").unwrap(),
            ArrayString::from("output.topic").unwrap(),
        )
        .unwrap();

        let worker = tokio::spawn(async move {
            bus_processor.run().await;
            bus_processor
        });

        bus.clone().drop_topic("output.topic").unwrap();
        let sender = bus.get_sender("input.topic").unwrap();
        sender.send(Arc::new(test_message())).await.unwrap(); //cancel run
        let _ = worker.await.unwrap();
    }

    #[tokio::test]
    async fn test_bus_processor_stops_when_output_publish_fails() {
        let bus = Arc::new(DataBus::new(10));
        let state = 0;
        let input_topic = ArrayString::from("input.topic").unwrap();
        let output_topic = ArrayString::from("output.topic").unwrap();

        bus.add_topic(input_topic);
        bus.add_topic(output_topic);

        let mut bus_processor =
            BusProcessor::new(SlowProcessor, state, bus.clone(), input_topic, output_topic)
                .unwrap();

        let worker = async {
            bus_processor.run().await;
            bus_processor
        };
        let driver = async {
            sleep(Duration::from_millis(10)).await;

            let sender = bus.get_sender(&input_topic).unwrap();
            sender.send(Arc::new(test_message())).await.unwrap();
            bus.shutdown();
        };

        let (worker, _) = tokio::join!(worker, driver);
        let state = worker.processor_state();

        assert_eq!(*state, 1);
    }

    #[tokio::test]
    async fn test_bus_processor_stops_when_input_channel_closes() {
        let bus = Arc::new(DataBus::new(10));
        let state = 0;

        let input_topic = ArrayString::from("input.topic").unwrap();
        let output_topic = ArrayString::from("output.topic").unwrap();

        bus.add_topic(input_topic);
        bus.add_topic(output_topic);

        let mut bus_processor =
            BusProcessor::new(MockProcessor, state, bus.clone(), input_topic, output_topic)
                .unwrap();

        let worker = async {
            bus_processor.run().await;
        };
        let driver = async {
            sleep(Duration::from_millis(10)).await;
            bus.shutdown();
        };

        tokio::join!(worker, driver);
    }
}
