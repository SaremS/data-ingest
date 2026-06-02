use bytes::Bytes;
use std::sync::Arc;

use databus::{message::Message, storer::Storer};

#[derive(Clone)]
pub struct ConsoleStorer {}

#[derive(Clone)]
pub struct ConsoleStorerState {}

impl Storer<Bytes, ConsoleStorerState> for ConsoleStorer {
    async fn store(
        &self,
        message: Arc<Message<Bytes>>,
        old_state: ConsoleStorerState,
    ) -> ConsoleStorerState {
        println!("{:?}", message.payload());
        old_state
    }
}
