use async_trait::async_trait;
use bytes::Bytes;

use databus::{message::Message, storer::Storer};

#[derive(Clone)]
pub struct ConsoleStorer {}

#[derive(Clone)]
pub struct ConsoleStorerState {}

#[async_trait]
impl<const VEC_CAP: usize, const STR_CAP: usize> Storer<Bytes, ConsoleStorerState, VEC_CAP, STR_CAP>
    for ConsoleStorer
{
    async fn store(
        &self,
        message: Message<Bytes, VEC_CAP, STR_CAP>,
        old_state: ConsoleStorerState,
    ) -> ConsoleStorerState {
        println!("{:?}", message.payload);
        old_state
    }
}
