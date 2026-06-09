use std::future::Future;

pub trait Runnable: Send + Sync {
    fn run(&mut self) -> impl Future<Output = ()> + Send;
}
