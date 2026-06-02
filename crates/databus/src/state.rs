use std::future::Future;

pub trait State<T: Clone + Send + Sync>: Send + Sync {
    fn get_state(&self) -> impl Future<Output = T> + Send;
    fn set_state(&self, state: T) -> impl Future<Output = ()> + Send;
}
