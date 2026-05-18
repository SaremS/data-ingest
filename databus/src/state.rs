use async_trait::async_trait;

#[async_trait]
pub trait State<T: Send + Sync>: Send + Sync {
    async fn get_state(&self) -> T;
    async fn set_state(&self, state: T);
}
