use async_trait::async_trait;

#[async_trait]
pub trait Runnable: Send + Sync {
    async fn run(&mut self);
    async fn stop(&self);
}
