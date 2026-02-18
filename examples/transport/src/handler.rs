// src/handler.rs
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct HandlerMsg; 
impl aloeproc::primitives::ProcData for HandlerMsg {}

pub struct MessageHandler;
#[async_trait]
impl aloeproc::actor::ActorState for MessageHandler {
    type D = HandlerMsg;
    type O = aloeproc::primitives::NoOutput;
    async fn on_tick(&mut self) -> anyhow::Result<bool> { Ok(true) }
    async fn on_signal(&mut self, _: aloeproc::primitives::ControlSignal) -> anyhow::Result<()> { Ok(()) }
    async fn on_data(&mut self, _: Self::D) -> anyhow::Result<()> { Ok(()) }
}