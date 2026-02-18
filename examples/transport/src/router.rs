// src/router.rs
use aloeproc::actor::ActorState;
use aloeproc::primitives::{ControlSignal, NoOutput, ProcData};
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct RouterMsg; 
impl ProcData for RouterMsg {}

pub struct PacketRouter;
#[async_trait]
impl ActorState for PacketRouter {
    type D = RouterMsg;
    type O = NoOutput;
    async fn on_tick(&mut self) -> anyhow::Result<bool> { Ok(true) }
    async fn on_signal(&mut self, _: ControlSignal) -> anyhow::Result<()> { Ok(()) }
    async fn on_data(&mut self, _: Self::D) -> anyhow::Result<()> { Ok(()) }
}