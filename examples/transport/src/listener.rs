use aloeproc::actor::ActorState;
use aloeproc::primitives::{ControlSignal, NoOutput, ProcData};
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct ListenerEvent; 
impl ProcData for ListenerEvent {}

pub struct ConnectionListener;

#[async_trait]
impl ActorState for ConnectionListener {
    type D = ListenerEvent;
    type O = NoOutput;

    async fn on_tick(&mut self) -> anyhow::Result<bool> {
        // Accept TCP connection...
        // Send "NewConnection" message to Controller or Manager?
        Ok(true)
    }

    async fn on_signal(&mut self, _sig: ControlSignal) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_data(&mut self, _data: Self::D) -> anyhow::Result<()> {
        Ok(())
    }
}