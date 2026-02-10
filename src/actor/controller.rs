// src/actor/controller.rs
use crate::actor::{ActorState, Orchestrator, orchestrator};
use crate::primitives::{ControlSignal, ProcData, OrchestratorStrategy};
use async_trait::async_trait;

pub struct ControllerData; 
impl ProcData for ControllerData {}


pub struct Controller<S: ActorState + 'static> {
    orchestrators: Vec<Orchestrator<S>>,
    
}

#[async_trait]
impl <S: ActorState + 'static> ActorState for Controller<S> {
    type D = ControllerData;

    async fn on_tick(&mut self) -> anyhow::Result<bool> {
        // for orchestrator in self.orchestrators{
        //     orchestrator.maintain().await;
        // }
        Ok(true)
    }

    async fn on_signal(&mut self, sig: ControlSignal) -> anyhow::Result<()> {
        // Propagate signals down the tree
        if sig == ControlSignal::Stop {
            // self.connections.broadcast(ControlSignal::Stop).await;
            // self.listener.broadcast(ControlSignal::Stop).await;
        }
        Ok(())
    }

    async fn on_data(&mut self, _data: Self::D) -> anyhow::Result<()> {
        Ok(())
    }
}