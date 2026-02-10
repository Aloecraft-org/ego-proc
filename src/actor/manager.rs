use crate::primitives::{ControlSignal, ProcData, ProcOutput, Report};
use crate::actor::{Actor, ActorState};
use std::collections::HashMap;
use async_trait::async_trait;
use uuid::Uuid;
use tokio::sync::mpsc;

pub struct ManagerState<D: ProcData, O: ProcOutput> {
    workers: HashMap<Uuid, mpsc::Sender<D>>,
    output_rx: mpsc::Receiver<O>
}

#[async_trait]
pub trait Manager: ActorState {
    type O: ProcOutput;

    async fn on_worker_health(&mut self, report: Report) -> anyhow::Result<()>;
    async fn on_worker_data(&mut self, worker_data: Self::O) -> anyhow::Result<()>;
}

