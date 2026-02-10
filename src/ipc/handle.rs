use crate::primitives::{ProcData, ControlSignal, Report};
use tokio::sync::{mpsc, broadcast};

#[derive(Clone, Debug)]
pub struct ActorHandle<D: ProcData> {
    pub health_tx: broadcast::Sender<Report>,
    pub control_tx: mpsc::Sender<ControlSignal>,
    pub data_tx: mpsc::Sender<D>,
}

impl<D: ProcData>  ActorHandle <D>{
    pub fn new(control_tx: mpsc::Sender<ControlSignal>, health_tx: broadcast::Sender<Report>, data_tx: mpsc::Sender<D>) -> Self {
        Self { health_tx, control_tx, data_tx}
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Report> {
        self.health_tx.subscribe()
    }
    pub async fn notify(&self, data: D) -> Result<(), mpsc::error::SendError<D>> {
        self.data_tx.send(data).await
    }
    pub async fn pause(&self) -> Result<(), mpsc::error::SendError<ControlSignal>> {
        self.control_tx.send(ControlSignal::Pause).await
    }
    pub async fn resume(&self) -> Result<(), mpsc::error::SendError<ControlSignal>> {
        self.control_tx.send(ControlSignal::Resume).await
    }
    pub async fn stop(&self) -> Result<(), mpsc::error::SendError<ControlSignal>> {
        self.control_tx.send(ControlSignal::Stop).await
    }
}