use crate::ipc::ProcData;
use crate::{ActorHealth, ControlSignal};
use crate::ControlSignal::Resume;

use tokio::sync::{broadcast, mpsc};

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub trait PlatformSendSync: Send + Sync {}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl<T: Send + Sync> PlatformSendSync for T {}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub trait PlatformSendSync {}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl<T> PlatformSendSync for T {}

#[derive(Clone, Debug)]
pub struct ActorHandle<D: Clone + PlatformSendSync> {
    pub health_tx: broadcast::Sender<ActorHealth>,
    pub control_tx: mpsc::Sender<ControlSignal>,
    pub data_tx: mpsc::Sender<D>,
}

impl<D: Clone + PlatformSendSync> ActorHandle<D> {
    pub fn new(
        control_tx: mpsc::Sender<ControlSignal>,
        health_tx: broadcast::Sender<ActorHealth>,
        data_tx: mpsc::Sender<D>,
    ) -> Self {
        Self {
            health_tx,
            control_tx,
            data_tx,
        }
    }
    pub fn subscribe(&self) -> broadcast::Receiver<ActorHealth> {
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
