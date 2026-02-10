use aloeproc::actor::ActorState;
use aloeproc::primitives::{ControlSignal, ProcData, ProcOutput};
use async_trait::async_trait;
use tokio::sync::mpsc;
use uuid::Uuid;

// Define what the worker sends back
#[derive(Debug, Clone)]
pub enum ConnectionOutput {
    BytesReceived(usize),
    ConnectionClosed(u32), // ID
}
impl ProcOutput for ConnectionOutput {}

#[derive(Debug, Clone)]
pub struct ConnectionData(pub Vec<u8>); // Represents a packet
impl ProcData for ConnectionData {}

pub struct ConnectionState {
    pub id: u32,
    manager_tx: mpsc::Sender<ConnectionOutput>,
}

impl ConnectionState {
    pub fn new(id: u32, manager_tx: mpsc::Sender<ConnectionOutput>) -> Self {
        Self { id, manager_tx }
    }
}

#[async_trait]
impl ActorState for ConnectionState {
    type D = ConnectionData;

    async fn on_tick(&mut self) -> anyhow::Result<bool> {
        
        let _ = self.manager_tx.send(ConnectionOutput::BytesReceived(1024)).await;
        Ok(true)
    }

    async fn on_signal(&mut self, _sig: ControlSignal) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_data(&mut self, data: Self::D) -> anyhow::Result<()> {
        println!("[Conn {}] Sending packet: {:?}", self.id, data);
        Ok(())
    }
}