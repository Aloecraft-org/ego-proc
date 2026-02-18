use crate::connection::{ConnectionState, ConnectionOutput};
use aloeproc::actor::{ActorState, Orchestrator};
use aloeproc::primitives::{ControlSignal, NoOutput, OrchestratorStrategy, ProcData};
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct ManagerCmd; // Placeholder for commands like "KickUser"
impl ProcData for ManagerCmd {}

pub struct ConnectionManager {
    connections: Orchestrator<ConnectionState>,
    next_connection_id: u32,
}

impl ConnectionManager {
    pub fn new() -> Self {
        let connections =
            Orchestrator::new(OrchestratorStrategy::Restart);

        Self {
            connections,
            next_connection_id: 0,
        }
    }

    fn handle_worker_output(&mut self, worker_id: uuid::Uuid, output: ConnectionOutput) {
        println!("[Manager] Worker {} reported: {:?}", worker_id, output);
    }
}

#[async_trait]
impl ActorState for ConnectionManager {
    type D = ManagerCmd;
    type O = NoOutput;

    async fn on_tick(&mut self) -> anyhow::Result<bool> {
        // 1. Maintain the fleet
        self.connections.maintain().await;

        // 2. Drain worker outputs — this is the Manager pattern
        while let Some((worker_id, output)) = self.connections.recv_output() {
            self.handle_worker_output(worker_id, output);
        }

        Ok(true)
    }

    async fn on_signal(&mut self, sig: ControlSignal) -> anyhow::Result<()> {
        if sig == ControlSignal::Stop {
            self.connections.broadcast(ControlSignal::Stop).await;
        }
        Ok(())
    }

    async fn on_data(&mut self, _data: Self::D) -> anyhow::Result<()> {
        // Handle "KickUser" or "Broadcast" commands here
        Ok(())
    }
}
