use crate::connection::{ConnectionData, ConnectionState, ConnectionOutput};
use aloeproc::actor::{ActorState, Orchestrator, Manager};
use aloeproc::primitives::{ControlSignal, OrchestratorStrategy, ProcData, Report};
use async_trait::async_trait;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct ManagerCmd; // Placeholder for commands like "KickUser"
impl ProcData for ManagerCmd {}

pub struct ConnectionManager {
    // This Manager manages the workers
    connections: Orchestrator<ConnectionState>,
    output_rx: mpsc::Receiver<ConnectionOutput>,
    next_connection_id: u32,
}

impl ConnectionManager {
    pub fn new() -> Self {
        let (worker_tx, output_rx) = mpsc::channel::<ConnectionOutput>(1024);

        // B. Setup Orchestrator with a Factory that injects the TX
        // We clone worker_tx so the factory can produce infinite workers
        let factory_tx = worker_tx.clone();
        let connections =
            Orchestrator::new(OrchestratorStrategy::Restart).with_factory(move || {
                // Every new worker gets a clone of the "phone line" to the manager
                ConnectionState::new(factory_tx.clone())
            });

        Self {
            connections,
            output_rx,
        }
    }
}

// 3. Implement the Manager Trait (Business Logic)
#[async_trait]
impl Manager for ConnectionManager {
    type O = ConnectionOutput;

    async fn on_worker_health(&mut self, report: Report) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_worker_data(&mut self, msg: Self::O) -> anyhow::Result<()> {
        println!("[Manager] Received report from worker: {:?}", msg);
        // Example: If worker reports "SocketClosed", maybe we log it or metrics
        Ok(())
    }
}

#[async_trait]
impl ActorState for ConnectionManager {
    type D = ManagerCmd;

    async fn on_tick(&mut self) -> anyhow::Result<bool> {
        // 1. Maintain the fleet
        self.connections.maintain().await;

        // 2. Report aggregate metrics?
        // println!("Active connections: {}", self.connections.count());

        Ok(true)
    }

    async fn on_signal(&mut self, sig: ControlSignal) -> anyhow::Result<()> {
        // Propagate signals down to workers
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
