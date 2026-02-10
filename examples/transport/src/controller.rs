use crate::manager::ConnectionManager;
use crate::listener::ConnectionListener;
use crate::router::PacketRouter;
use crate::handler::MessageHandler;

use aloeproc::actor::{ActorState, Orchestrator};
use aloeproc::ipc::ActorHandle;
use aloeproc::primitives::{ControlSignal, ProcData, OrchestratorStrategy};
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct ControllerCmd;
impl ProcData for ControllerCmd {}


pub struct TransportController {
    // 1. The Manager (Nested Supervision)
    conn_manager: Orchestrator<ConnectionManager>,
    
    // 2. The Workers/Services
    listener: Orchestrator<ConnectionListener>,
    router: Orchestrator<PacketRouter>,
    handler: Orchestrator<MessageHandler>,
}

impl TransportController {
    pub fn new() -> Self {
        // Initialize Orchestrators
        // We generally want these services to RESTART if they crash
        let mut conn_manager = Orchestrator::new(OrchestratorStrategy::Restart)
            .with_factory(|| ConnectionManager::new());

        let mut listener = Orchestrator::new(OrchestratorStrategy::Restart)
            .with_factory(|| ConnectionListener);

        let mut router = Orchestrator::new(OrchestratorStrategy::Restart)
            .with_factory(|| PacketRouter);
            
        let mut handler = Orchestrator::new(OrchestratorStrategy::Restart)
            .with_factory(|| MessageHandler);

        // Spawn the singletons immediately?
        // Or wait for a "Start" signal? Let's spawn now.
        conn_manager.spawn(ConnectionManager::new());
        listener.spawn(ConnectionListener);
        router.spawn(PacketRouter);
        handler.spawn(MessageHandler);

        Self {
            conn_manager,
            listener,
            router,
            handler,
        }
    }

    // fn get_connection_manager(&self) -> &ActorHandle {
    //     self.conn_manager.handles.values().next().unwrap()
    // }

    // fn get_connection_listener(&self) -> &ActorHandle {
    //     self.listener.handles.values().next().unwrap()
    // }

    // fn get_packet_router(&self) -> &ActorHandle {
    //     self.router.handles.values().next().unwrap()
    // }
    
    // fn get_message_handler(&self) -> &ActorHandle {
    //     self.handler.handles.values().next().unwrap()
    // }

}

#[async_trait]
impl ActorState for TransportController {
    type D = ControllerCmd;

    async fn on_tick(&mut self) -> anyhow::Result<bool> {
        // SUPERVISION LOOP
        // "Are my department heads still alive?"
        self.conn_manager.maintain().await;
        self.listener.maintain().await;
        self.router.maintain().await;
        self.handler.maintain().await;

        Ok(true)
    }

    async fn on_signal(&mut self, sig: ControlSignal) -> anyhow::Result<()> {
        // "SHUT DOWN EVERYTHING"
        if sig == ControlSignal::Stop {
            self.conn_manager.broadcast(ControlSignal::Stop).await;
            self.listener.broadcast(ControlSignal::Stop).await;
            self.router.broadcast(ControlSignal::Stop).await;
            self.handler.broadcast(ControlSignal::Stop).await;
        }
        Ok(())
    }

    async fn on_data(&mut self, _data: Self::D) -> anyhow::Result<()> {
        Ok(())
    }
}