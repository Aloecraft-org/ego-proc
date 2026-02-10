use crate::actor::{Actor, ActorState};
use crate::ipc::ActorHandle;
use crate::primitives::{ControlSignal, OrchestratorStrategy, ProcData, ProcOutput};
use std::collections::HashMap;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use uuid::Uuid;

/// A strictly typed manager for a specific kind of Actor.
/// usage: `connections: Orchestrator<ConnectionState>`
pub struct Orchestrator<S: ActorState + 'static> {
    /// The policy for this group of actors.
    strategy: OrchestratorStrategy,

    /// Active tasks (so we can detect death).
    tasks: HashMap<Uuid, JoinHandle<()>>,

    /// Control channels (so we can send signals).
    pub handles: HashMap<Uuid, ActorHandle<S::D>>,

    /// Factory: We need a way to recreate the state if we are Restarting.
    /// (Optional: Only needed for Restart strategy)
    factory: Option<Box<dyn Fn() -> S + Send + Sync>>,

    current_restarts: u32,
}

impl<S: ActorState> Orchestrator<S> {
    pub fn new(strategy: OrchestratorStrategy) -> Self {
        Self {
            strategy,
            tasks: HashMap::new(),
            handles: HashMap::new(),
            factory: None,
            current_restarts: 0,
        }
    }

    /// Set a factory function for auto-restarting singletons.
    pub fn with_factory<F>(mut self, f: F) -> Self
    where
        F: Fn() -> S + Send + Sync + 'static,
    {
        self.factory = Some(Box::new(f));
        self
    }

    /// Spawn a new instance of this actor type.
    pub fn spawn(&mut self, state: S) -> Uuid {
        let (control_tx, control_rx) = tokio::sync::mpsc::channel::<ControlSignal>(8);
        let (data_tx, data_rx) = tokio::sync::mpsc::channel::<S::D>(8);
        let (health_tx, _) = broadcast::channel(16);
        // Create the Runner
        let actor = Actor::<S>::new(state, control_rx, health_tx.clone(), data_rx);
        let id = actor.id;
        // Spawn the Task
        let join_handle = tokio::spawn(async move {
            actor.run().await;
        });
        let actor_handle = ActorHandle::<S::D> {
            health_tx,
            control_tx,
            data_tx,
        };
        self.tasks.insert(id, join_handle);
        self.handles.insert(id, actor_handle);
        id
    }

    pub async fn maintain(&mut self) {
        // 1. Find dead tasks
        let dead_ids: Vec<Uuid> = self
            .tasks
            .iter()
            .filter(|(_, h)| h.is_finished())
            .map(|(id, _)| *id)
            .collect();

        // 2. Handle them
        for id in dead_ids {
            // Clean up the dead actor's remains
            self.tasks.remove(&id);
            self.handles.remove(&id);

            match self.strategy {
                OrchestratorStrategy::Restart => {
                    self.perform_restart(id);
                }

                // NEW: Handle the limit
                OrchestratorStrategy::RestartAtMost(limit) => {
                    if self.current_restarts < limit {
                        self.current_restarts += 1;
                        log::warn!(
                            "Actor {} died. Restarting ({}/{})",
                            id,
                            self.current_restarts,
                            limit
                        );
                        self.perform_restart(id);
                    } else {
                        log::error!(
                            "Actor {} died. Restart limit ({}) reached. Giving up.",
                            id,
                            limit
                        );
                    }
                }

                OrchestratorStrategy::OneShot => {
                    log::info!("Actor {} finished naturally.", id);
                }
                OrchestratorStrategy::Escalate => {
                    log::error!("Critical actor {} died! Escalating...", id);
                    // In a real app, you might set a flag here to kill the Controller
                    // e.g., self.status = Status::Error;
                }
            }
        }
    }

    // Helper to avoid duplicating the factory code
    fn perform_restart(&mut self, old_id: Uuid) {
        if let Some(factory) = &self.factory {
            let new_state = (factory)();
            let new_id = self.spawn(new_state);
            log::info!("Respawned {} as {}", old_id, new_id);
        } else {
            log::error!("Cannot restart actor {}: No factory provided", old_id);
        }
    }

    pub fn get_handle(&self, id: Uuid) -> Option<&ActorHandle<S::D>> {
        self.handles.get(&id)
    }

    /// Send a signal to a specific actor.
    pub async fn send_signal(&self, id: Uuid, signal: ControlSignal) {
        if let Some(actor_handle) = self.handles.get(&id) {
            let _ = actor_handle.control_tx.send(signal).await;
        }
    }

    /// Broadcast a signal to ALL actors in this group.
    pub async fn broadcast(&self, signal: ControlSignal) {
        for actor_handle in self.handles.values() {
            let _ = actor_handle.control_tx.send(signal).await;
        }
    }
}
