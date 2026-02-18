use crate::actor::{Actor, ActorState};
use crate::ipc::ActorHandle;
use crate::primitives::{ControlSignal, OrchestratorStrategy, ProcData, ProcOutput};
use std::collections::HashMap;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use uuid::Uuid;

/// A strictly typed manager for a specific kind of Actor.
/// usage: `connections: Orchestrator<ConnectionState>`
pub struct Orchestrator<S: ActorState + 'static> {
    /// The policy for this group of actors.
    strategy: OrchestratorStrategy,

    /// Active tasks (so we can detect death).
    tasks: HashMap<Uuid, aloeplatform::TaskHandle<()>>,

    /// Control channels (so we can send signals).
    pub handles: HashMap<Uuid, ActorHandle<S::D>>,

    /// Factory: We need a way to recreate the state if we are Restarting.
    /// (Optional: Only needed for Restart strategy)
    factory: Option<Box<dyn Fn() -> S + Send + Sync>>,

    /// Domain-ID registry: maps domain-specific names to actor UUIDs.
    /// e.g. "conn:42" -> Uuid
    domain_map: HashMap<String, Uuid>,

    /// Reverse lookup: actor UUID -> domain name (for remapping on restart).
    reverse_domain_map: HashMap<Uuid, String>,

    /// Upward data flow: all actors send tagged output here.
    output_tx: mpsc::Sender<(Uuid, S::O)>,
    output_rx: mpsc::Receiver<(Uuid, S::O)>,

    current_restarts: u32,
}

impl<S: ActorState> Orchestrator<S> {
    pub fn new(strategy: OrchestratorStrategy) -> Self {
        let (output_tx, output_rx) = mpsc::channel(256);
        Self {
            strategy,
            tasks: HashMap::new(),
            handles: HashMap::new(),
            factory: None,
            domain_map: HashMap::new(),
            reverse_domain_map: HashMap::new(),
            output_tx,
            output_rx,
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
        let (control_tx, control_rx) = mpsc::channel::<ControlSignal>(8);
        let (data_tx, data_rx) = mpsc::channel::<S::D>(8);
        let (health_tx, _) = broadcast::channel(16);
        let output_tx = self.output_tx.clone();
        // Create the Runner
        let actor = Actor::<S>::new(state, control_rx, health_tx.clone(), data_rx, output_tx);
        let id = actor.id;
        // Spawn the Task
        let join_handle = aloeplatform::spawn(async move {
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

    /// Spawn with a one-shot factory that receives context.
    /// Useful when each actor needs unique initialization data (connection ID, config, etc).
    pub fn spawn_with<C>(&mut self, factory: impl FnOnce(C) -> S, ctx: C) -> Uuid {
        let state = factory(ctx);
        self.spawn(state)
    }

    /// Spawn an actor and register it under a domain-specific name.
    /// The name is preserved across restarts.
    pub fn spawn_named(&mut self, name: String, state: S) -> Uuid {
        let id = self.spawn(state);
        self.domain_map.insert(name.clone(), id);
        self.reverse_domain_map.insert(id, name);
        id
    }

    /// Look up an actor handle by domain name.
    pub fn get_by_name(&self, name: &str) -> Option<&ActorHandle<S::D>> {
        let id = self.domain_map.get(name)?;
        self.handles.get(id)
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

            // Preserve the domain name so we can remap after restart
            let domain_name = self.reverse_domain_map.remove(&id);

            match self.strategy {
                OrchestratorStrategy::Restart => {
                    if let Some(new_id) = self.perform_restart(id) {
                        if let Some(name) = &domain_name {
                            self.domain_map.insert(name.clone(), new_id);
                            self.reverse_domain_map.insert(new_id, name.clone());
                        }
                    }
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
                        if let Some(new_id) = self.perform_restart(id) {
                            if let Some(name) = &domain_name {
                                self.domain_map.insert(name.clone(), new_id);
                                self.reverse_domain_map.insert(new_id, name.clone());
                            }
                        }
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
    fn perform_restart(&mut self, old_id: Uuid) -> Option<Uuid> {
        if let Some(factory) = &self.factory {
            let new_state = (factory)();
            let new_id = self.spawn(new_state);
            log::info!("Respawned {} as {}", old_id, new_id);
            Some(new_id)
        } else {
            log::error!("Cannot restart actor {}: No factory provided", old_id);
            None
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

    /// Non-blocking receive of the next worker output.
    /// Call in a loop from the manager's on_tick to drain all pending output.
    pub fn recv_output(&mut self) -> Option<(Uuid, S::O)> {
        self.output_rx.try_recv().ok()
    }

    /// Broadcast `Stop` to all managed actors. Returns the number of actors signaled.
    ///
    /// This is async because `control_tx.send()` is async. For fire-and-forget
    /// shutdown from sync context, wrap in `aloeplatform::spawn`.
    pub async fn shutdown(&self) -> usize {
        let mut count = 0;
        for handle in self.handles.values() {
            if handle.control_tx.send(ControlSignal::Stop).await.is_ok() {
                count += 1;
            }
        }
        count
    }
}
