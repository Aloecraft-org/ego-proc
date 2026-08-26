use crate::ipc::{ProcData, ProcOutput};
use crate::{ActorHealth, ControlSignal, LifecycleStatus};
use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub trait PlatformSendSync: Send + Sync {}
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl<T: ?Sized + Send + Sync> PlatformSendSync for T {}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub trait PlatformSendSync {}
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl<T: ?Sized> PlatformSendSync for T {}

#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    async_trait::async_trait
)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait::async_trait(?Send))]
pub trait ActorState: PlatformSendSync {
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    type D: Clone + Send + Sync;
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    type D: Clone;
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    type O: Clone + Send + Sync;
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    type O: Clone;

    fn interval(&self) -> Duration {
        Duration::from_millis(100)
    }
    async fn on_tick(&mut self) -> anyhow::Result<bool>;
    async fn on_signal(&mut self, signal: ControlSignal) -> anyhow::Result<()>;
    async fn on_data(&mut self, data: Self::D) -> anyhow::Result<()>;

    /// Collect any output the state has buffered since the last tick.
    /// Default: no output. Override to push data upward to the managing Orchestrator.
    fn take_output(&mut self) -> Vec<Self::O> {
        vec![]
    }
}

pub struct Actor<S: ActorState> {
    pub id: Uuid,
    pub lifecycle_status: LifecycleStatus,
    pub state: S,
    pub sig_notify: bool,
    pub health_tx: broadcast::Sender<ActorHealth>,
    pub control_rx: mpsc::Receiver<ControlSignal>,
    pub data_rx: mpsc::Receiver<S::D>,
    pub output_tx: mpsc::Sender<(Uuid, S::O)>,
}

impl<S: ActorState> Actor<S> {
    pub fn new(
        state: S,
        control_rx: mpsc::Receiver<ControlSignal>,
        health_tx: broadcast::Sender<ActorHealth>,
        data_rx: mpsc::Receiver<S::D>,
        output_tx: mpsc::Sender<(Uuid, S::O)>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            lifecycle_status: LifecycleStatus::Ready,
            state,
            sig_notify: false,
            control_rx,
            health_tx,
            data_rx,
            output_tx,
        }
    }

    pub async fn run(mut self) {
        let interval_duration = self.state.interval();
        let mut ticker = ego_platform::time::Interval::new(interval_duration);
        ticker.set_missed_tick_behavior(ego_platform::time::MissedTickBehavior::Skip);

        self.lifecycle_status = LifecycleStatus::Running;

        while self.lifecycle_status == LifecycleStatus::Paused
            || self.lifecycle_status == LifecycleStatus::Running
        {
            tokio::select! {
                // 1. Handle Signals
                Some(sig) = self.control_rx.recv() => {
                    match sig {
                        ControlSignal::Stop => { self.lifecycle_status = LifecycleStatus::Ready }
                        ControlSignal::Pause => { self.lifecycle_status = LifecycleStatus::Paused }
                        ControlSignal::Resume => { self.lifecycle_status = LifecycleStatus::Running }
                        _ => {}
                    }

                    // Still let the state react if it wants to
                    if let Err(e) = self.state.on_signal(sig).await {
                        log::error!("[{}] Signal Error: {:?}", self.id, e);
                    }
                }

                _ = ticker.tick() => {

                    if self.lifecycle_status == LifecycleStatus::Paused { continue; }

                    // 2. The Loop
                    let start = ego_platform::Instant::now();

                    while let Ok(data) = self.data_rx.try_recv() {
                        if let Err(e) = self.state.on_data(data).await {
                            log::error!("[{}] Data Error: {:?}", self.id, e);
                            // TODO: Transition to NotReady
                            self.lifecycle_status = LifecycleStatus::Ready;
                        }
                    }

                    // Run Logic
                    match self.state.on_tick().await {
                        Ok(should_continue) => {
                            if !should_continue { self.lifecycle_status = LifecycleStatus::Ready; }
                        }
                        Err(e) => {
                            log::error!("[{}] Crashed: {:?}", self.id, e);
                            // TODO: Transition to NotReady
                            self.lifecycle_status = LifecycleStatus::Ready;
                        }
                    }
                    // Drain output from state and forward tagged with our ID
                    for item in self.state.take_output() {
                        let _ = self.output_tx.send((self.id, item)).await;
                    }

                    // Measure Health & ActorHealth
                    let elapsed = start.elapsed();
                    let actor_health = self.measure_health(elapsed, interval_duration);
                    let _ = self.health_tx.send(actor_health);
                }
            }
        }
    }

    fn measure_health(&self, elapsed: Duration, interval: Duration) -> ActorHealth {
        let saturation = elapsed.as_secs_f64() / interval.as_secs_f64();
        ActorHealth {
            actor_id: self.id.to_string(),
            saturation,
            lifecycle_status: self.lifecycle_status as i32,
            last_tick_ms: elapsed.as_millis() as u64,
        }
    }
}
