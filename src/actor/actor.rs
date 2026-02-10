use crate::primitives::{ControlSignal, ProcData, Report};
use tokio::sync::{broadcast, mpsc};
use async_trait::async_trait;
use uuid::Uuid;
use std::time::Duration;

#[async_trait::async_trait]
pub trait ActorState: Send + Sync {
    type D: ProcData;

    fn interval(&self) -> Duration {
        Duration::from_millis(100)
    }
    async fn on_tick(&mut self) -> anyhow::Result<bool>;
    async fn on_signal(&mut self, signal: ControlSignal) -> anyhow::Result<()>;
    async fn on_data(&mut self, data: Self::D) -> anyhow::Result<()>;
}

pub struct Actor<S: ActorState> {
    pub id: Uuid,
    pub state: S,
    pub sig_notify: bool,
    pub health_tx: broadcast::Sender<Report>,
    pub control_rx: mpsc::Receiver<ControlSignal>,
    pub data_rx:  mpsc::Receiver<S::D>,
}

impl<S: ActorState> Actor<S> {
    pub fn new(
        state: S,
        control_rx: mpsc::Receiver<ControlSignal>,
        health_tx: broadcast::Sender<Report>,
        data_rx: mpsc::Receiver<S::D>
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            state,
            sig_notify: false,
            control_rx,
            health_tx,
            data_rx
        }
    }

    pub async fn run(mut self) {
        let interval_duration = self.state.interval();
        let mut ticker = tokio::time::interval(interval_duration);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut running = true;
        let mut paused = false;

        while running {
            tokio::select! {
                // 1. Handle Signals
                Some(sig) = self.control_rx.recv() => {
                    match sig {
                        ControlSignal::Stop => running = false,
                        ControlSignal::Pause => paused = true,
                        ControlSignal::Resume => paused = false,
                        _ => {}
                    }
                
                    // Still let the state react if it wants to
                    if let Err(e) = self.state.on_signal(sig).await {
                        log::error!("[{}] Signal Error: {:?}", self.id, e);
                    }
                }

                _ = ticker.tick() => {

                    if paused { continue; }
                    
                    // 2. The Loop
                    let start = std::time::Instant::now();
                    
                    while let Ok(data) = self.data_rx.try_recv() {
                        if let Err(e) = self.state.on_data(data).await {
                            log::error!("[{}] Data Error: {:?}", self.id, e);
                        }
                    }


                    // Run Logic
                    match self.state.on_tick().await {
                        Ok(should_continue) => {
                            if !should_continue { running = false; }
                        }
                        Err(e) => {
                            log::error!("[{}] Crashed: {:?}", self.id, e);
                            running = false;
                        }
                    }
                    // Measure Health & Report
                    let elapsed = start.elapsed();
                    let report = self.measure_health(elapsed, interval_duration);
                    let _ = self.health_tx.send(report);
                }
            }
        }
    }

    fn measure_health(&self, elapsed: Duration, interval: Duration) -> Report {
        let saturation = elapsed.as_secs_f64() / interval.as_secs_f64();
        Report {
            actor_id: self.id,
            saturation,
            is_alive: true,
            last_tick_ms: elapsed.as_millis() as u64,
        }
    }
}

