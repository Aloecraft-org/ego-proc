// tests/orchestrator_lifecycle.rs

mod common;
use common::{async_test, test};

use aloeproc::actor::{ActorState, Orchestrator};
use aloeproc::ipc::{ProcData, ProcOutput, NoOutput};
use ego2_proto::aloeproc::{ControlSignal, OrchestrationStrategy, OrchestrationType};
use async_trait::async_trait;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

// --- Configurable test actor for Orchestrator tests ---

#[derive(Debug, Clone)]
struct OrcData;
impl ProcData for OrcData {}

#[derive(Debug, Clone, PartialEq)]
struct OrcOutput(u32);
impl ProcOutput for OrcOutput {}

struct OrcActor {
    crash_on_tick: u32, // 0 = never crash
    tick_count: u32,
    output_buffer: Vec<OrcOutput>,
    spawn_counter: Arc<AtomicU32>,
}

impl OrcActor {
    fn normal(spawn_counter: Arc<AtomicU32>) -> Self {
        spawn_counter.fetch_add(1, Ordering::SeqCst);
        Self {
            crash_on_tick: 0,
            tick_count: 0,
            output_buffer: Vec::new(),
            spawn_counter,
        }
    }

    fn crash_after(tick: u32, spawn_counter: Arc<AtomicU32>) -> Self {
        spawn_counter.fetch_add(1, Ordering::SeqCst);
        Self {
            crash_on_tick: tick,
            tick_count: 0,
            output_buffer: Vec::new(),
            spawn_counter,
        }
    }
}

#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait(?Send))]
impl ActorState for OrcActor {
    type D = OrcData;
    type O = OrcOutput;

    fn interval(&self) -> Duration {
        Duration::from_millis(10)
    }

    async fn on_tick(&mut self) -> anyhow::Result<bool> {
        self.tick_count += 1;
        self.output_buffer.push(OrcOutput(self.tick_count));
        if self.crash_on_tick > 0 && self.tick_count >= self.crash_on_tick {
            anyhow::bail!("Intentional crash at tick {}", self.tick_count);
        }
        Ok(true)
    }

    async fn on_signal(&mut self, _: ControlSignal) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_data(&mut self, _: Self::D) -> anyhow::Result<()> {
        Ok(())
    }

    fn take_output(&mut self) -> Vec<Self::O> {
        std::mem::take(&mut self.output_buffer)
    }
}

// --- Tests ---

#[async_test]
async fn test_spawn_and_maintain() {
    let counter = Arc::new(AtomicU32::new(0));
    let mut orch = Orchestrator::<OrcActor>::new(OrchestrationStrategy::oneshot());

    let id = orch.spawn(OrcActor::normal(counter.clone()));
    assert!(orch.get_handle(id).is_some());

    // Maintain should see it alive (not finished)
    orch.maintain().await;
    assert!(orch.get_handle(id).is_some());
}

#[async_test]
async fn test_death_detection() {
    let counter = Arc::new(AtomicU32::new(0));
    let mut orch = Orchestrator::<OrcActor>::new(OrchestrationStrategy::oneshot());

    let id = orch.spawn(OrcActor::crash_after(1, counter.clone()));

    // Wait for crash
    aloeplatform::sleep(Duration::from_millis(50)).await;

    // Maintain should clean it up (OneShot = no restart)
    orch.maintain().await;
    assert!(orch.get_handle(id).is_none(), "Dead actor should be removed");
}

#[async_test]
async fn test_restart_strategy() {
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();

    let mut orch = Orchestrator::<OrcActor>::new(OrchestrationStrategy::restart())
        .with_factory(move || OrcActor::crash_after(1, counter_clone.clone()));

    let old_id = orch.spawn(OrcActor::crash_after(1, counter.clone()));

    // Wait for crash
    aloeplatform::sleep(Duration::from_millis(50)).await;
    orch.maintain().await;

    // Old ID should be gone, but a new actor should exist
    assert!(orch.get_handle(old_id).is_none());
    assert_eq!(orch.handles.len(), 1, "Should have respawned one actor");

    let new_id = *orch.handles.keys().next().unwrap();
    assert_ne!(old_id, new_id);
}

#[async_test]
async fn test_restart_limit() {
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();

    let mut orch = Orchestrator::<OrcActor>::new(OrchestrationStrategy::restart_at_most(2))
        .with_factory(move || OrcActor::crash_after(1, counter_clone.clone()));

    orch.spawn(OrcActor::crash_after(1, counter.clone()));

    // Cycle through restarts: die -> restart (1), die -> restart (2), die -> give up
    for _ in 0..3 {
        aloeplatform::sleep(Duration::from_millis(50)).await;
        orch.maintain().await;
    }

    // After 2 restarts, the 3rd death should not restart
    // Total spawns: 1 initial + 2 restarts = 3
    // But after the 3rd crash + maintain, handles should be empty
    aloeplatform::sleep(Duration::from_millis(50)).await;
    orch.maintain().await;

    assert!(orch.handles.is_empty(), "Should have given up after restart limit");
}

#[async_test]
async fn test_oneshot_no_restart() {
    let counter = Arc::new(AtomicU32::new(0));
    let mut orch = Orchestrator::<OrcActor>::new(OrchestrationStrategy::oneshot());

    orch.spawn(OrcActor::crash_after(1, counter.clone()));

    aloeplatform::sleep(Duration::from_millis(50)).await;
    orch.maintain().await;

    assert!(orch.handles.is_empty(), "OneShot should not restart");
}

#[async_test]
async fn test_broadcast_stop() {
    let counter = Arc::new(AtomicU32::new(0));
    let mut orch = Orchestrator::<OrcActor>::new(OrchestrationStrategy::oneshot());

    let id1 = orch.spawn(OrcActor::normal(counter.clone()));
    let id2 = orch.spawn(OrcActor::normal(counter.clone()));

    aloeplatform::sleep(Duration::from_millis(30)).await;
    assert!(orch.get_handle(id1).is_some());
    assert!(orch.get_handle(id2).is_some());

    // Broadcast stop
    orch.broadcast(ControlSignal::Stop).await;
    aloeplatform::sleep(Duration::from_millis(50)).await;
    orch.maintain().await;

    assert!(orch.handles.is_empty(), "All actors should be stopped");
}

#[async_test]
async fn test_named_spawn_lookup() {
    let counter = Arc::new(AtomicU32::new(0));
    let mut orch = Orchestrator::<OrcActor>::new(OrchestrationStrategy::oneshot());
    let id = orch.spawn_named("conn:42".into(), OrcActor::normal(counter.clone()));

    // Look up by name
    let handle = orch.get_by_name("conn:42");
    assert!(handle.is_some(), "Should find actor by domain name");

    // Look up by UUID should also work
    assert!(orch.get_handle(id).is_some());

    // Non-existent name
    assert!(orch.get_by_name("conn:99").is_none());

    // Clean up
    orch.broadcast(ControlSignal::Stop).await;
}

#[async_test]
async fn test_output_collection() {
    let counter = Arc::new(AtomicU32::new(0));
    let mut orch = Orchestrator::<OrcActor>::new(OrchestrationStrategy::oneshot());

    let id = orch.spawn(OrcActor::normal(counter.clone()));

    // Wait for a few ticks to produce output
    aloeplatform::sleep(Duration::from_millis(60)).await;

    // Drain output
    let mut outputs = Vec::new();
    while let Some((actor_id, output)) = orch.recv_output() {
        assert_eq!(actor_id, id);
        outputs.push(output);
    }

    assert!(!outputs.is_empty(), "Should have received output from worker");
    // First output should be OrcOutput(1)
    assert_eq!(outputs[0], OrcOutput(1));

    // Clean up
    orch.broadcast(ControlSignal::Stop).await;
}