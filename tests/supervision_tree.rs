// tests/supervision_tree.rs

mod common;
use common::{async_test, test};

use ego_proc::actor::{ActorState, Orchestrator};

use ego_proc::ipc::{NoOutput, ProcData, ProcOutput};
use async_trait::async_trait;
use ego_proc::{ControlSignal, OrchestrationStrategy, OrchestrationType};

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

// --- Worker ---

#[derive(Debug, Clone)]
struct WorkerData;
impl ProcData for WorkerData {}

#[derive(Debug, Clone, PartialEq)]
struct WorkerOutput(u32);
impl ProcOutput for WorkerOutput {}

struct Worker {
    id: u32,
    crash_on_tick: u32,
    tick_count: u32,
    output_buffer: Vec<WorkerOutput>,
}

impl Worker {
    fn new(id: u32) -> Self {
        Self {
            id,
            crash_on_tick: 0,
            tick_count: 0,
            output_buffer: Vec::new(),
        }
    }

    fn crash_after(id: u32, tick: u32) -> Self {
        Self {
            id,
            crash_on_tick: tick,
            tick_count: 0,
            output_buffer: Vec::new(),
        }
    }
}

#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait(?Send))]
impl ActorState for Worker {
    type D = WorkerData;
    type O = WorkerOutput;

    fn interval(&self) -> Duration {
        Duration::from_millis(10)
    }

    async fn on_tick(&mut self) -> anyhow::Result<bool> {
        self.tick_count += 1;
        self.output_buffer.push(WorkerOutput(self.id));
        if self.crash_on_tick > 0 && self.tick_count >= self.crash_on_tick {
            anyhow::bail!("Worker {} crashed", self.id);
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

// --- Manager (ActorState that owns an Orchestrator) ---

#[derive(Debug, Clone)]
struct ManagerCmd;
impl ProcData for ManagerCmd {}

struct TestManager {
    workers: Orchestrator<Worker>,
    received_outputs: Vec<WorkerOutput>,
}

impl TestManager {
    fn new() -> Self {
        let workers =
            Orchestrator::new(OrchestrationStrategy::restart()).with_factory(|| Worker::new(0));
        Self {
            workers,
            received_outputs: Vec::new(),
        }
    }
}

#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait(?Send))]
impl ActorState for TestManager {
    type D = ManagerCmd;
    type O = NoOutput;

    fn interval(&self) -> Duration {
        Duration::from_millis(10)
    }

    async fn on_tick(&mut self) -> anyhow::Result<bool> {
        self.workers.maintain().await;
        while let Some((_id, output)) = self.workers.recv_output() {
            self.received_outputs.push(output);
        }
        Ok(true)
    }

    async fn on_signal(&mut self, sig: ControlSignal) -> anyhow::Result<()> {
        if sig == ControlSignal::Stop {
            self.workers.broadcast(ControlSignal::Stop).await;
        }
        Ok(())
    }

    async fn on_data(&mut self, _: Self::D) -> anyhow::Result<()> {
        Ok(())
    }
}

// --- Tests ---

#[async_test]
async fn test_manager_pattern() {
    // Manager supervises workers, drains output
    let mut orch = Orchestrator::<TestManager>::new(OrchestrationStrategy::oneshot());
    let mut mgr = TestManager::new();

    // Spawn a worker inside the manager before handing it to the orchestrator
    mgr.workers.spawn(Worker::new(1));

    let mgr_id = orch.spawn(mgr);

    // Let the system run — manager ticks, workers tick, output flows
    ego_platform::sleep(Duration::from_millis(100)).await;

    // Stop everything
    orch.broadcast(ControlSignal::Stop).await;
    ego_platform::sleep(Duration::from_millis(50)).await;
    orch.maintain().await;

    assert!(orch.handles.is_empty(), "Manager should have stopped");
}

#[async_test]
async fn test_kill_worker_manager_restarts() {
    let mut mgr = TestManager::new();

    // Spawn a worker that crashes after 1 tick
    mgr.workers.spawn(Worker::crash_after(1, 1));

    let mut orch = Orchestrator::<TestManager>::new(OrchestrationStrategy::oneshot());
    let _mgr_id = orch.spawn(mgr);

    // Wait for worker to crash and manager to restart it
    ego_platform::sleep(Duration::from_millis(100)).await;

    // Stop and clean up
    orch.broadcast(ControlSignal::Stop).await;
    ego_platform::sleep(Duration::from_millis(50)).await;
    orch.maintain().await;
}

#[async_test]
async fn test_graceful_shutdown_tree() {
    let mut mgr = TestManager::new();
    mgr.workers.spawn(Worker::new(1));
    mgr.workers.spawn(Worker::new(2));

    let mut orch = Orchestrator::<TestManager>::new(OrchestrationStrategy::oneshot());
    orch.spawn(mgr);

    // Let it run
    ego_platform::sleep(Duration::from_millis(50)).await;

    // Stop propagates: orch -> manager -> workers
    orch.broadcast(ControlSignal::Stop).await;
    ego_platform::sleep(Duration::from_millis(100)).await;
    orch.maintain().await;

    assert!(orch.handles.is_empty(), "Everything should be stopped");
}
