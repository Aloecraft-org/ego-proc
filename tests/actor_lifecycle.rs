// 1. Import the WASM test macro
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen_test::wasm_bindgen_test;

// 2. Configure the WASM test runner (run in browser or node)
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

// 3. Create a helper macro to switch between Tokio and Wasm automatically
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use tokio::test as async_test;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen_test as async_test;


use aloeproc::actor::{Actor, ActorState};
use aloeproc::primitives::{ControlSignal, NoOutput, ProcData, ProcOutput, Report};
use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;

// --- Test Actor ---

#[derive(Debug, Clone)]
struct TestData(String);
impl ProcData for TestData {}

#[derive(Debug, Clone, PartialEq)]
struct TestOutput(String);
impl ProcOutput for TestOutput {}

struct TestActor {
    tick_count: u32,
    max_ticks: Option<u32>,
    should_crash: bool,
    received_data: Vec<String>,
    output_buffer: Vec<TestOutput>,
    /// External channel so the test can observe ticks
    tick_tx: Option<mpsc::Sender<u32>>,
}

impl TestActor {
    fn new() -> Self {
        Self {
            tick_count: 0,
            max_ticks: None,
            should_crash: false,
            received_data: Vec::new(),
            output_buffer: Vec::new(),
            tick_tx: None,
        }
    }

    fn with_max_ticks(mut self, n: u32) -> Self {
        self.max_ticks = Some(n);
        self
    }

    fn with_crash(mut self) -> Self {
        self.should_crash = true;
        self
    }

    fn with_tick_tx(mut self, tx: mpsc::Sender<u32>) -> Self {
        self.tick_tx = Some(tx);
        self
    }
}

#[async_trait]
impl ActorState for TestActor {
    type D = TestData;
    type O = TestOutput;

    fn interval(&self) -> Duration {
        Duration::from_millis(10)
    }

    async fn on_tick(&mut self) -> anyhow::Result<bool> {
        if self.should_crash {
            anyhow::bail!("Intentional crash");
        }
        self.tick_count += 1;
        if let Some(tx) = &self.tick_tx {
            let _ = tx.send(self.tick_count).await;
        }
        if let Some(max) = self.max_ticks {
            if self.tick_count >= max {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn on_signal(&mut self, _signal: ControlSignal) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_data(&mut self, data: Self::D) -> anyhow::Result<()> {
        self.received_data.push(data.0.clone());
        self.output_buffer.push(TestOutput(format!("ECHO: {}", data.0)));
        Ok(())
    }

    fn take_output(&mut self) -> Vec<Self::O> {
        std::mem::take(&mut self.output_buffer)
    }
}

// --- Helper: spawn a raw Actor and return channels ---

fn spawn_test_actor(
    state: TestActor,
) -> (
    Uuid,
    mpsc::Sender<ControlSignal>,
    mpsc::Sender<TestData>,
    broadcast::Sender<Report>,
    mpsc::Receiver<(Uuid, TestOutput)>,
    aloeplatform::spawn::TaskHandle<()>,
) {
    let (control_tx, control_rx) = mpsc::channel(8);
    let (data_tx, data_rx) = mpsc::channel(8);
    let (health_tx, _) = broadcast::channel(16);
    let (output_tx, output_rx) = mpsc::channel(64);

    let actor = Actor::new(state, control_rx, health_tx.clone(), data_rx, output_tx);
    let id = actor.id;
    let handle = aloeplatform::spawn(async move { actor.run().await });

    (id, control_tx, data_tx, health_tx, output_rx, handle)
}

// --- Tests ---

#[async_test]
async fn test_actor_runs_and_ticks() {
    let (tick_tx, mut tick_rx) = mpsc::channel(64);
    let state = TestActor::new().with_tick_tx(tick_tx);
    let (_id, _ctl, _data, health_tx, _out, handle) = spawn_test_actor(state);

    // Subscribe to health reports
    let mut health_rx = health_tx.subscribe();

    // Wait for a few ticks
    let tick = tick_rx.recv().await.expect("Should receive tick");
    assert!(tick >= 1);

    // Health report should arrive
    let report = health_rx.recv().await.expect("Should receive health report");
    assert!(report.is_alive);
    assert!(report.saturation >= 0.0);

    // Clean up
    drop(_ctl); // drop control_tx to close the channel, actor will exit
    let _ = aloeplatform::time::timeout(Duration::from_millis(200), handle).await;
}

#[async_test]
async fn test_actor_stops_on_signal() {
    let state = TestActor::new();
    let (_id, ctl, _data, _health, _out, handle) = spawn_test_actor(state);

    // Let it tick once
    aloeplatform::sleep(Duration::from_millis(30)).await;

    // Send stop
    ctl.send(ControlSignal::Stop).await.unwrap();

    // Actor task should complete
    let result = aloeplatform::time::timeout(Duration::from_millis(200), handle).await;
    assert!(result.is_ok(), "Actor should have stopped");
}

#[async_test]
async fn test_actor_pause_resume() {
    let (tick_tx, mut tick_rx) = mpsc::channel(64);
    let state = TestActor::new().with_tick_tx(tick_tx);
    let (_id, ctl, _data, _health, _out, handle) = spawn_test_actor(state);

    // Wait for at least one tick
    tick_rx.recv().await.expect("Should tick");

    // Pause
    ctl.send(ControlSignal::Pause).await.unwrap();
    aloeplatform::sleep(Duration::from_millis(50)).await;

    // Drain any ticks that arrived before pause took effect
    while tick_rx.try_recv().is_ok() {}

    // No ticks should arrive while paused
    let result = aloeplatform::time::timeout(Duration::from_millis(60), tick_rx.recv()).await;
    assert!(result.is_err(), "Should not tick while paused");

    // Resume
    ctl.send(ControlSignal::Resume).await.unwrap();

    // Should tick again
    let tick = aloeplatform::time::timeout(Duration::from_millis(200), tick_rx.recv()).await;
    assert!(tick.is_ok(), "Should tick after resume");

    // Clean up
    ctl.send(ControlSignal::Stop).await.unwrap();
    let _ = aloeplatform::time::timeout(Duration::from_millis(200), handle).await;
}

#[async_test]
async fn test_actor_natural_completion() {
    let state = TestActor::new().with_max_ticks(3);
    let (_id, _ctl, _data, _health, _out, handle) = spawn_test_actor(state);

    // Actor should complete after 3 ticks
    let result = aloeplatform::time::timeout(Duration::from_millis(500), handle).await;
    assert!(result.is_ok(), "Actor should have completed naturally");
}

#[async_test]
async fn test_actor_crash() {
    let state = TestActor::new().with_crash();
    let (_id, _ctl, _data, _health, _out, handle) = spawn_test_actor(state);

    // Actor should exit quickly after crashing
    let result = aloeplatform::time::timeout(Duration::from_millis(500), handle).await;
    assert!(result.is_ok(), "Actor should have exited after crash");
}

#[async_test]
async fn test_actor_receives_data() {
    let state = TestActor::new();
    let (_id, ctl, data_tx, _health, _out, handle) = spawn_test_actor(state);

    // Let it start
    aloeplatform::sleep(Duration::from_millis(30)).await;

    // Send data
    data_tx.send(TestData("hello".into())).await.unwrap();

    // Give it time to process
    aloeplatform::sleep(Duration::from_millis(30)).await;

    // Clean up
    ctl.send(ControlSignal::Stop).await.unwrap();
    let _ = aloeplatform::time::timeout(Duration::from_millis(200), handle).await;
}

#[async_test]
async fn test_actor_output() {
    let state = TestActor::new();
    let (_id, ctl, data_tx, _health, mut out_rx, handle) = spawn_test_actor(state);

    // Let it start
    aloeplatform::sleep(Duration::from_millis(30)).await;

    // Send data (on_data pushes to output_buffer)
    data_tx.send(TestData("world".into())).await.unwrap();

    // Wait for output to be drained on next tick
    let (actor_id, output) = aloeplatform::time::timeout(Duration::from_millis(200), out_rx.recv())
        .await
        .expect("Should receive output in time")
        .expect("Channel should not be closed");

    assert_eq!(actor_id, _id);
    assert_eq!(output, TestOutput("ECHO: world".into()));

    // Clean up
    ctl.send(ControlSignal::Stop).await.unwrap();
    let _ = aloeplatform::time::timeout(Duration::from_millis(200), handle).await;
}