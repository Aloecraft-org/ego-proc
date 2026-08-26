mod common;
use common::async_test;

use async_trait::async_trait;
use ego_proc::actor::{ActorState, Orchestrator};
use ego_proc::ipc::{NoOutput, ProcData};
use ego_proc::{ControlSignal, OrchestrationStrategy};
use std::time::Duration;
use tokio::sync::mpsc;

// 1. Define the Data Packet
#[derive(Debug, Clone, PartialEq)]
struct TestMsg(String);
impl ProcData for TestMsg {}

// 2. Define the State
struct EchoState {
    // Channel to talk back to the test runner
    reply_tx: mpsc::Sender<String>,
}

#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait(?Send))]
impl ActorState for EchoState {
    type D = TestMsg;
    type O = NoOutput;

    fn interval(&self) -> Duration {
        Duration::from_millis(10)
    }

    async fn on_tick(&mut self) -> anyhow::Result<bool> {
        // Just keep swimming
        Ok(true)
    }

    async fn on_signal(&mut self, _signal: ControlSignal) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_data(&mut self, data: Self::D) -> anyhow::Result<()> {
        if data.0 == "CRASH" {
            // WASI FRIENDLY "CRASH":
            // We return an error, which causes the Actor loop
            // (in actor.rs) to log the error and break running = false.
            // This stops the actor naturally without aborting the process.
            return Err(anyhow::anyhow!("Simulated Crash"));
        }
        let _ = self.reply_tx.send(format!("ECHO: {}", data.0)).await;
        Ok(())
    }
}

#[async_test]
async fn test_actor_communication() {
    // Setup reply channel (Test listens to this)
    let (reply_tx, mut reply_rx) = mpsc::channel(1);

    // Setup Orchestrator
    let mut orch = Orchestrator::<EchoState>::new(OrchestrationStrategy::oneshot());

    // Spawn Actor
    let state = EchoState { reply_tx };
    let id = orch.spawn(state);

    // Get the handle from the Orchestrator (Assuming you have a getter or pub access)
    // For this test, we can use the `send_signal` or broadcast,
    // but typically you'd expose a way to get the handle.
    // Let's assume we added `pub fn get_handle(&self, id: Uuid) -> Option<&ActorHandle<S::D>>`
    // OR we just use the Orchestrator's methods if they exist.

    // NOTE: Since your Orchestrator code didn't have a `get_handle` method,
    // we will simulate the handle usage or just assume we can add one.
    // Let's stick to what's public:

    // We can't easily send data via Orchestrator (it only had send_signal).
    // *Recommendation*: Add `pub fn get_handle(...)` to Orchestrator.
    // For now, I will assume we can access `orch.handles` or similar.

    // WORKAROUND for test: We need the handle to send Data.
    let handle = orch.get_handle(id).expect("Handle should exist");

    // 1. Send Data
    handle.notify(TestMsg("Hello".to_string())).await.unwrap();

    // 2. Assert Reply
    let response = reply_rx.recv().await.expect("Should receive echo");
    assert_eq!(response, "ECHO: Hello");

    // 3. Send Stop
    handle.stop().await.unwrap();

    // 4. Verify Shutdown
    ego_platform::sleep(Duration::from_millis(50)).await;
    orch.maintain().await;

    // The actor should be gone from the orchestrator
    assert!(orch.get_handle(id).is_none());
}

#[async_test]
async fn test_orchestrator_restarts_crashed_actor() {
    let (reply_tx, mut reply_rx) = mpsc::channel(10);
    let reply_tx_clone = reply_tx.clone();

    // 1. Setup Orchestrator with RESTART strategy and a FACTORY
    let mut orch =
        Orchestrator::<EchoState>::new(OrchestrationStrategy::restart()).with_factory(move || {
            EchoState {
                reply_tx: reply_tx_clone.clone(),
            }
        });

    // 2. Spawn initial actor
    let initial_state = EchoState { reply_tx };
    let old_id = orch.spawn(initial_state);

    let handle = orch.get_handle(old_id).unwrap();

    // 3. Verify it's alive
    handle.notify(TestMsg("Alive?".to_string())).await.unwrap();
    assert_eq!(reply_rx.recv().await.unwrap(), "ECHO: Alive?");

    // 4. KILL IT (Trigger Panic via Data)
    // Note: Panics in tokio tasks are caught by the JoinHandle.
    // We send a message that causes `on_data` to panic.
    let _ = handle.notify(TestMsg("CRASH".to_string())).await;

    // 5. Wait for death
    ego_platform::sleep(Duration::from_millis(150)).await;

    // 6. Run Maintenance
    // This should see the finished JoinHandle and run the factory
    orch.maintain().await;

    ego_platform::sleep(Duration::from_millis(150)).await;

    // 7. Verify Respawn
    // We can't ask for `old_id` anymore. We need to find the NEW id.
    // (Assuming Orchestrator has a way to list IDs or we just grab the only one)
    let new_id = *orch.handles.keys().next().unwrap();

    assert_ne!(old_id, new_id, "The spawned actor should have a NEW UUID");

    // 8. Verify the new guy works
    let new_handle = orch.get_handle(new_id).unwrap();
    new_handle
        .notify(TestMsg("I am back".to_string()))
        .await
        .unwrap();

    assert_eq!(reply_rx.recv().await.unwrap(), "ECHO: I am back");
}
