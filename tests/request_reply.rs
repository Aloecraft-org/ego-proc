//! Deadline'd request-reply: the reply future resolves with the answer or a
//! typed timeout, so callers never hang on a wedged actor.

mod common;
use common::async_test;

use async_trait::async_trait;
use ego_proc::actor::{ActorState, Orchestrator};
use ego_proc::ipc::{NoOutput, RequestError, RequestToken};
use ego_proc::{ControlSignal, OrchestrationStrategy};
use std::time::Duration;

#[derive(Clone)]
enum Query {
    /// Add the value to the running sum and reply with the new sum.
    Add(u64, RequestToken<u64>),
    /// Hold the token forever without replying (a wedged handler).
    Hold(RequestToken<u64>),
    /// Drop the token without replying.
    Discard(RequestToken<u64>),
}

struct Summer {
    sum: u64,
    held: Vec<RequestToken<u64>>,
}

#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait(?Send))]
impl ActorState for Summer {
    type D = Query;
    type O = NoOutput;

    fn interval(&self) -> Duration {
        Duration::from_millis(10)
    }

    async fn on_tick(&mut self) -> anyhow::Result<bool> {
        Ok(true)
    }

    async fn on_signal(&mut self, _: ControlSignal) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_data(&mut self, data: Self::D) -> anyhow::Result<()> {
        match data {
            Query::Add(value, token) => {
                self.sum += value;
                token.reply(self.sum);
            }
            Query::Hold(token) => self.held.push(token),
            Query::Discard(_token) => {}
        }
        Ok(())
    }
}

fn spawn_summer() -> (Orchestrator<Summer>, uuid::Uuid) {
    let mut orch = Orchestrator::<Summer>::new(OrchestrationStrategy::oneshot());
    let id = orch.spawn(Summer {
        sum: 0,
        held: Vec::new(),
    });
    (orch, id)
}

#[async_test]
async fn test_request_resolves_with_reply() {
    let (orch, id) = spawn_summer();
    let handle = orch.get_handle(id).unwrap();

    let sum = handle
        .request(Duration::from_secs(2), |token| Query::Add(40, token))
        .await
        .expect("reply within deadline");
    assert_eq!(sum, 40);

    let sum = handle
        .request(Duration::from_secs(2), |token| Query::Add(2, token))
        .await
        .expect("second reply within deadline");
    assert_eq!(sum, 42, "requests hit the same running actor state");

    orch.shutdown().await;
}

#[async_test]
async fn test_missed_deadline_is_a_real_typed_reply() {
    let (orch, id) = spawn_summer();
    let handle = orch.get_handle(id).unwrap();

    // The actor holds the token without answering; the caller must get a
    // typed timeout instead of hanging.
    let result = handle
        .request::<u64>(Duration::from_millis(50), Query::Hold)
        .await;
    match result {
        Err(RequestError::TimedOut { correlation_id }) => {
            assert!(!correlation_id.is_nil());
        }
        other => panic!("expected TimedOut, got {other:?}"),
    }

    orch.shutdown().await;
}

#[async_test]
async fn test_dropped_token_resolves_early() {
    let (orch, id) = spawn_summer();
    let handle = orch.get_handle(id).unwrap();

    // A discarded token is reported as Dropped well before the (long)
    // deadline would fire.
    let start = ego_platform::Instant::now();
    let result = handle
        .request::<u64>(Duration::from_secs(30), Query::Discard)
        .await;
    match result {
        Err(RequestError::Dropped { .. }) => {}
        other => panic!("expected Dropped, got {other:?}"),
    }
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "Dropped must resolve early, not wait out the deadline"
    );

    orch.shutdown().await;
}

#[async_test]
async fn test_send_failure_is_typed() {
    let (mut orch, id) = spawn_summer();
    let handle = orch.get_handle(id).unwrap().clone();

    // Stop the actor and let its task wind down so the data channel closes.
    orch.send_signal(id, ControlSignal::Stop).await;
    ego_platform::sleep(Duration::from_millis(100)).await;
    orch.maintain().await;

    let result = handle
        .request::<u64>(Duration::from_millis(200), |token| Query::Add(1, token))
        .await;
    match result {
        Err(RequestError::SendFailed { .. }) => {}
        other => panic!("expected SendFailed, got {other:?}"),
    }
}

#[async_test]
async fn test_token_first_reply_wins() {
    let (orch, id) = spawn_summer();
    let handle = orch.get_handle(id).unwrap();

    let sum = handle
        .request(Duration::from_secs(2), |token: RequestToken<u64>| {
            // The channel's Clone bound means tokens can be duplicated;
            // only the first reply across clones is delivered.
            let dup = token.clone();
            assert_eq!(token.correlation_id(), dup.correlation_id());
            Query::Add(7, token)
        })
        .await
        .unwrap();
    assert_eq!(sum, 7);

    orch.shutdown().await;
}
