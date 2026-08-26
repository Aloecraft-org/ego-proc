//! Tests for the `ForeignActor` contract and the `Foreign<T>` adapter,
//! using a toy byte-counter state machine as the opaque handle.

mod common;
use common::async_test;

use ego_proc::OrchestrationStrategy;
use ego_proc::actor::{
    ActorState, BackpressurePolicy, Delivery, Drove, Foreign, ForeignActor, ForeignMetrics,
    Orchestrator, PortMsg, WaitHint,
};
use std::collections::VecDeque;
use std::time::Duration;

/// A toy foreign actor: counts bytes delivered to it and emits the running
/// total on port "count" after each message it digests.
struct ByteCounter {
    count: u64,
    queue: VecDeque<Vec<u8>>,
    capacity: usize,
    out: Vec<(String, Vec<u8>)>,
    drive_calls: u64,
    budget_spent: u64,
    budget_limit: u64,
    /// If set, park with this timeout when idle (None = park indefinitely).
    idle_park_timeout: Option<Duration>,
}

impl ByteCounter {
    fn new(capacity: usize) -> Self {
        Self {
            count: 0,
            queue: VecDeque::new(),
            capacity,
            out: Vec::new(),
            drive_calls: 0,
            budget_spent: 0,
            budget_limit: 0,
            idle_park_timeout: None,
        }
    }

    fn with_budget(mut self, spent: u64, limit: u64) -> Self {
        self.budget_spent = spent;
        self.budget_limit = limit;
        self
    }

    fn with_idle_park_timeout(mut self, timeout: Duration) -> Self {
        self.idle_park_timeout = Some(timeout);
        self
    }
}

impl ForeignActor for ByteCounter {
    fn drive(&mut self) -> anyhow::Result<Drove> {
        self.drive_calls += 1;
        match self.queue.pop_front() {
            Some(bytes) => {
                self.count += bytes.len() as u64;
                self.out
                    .push(("count".to_string(), self.count.to_le_bytes().to_vec()));
                Ok(Drove::Progressed)
            }
            None => Ok(Drove::Parked(WaitHint {
                timeout: self.idle_park_timeout,
            })),
        }
    }

    fn deliver(&mut self, port: &str, bytes: &[u8]) -> Delivery {
        if port == "reject" {
            return Delivery::Refused;
        }
        if self.queue.len() >= self.capacity {
            return Delivery::Full;
        }
        self.queue.push_back(bytes.to_vec());
        Delivery::Accepted
    }

    fn drain(&mut self, sink: &mut dyn FnMut(&str, &[u8])) {
        for (port, bytes) in self.out.drain(..) {
            sink(&port, &bytes);
        }
    }

    fn metrics(&self) -> ForeignMetrics {
        ForeignMetrics {
            budget_spent: Some(self.budget_spent),
            budget_limit: Some(self.budget_limit),
            bytes_held: Some(self.count),
        }
    }

    fn passivate(&mut self) -> Option<Vec<u8>> {
        if self.queue.is_empty() {
            Some(self.count.to_le_bytes().to_vec())
        } else {
            None
        }
    }
}

fn msg(port: &str, bytes: &[u8]) -> PortMsg {
    PortMsg {
        port: port.to_string(),
        bytes: bytes.to_vec(),
    }
}

// --- Adapter-level (unit-style) tests ---

#[async_test]
async fn test_deliver_drive_drain() {
    let mut foreign = Foreign::new(ByteCounter::new(8));

    foreign.on_data(msg("in", &[1, 2, 3])).await.unwrap();
    foreign.on_data(msg("in", &[4, 5])).await.unwrap();
    assert!(foreign.on_tick().await.unwrap());

    let out = foreign.take_output();
    assert_eq!(out.len(), 2, "one count report per digested message");
    assert_eq!(out[0].port, "count");
    assert_eq!(out[0].bytes, 3u64.to_le_bytes().to_vec());
    assert_eq!(out[1].bytes, 5u64.to_le_bytes().to_vec());
}

#[async_test]
async fn test_full_backpressure_is_buffered_then_retried() {
    // Capacity 1: the second on_data hits Full and is buffered by the
    // adapter, then retried on the next tick.
    let mut foreign = Foreign::new(ByteCounter::new(1));

    foreign.on_data(msg("in", &[1])).await.unwrap();
    foreign.on_data(msg("in", &[2, 3])).await.unwrap(); // Full -> buffered

    assert!(foreign.on_tick().await.unwrap()); // digests #1, flushes #2 next tick
    assert!(foreign.on_tick().await.unwrap());

    assert_eq!(
        foreign.inner().count,
        3,
        "both messages eventually digested"
    );
}

#[async_test]
async fn test_full_backpressure_refuse_policy_surfaces() {
    let mut foreign =
        Foreign::new(ByteCounter::new(1)).with_backpressure(BackpressurePolicy::Refuse);

    foreign.on_data(msg("in", &[1])).await.unwrap();
    let err = foreign.on_data(msg("in", &[2])).await.unwrap_err();
    assert!(
        err.to_string().contains("full"),
        "Full must be surfaced: {err}"
    );
}

#[async_test]
async fn test_full_backpressure_buffer_overflow_surfaces() {
    let mut foreign =
        Foreign::new(ByteCounter::new(1)).with_backpressure(BackpressurePolicy::Buffer(1));

    foreign.on_data(msg("in", &[1])).await.unwrap();
    foreign.on_data(msg("in", &[2])).await.unwrap(); // buffered
    let err = foreign.on_data(msg("in", &[3])).await.unwrap_err();
    assert!(
        err.to_string().contains("overflow"),
        "buffer overflow must be surfaced: {err}"
    );
}

#[async_test]
async fn test_refused_delivery_surfaces() {
    let mut foreign = Foreign::new(ByteCounter::new(8));
    let err = foreign.on_data(msg("reject", &[1])).await.unwrap_err();
    assert!(err.to_string().contains("refused"), "{err}");
}

#[async_test]
async fn test_indefinite_park_skips_driving_until_data() {
    let mut foreign = Foreign::new(ByteCounter::new(8));

    // First idle tick parks the handle (no deadline).
    assert!(foreign.on_tick().await.unwrap());
    let calls_after_park = foreign.inner().drive_calls;

    // Subsequent ticks are cheap no-ops: drive is not called.
    foreign.on_tick().await.unwrap();
    foreign.on_tick().await.unwrap();
    assert_eq!(foreign.inner().drive_calls, calls_after_park);

    // New data unparks.
    foreign.on_data(msg("in", &[9])).await.unwrap();
    foreign.on_tick().await.unwrap();
    assert!(foreign.inner().drive_calls > calls_after_park);
    assert_eq!(foreign.inner().count, 1);
}

#[async_test]
async fn test_timed_park_resumes_after_deadline() {
    let mut foreign =
        Foreign::new(ByteCounter::new(8).with_idle_park_timeout(Duration::from_millis(30)));

    assert!(foreign.on_tick().await.unwrap()); // parks with a 30ms hint
    let calls_after_park = foreign.inner().drive_calls;

    foreign.on_tick().await.unwrap(); // still parked
    assert_eq!(foreign.inner().drive_calls, calls_after_park);

    ego_platform::sleep(Duration::from_millis(40)).await;
    foreign.on_tick().await.unwrap(); // deadline passed: drives again
    assert!(foreign.inner().drive_calls > calls_after_park);
}

#[async_test]
async fn test_saturation_derived_from_metrics() {
    let foreign = Foreign::new(ByteCounter::new(8).with_budget(25, 100));
    assert_eq!(foreign.saturation(), Some(0.25));

    let no_budget = Foreign::new(ByteCounter::new(8));
    assert_eq!(no_budget.saturation(), None, "0-limit budget reports None");
}

// --- Orchestrator-level (integration) tests ---

#[async_test]
async fn test_orchestrated_spawn_tick_deliver_drain() {
    let mut orch = Orchestrator::<Foreign<ByteCounter>>::new(OrchestrationStrategy::oneshot());
    let id =
        orch.spawn(Foreign::new(ByteCounter::new(8)).with_tick_interval(Duration::from_millis(10)));

    orch.send_data(id, msg("in", &[1, 2, 3, 4])).await.unwrap();
    ego_platform::sleep(Duration::from_millis(60)).await;

    let (from, out) = orch
        .recv_output()
        .expect("count report should have arrived");
    assert_eq!(from, id);
    assert_eq!(out.port, "count");
    assert_eq!(out.bytes, 4u64.to_le_bytes().to_vec());

    orch.shutdown().await;
}

#[async_test]
async fn test_saturation_visible_in_actor_health() {
    let mut orch = Orchestrator::<Foreign<ByteCounter>>::new(OrchestrationStrategy::oneshot());
    let id = orch.spawn(
        Foreign::new(ByteCounter::new(8).with_budget(50, 100))
            .with_tick_interval(Duration::from_millis(10)),
    );

    let mut health_rx = orch.get_handle(id).unwrap().subscribe();
    let report = ego_platform::timeout(Duration::from_secs(2), health_rx.recv())
        .await
        .expect("health report within deadline")
        .expect("health channel open");

    // The measured budget ratio wins over the run loop's tick-latency
    // estimate (which would be ~0 for this near-idle actor... and never
    // exactly 0.5).
    assert_eq!(report.saturation, 0.5);

    orch.shutdown().await;
}
