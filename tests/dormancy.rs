//! Dormancy tests: passivate → Dormant slot (same Uuid) → reactivate with
//! state preserved → wake-on-data, plus the typed refusals.

mod common;
use common::async_test;

use ego_proc::OrchestrationStrategy;
use ego_proc::actor::{
    Delivery, DormantStore, Drove, Foreign, ForeignActor, ForeignMetrics, InMemoryDormantStore,
    Orchestrator, PassivateError, PortMsg, SendDataError, WaitHint, WakePolicy,
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

/// Toy handle: counts bytes, passivates to the count's le-bytes, rehydrates
/// from them. Refuses to passivate while input is undigested.
struct Counter {
    count: u64,
    queue: VecDeque<Vec<u8>>,
    stuck: bool,
}

impl Counter {
    fn new() -> Self {
        Self {
            count: 0,
            queue: VecDeque::new(),
            stuck: false,
        }
    }

    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("expected 8 bytes"))?;
        Ok(Self {
            count: u64::from_le_bytes(arr),
            queue: VecDeque::new(),
            stuck: false,
        })
    }

    fn stuck() -> Self {
        Self {
            stuck: true,
            ..Self::new()
        }
    }
}

impl ForeignActor for Counter {
    fn drive(&mut self) -> anyhow::Result<Drove> {
        if self.stuck {
            return Ok(Drove::Parked(WaitHint { timeout: None }));
        }
        match self.queue.pop_front() {
            Some(bytes) => {
                self.count += bytes.len() as u64;
                Ok(Drove::Progressed)
            }
            None => Ok(Drove::Parked(WaitHint { timeout: None })),
        }
    }

    fn deliver(&mut self, _port: &str, bytes: &[u8]) -> Delivery {
        self.queue.push_back(bytes.to_vec());
        Delivery::Accepted
    }

    fn drain(&mut self, sink: &mut dyn FnMut(&str, &[u8])) {
        // Report the running count once per drain so tests can observe it.
        sink("count", &self.count.to_le_bytes());
    }

    fn metrics(&self) -> ForeignMetrics {
        ForeignMetrics::default()
    }

    fn passivate(&mut self) -> Option<Vec<u8>> {
        if self.queue.is_empty() {
            Some(self.count.to_le_bytes().to_vec())
        } else {
            None
        }
    }
}

fn new_orch() -> Orchestrator<Foreign<Counter>> {
    Orchestrator::new(OrchestrationStrategy::oneshot()).with_rehydrator(|bytes| {
        Ok(Foreign::new(Counter::from_bytes(bytes)?).with_tick_interval(Duration::from_millis(10)))
    })
}

fn spawn_counter(orch: &mut Orchestrator<Foreign<Counter>>) -> Uuid {
    orch.spawn(Foreign::new(Counter::new()).with_tick_interval(Duration::from_millis(10)))
}

fn msg(bytes: &[u8]) -> PortMsg {
    PortMsg {
        port: "in".to_string(),
        bytes: bytes.to_vec(),
    }
}

/// Latest observed count from the orchestrator's output stream.
fn last_count(orch: &mut Orchestrator<Foreign<Counter>>, from: Uuid) -> Option<u64> {
    let mut last = None;
    while let Some((id, out)) = orch.recv_output() {
        if id == from && out.port == "count" {
            last = Some(u64::from_le_bytes(out.bytes.try_into().unwrap()));
        }
    }
    last
}

#[async_test]
async fn test_passivate_reactivate_preserves_uuid_and_state() {
    let mut orch = new_orch();
    let id = spawn_counter(&mut orch);

    orch.send_data(id, msg(&[1, 2, 3])).await.unwrap();
    ego_platform::sleep(Duration::from_millis(60)).await;
    assert_eq!(last_count(&mut orch, id), Some(3));

    // Passivate: slot goes dormant, Uuid persists, handle disappears.
    orch.passivate(id, WakePolicy::WakeOnData).await.unwrap();
    assert!(orch.is_dormant(&id));
    assert!(orch.get_handle(id).is_none());
    assert!(orch.dormant_ids().any(|d| *d == id));

    // maintain() treats Dormant as alive-but-swapped: no restart, no cleanup.
    orch.maintain().await;
    assert!(
        orch.is_dormant(&id),
        "maintain must not touch dormant slots"
    );
    assert!(
        orch.handles.is_empty(),
        "maintain must not respawn dormant slots"
    );

    // Reactivate: same Uuid, state preserved.
    let back = orch.reactivate(id).unwrap();
    assert_eq!(back, id, "the Uuid must persist across the transition");
    assert!(!orch.is_dormant(&id));
    assert!(orch.get_handle(id).is_some());

    orch.send_data(id, msg(&[4, 5])).await.unwrap();
    ego_platform::sleep(Duration::from_millis(60)).await;
    assert_eq!(
        last_count(&mut orch, id),
        Some(5),
        "count continues from the passivated state"
    );

    orch.shutdown().await;
}

#[async_test]
async fn test_passivate_refused_when_not_at_stop_point() {
    let mut orch = new_orch();
    // A stuck handle never digests its queue, so it is never at a stop point
    // once data has been delivered.
    let id =
        orch.spawn(Foreign::new(Counter::stuck()).with_tick_interval(Duration::from_millis(10)));

    orch.send_data(id, msg(&[1])).await.unwrap();
    ego_platform::sleep(Duration::from_millis(50)).await;

    match orch.passivate(id, WakePolicy::WakeOnData).await {
        Err(PassivateError::NotAtStopPoint(refused)) => assert_eq!(refused, id),
        other => panic!("expected NotAtStopPoint, got {other:?}"),
    }

    // The refusal left the actor running.
    assert!(!orch.is_dormant(&id));
    assert!(orch.get_handle(id).is_some());

    orch.shutdown().await;
}

#[async_test]
async fn test_wake_on_data() {
    let mut orch = new_orch();
    let id = spawn_counter(&mut orch);

    orch.send_data(id, msg(&[1, 2])).await.unwrap();
    ego_platform::sleep(Duration::from_millis(60)).await;
    orch.passivate(id, WakePolicy::WakeOnData).await.unwrap();
    assert!(orch.is_dormant(&id));

    // send_data to the dormant slot rehydrates, then delivers.
    orch.send_data(id, msg(&[3, 4, 5])).await.unwrap();
    assert!(
        !orch.is_dormant(&id),
        "wake-on-data must reactivate the slot"
    );
    assert!(orch.get_handle(id).is_some());

    ego_platform::sleep(Duration::from_millis(60)).await;
    assert_eq!(last_count(&mut orch, id), Some(5));

    orch.shutdown().await;
}

#[async_test]
async fn test_dormant_refusal_is_typed_never_silent() {
    let mut orch = new_orch();
    let id = spawn_counter(&mut orch);

    ego_platform::sleep(Duration::from_millis(30)).await;
    orch.passivate(id, WakePolicy::Refuse).await.unwrap();

    match orch.send_data(id, msg(&[1])).await {
        Err(SendDataError::DormantRefused(refused)) => assert_eq!(refused, id),
        other => panic!("expected DormantRefused, got {other:?}"),
    }
    assert!(
        orch.is_dormant(&id),
        "a refused send must leave the slot dormant"
    );
}

#[async_test]
async fn test_send_data_to_unknown_actor_is_typed() {
    let mut orch = new_orch();
    let id = Uuid::new_v4();
    match orch.send_data(id, msg(&[1])).await {
        Err(SendDataError::UnknownActor(unknown)) => assert_eq!(unknown, id),
        other => panic!("expected UnknownActor, got {other:?}"),
    }
}

/// A store two orchestrators can share, standing in for a durable one.
#[derive(Clone, Default)]
struct SharedStore(Arc<Mutex<InMemoryDormantStore>>);

impl DormantStore for SharedStore {
    fn put(&mut self, id: Uuid, bytes: Vec<u8>) -> anyhow::Result<()> {
        self.0.lock().unwrap().put(id, bytes)
    }
    fn get(&self, id: &Uuid) -> anyhow::Result<Option<Vec<u8>>> {
        self.0.lock().unwrap().get(id)
    }
    fn del(&mut self, id: &Uuid) -> anyhow::Result<()> {
        self.0.lock().unwrap().del(id)
    }
}

#[async_test]
async fn test_restarted_orchestrator_thaws_actors_it_never_spawned() {
    let store = SharedStore::default();

    // First orchestrator: run an actor to count 4, then passivate.
    let mut orch1 = new_orch().with_dormant_store(store.clone());
    let id = spawn_counter(&mut orch1);
    orch1.send_data(id, msg(&[9, 9, 9, 9])).await.unwrap();
    ego_platform::sleep(Duration::from_millis(60)).await;
    orch1.passivate(id, WakePolicy::WakeOnData).await.unwrap();
    drop(orch1);

    // "Restarted" orchestrator: same rehydrator + store, never spawned `id`.
    let mut orch2 = new_orch().with_dormant_store(store);
    let back = orch2.reactivate(id).unwrap();
    assert_eq!(back, id);

    orch2.send_data(id, msg(&[7])).await.unwrap();
    ego_platform::sleep(Duration::from_millis(60)).await;
    assert_eq!(
        last_count(&mut orch2, id),
        Some(5),
        "state thawed from bytes alone"
    );

    orch2.shutdown().await;
}
