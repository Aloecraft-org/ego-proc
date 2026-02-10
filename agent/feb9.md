## Phase 1 & 2 Implementation Plan

### 1. Domain ID + Spawn Context

The problem: factories are created once but you need per-spawn context (connection ID, config, etc).

**Change `spawn` to accept optional context:**

```rust
// Orchestrator gets a new spawn signature
pub fn spawn_with<C>(&mut self, factory: impl FnOnce(C) -> S, ctx: C) -> Uuid

// Original spawn still works for no-context cases
pub fn spawn(&mut self, state: S) -> Uuid
```

**Add a domain-ID registry inside Orchestrator:**

```rust
// Inside Orchestrator<S>
domain_map: HashMap<String, Uuid>,  // "conn:42" -> actor UUID

pub fn spawn_named(&mut self, name: String, state: S) -> Uuid
pub fn get_by_name(&self, name: &str) -> Option<&ActorHandle<S::D>>
```

This keeps UUID as the internal identity while giving managers a way to map domain concepts. String keys are simple and flexible enough for connection IDs, session IDs, etc.

---

### 2. Formalize Upward Data Flow (ProcOutput)

**Add an associated output type to ActorState:**

```rust
#[async_trait]
pub trait ActorState: Send + Sync {
    type D: ProcData;           // downward (commands in)
    type O: ProcOutput;         // upward (reports out)
    // ... existing methods ...
}
```

**Wire it through Actor and Orchestrator:**

- `Actor<S>` gets an `output_tx: mpsc::Sender<(Uuid, S::O)>` — tagged with actor ID so the manager knows who sent it.
- `Orchestrator<S>` owns the `output_rx: mpsc::Receiver<(Uuid, S::O)>` end and exposes `pub fn recv_output(&mut self) -> Option<(Uuid, S::O)>`.
- On spawn, Orchestrator clones its `output_tx` into each new Actor.

For actors that don't produce output, add a unit type:

```rust
pub struct NoOutput;
impl ProcOutput for NoOutput {}
```

---

### 3. Clarify Manager

Manager is not a separate trait — it's the **pattern** of an ActorState that owns an Orchestrator. Remove `manager.rs` from the framework. Instead, document the pattern:

```rust
// This IS the Manager pattern — just an ActorState that supervises children
pub struct ConnectionManager {
    connections: Orchestrator<ConnectionState>,
}

impl ActorState for ConnectionManager {
    type D = ManagerCmd;
    type O = ManagerEvent;  // aggregated events going up to controller

    async fn on_tick(&mut self) -> anyhow::Result<bool> {
        self.connections.maintain().await;

        // Drain worker outputs — THIS is the manager's job
        while let Some((worker_id, output)) = self.connections.recv_output() {
            self.handle_worker_output(worker_id, output).await;
        }
        Ok(true)
    }
}
```

The `Manager` trait and `ManagerState` struct get deleted. The concept lives as a documented pattern, not a trait. This means your "Worker/Manager" hybrids from the architecture are just ActorStates that happen to own an Orchestrator — no special framework support needed.

---

### 4. Delete Generic Controller

Remove `src/actor/controller.rs`. Controllers are user-defined structs (like `TransportController` in the example). No framework type needed.

---

### Phase 2: Tests

All tests in `tests/` at the crate root.

**Test Actor: the building block for everything**

```rust
struct TestActor {
    tick_count: u32,
    max_ticks: Option<u32>,      // completes after N ticks
    should_crash: bool,           // panics on next tick
    output_tx: mpsc::Sender<(Uuid, TestOutput)>,
}
```

This single configurable actor covers all lifecycle scenarios.

**Test file 1: `tests/actor_lifecycle.rs`**

| Test | What it verifies |
|---|---|
| `test_actor_runs_and_ticks` | Actor ticks, health reports arrive |
| `test_actor_stops_on_signal` | Send Stop, actor task completes |
| `test_actor_pause_resume` | Pause stops ticking, Resume restarts |
| `test_actor_natural_completion` | Return `Ok(false)` from on_tick, actor exits |
| `test_actor_crash` | Panic/error in on_tick, actor exits |
| `test_actor_receives_data` | Send data via handle, on_data fires |
| `test_actor_output` | Actor sends output, receiver gets it |

**Test file 2: `tests/orchestrator_lifecycle.rs`**

| Test | What it verifies |
|---|---|
| `test_spawn_and_maintain` | Spawn actor, maintain sees it alive |
| `test_death_detection` | Actor crashes, maintain detects it |
| `test_restart_strategy` | Dead actor gets respawned via factory |
| `test_restart_limit` | RestartAtMost(3) stops after 3 |
| `test_oneshot_no_restart` | Dead OneShot actor stays dead |
| `test_escalate_flag` | Escalate sets an error state (need to add this) |
| `test_broadcast_stop` | All actors stop on broadcast |
| `test_named_spawn_lookup` | spawn_named + get_by_name round-trips |
| `test_output_collection` | recv_output returns worker outputs with correct IDs |

**Test file 3: `tests/supervision_tree.rs`**

| Test | What it verifies |
|---|---|
| `test_manager_pattern` | Manager actor supervises workers, drains outputs |
| `test_kill_worker_manager_restarts` | Worker dies, manager's maintain restarts it |
| `test_kill_manager_escalates` | Manager dies, controller-level orchestrator handles it |
| `test_graceful_shutdown_tree` | Stop propagates controller → manager → workers, all tasks complete |

---

### Implementation Order

1. Add `type O` to ActorState, wire through Actor/Orchestrator (touches everything, do it first)
2. Add `spawn_named` / domain map to Orchestrator
3. Delete Manager trait and generic Controller
4. Update transport example to compile against new API
5. Write TestActor
6. Tests file 1 (actor)
7. Tests file 2 (orchestrator)
8. Tests file 3 (supervision tree)

Steps 1–4 are probably a single focused session. Tests can be incremental after that.