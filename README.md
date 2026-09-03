# ego-proc

<div align="center">

<img src="doc/icon.png" style="height:96px; width:96px;"/>

**it's ego-proc**

[![CI](https://github.com/Aloecraft-org/ego-proc/actions/workflows/ci.yml/badge.svg)](https://github.com/Aloecraft-org/ego-proc/actions/workflows/ci.yml)

</div>

## What It Is

Actor and orchestration primitives for the ego ecosystem, built on
[ego-platform](https://github.com/Aloecraft-org/ego-platform) so the same
actor code runs on **native**, **WASI P2**, and **browser** targets.

- **`Actor` / `ActorState`** — a tick-driven actor loop: implement `on_tick`,
  `on_signal`, and `on_data`, and optionally buffer output for the layer above
- **`Orchestrator<S>`** — a strictly typed supervisor for a group of actors:
  spawning (anonymous or named), health monitoring, restart strategies
  (`oneshot`, `restart`, `restart_at_most(n)`), signal broadcast, and
  upward output draining
- **`ActorHandle`** — clonable handle carrying control, data, and health
  channels for one actor, plus deadline'd request-reply (`handle.request`):
  the reply future resolves with the answer or a typed timeout, so callers
  never hang on a wedged actor
- **`ForeignActor` / `Foreign<T>`** — a lower-level contract for actors whose
  state lives behind an opaque handle (an embedded interpreter, a wasm
  instance, C-library state), lifted into `ActorState` by one adapter that
  owns pacing, park bookkeeping, and backpressure policy
- **Dormancy** — `passivate` a live actor to bytes and `reactivate` it later
  under the same `Uuid` (optionally woken automatically by incoming data);
  where the bytes live is a pluggable `DormantStore`, in-memory by default
- **Protobuf control plane** — `ControlSignal`, `LifecycleStatus`,
  `ActorHealth`, and `OrchestrationStrategy` are defined in
  [`proto/ego_proc.proto`](proto/ego_proc.proto) and generated at build time
  with prost, with serde support for JS-friendly JSON

Supervision trees compose naturally: a manager actor can own its own
`Orchestrator` of workers, and so on.

## Example

```rust
use ego_proc::actor::{ActorState, Orchestrator};
use ego_proc::ipc::{NoOutput, ProcData};
use ego_proc::{ControlSignal, OrchestrationStrategy};
use async_trait::async_trait;

#[derive(Clone)]
struct Ping(String);
impl ProcData for Ping {}

struct Worker;

#[async_trait]
impl ActorState for Worker {
    type D = Ping;
    type O = NoOutput;

    async fn on_tick(&mut self) -> anyhow::Result<bool> {
        // Do periodic work; return false to stop.
        Ok(true)
    }
    async fn on_signal(&mut self, _s: ControlSignal) -> anyhow::Result<()> {
        Ok(())
    }
    async fn on_data(&mut self, data: Ping) -> anyhow::Result<()> {
        log::info!("got: {}", data.0);
        Ok(())
    }
}

# async fn demo() {
let mut orch = Orchestrator::<Worker>::new(OrchestrationStrategy::restart());
let id = orch.spawn(Worker);
orch.send_signal(id, ControlSignal::Pause).await;
orch.broadcast(ControlSignal::Stop).await;
# }
```

## Building

### Prerequisites

- Rust 1.88+ (the crate uses the 2024 edition)
- [`protoc`](https://protobuf.dev/) on `PATH` (the build script compiles
  `proto/ego_proc.proto` via prost-build)
- Targets: `rustup target add wasm32-wasip2 wasm32-unknown-unknown`
- WASI test runtime: [wasmtime](https://wasmtime.dev/) (used as the cargo
  runner, see `.cargo/config.toml`)
- Browser tests: `wasm-bindgen-cli` **matching the `wasm-bindgen` version in
  `Cargo.lock`** (pinned via ego-platform, currently 0.2.127) plus a browser
  and webdriver (e.g. Firefox + geckodriver)

The devcontainer in `.devcontainer/` has all of this preinstalled.

### Quick Build

```bash
make check   # cargo check on all three targets
make test    # run tests on all three targets
make build   # build all three targets
```

Each verb also has per-platform variants (`make test_native`, `make
test_wasi`, `make test_browser`; same for `build` and `check`). Append
`quiet` to suppress rustc warnings (`make test quiet`).

## Continuous Integration

GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs
the same steps as the devcontainer workflow on every push and pull request:

- `make fmt_check` and `make clippy` (all three targets, warnings denied)
- Native build + tests on Linux, macOS, and Windows
- WASI build + tests under wasmtime
- Browser build + tests under `wasm-bindgen-test-runner` with a headless browser

## Contributing

1. All tests pass: `make test`
2. Code is formatted: `make fmt`
3. No clippy warnings on any target: `make clippy`

`make ci` runs the same sequence as GitHub Actions.
