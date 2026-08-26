//! Actor and orchestration primitives for the ego ecosystem.
//!
//! Built on [`ego_platform`] so the same actor code runs on native, WASI P2,
//! and browser targets. Control-plane types ([`ControlSignal`],
//! [`ActorHealth`], [`OrchestrationStrategy`], ...) are defined in
//! `proto/ego_proc.proto` and generated at build time via prost.

pub mod actor;
pub mod ipc;

include!(concat!(env!("OUT_DIR"), "/ego2_proto.ego_proc.rs"));

impl OrchestrationStrategy {
    /// Actors run to completion and are not restarted.
    pub fn oneshot() -> Self {
        Self {
            strategy_type: OrchestrationType::OneShot as i32,
            restart_limit: -1,
        }
    }

    /// Restart dead actors, giving up after `restart_limit` restarts.
    pub fn restart_at_most(restart_limit: i32) -> Self {
        Self {
            strategy_type: OrchestrationType::Restart as i32,
            restart_limit,
        }
    }

    /// Restart dead actors indefinitely.
    pub fn restart() -> Self {
        Self {
            strategy_type: OrchestrationType::Restart as i32,
            restart_limit: -1,
        }
    }
}
