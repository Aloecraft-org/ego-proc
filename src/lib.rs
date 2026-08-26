pub mod actor;
pub mod ipc;

include!(concat!(env!("OUT_DIR"), "/ego2_proto.ego_proc.rs"));

impl OrchestrationStrategy {
    pub fn oneshot() -> Self {
        Self {
            strategy_type: OrchestrationType::OneShot as i32,
            restart_limit: -1
        }
    }

    pub fn restart_at_most(restart_limit: i32) -> Self {
        Self {
            strategy_type: OrchestrationType::Restart as i32,
            restart_limit: restart_limit
        }
    }

    pub fn restart() -> Self {
        Self {
            strategy_type: OrchestrationType::Restart as i32,
            restart_limit: -1
        }
    }
}