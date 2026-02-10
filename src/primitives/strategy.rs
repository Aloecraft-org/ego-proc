
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrchestratorStrategy {
    /// Restart the actor indefinitely.
    Restart,
    /// Restart up to N times, then stop.
    RestartAtMost(u32),
    /// If the actor dies, do nothing (fire and forget).
    OneShot,
    /// If the actor dies, kill the parent (escalate error).
    Escalate,
}