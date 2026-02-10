use uuid::Uuid;

/// Metrics reported by the Supervisor/Monitor.
#[derive(Debug, Clone)]
pub struct Report {
    pub saturation: f64, // 0.0 to 1.0 (Tick Duration / Interval)
    pub last_tick_ms: u64,
    pub is_alive: bool,
    pub actor_id: Uuid,
}