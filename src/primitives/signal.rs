#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ControlSignal {
    Start,
    Stop,
    Pause,
    Resume,
    Notify,
}

pub trait ProcData: Send + Sync {}

pub trait ProcOutput: Send + Sync {}

/// Unit type for actors that produce no upward output.
pub struct NoOutput;
impl ProcOutput for NoOutput {}
