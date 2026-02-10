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