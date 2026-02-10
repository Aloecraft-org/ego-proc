#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Status {
    New,
    Running,
    Paused,
    Complete,
    Error
}