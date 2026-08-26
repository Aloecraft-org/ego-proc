// The submodule sharing its parent's name is intentional: `actor::Actor` is
// the module's headline type and is re-exported below.
#[allow(clippy::module_inception)]
mod actor;
pub mod dormant;
pub mod foreign;
mod host;
mod orchestrator;

pub use actor::{Actor, ActorState, PassivateRequest};
pub use dormant::{
    DormantStore, InMemoryDormantStore, PassivateError, ReactivateError, SendDataError, WakePolicy,
};
pub use foreign::{
    BackpressurePolicy, Delivery, Drove, Foreign, ForeignActor, ForeignMetrics, PortMsg, WaitHint,
};
pub use host::HostController;
pub use orchestrator::Orchestrator;
