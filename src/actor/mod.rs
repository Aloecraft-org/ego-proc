// The submodule sharing its parent's name is intentional: `actor::Actor` is
// the module's headline type and is re-exported below.
#[allow(clippy::module_inception)]
mod actor;
mod host;
mod orchestrator;

pub use actor::{Actor, ActorState};
pub use host::HostController;
pub use orchestrator::Orchestrator;
