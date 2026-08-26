//! Dormancy: passivate a live actor's state to bytes and later reactivate
//! it under the same Uuid. Dormancy is a lifecycle state on a *slot*, not
//! ownership and not destruction.
//!
//! Storage is a hook, not a feature: the orchestrator owns the transition,
//! never the persistence policy. Consumers plug a durable [`DormantStore`]
//! in; the default keeps bytes in memory.

use crate::ipc::PlatformSendSync;
use std::collections::HashMap;
use uuid::Uuid;

/// Where passivated actor bytes live while a slot is dormant.
pub trait DormantStore: PlatformSendSync {
    fn put(&mut self, id: Uuid, bytes: Vec<u8>) -> anyhow::Result<()>;
    fn get(&self, id: &Uuid) -> anyhow::Result<Option<Vec<u8>>>;
    fn del(&mut self, id: &Uuid) -> anyhow::Result<()>;
}

/// In-memory default store. Bytes do not survive the process.
#[derive(Default)]
pub struct InMemoryDormantStore {
    slots: HashMap<Uuid, Vec<u8>>,
}

impl DormantStore for InMemoryDormantStore {
    fn put(&mut self, id: Uuid, bytes: Vec<u8>) -> anyhow::Result<()> {
        self.slots.insert(id, bytes);
        Ok(())
    }

    fn get(&self, id: &Uuid) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.slots.get(id).cloned())
    }

    fn del(&mut self, id: &Uuid) -> anyhow::Result<()> {
        self.slots.remove(id);
        Ok(())
    }
}

/// What `send_data` does when it targets a dormant slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WakePolicy {
    /// Rehydrate the actor, then deliver the data.
    #[default]
    WakeOnData,
    /// Leave the slot dormant and return a typed refusal. Never silent.
    Refuse,
}

#[derive(Debug)]
pub enum PassivateError {
    /// No live actor with this id.
    UnknownActor(Uuid),
    /// The actor is not at a stop point; it keeps running.
    NotAtStopPoint(Uuid),
    /// The actor did not answer the passivation request (wedged or exiting).
    NoResponse(Uuid),
    /// The store rejected the bytes. The actor has already stopped by this
    /// point, so the bytes are handed back to the caller for recovery.
    Store {
        source: anyhow::Error,
        bytes: Vec<u8>,
    },
}

impl std::fmt::Display for PassivateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownActor(id) => write!(f, "no live actor {id}"),
            Self::NotAtStopPoint(id) => write!(f, "actor {id} is not at a stop point"),
            Self::NoResponse(id) => write!(f, "actor {id} did not answer the passivation request"),
            Self::Store { source, .. } => write!(f, "dormant store rejected the bytes: {source}"),
        }
    }
}

impl std::error::Error for PassivateError {}

#[derive(Debug)]
pub enum ReactivateError {
    /// Neither the dormant registry nor the store knows this id.
    UnknownActor(Uuid),
    /// No rehydrator registered; see `Orchestrator::with_rehydrator`.
    NoRehydrator,
    /// The rehydrator failed to build a state from the stored bytes.
    Rehydrate(anyhow::Error),
    Store(anyhow::Error),
}

impl std::fmt::Display for ReactivateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownActor(id) => write!(f, "no dormant actor {id}"),
            Self::NoRehydrator => write!(f, "no rehydrator registered (use with_rehydrator)"),
            Self::Rehydrate(e) => write!(f, "rehydration failed: {e}"),
            Self::Store(e) => write!(f, "dormant store error: {e}"),
        }
    }
}

impl std::error::Error for ReactivateError {}

#[derive(Debug)]
pub enum SendDataError {
    /// No live or dormant actor with this id.
    UnknownActor(Uuid),
    /// The slot is dormant and its wake policy is [`WakePolicy::Refuse`].
    DormantRefused(Uuid),
    /// Waking the slot failed.
    Reactivate(ReactivateError),
    /// The actor's data channel is closed (task gone).
    ChannelClosed(Uuid),
}

impl std::fmt::Display for SendDataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownActor(id) => write!(f, "no actor {id}"),
            Self::DormantRefused(id) => {
                write!(f, "actor {id} is dormant and its wake policy refuses data")
            }
            Self::Reactivate(e) => write!(f, "wake-on-data failed: {e}"),
            Self::ChannelClosed(id) => write!(f, "data channel to actor {id} is closed"),
        }
    }
}

impl std::error::Error for SendDataError {}

impl From<ReactivateError> for SendDataError {
    fn from(e: ReactivateError) -> Self {
        Self::Reactivate(e)
    }
}
