# Current Status:

### 1. Domain ID + Spawn Context

The problem: factories are created once but you need per-spawn context (connection ID, config, etc).

**Change `spawn` to accept optional context:**

```rust
// Orchestrator gets a new spawn signature
pub fn spawn_with<C>(&mut self, factory: impl FnOnce(C) -> S, ctx: C) -> Uuid

// Original spawn still works for no-context cases
pub fn spawn(&mut self, state: S) -> Uuid
```

**Add a domain-ID registry inside Orchestrator:**

```rust
// Inside Orchestrator<S>
domain_map: HashMap<String, Uuid>,  // "conn:42" -> actor UUID

pub fn spawn_named(&mut self, name: String, state: S) -> Uuid
pub fn get_by_name(&self, name: &str) -> Option<&ActorHandle<S::D>>
```

This keeps UUID as the internal identity while giving managers a way to map domain concepts. String keys are simple and flexible enough for connection IDs, session IDs, etc.

## Status:

- Starting Checkpoint 1. Domain ID + Spawn Context

## History:

- n/a

## Current Output:

- n/a

## NOTES:

- ConnectionInfo went into connection, otherwise I just moved TransportType NatType and PeerRelationship to src/types/mod.rs
- client/session.rs changed ClientSession to RuntimeSession 
    + leaving crypto/session.rs alone until we do a more comprehensive crypto overhaul