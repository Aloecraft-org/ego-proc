//! A lower-level contract for actors whose state lives behind an opaque
//! handle — an embedded interpreter, a wasm instance, any C-library state —
//! lifted into [`ActorState`] by one blanket adapter ([`Foreign`]) so the
//! orchestrator never has to distinguish: `Orchestrator<Foreign<T>>` is just
//! an `Orchestrator<S>`.
//!
//! The vocabulary is deliberately opaque: bytes, port names, parks. This
//! crate never learns a consumer's types; a consumer implements
//! [`ForeignActor`] for its handle in its own crate.

use crate::ControlSignal;
use crate::actor::ActorState;
use crate::ipc::PlatformSendSync;
use std::collections::VecDeque;
use std::time::Duration;

/// The outcome of driving a foreign actor one step.
pub enum Drove {
    /// Made progress and has more work; the adapter keeps driving until the
    /// slice budget is spent.
    Progressed,
    /// No more progress can be made right now; see [`WaitHint`].
    Parked(WaitHint),
    /// The actor ran to completion.
    Done,
    /// The actor failed irrecoverably.
    Failed(anyhow::Error),
}

/// How long a parked actor expects to wait before it is worth driving again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaitHint {
    /// `None` = park with no deadline: only new input can unpark the actor.
    pub timeout: Option<Duration>,
}

/// An opaque message addressed to a named port on a foreign actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortMsg {
    pub port: String,
    pub bytes: Vec<u8>,
}

/// The result of delivering bytes to a foreign actor's port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    Accepted,
    /// The port cannot take more right now — backpressure, kept visible.
    Full,
    /// The port does not exist or will never take this message.
    Refused,
}

/// Whatever the foreign handle can measure about itself. All fields are
/// optional: report what you can, omit what you can't.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ForeignMetrics {
    /// Execution budget spent so far, in handle-defined units.
    pub budget_spent: Option<u64>,
    /// Execution budget limit, in the same units as `budget_spent`.
    pub budget_limit: Option<u64>,
    /// Bytes of state currently held by the handle.
    pub bytes_held: Option<u64>,
}

impl ForeignMetrics {
    /// Derive a saturation ratio from the budget fields, if both are present
    /// and the limit is non-zero.
    pub fn saturation(&self) -> Option<f64> {
        match (self.budget_spent, self.budget_limit) {
            (Some(spent), Some(limit)) if limit > 0 => Some(spent as f64 / limit as f64),
            _ => None,
        }
    }
}

/// An actor whose state lives behind an opaque handle. Implement this for
/// the handle type; wrap it in [`Foreign`] to get an [`ActorState`].
///
/// All methods are synchronous: the handle is driven in short slices from
/// the adapter's tick, never awaited into.
pub trait ForeignActor: PlatformSendSync {
    /// Advance one step. Cheap enough to call in a loop; the adapter
    /// enforces the slice budget.
    fn drive(&mut self) -> anyhow::Result<Drove>;

    /// Offer bytes to a named port. Must not block; report [`Delivery::Full`]
    /// instead.
    fn deliver(&mut self, port: &str, bytes: &[u8]) -> Delivery;

    /// Hand any produced output to `sink`, port by port.
    fn drain(&mut self, sink: &mut dyn FnMut(&str, &[u8]));

    /// Measured (not self-reported-aspirational) resource metrics.
    fn metrics(&self) -> ForeignMetrics;

    /// Serialize state to bytes if the handle is at a stop point, else `None`.
    fn passivate(&mut self) -> Option<Vec<u8>>;
}

/// What the adapter does when a port reports [`Delivery::Full`].
/// The policy lives here, once, instead of in every consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressurePolicy {
    /// Fail the delivery immediately (the actor errors and supervision
    /// takes over).
    Refuse,
    /// Buffer up to this many messages and retry them at the next tick;
    /// overflowing the buffer is an error.
    Buffer(usize),
}

enum Park {
    /// Running normally.
    No,
    /// Parked until roughly this instant.
    Until(ego_platform::Instant),
    /// Parked with no deadline; only new input unparks.
    Indefinite,
}

/// Adapter lifting any [`ForeignActor`] into an [`ActorState`] with
/// [`PortMsg`] data in and out. Owns pacing (slice budget, park
/// bookkeeping) and the backpressure policy.
pub struct Foreign<T: ForeignActor> {
    inner: T,
    tick_interval: Duration,
    slice_budget: Duration,
    backpressure: BackpressurePolicy,
    pending: VecDeque<PortMsg>,
    park: Park,
    outbox: Vec<PortMsg>,
}

impl<T: ForeignActor> Foreign<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            tick_interval: Duration::from_millis(10),
            slice_budget: Duration::from_millis(5),
            backpressure: BackpressurePolicy::Buffer(64),
            pending: VecDeque::new(),
            park: Park::No,
            outbox: Vec::new(),
        }
    }

    /// Base tick interval for the run loop (parks make individual ticks
    /// cheap no-ops rather than stretching the interval).
    pub fn with_tick_interval(mut self, interval: Duration) -> Self {
        self.tick_interval = interval;
        self
    }

    /// Maximum wall-clock time to spend driving the handle per tick.
    pub fn with_slice_budget(mut self, budget: Duration) -> Self {
        self.slice_budget = budget;
        self
    }

    pub fn with_backpressure(mut self, policy: BackpressurePolicy) -> Self {
        self.backpressure = policy;
        self
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Retry buffered deliveries. Messages that still report `Full` stay
    /// buffered for the next tick; `Refused` is an error. Anything accepted
    /// counts as new input, which clears the park.
    fn flush_pending(&mut self) -> anyhow::Result<()> {
        while let Some(msg) = self.pending.pop_front() {
            match self.inner.deliver(&msg.port, &msg.bytes) {
                Delivery::Accepted => {
                    self.park = Park::No;
                }
                Delivery::Full => {
                    self.pending.push_front(msg);
                    break;
                }
                Delivery::Refused => {
                    anyhow::bail!("delivery to port '{}' refused", msg.port);
                }
            }
        }
        Ok(())
    }

    fn drain_inner(&mut self) {
        let outbox = &mut self.outbox;
        self.inner.drain(&mut |port, bytes| {
            outbox.push(PortMsg {
                port: port.to_string(),
                bytes: bytes.to_vec(),
            });
        });
    }

    /// True while a park is still in effect (no deadline, or deadline in
    /// the future). A passed deadline clears the park.
    fn still_parked(&mut self) -> bool {
        match self.park {
            Park::No => false,
            Park::Indefinite => true,
            Park::Until(deadline) => {
                if ego_platform::Instant::now() >= deadline {
                    self.park = Park::No;
                    false
                } else {
                    true
                }
            }
        }
    }
}

#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    async_trait::async_trait
)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait::async_trait(?Send))]
impl<T: ForeignActor> ActorState for Foreign<T> {
    type D = PortMsg;
    type O = PortMsg;

    fn interval(&self) -> Duration {
        self.tick_interval
    }

    async fn on_tick(&mut self) -> anyhow::Result<bool> {
        self.flush_pending()?;

        // A parked handle with nothing newly delivered has no work: make
        // the tick a cheap no-op until the hinted deadline (or new data)
        // unparks it.
        if self.still_parked() {
            return Ok(true);
        }

        let start = ego_platform::Instant::now();
        loop {
            match self.inner.drive() {
                Ok(Drove::Progressed) => {}
                Ok(Drove::Parked(hint)) => {
                    self.park = match hint.timeout {
                        Some(t) => Park::Until(ego_platform::Instant::now() + t),
                        None => Park::Indefinite,
                    };
                    break;
                }
                Ok(Drove::Done) => {
                    self.drain_inner();
                    return Ok(false);
                }
                Ok(Drove::Failed(e)) => return Err(e),
                Err(e) => return Err(e),
            }
            if start.elapsed() >= self.slice_budget {
                break;
            }
        }
        Ok(true)
    }

    async fn on_signal(&mut self, signal: ControlSignal) -> anyhow::Result<()> {
        // On Stop, give the handle a chance to reach a clean stop point; if
        // it can't, its state is simply dropped. Orchestrator-driven
        // passivation (which keeps the bytes) goes through `passivate()`.
        if signal == ControlSignal::Stop && self.inner.passivate().is_none() {
            log::debug!("foreign actor stopped while not at a stop point; state dropped");
        }
        Ok(())
    }

    async fn on_data(&mut self, data: Self::D) -> anyhow::Result<()> {
        // New input may unblock whatever the handle was waiting on.
        self.park = Park::No;
        match self.inner.deliver(&data.port, &data.bytes) {
            Delivery::Accepted => Ok(()),
            Delivery::Full => match self.backpressure {
                BackpressurePolicy::Refuse => {
                    anyhow::bail!(
                        "port '{}' is full and the backpressure policy is Refuse",
                        data.port
                    )
                }
                BackpressurePolicy::Buffer(cap) => {
                    if self.pending.len() >= cap {
                        anyhow::bail!(
                            "port '{}' is full and the retry buffer overflowed ({} messages)",
                            data.port,
                            cap
                        );
                    }
                    self.pending.push_back(data);
                    Ok(())
                }
            },
            Delivery::Refused => anyhow::bail!("delivery to port '{}' refused", data.port),
        }
    }

    fn take_output(&mut self) -> Vec<Self::O> {
        self.drain_inner();
        std::mem::take(&mut self.outbox)
    }

    fn saturation(&self) -> Option<f64> {
        self.inner.metrics().saturation()
    }

    fn passivate(&mut self) -> Option<Vec<u8>> {
        // Undelivered input is real state we can't capture: refuse.
        if !self.pending.is_empty() {
            return None;
        }
        self.inner.passivate()
    }
}
