//! Deadline'd request-reply over the existing data channels.
//!
//! The caller builds its normal data message around a [`RequestToken`]; the
//! receiving actor calls [`RequestToken::reply`] when it has an answer. The
//! returned future resolves with the answer or a typed timeout — a missed
//! deadline produces a real result, so callers never hang on a wedged
//! actor. This generalizes per-consumer timeout code (connector-call
//! timeouts and the like) into one utility.
//!
//! ```ignore
//! // D is the actor's data type, e.g.:
//! enum Query { Lookup { key: String, token: RequestToken<u64> } }
//!
//! let answer = handle
//!     .request(Duration::from_millis(250), |token| Query::Lookup {
//!         key: "hits".into(),
//!         token,
//!     })
//!     .await; // Ok(u64) | Err(RequestError::TimedOut { .. } | ...)
//! ```

use crate::ipc::{ActorHandle, PlatformSendSync};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use uuid::Uuid;

/// One-shot reply capability with a correlation id, embedded by the caller
/// into an ordinary data message. Clonable so it satisfies the data
/// channel's `Clone` bound; only the first `reply` across all clones wins.
#[derive(Debug)]
pub struct RequestToken<R> {
    correlation_id: Uuid,
    slot: Arc<Mutex<Option<oneshot::Sender<R>>>>,
}

impl<R> Clone for RequestToken<R> {
    fn clone(&self) -> Self {
        Self {
            correlation_id: self.correlation_id,
            slot: self.slot.clone(),
        }
    }
}

impl<R> RequestToken<R> {
    pub fn correlation_id(&self) -> Uuid {
        self.correlation_id
    }

    /// Answer the request. Returns `false` if the reply could not be
    /// delivered: the token was already used, or the requester gave up
    /// (deadline passed, future dropped).
    pub fn reply(&self, answer: R) -> bool {
        let sender = match self.slot.lock() {
            Ok(mut slot) => slot.take(),
            Err(_) => None,
        };
        match sender {
            Some(tx) => tx.send(answer).is_ok(),
            None => false,
        }
    }
}

/// Why a request produced no answer.
#[derive(Debug)]
pub enum RequestError {
    /// The data channel rejected the message (actor gone).
    SendFailed { correlation_id: Uuid },
    /// The deadline elapsed before a reply arrived.
    TimedOut { correlation_id: Uuid },
    /// The actor dropped the token without replying — reported immediately
    /// instead of waiting out the deadline.
    Dropped { correlation_id: Uuid },
}

impl RequestError {
    pub fn correlation_id(&self) -> Uuid {
        match self {
            Self::SendFailed { correlation_id }
            | Self::TimedOut { correlation_id }
            | Self::Dropped { correlation_id } => *correlation_id,
        }
    }
}

impl std::fmt::Display for RequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SendFailed { correlation_id } => {
                write!(f, "request {correlation_id}: send failed (actor gone)")
            }
            Self::TimedOut { correlation_id } => {
                write!(f, "request {correlation_id}: deadline elapsed")
            }
            Self::Dropped { correlation_id } => {
                write!(
                    f,
                    "request {correlation_id}: actor dropped the token without replying"
                )
            }
        }
    }
}

impl std::error::Error for RequestError {}

/// Send a request built by `make` and await the reply, bounded by
/// `deadline`. Resolves early with [`RequestError::Dropped`] if the actor
/// discards the token.
pub async fn request<D, R>(
    handle: &ActorHandle<D>,
    deadline: Duration,
    make: impl FnOnce(RequestToken<R>) -> D,
) -> Result<R, RequestError>
where
    D: Clone + PlatformSendSync,
{
    let correlation_id = Uuid::new_v4();
    let (tx, rx) = oneshot::channel();
    let token = RequestToken {
        correlation_id,
        slot: Arc::new(Mutex::new(Some(tx))),
    };

    handle
        .notify(make(token))
        .await
        .map_err(|_| RequestError::SendFailed { correlation_id })?;

    match ego_platform::timeout(deadline, rx).await {
        Ok(Ok(answer)) => Ok(answer),
        Ok(Err(_)) => Err(RequestError::Dropped { correlation_id }),
        Err(_) => Err(RequestError::TimedOut { correlation_id }),
    }
}

impl<D: Clone + PlatformSendSync> ActorHandle<D> {
    /// Deadline'd request-reply over this handle's data channel; see the
    /// [module docs](self).
    pub async fn request<R>(
        &self,
        deadline: Duration,
        make: impl FnOnce(RequestToken<R>) -> D,
    ) -> Result<R, RequestError> {
        request(self, deadline, make).await
    }
}
