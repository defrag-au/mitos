//! Platform v3's lifecycle, as pure state machines.
//!
//! See `docs/strategy/MITOS_PLATFORM_V3.md`. This crate is §5 of that document
//! — the five machines it says must exist as types and tests before anything
//! is wired into the host — and it is deliberately the ONLY place a lifecycle
//! decision is made.
//!
//! # The pattern, which everything downstream follows
//!
//! ```text
//!   step(state, event, context) -> Result<Transition { next, effects }, Rejected>
//! ```
//!
//! - **State** is a plain value, serialisable, persisted by the host.
//! - **Events** are things that happened: a subscribe arrived, a fetch
//!   finished, time passed. The host translates the world into events.
//! - **Effects** are things the host must now DO: start a follower, copy a
//!   state file, pause at a block boundary. This crate never does them.
//! - **Rejected** is a refusal with a typed reason. A refusal changes nothing.
//!
//! Three rules make that safe to build a host on:
//!
//! 1. **No clock.** `std::time` never appears. The caller passes [`UnixSecs`]
//!    in, so every timing rule is testable without sleeping and a test can
//!    move time in whatever steps expose a bug.
//! 2. **Every effect is idempotent.** The host persists the next state and
//!    then executes effects; a crash between the two means the effects run
//!    again on restart. `recover` functions return exactly the effects to
//!    re-issue for a persisted state, and they are the same effects.
//! 3. **Named decisions, not booleans.** Anything that reads as a yes/no in
//!    the design is an enum here, so a call site states which case it means
//!    and a new case is a compile error at every match rather than a silent
//!    `false`.
//!
//! # What is NOT here
//!
//! Wire types (`mitos-protocol`), manifest parsing (`mitos-platform::manifest`
//! — though [`upgrade::UpgradeEntry`] is the shape its `[[upgrade]]` table
//! deserialises into), storage layout, and anything that touches a follower.

pub mod access;
pub mod artifact;
pub mod client_move;
pub mod handoff;
pub mod ids;
pub mod instance;
pub mod lease;
pub mod policy;
pub mod routing;
pub mod state_prep;
pub mod upgrade;

use serde::{Deserialize, Serialize};

/// Seconds since the Unix epoch, supplied by the caller. See the crate docs
/// for why this crate never reads a clock itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UnixSecs(pub u64);

impl UnixSecs {
    /// Whole seconds from `earlier` to `self`, saturating at zero. Saturating
    /// because a host clock stepping backwards must read as "no time has
    /// passed", never as a u64 wrap that fires every timer at once.
    pub fn since(self, earlier: UnixSecs) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

/// The result of accepting an event: the state to persist and the effects to
/// execute, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition<S, E> {
    pub next: S,
    pub effects: Vec<E>,
}

impl<S, E> Transition<S, E> {
    /// The event was accepted and changed nothing the host must act on.
    pub fn quiet(next: S) -> Self {
        Self {
            next,
            effects: Vec::new(),
        }
    }

    pub fn with(next: S, effects: Vec<E>) -> Self {
        Self { next, effects }
    }
}
