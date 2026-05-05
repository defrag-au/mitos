//! Wire-format types shared between mitos host + CF companion.
//!
//! This module currently holds *new* wire types introduced by the
//! companion runtime work (PR 1+). The legacy types
//! (`ClientMessage`, `ServerMessage`, `SubscribeReply`) still live
//! in `mitos/crates/mitos-core/src/replicate.rs` because they
//! reference `dolos_core::ChainPoint`; consolidating them is a
//! follow-up out of PR 1 scope.
//!
//! Until then, `wire::ChainPoint` here is **type-distinct** from
//! `dolos_core::ChainPoint` but **CBOR wire-compatible** (Pallas's
//! `Hash<32>` serialises as hex text, matching the `String` here).
//! Conversion at the WS boundary lands when the legacy types are
//! consolidated.

use serde::{Deserialize, Serialize};

/// Cardano chain point.
///
/// **Hash is hex-text, not bytes.** `Specific` is `[u64, text]` on
/// the wire — using `Vec<u8>` would fail to decode every frame
/// produced by mitos-core's serializer (which uses pallas's hex-text
/// `Hash<32>` impl).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChainPoint {
    Origin,
    Slot(u64),
    Specific(u64, String),
}

impl ChainPoint {
    /// Slot number, if any. `Origin` returns `None`.
    pub fn slot(&self) -> Option<u64> {
        match self {
            ChainPoint::Origin => None,
            ChainPoint::Slot(s) => Some(*s),
            ChainPoint::Specific(s, _) => Some(*s),
        }
    }

    /// Hash hex string, if any. `Origin` and `Slot` return `None`.
    pub fn hash(&self) -> Option<&str> {
        match self {
            ChainPoint::Origin | ChainPoint::Slot(_) => None,
            ChainPoint::Specific(_, h) => Some(h.as_str()),
        }
    }
}
