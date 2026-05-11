//! Mitos's own `ChainPoint` type.
//!
//! Owned (not re-exported from dolos) for two reasons:
//!
//! 1. **Dolos version drift.** Earlier mitos pulled
//!    `dolos_core::ChainPoint` directly from every consumer site;
//!    a dolos bump rippled across the workspace. This type
//!    insulates everything above the data plane: dolos changes
//!    land as adapter-impl changes here, not API breaks above.
//!
//! 2. **WIT stability.** This type backs the v2 WIT's `chain-point`
//!    variant. Adding a field to a record is a major-bump-breaking
//!    change in component ABI; we want this type to evolve on
//!    mitos's schedule, not dolos's.
//!
//! Variant shape mirrors `dolos_core::ChainPoint` so the conversion
//! is one-to-one and serde-compatible (so existing on-disk cursors
//! deserialize unchanged). The hash field is `pallas::Hash<32>` —
//! the canonical Cardano 32-byte hash type — to preserve dolos's
//! hex-text serde format on the wire.

use pallas_primitives::Hash;
use serde::{Deserialize, Serialize};

/// Where on chain we are. Variant so we can represent genesis
/// pre-state (`Origin`), slot-only points (some indexers' captured
/// cursors), and fully-specified `(slot, hash)` points uniformly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChainPoint {
    /// Genesis pre-state. Used by indexers that haven't applied
    /// any block yet.
    Origin,
    /// Slot-only point (no block hash). Captured when the
    /// originating event source had only the slot.
    Slot(u64),
    /// Fully-specified point: slot + 32-byte block hash. The
    /// canonical shape for events emitted from real-block
    /// dispatch.
    Specific(u64, Hash<32>),
}

impl ChainPoint {
    /// Slot number, with `Origin` reported as `0`. Useful for
    /// cursor comparison without needing to match every variant.
    pub fn slot(&self) -> u64 {
        match self {
            ChainPoint::Origin => 0,
            ChainPoint::Slot(s) => *s,
            ChainPoint::Specific(s, _) => *s,
        }
    }

    /// Block hash if known. `None` for `Origin` and `Slot`-only
    /// points.
    pub fn hash(&self) -> Option<&Hash<32>> {
        match self {
            ChainPoint::Origin | ChainPoint::Slot(_) => None,
            ChainPoint::Specific(_, h) => Some(h),
        }
    }

    /// Block hash as raw bytes if known. Convenience for callers
    /// (especially the WIT boundary) that want `&[u8]` rather
    /// than the `Hash<32>` wrapper.
    pub fn hash_bytes(&self) -> Option<&[u8]> {
        self.hash().map(|h| h.as_ref())
    }
}

// ------------------------------------------------------------------
// Dolos adapters — one-to-one variant conversion. The single seam
// where dolos's chain-point shape crosses into mitos's owned type.
// ------------------------------------------------------------------

impl From<dolos_core::ChainPoint> for ChainPoint {
    fn from(p: dolos_core::ChainPoint) -> Self {
        match p {
            dolos_core::ChainPoint::Origin => ChainPoint::Origin,
            dolos_core::ChainPoint::Slot(s) => ChainPoint::Slot(s),
            dolos_core::ChainPoint::Specific(s, h) => ChainPoint::Specific(s, h),
        }
    }
}

impl From<ChainPoint> for dolos_core::ChainPoint {
    fn from(p: ChainPoint) -> Self {
        match p {
            ChainPoint::Origin => dolos_core::ChainPoint::Origin,
            ChainPoint::Slot(s) => dolos_core::ChainPoint::Slot(s),
            ChainPoint::Specific(s, h) => dolos_core::ChainPoint::Specific(s, h),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_hash() -> Hash<32> {
        Hash::new([0xab; 32])
    }

    #[test]
    fn slot_accessor_handles_all_variants() {
        assert_eq!(ChainPoint::Origin.slot(), 0);
        assert_eq!(ChainPoint::Slot(42).slot(), 42);
        assert_eq!(ChainPoint::Specific(100, sample_hash()).slot(), 100);
    }

    #[test]
    fn hash_is_none_for_origin_and_slot_only() {
        assert!(ChainPoint::Origin.hash().is_none());
        assert!(ChainPoint::Slot(42).hash().is_none());
        let h = sample_hash();
        assert_eq!(ChainPoint::Specific(100, h).hash(), Some(&sample_hash()));
    }

    #[test]
    fn dolos_round_trip_origin() {
        let mitos: ChainPoint = dolos_core::ChainPoint::Origin.into();
        assert_eq!(mitos, ChainPoint::Origin);
        let back: dolos_core::ChainPoint = mitos.into();
        assert!(matches!(back, dolos_core::ChainPoint::Origin));
    }

    #[test]
    fn dolos_round_trip_slot() {
        let mitos: ChainPoint = dolos_core::ChainPoint::Slot(42).into();
        assert_eq!(mitos, ChainPoint::Slot(42));
        let back: dolos_core::ChainPoint = mitos.into();
        assert!(matches!(back, dolos_core::ChainPoint::Slot(42)));
    }

    #[test]
    fn dolos_round_trip_specific() {
        let h = sample_hash();
        let mitos: ChainPoint = dolos_core::ChainPoint::Specific(100, h).into();
        assert_eq!(mitos, ChainPoint::Specific(100, h));
        let back: dolos_core::ChainPoint = mitos.into();
        match back {
            dolos_core::ChainPoint::Specific(s, hh) => {
                assert_eq!(s, 100);
                assert_eq!(hh.as_ref(), h.as_ref());
            }
            _ => panic!("expected Specific"),
        }
    }

    /// Serde produces an identical CBOR encoding to dolos's type
    /// — so existing on-disk cursors written under the dolos type
    /// deserialize cleanly under the mitos type and vice-versa.
    /// This is load-bearing: we don't want a destructive
    /// migration to discard cursor checkpoints.
    #[test]
    fn cbor_serde_compatible_with_dolos() {
        let mitos = ChainPoint::Specific(123, sample_hash());
        let dolos: dolos_core::ChainPoint = mitos.clone().into();

        let mut mitos_buf = Vec::new();
        ciborium::ser::into_writer(&mitos, &mut mitos_buf).unwrap();
        let mut dolos_buf = Vec::new();
        ciborium::ser::into_writer(&dolos, &mut dolos_buf).unwrap();

        assert_eq!(mitos_buf, dolos_buf);

        // Round-trip dolos bytes back through mitos shape.
        let parsed: ChainPoint = ciborium::de::from_reader(dolos_buf.as_slice()).unwrap();
        assert_eq!(parsed, mitos);
    }
}
