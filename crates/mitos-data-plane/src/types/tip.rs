//! `ChainTip` — chain-point + slot-time metadata returned with
//! every query response.
//!
//! Surfacing tip in responses lets paginated queries detect
//! drift. Different from `dolos_core::ChainPoint` only in that
//! the data-plane wire form may eventually want richer metadata
//! (era, epoch, slot-time) and we'd rather not be forced into
//! `dolos_core` at that surface.

use dolos_core::ChainPoint;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainTip {
    pub point: ChainPoint,
}

impl ChainTip {
    pub fn at(point: ChainPoint) -> Self {
        Self { point }
    }

    pub fn origin() -> Self {
        Self {
            point: ChainPoint::Origin,
        }
    }

    pub fn slot(&self) -> u64 {
        match &self.point {
            ChainPoint::Origin => 0,
            ChainPoint::Slot(s) => *s,
            ChainPoint::Specific(s, _) => *s,
        }
    }
}

impl From<ChainPoint> for ChainTip {
    fn from(point: ChainPoint) -> Self {
        Self { point }
    }
}
