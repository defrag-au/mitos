//! `ChainTip` — chain-point + slot-time metadata returned with
//! every query response.
//!
//! Surfacing tip in responses lets paginated queries detect
//! drift. Uses mitos's owned `ChainPoint` (in `chain_point.rs`)
//! so dolos version drift doesn't reach this surface.

use serde::{Deserialize, Serialize};

use super::ChainPoint;

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
        self.point.slot()
    }
}

impl From<ChainPoint> for ChainTip {
    fn from(point: ChainPoint) -> Self {
        Self { point }
    }
}
