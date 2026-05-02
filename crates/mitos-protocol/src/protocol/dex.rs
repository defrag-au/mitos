//! DEX domain — minimal stub for Phase 1.
//!
//! Validates the kind-as-outer / brand-as-data pattern across a
//! second domain so the cross-domain selector machinery is
//! exercised in tests. The classifier already emits `TxType::DexSwap`
//! for splash / cswap / minswap; lifting it into this enum is a
//! Phase 2 translator concern.
//!
//! Only `Swap` is modelled today. `AddLiquidity` /
//! `RemoveLiquidity` / `PoolCreate` etc. land when a consumer
//! surfaces a real query that needs them.

use cardano_assets::AssetId;
use enumset::EnumSetType;
use serde::{Deserialize, Serialize};

use super::common::{Address, Lovelace};

#[derive(Debug, EnumSetType, Hash, Serialize, Deserialize)]
pub enum DexBrand {
    Splash,
    Cswap,
    Minswap,
    MinswapV2,
    Sundae,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Dex {
    Swap(SwapPayload),
}

#[derive(Debug, EnumSetType, Hash, Serialize, Deserialize)]
pub enum DexEventKind {
    Swap,
}

impl From<&Dex> for DexEventKind {
    fn from(d: &Dex) -> Self {
        match d {
            Dex::Swap(_) => DexEventKind::Swap,
        }
    }
}

impl Dex {
    pub fn brand(&self) -> DexBrand {
        match self {
            Dex::Swap(p) => p.brand,
        }
    }

    pub fn kind(&self) -> DexEventKind {
        self.into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwapPayload {
    pub brand: DexBrand,
    pub asset_in: AssetId,
    pub amount_in: Lovelace,
    pub asset_out: AssetId,
    pub amount_out: Lovelace,
    pub trader: Address,
}
