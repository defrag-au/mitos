//! Lending domain — placeholder stub.
//!
//! Carries one variant (`Borrow`) just to validate the kind-as-outer
//! pattern across a third domain. No classifier produces these
//! events yet; recognising lending protocols (Liqwid etc.) is
//! future work — likely as a brand-specific community module
//! per `LAYERED_RESPONSIBILITIES.md`, not an extension of this
//! type set.

use cardano_assets::{AssetId, PolicyId};
use enumset::EnumSetType;
use serde::{Deserialize, Serialize};

use super::common::{Address, Lovelace};

#[derive(Debug, EnumSetType, Hash, Serialize, Deserialize)]
pub enum LendingBrand {
    Liqwid,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Lending {
    Borrow(BorrowPayload),
}

#[derive(Debug, EnumSetType, Hash, Serialize, Deserialize)]
pub enum LendingEventKind {
    Borrow,
}

impl From<&Lending> for LendingEventKind {
    fn from(l: &Lending) -> Self {
        match l {
            Lending::Borrow(_) => LendingEventKind::Borrow,
        }
    }
}

impl Lending {
    pub fn brand(&self) -> LendingBrand {
        match self {
            Lending::Borrow(p) => p.brand,
        }
    }

    pub fn kind(&self) -> LendingEventKind {
        self.into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BorrowPayload {
    pub brand: LendingBrand,
    pub collateral_policy: PolicyId,
    pub collateral_asset: AssetId,
    pub borrowed_lovelace: Lovelace,
    pub borrower: Address,
}
