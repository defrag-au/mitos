//! `UtxoPredicate` algebra + the typed pattern hierarchy.
//!
//! Mirrors utxorpc's `UtxoPredicate` shape — composable via
//! `and` / `or` / `not` over `UtxoPattern` — but replaces
//! utxorpc's parallel `bytes` address fields with a typed Rust
//! discriminated union (`AddressPattern`). Same predicate
//! grammar; stronger types.
//!
//! Addresses surface as bech32 strings (matching the convention
//! `cardano_assets::PolicyId` uses) so the predicate types
//! serialise cleanly without custom serde glue. The
//! `LocalDataPlane` impl converts to `pallas_addresses::Address`
//! at the boundary when it actually needs to match.

use cardano_assets::{Fingerprint, PolicyId};
use pallas_primitives::Hash;
use serde::{Deserialize, Serialize};

/// Composable predicate over UTxO outputs. Algebra:
///
/// - `Match(pattern)` — a single condition the output must
///   satisfy.
/// - `Not(p)` — output must NOT match `p`.
/// - `AnyOf(ps)` — at least one `p` matches (logical OR).
/// - `AllOf(ps)` — every `p` matches (logical AND).
///
/// `AnyOf(vec![])` matches nothing (empty disjunction);
/// `AllOf(vec![])` matches everything (empty conjunction).
/// These edge cases align with utxorpc + Boolean-algebra
/// convention; consumers shouldn't rely on them but the
/// behaviour is well-defined.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UtxoPredicate {
    Match(UtxoPattern),
    Not(Box<UtxoPredicate>),
    AnyOf(Vec<UtxoPredicate>),
    AllOf(Vec<UtxoPredicate>),
}

impl UtxoPredicate {
    /// Convenience: a predicate matching everything. The empty
    /// conjunction.
    pub fn any() -> Self {
        UtxoPredicate::AllOf(vec![])
    }

    /// Convenience: a predicate matching nothing. The empty
    /// disjunction.
    pub fn none() -> Self {
        UtxoPredicate::AnyOf(vec![])
    }

    /// Pattern-match shorthand: wraps a pattern as `Match(p)`.
    pub fn matching(pattern: UtxoPattern) -> Self {
        UtxoPredicate::Match(pattern)
    }
}

/// A primitive UTxO pattern. All-`None` matches every output;
/// each populated field narrows. Multiple fields combine with
/// AND semantics within one `UtxoPattern` (pattern with
/// `address: Some + asset: Some` matches outputs at that address
/// AND containing that asset). Use the surrounding
/// `UtxoPredicate` algebra for OR / NOT composition.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UtxoPattern {
    pub address: Option<AddressPattern>,
    pub asset: Option<AssetPattern>,
    pub output_ref: Option<OutputRefPattern>,
}

impl UtxoPattern {
    pub fn at_address(addr_bech32: impl Into<String>) -> Self {
        Self {
            address: Some(AddressPattern::Exact(addr_bech32.into())),
            ..Default::default()
        }
    }

    pub fn under_policy(policy: PolicyId) -> Self {
        Self {
            asset: Some(AssetPattern::Policy(policy)),
            ..Default::default()
        }
    }
}

/// Address-axis pattern. Replaces utxorpc's three parallel
/// `bytes` fields with a typed discriminated union — clearer at
/// the construction site, matchable exhaustively, no runtime
/// validation of "did you set the right combination of fields".
///
/// `Exact` carries a bech32-encoded address as `String` so
/// serde works without custom glue. Conversion to
/// `pallas_addresses::Address` happens at the impl boundary
/// where matching occurs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AddressPattern {
    /// Match exactly one address (bech32-encoded — `addr1...`,
    /// `stake1...`, or Byron base58 form).
    Exact(String),
    /// Match by payment credential — any delegation. Useful for
    /// "all UTxOs whose payment cred is this script". The
    /// 28-byte raw credential.
    PaymentCredential(#[serde(with = "serde_bytes")] Vec<u8>),
    /// Match by stake credential — any payment. Useful for "all
    /// UTxOs delegated to this stake key". 28-byte raw credential.
    StakeCredential(#[serde(with = "serde_bytes")] Vec<u8>),
    /// Match by both halves. Equivalent to `Exact` for fully-
    /// specified Shelley addresses but accepts the components
    /// separately so callers don't need to reconstruct an
    /// `Address` to express the constraint.
    PaymentAndStake {
        #[serde(with = "serde_bytes")]
        payment: Vec<u8>,
        #[serde(with = "serde_bytes")]
        stake: Vec<u8>,
    },
    /// Match any Shelley address (excludes Byron bootstrap).
    AnyShelley,
    /// Match any Byron bootstrap address.
    AnyByron,
}

/// Asset-axis pattern.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AssetPattern {
    /// Any asset under this policy.
    Policy(PolicyId),
    /// Specific asset by `(policy, name)`. Asset name as hex.
    PolicyAndName {
        policy: PolicyId,
        asset_name_hex: String,
    },
    /// Match by CIP-14 fingerprint. Plane resolves to
    /// `(policy, name)` server-side.
    Fingerprint(Fingerprint),
}

/// Output-ref-axis pattern. Combines tx-hash + index narrowing.
/// One or both may be set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputRefPattern {
    pub tx_hash: Option<Hash<32>>,
    pub index: Option<u32>,
}
