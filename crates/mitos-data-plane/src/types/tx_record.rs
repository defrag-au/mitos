//! `TxRecord` — full TX rollup the data plane returns from
//! `read_tx(tx_hash)`.
//!
//! Mirrors the v2 WIT `tx-record` shape with stronger Rust
//! types. Modules that match one party in an atomic swap, or
//! need a marketplace-fee output sibling, or want loan
//! settlement detail use `read-tx` to fetch the full TX
//! context in one host call.
//!
//! ## Resolution caveat
//!
//! `prior_output` and `prior_datum` on consumed inputs are only
//! populated when the prior UTxO is still resolvable through
//! the data plane (current-state lookup or the witness-set
//! datum index). For a TX whose inputs are already spent —
//! the typical case for any TX older than the one being
//! processed — both fields will be `None`. The WIT projection
//! at the host boundary fills empty values with a sentinel
//! empty `TypedOutput`; modules check `prior_output.address.is_empty()`
//! to detect "data plane couldn't resolve."
//!
//! Module authors who rely on prior outputs for already-spent
//! inputs should process the relevant TX in real-time via the
//! `consumed-event` payload (which the platform synthesises
//! with prior_output populated from current state at apply
//! time) rather than retrospectively via `read-tx`.

use cardano_assets::PolicyId;
use pallas_primitives::Hash;
use serde::{Deserialize, Serialize};

use crate::types::{ChainPoint, OutputRef, TypedDatum, TypedOutput, ValidityInterval};

/// Full TX rollup. Built by `LocalDataPlane::read_tx` from the
/// archive's block CBOR + (best-effort) current-state UTxO
/// lookups for the input refs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TxRecord {
    pub tx_hash: Hash<32>,
    pub tx_idx: u32,
    /// Chain point at which the TX was applied.
    pub cursor: ChainPoint,
    pub inputs: Vec<ConsumedInput>,
    pub reference_inputs: Vec<ReferencedInput>,
    pub outputs: Vec<TypedOutput>,
    pub mint: Vec<MintEntry>,
    pub required_signers: Vec<Hash<28>>,
    pub validity_interval: ValidityInterval,
    /// Aux-data CBOR (TX metadata + native scripts + plutus
    /// scripts wrapper). Same shape `tx_metadata` returns.
    pub aux_data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConsumedInput {
    pub oref: OutputRef,
    /// `None` when the prior UTxO couldn't be resolved (already
    /// spent / pruned). Modules detect via the host's WIT
    /// projection — empty `address` means unresolved.
    pub prior_output: Option<TypedOutput>,
    pub prior_datum: Option<TypedDatum>,
    /// Plutus redeemer when the prior output was script-locked.
    pub redeemer: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReferencedInput {
    pub oref: OutputRef,
    /// Same resolution caveat as `ConsumedInput.prior_output` —
    /// `None` when the host couldn't resolve.
    pub prior_output: Option<TypedOutput>,
    pub prior_datum: Option<TypedDatum>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MintEntry {
    pub policy: PolicyId,
    pub asset_name: Vec<u8>,
    /// Signed: positive = mint, negative = burn.
    pub quantity_delta: i64,
}
