//! Cross-domain primitives. Kept here so each domain module
//! references the same `Address`/`Lovelace`/`OutputRef`/`PlutusBytes`
//! shapes and stays free of domain-crossing imports.
//!
//! These are the *minimum-viable* types for Phase 1 — string
//! addresses match the existing classifier's representation; raw
//! plutus data is left as opaque bytes until a consumer surfaces a
//! genuine need to decode at the framework boundary.

use cardano_assets::PolicyId;
use serde::{Deserialize, Serialize};

use super::Domain;

/// Bech32 Cardano address. Kept stringly here to match the existing
/// classifier output and avoid forcing a typed-address dep into
/// `mitos-core`.
pub type Address = String;

/// Lovelace amount. Newtype-free for ergonomic arithmetic; the field
/// names in payload structs are explicit (`price_lovelace`,
/// `royalty_lovelace`) so misuse at call sites is rare.
pub type Lovelace = u64;

/// Hex-encoded raw Plutus datum / redeemer. Phase 1 keeps these
/// opaque; consumers that need the decoded form re-decode locally
/// using their preferred Plutus library. Storing as bytes (rather
/// than a typed `PlutusData`) avoids a pallas dep at the type-defs
/// crate boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlutusBytes(#[serde(with = "serde_bytes")] pub Vec<u8>);

impl PlutusBytes {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

/// Lightweight transaction-output reference — same shape as
/// `dolos_core::TxoRef` but local so the protocol module doesn't
/// transitively pull dolos types into a consumer that just wants the
/// event vocabulary. Hex-stringly to keep wire format human-readable.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OutputRef {
    pub tx_hash: String,
    pub index: u32,
}

impl OutputRef {
    pub fn new(tx_hash: impl Into<String>, index: u32) -> Self {
        Self {
            tx_hash: tx_hash.into(),
            index,
        }
    }
}

/// What mitos emits over the replication channel for protocol
/// indexers (marketplace, dex, lending). One record per
/// `(policy, domain_event)` pair — a tx that touches N policies
/// across N protocols emits N records, regardless of how many
/// assets within each policy the tx referenced.
///
/// `asset_name_hex` is `Some` for events targeting a specific
/// asset within the policy (single-asset sales, single-asset
/// offers) and `None` for events that target the policy as a
/// whole (collection-wide offers) or span multiple assets in the
/// same policy (bundle listings — the asset list lives in the
/// payload, but only one event is emitted per policy).
///
/// `policy_id` and `asset_name_hex` are what the asset-axis of
/// `Interest` matches against. Brand and event-kind are projected
/// out of `domain` via methods on the `Domain` arm's payload (e.g.
/// `marketplace.brand()` / `marketplace.kind()`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolEvent {
    pub policy_id: PolicyId,
    pub asset_name_hex: Option<String>,
    pub tx_hash: String,
    pub slot: u64,
    pub domain: Domain,
}
