//! Wire-format event types for the jpg.store collection-offer
//! (jpg-co) community module.
//!
//! Both halves of the dApp reference these definitions so the
//! CBOR shape can't drift between encoder (wasm module at
//! `mitos/community-modules/jpg-co/jpg_co.rs`) and decoder
//! (Worker DO at
//! `cnft.dev-workers/workers/jpg-store-mirror/src/`).
//!
//! ## Wire shape
//!
//! `JpgCoChange` is internally tagged (`#[serde(tag = "type")]`) so
//! the discriminator + payload share one CBOR map. Hex-string
//! fields (policy/pkh/tx_hash) — strings are larger on the wire
//! than raw byte arrays but easier to log/store, and the volumes
//! here are modest (low thousands of unspent COs total).
//!
//! ## Field choices
//!
//! - `offerer_pkh` — 56-char lowercase hex (28 bytes). Decoded
//!   from datum field 0 by the module.
//! - `tx_hash` — 64-char lowercase hex (32 bytes).
//! - `target_policy` — `Option<String>` because some CO datum
//!   variants carry an asset allowlist instead of a single policy
//!   id; module passes `None` in that case and the companion
//!   stores the allowlist separately (out-of-scope for v1).
//! - `datum_cbor` — raw bytes via `serde_bytes` for compact CBOR
//!   encoding (without `serde_bytes` a `Vec<u8>` would serialise
//!   as a CBOR array of ~350 ints per CO datum, ~10x bloat).
//! - `co_version` — string `"V2"` / `"V3"` rather than a
//!   `#[serde(tag)]` enum because the discriminator already lives
//!   on the outer `JpgCoChange::Created`; nesting another tagged
//!   enum would duplicate.

use serde::{Deserialize, Serialize};

/// CO events emitted by the jpg-co indexer module. Internally
/// tagged so the CBOR shape is `{"type": "Created", ...}` /
/// `{"type": "Spent", ...}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum JpgCoChange {
    /// A new collection offer landed at one of the watched CO
    /// script addresses (V2 or V3). The module decoded the
    /// datum and extracted the offerer's payment key hash plus
    /// the (optional) target policy.
    Created {
        /// 56-char lowercase hex, 28-byte payment-key-hash from
        /// datum field 0. The companion DO indexes
        /// `collection_offers` by this so `/api/my-cos?pkh=…`
        /// is a single SQL lookup.
        offerer_pkh: String,

        /// 64-char lowercase hex tx hash that produced this CO.
        tx_hash: String,

        /// Output index within `tx_hash`.
        output_index: u32,

        /// Lovelace locked in the CO (the offer amount + min
        /// UTxO + provider deposits).
        lovelace: u64,

        /// Raw datum CBOR. The companion stores this verbatim
        /// so cancel-time has zero data-plane round trips —
        /// `build_cancel_offer_tx` consumes it directly.
        #[serde(with = "serde_bytes")]
        datum_cbor: Vec<u8>,

        /// Which jpg.store CO contract version this output is at.
        /// Used at cancel time to pick the correct cancel TX
        /// shape (V2 / V3 differ in script address only, but
        /// the field gives explicit forward-compat).
        co_version: JpgCoVersion,

        /// 56-char lowercase hex policy id when the datum carries
        /// a single target collection. `None` when the datum
        /// encodes a multi-asset allowlist (rare; v1 stores the
        /// row but doesn't expose policy-filtered queries for
        /// these — out-of-scope).
        target_policy: Option<String>,

        /// Asset names (lowercase hex of the on-chain bytes) the
        /// offer is constrained to. Empty means a collection-wide
        /// offer (any asset under `target_policy`); non-empty
        /// means an asset-specific offer (e.g. a single ADA Handle
        /// rather than the Handle collection at large).
        ///
        /// `#[serde(default)]` keeps wire compatibility with
        /// pre-asset-aware emitters: rows produced before this
        /// field existed deserialise to an empty vec, which the
        /// companion + UI treat as collection-wide.
        #[serde(default)]
        target_asset_names: Vec<String>,
    },

    /// A previously-tracked CO output was consumed (cancel,
    /// accept, or any other spend). The companion DELETEs by
    /// `(tx_hash, output_index)`.
    ///
    /// `offerer_pkh` rides along even though the DELETE doesn't
    /// strictly need it — it's useful for audit trails / event
    /// logs without forcing the companion to read-before-delete.
    Spent {
        /// Same field set as the matching `Created`'s primary
        /// key.
        tx_hash: String,
        output_index: u32,
        offerer_pkh: String,
    },
}

/// jpg.store CO contract version. V2 and V3 share the same
/// underlying script (`9068a7a3...`); they differ only in
/// address-encoding (V2 uses one staking credential, V3 uses
/// another). New COs created by jpg.store today are at V3; V2
/// is held for legacy compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JpgCoVersion {
    V2,
    V3,
}

impl JpgCoVersion {
    /// String form for SQL storage (companion stores the version
    /// as `TEXT 'V2' | 'V3'`).
    pub fn as_str(&self) -> &'static str {
        match self {
            JpgCoVersion::V2 => "V2",
            JpgCoVersion::V3 => "V3",
        }
    }
}
