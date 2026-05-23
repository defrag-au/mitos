//! Wire-format event types for the `collection-metadata`
//! community module.
//!
//! See `docs/design/COLLECTION_MODULES.md` for the design.
//! `collection-metadata` is the sibling of `collection-holders`:
//! `collection-holders` answers "who currently holds what", this
//! module answers "what is the current metadata for each asset
//! identity".
//!
//! **Phase 2 (this file at landing time):** CIP-68 only. The
//! module observes CIP-68 reference-token (`000643b0`-prefixed
//! asset names) production at any address and decodes the
//! attached datum as PlutusData Constructor 0, extracting the
//! metadata map.
//!
//! **Phase 3 (deferred):** CIP-25 facade. Same `MetadataEvent`
//! shape, populated from `tx.metadata[721]` lookups via the
//! data plane's `tx_metadata` host-fn + `MaestroFallbackPlane`
//! for historical mints past the archive horizon.
//!
//! ## Why `metadata_json: String` rather than a typed `Value` enum
//!
//! Matches the `cip-25-mint` precedent: the wasm module produces
//! JSON via `serde_json::to_string` and the consumer parses as
//! it likes. A typed enum would carry recursive serialisation
//! complexity (Plutus-data maps are deeply heterogeneous) and
//! force every consumer to learn our specific encoding. JSON is
//! the lingua-franca of metadata consumers.
//!
//! ## Why `asset_name_hex` carries the shared suffix only
//!
//! CIP-68 splits one collectible identity across two asset names:
//! the reference token (`000643b0<suffix>`) and the user token
//! (`000de140<suffix>` for NFTs, `0014df10<suffix>` for RFTs).
//! Both share the trailing `<suffix>`. Emitting `asset_name_hex`
//! as the bare suffix lets consumers correlate metadata to
//! holdings by stripping the user-token prefix once at the
//! join, rather than juggling three prefix variants per asset.

use serde::{Deserialize, Serialize};

/// One canonical metadata record for a CIP-68 (or, in Phase 3,
/// CIP-25) collectible identity.
///
/// `metadata_json` carries the decoded metadata map as a JSON
/// string. The shape mirrors CBOR map structure: nested maps
/// become objects, lists become arrays, bytes that are valid
/// UTF-8 become strings, otherwise base64-encoded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataEntry {
    /// Lowercase hex of the **shared name suffix** — the bytes
    /// after the CIP-67 label prefix. For a Mekka Dynasty
    /// `_100` reference token whose full asset name is
    /// `000643b04d4430393539`, this field is `4d4430393539`
    /// (UTF-8 "MD0959").
    pub asset_name_hex: String,
    /// JSON-stringified CIP-68 metadata map (the first field of
    /// the datum's Constructor 0). `None` when the datum was
    /// present but decoding the metadata map failed (malformed
    /// or unexpected shape).
    pub metadata_json: Option<String>,
    pub standard: MetadataStandard,
    /// CIP-68 datum version field (second field of Constructor
    /// 0). Bumped by the ref-token holder on each datum
    /// rotation per spec. CIP-25 entries (Phase 3) will carry
    /// `version = 1` always.
    pub version: u64,
    /// `true` for CIP-25 (mint-time-only, no update path);
    /// `false` for CIP-68 (datum-rotation supported). Phase 2
    /// always emits `false`.
    pub immutable: bool,
    /// 64-char lowercase hex of the TX that produced this
    /// version of the datum.
    pub source_tx: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataStandard {
    /// Reserved for Phase 3 (mint-tx-metadata-driven decode).
    Cip25,
    /// CIP-68 datum shape `Constructor 0 [metadata, version]`.
    Cip68V1,
    /// CIP-68 datum shape `Constructor 0 [metadata, version, extra]`.
    Cip68V2,
}

/// Opens a chunked snapshot for one policy. Same chunked
/// pattern + same WASM-budget rationale as
/// `collection_holders::SnapshotBegin` — emitting the whole
/// metadata list in one fuel budget traps for a large policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotBegin {
    /// 56-char lowercase hex of the policy id this snapshot
    /// covers.
    pub policy: String,
    /// Slot at which this snapshot is valid.
    pub cursor_slot: u64,
    /// 64-char lowercase hex of the block hash at `cursor_slot`.
    /// Empty when the host doesn't surface a block hash for the
    /// scan anchor.
    pub cursor_hash_hex: String,
}

/// One bounded slice of a snapshot's metadata entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotChunk {
    /// 56-char lowercase hex of the policy id.
    pub policy: String,
    /// Bounded slice of the snapshot's entries. Sorted by
    /// `asset_name_hex` ascending for determinism.
    pub entries: Vec<MetadataEntry>,
}

/// Closes a chunked snapshot. Consumer marks the policy's
/// metadata projection authoritative.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotEnd {
    /// 56-char lowercase hex of the policy id.
    pub policy: String,
    /// Total entries emitted across the sequence — sanity check.
    pub entry_count: u64,
}

/// First time the module observed metadata for an asset
/// identity. Emitted on the first `produced` event for a
/// `000643b0`-prefixed asset name not previously in the
/// ledger. Typically corresponds to a CIP-68 ref-token mint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataInitial {
    pub policy: String,
    pub slot: u64,
    pub tx_hash: String,
    pub entry: MetadataEntry,
}

/// Subsequent observation where the datum changed for an
/// asset identity already in the ledger. Emitted on the
/// `produced` half of a Consumed+Produced cycle for the same
/// ref-token asset name where the new datum's CBOR hash
/// differs from the prior. Same-datum respends (rare, e.g.
/// stake-delegation changes on the ref UTxO) do NOT emit
/// `Updated` — only meaningful metadata changes propagate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataUpdated {
    pub policy: String,
    pub slot: u64,
    pub tx_hash: String,
    pub entry: MetadataEntry,
    /// Version field from the prior datum. `entry.version`
    /// reflects the new datum.
    pub prior_version: u64,
}

/// The ref token was consumed without being re-produced.
/// Removes the metadata entry from any consumer projection.
/// Rare in practice — CIP-68 ref tokens are typically held
/// indefinitely by the project treasury — but legal per the
/// spec.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataBurned {
    pub policy: String,
    pub slot: u64,
    pub tx_hash: String,
    /// Lowercase hex of the shared suffix (bytes after the
    /// `000643b0` ref-token label prefix).
    pub asset_name_hex: String,
}

/// One emission. A chunked snapshot
/// (`SnapshotBegin` → `SnapshotChunk` × N → `SnapshotEnd`) is
/// an authoritative full-state replacement; `Initial` / `Updated`
/// / `Burned` are incremental.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MetadataEvent {
    SnapshotBegin(SnapshotBegin),
    SnapshotChunk(SnapshotChunk),
    SnapshotEnd(SnapshotEnd),
    Initial(MetadataInitial),
    Updated(MetadataUpdated),
    Burned(MetadataBurned),
}

#[cfg(feature = "decode")]
pub fn decode_emit(channel: u32, payload: &[u8]) -> Option<String> {
    if channel != 0 {
        return None;
    }
    let event: MetadataEvent = ciborium::de::from_reader(payload).ok()?;
    serde_json::to_string_pretty(&event).ok()
}
