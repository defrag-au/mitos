//! Wire-format event types for the `cip-25-mint` community
//! module — CIP-25 NFT/FT mints with label-721 metadata decoded
//! to JSON.
//!
//! One event per positive `tx.mint` entry where the TX carries
//! label-721 metadata under the asset's policy. `metadata_json`
//! is `None` when the TX has label-721 data but decoding the
//! entry for this specific asset failed (malformed metadata) —
//! the mint event still emits with the asset details so
//! consumers can observe the mint and reconcile metadata
//! out-of-band.
//!
//! See `mitos/docs/design/MINT_BURN_MODULES.md` for the family
//! design + the CIP-25 decode strategy.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cip25Mint {
    /// 56-char lowercase hex policy id.
    pub policy: String,
    /// Lowercase hex of the on-chain asset-name bytes.
    pub asset_name_hex: String,
    /// 64-char lowercase hex tx hash that contained the mint.
    pub tx_hash: String,
    /// Quantity minted in this TX (the absolute value of the
    /// positive `quantity_delta`). 1 for typical NFTs, N for
    /// CIP-25 fungibles.
    pub quantity: u64,
    /// JSON-stringified label-721 metadata entry for this
    /// asset. `None` if the TX has label-721 data but the entry
    /// for this `(policy, asset_name)` either isn't present or
    /// failed to decode.
    ///
    /// The JSON shape mirrors the CBOR map structure: nested
    /// maps become objects; lists become arrays; bytes that are
    /// valid UTF-8 become strings, otherwise base64-encoded
    /// strings. Consumers can `serde_json::from_str` into their
    /// own typed shape or pass to `cardano_assets::AssetMetadata`
    /// for the project-standard decode.
    pub metadata_json: Option<String>,
    /// Absolute slot of the TX's block. Lets a consumer confirm
    /// at a precise chain point and revert by slot on a rollback
    /// (`ServerMessage::Undo`). `#[serde(default)]` keeps the
    /// field additive — payloads emitted before this field was
    /// introduced decode with `slot = 0`.
    #[serde(default)]
    pub slot: u64,
}

#[cfg(feature = "decode")]
pub fn decode_emit(channel: u32, payload: &[u8]) -> Option<String> {
    if channel != 0 {
        return None;
    }
    let event: Cip25Mint = ciborium::de::from_reader(payload).ok()?;
    serde_json::to_string_pretty(&event).ok()
}
