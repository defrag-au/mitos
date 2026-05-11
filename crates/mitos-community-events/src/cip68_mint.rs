//! Wire-format event types for the `cip-68-mint` community
//! module — paired CIP-68 reference + user-token mints.
//!
//! One event per `(reference, user)` pair detected in a TX:
//!
//! - Typical case: TX mints both the `_100`-prefixed reference
//!   token (quantity 1, holds metadata datum) and the
//!   `_222`/`_333`/`_444`-prefixed user token (quantity 1 for
//!   NFTs, N for FTs/RFTs). Emitted as `Cip68Mint` with
//!   `reference = Some(...)`.
//! - User-only re-mint (e.g. RFT re-stocking after burn): TX
//!   mints only the user token; the reference asset already
//!   exists on chain in a prior TX. Emitted as `Cip68Mint` with
//!   `reference = None` — consumer fetches the existing
//!   reference's current datum via chain queries if it needs
//!   metadata.
//!
//! Reference-only mints (initial deploys without user tokens)
//! are not emitted by this module in v1; consumers needing them
//! can subscribe to `standard-burn`'s sibling shape or layer a
//! dedicated module on top.
//!
//! See `mitos/docs/design/MINT_BURN_MODULES.md` for the family
//! design.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cip68Mint {
    /// 56-char lowercase hex policy id.
    pub policy: String,
    /// 64-char lowercase hex tx hash.
    pub tx_hash: String,
    /// Reference-token details when minted in the same TX.
    /// `None` for user-only re-mints (RFT re-stocking, etc).
    pub reference: Option<Cip68RefInfo>,
    /// User-token details — always present (the event exists
    /// because a user-token mint was observed).
    pub user: Cip68UserInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cip68RefInfo {
    /// Hex of the `_100`-prefix-tagged asset name (full
    /// on-chain bytes — CIP-67 4-byte header included).
    pub asset_name_hex: String,
    /// Quantity minted in this TX (typically 1).
    pub quantity: u64,
    /// Raw datum CBOR — preserved for forensics + consumers
    /// that need to decode application-specific `extra`
    /// fields (CIP-68 Constructor-0 field 2).
    #[serde(with = "serde_bytes")]
    pub datum_cbor: Vec<u8>,
    /// JSON-stringified CIP-68 metadata map (the Constructor-0
    /// field 0). `None` if datum decode failed or the
    /// structure doesn't match the CIP-68 shape — the raw
    /// `datum_cbor` is still emitted for application-specific
    /// fallback decode.
    pub metadata_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cip68UserInfo {
    /// Hex of the `_222` / `_333` / `_444`-prefix-tagged asset
    /// name (full on-chain bytes, CIP-67 4-byte header
    /// included).
    pub asset_name_hex: String,
    /// Quantity minted. 1 for `_222` (NFT), N for `_333` (FT)
    /// or `_444` (RFT — restricted fungible).
    pub quantity: u64,
    /// CIP-67 label prefix: 222 (NFT) / 333 (FT) / 444 (RFT).
    pub cip67_label: u32,
}

#[cfg(feature = "decode")]
pub fn decode_emit(channel: u32, payload: &[u8]) -> Option<String> {
    if channel != 0 {
        return None;
    }
    let event: Cip68Mint = ciborium::de::from_reader(payload).ok()?;
    serde_json::to_string_pretty(&event).ok()
}
