//! Wire-format event types for the `credit-address` community
//! module — value (ADA + any native assets) landing in an output
//! at an address the consumer flagged as interesting.
//!
//! The mirror of `burn-address`: where that watches outputs at a
//! "sink" address and reports the assets sent there, this watches
//! outputs at ANY watched address and reports the whole credit
//! (lovelace + assets). It is deliberately dumb about INTENT — a
//! credit might be a buyer payment, a fund top-up, change, or
//! junk; classifying it is the consumer's job (e.g. a mint engine
//! checks a CIP-674 tag to tell a payment from a top-up).
//!
//! Interest is **dynamic addresses** (`kind = "address"`), so a
//! consumer can subscribe a per-tenant receiving address created
//! at runtime — e.g. each collection's deposit address — without
//! a module rebuild.
//!
//! One event per OUTPUT landing at a watched address (NOT per
//! asset): the output's total `lovelace` plus the list of native
//! assets it carries. A pure-ADA payment emits one event with an
//! empty `assets`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressCredit {
    /// The watched address the output landed at (bech32). Echoed
    /// back so consumers watching multiple addresses can route the
    /// credit (e.g. address → tenant/collection).
    pub address: String,
    /// 64-char lowercase hex tx hash that produced the output.
    pub tx_hash: String,
    /// Output index within `tx_hash`.
    pub output_index: u32,
    /// Lovelace (ADA) in the credited output.
    pub lovelace: u64,
    /// The payer — resolved bech32 address of the tx's
    /// largest-lovelace input. For a buyer payment this is the
    /// sender: the consumer's NFT-delivery + refund counterparty.
    /// The module resolves it from dolos alone (the tx's consumed
    /// inputs on the live path, `read_tx` on the cold-start walk),
    /// so the consumer needs no external indexer to act on a credit.
    pub from_address: String,
    /// Slot of the block carrying `tx_hash`. Lets the consumer order
    /// credits and advance a scan cursor.
    pub slot: u64,
    /// Raw transaction-metadata CBOR (the CIP-20/674 label map), or
    /// `None` when the tx carries no metadata — the common case for a
    /// plain buyer payment. Forwarded so the consumer can classify
    /// INTENT without an external indexer: e.g. a mint engine reads a
    /// CIP-674 marker here to tell an operator wallet top-up from a
    /// buyer payment (both land at the same watched address).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Vec<u8>>,
    /// Native assets carried by the output, if any. Empty for a
    /// pure-ADA credit (the common payment case).
    pub assets: Vec<CreditedAsset>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreditedAsset {
    /// 56-char lowercase hex policy id.
    pub policy: String,
    /// Lowercase hex of the on-chain asset-name bytes.
    pub asset_name_hex: String,
    /// Quantity of this asset in the output.
    pub quantity: u64,
}

#[cfg(feature = "decode")]
pub fn decode_emit(channel: u32, payload: &[u8]) -> Option<String> {
    if channel != 0 {
        return None;
    }
    let event: AddressCredit = ciborium::de::from_reader(payload).ok()?;
    serde_json::to_string_pretty(&event).ok()
}
