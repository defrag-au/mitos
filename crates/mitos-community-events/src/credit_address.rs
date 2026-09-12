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
    /// Raw PlutusData CBOR of an INLINE datum on the credited
    /// output, or `None` when it carries none.
    ///
    /// Forwarded raw for the same reason `metadata` is: this
    /// module stays dumb about intent, and the consumer owns the
    /// schema. A softburn consumer decodes a claim tag here; the
    /// same field carries a registry definition, a CIP-68 fuel
    /// datum, an escrow datum and a protocol config, depending on
    /// which watched address the credit landed at.
    ///
    /// **Inline only.** A datum-by-hash resolves through the
    /// witness set, which is not available on the live path — the
    /// same lag that makes `metadata` unreliable at dispatch time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datum: Option<Vec<u8>>,
    /// Distinct stake credentials (28-byte hashes, hex) across
    /// EVERY input of the transaction, in first-seen order.
    ///
    /// This is what lets a consumer attribute a credit to a
    /// wallet without an external indexer — and, more to the
    /// point, lets it refuse to. `from_address` above is the
    /// LARGEST input and is a presentation convenience; it must
    /// never be used to decide whose money this was.
    ///
    /// An input whose address carries no delegation part
    /// (enterprise, or a true enterprise-script) contributes
    /// nothing here, so a transaction mixing enterprise and
    /// delegated inputs yields the delegated stake alone.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_stake_credentials: Vec<String>,
    /// How many inputs could NOT be resolved to a prior output at
    /// all — pruned past the archive horizon, and dispatched as a
    /// placeholder with an empty address.
    ///
    /// **Load-bearing.** Without it, `input_stake_credentials` is
    /// silently partial: a two-wallet transaction whose second
    /// input is unresolved looks exactly like a one-wallet
    /// transaction, and a consumer would attribute it
    /// confidently to whoever happened to be resolvable. A
    /// non-zero count means "this set is incomplete", and the
    /// only safe reading is that nobody can be named.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub inputs_unresolved: u32,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
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
