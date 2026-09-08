//! Typed request/response shapes for the `serve` surface and the CLI's JSON
//! output. Hex everywhere bytes appear; no `serde_json::Value`.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputAsset {
    /// Policy id, hex.
    pub policy: String,
    /// On-chain asset name bytes, hex.
    pub name: String,
    pub quantity: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedOutput {
    pub index: u32,
    /// bech32 (Shelley) or base58 (Byron); `<unparsable>` if pallas refuses it.
    pub address: String,
    pub lovelace: u64,
    pub assets: Vec<OutputAsset>,
    pub datum_hash: Option<String>,
    /// Inline datum CBOR, hex.
    pub inline_datum: Option<String>,
    pub has_script_ref: bool,
    /// The output as re-encoded CBOR, hex.
    pub cbor: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TxResponse {
    pub tx_hash: String,
    pub era: String,
    pub chunk: u16,
    pub offset: u32,
    /// The transaction BODY (the hashed item), hex.
    pub body_cbor: String,
    pub outputs: Vec<ResolvedOutput>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutputResponse {
    pub tx_hash: String,
    pub index: u32,
    pub era: String,
    pub chunk: u16,
    pub output: ResolvedOutput,
}

/// The result of an auxiliary-data (metadata) lookup.
///
/// A sum type rather than a struct with an optional payload, because the three
/// outcomes are what the caller actually branches on and only one of them has
/// bytes: `found` carries the CBOR, `no_metadata` is a FINAL negative (the tx
/// is indexed and definitively has none), and `unknown_tx` means the index
/// cannot say, so ask another source.
///
/// Collapsing the two negatives — into an `Option`, or by answering one of
/// them with a 404 — is the specific mistake this shape exists to prevent.
/// Most transactions carry no metadata, so a caller that cannot tell them
/// apart falls through to a remote provider on nearly every lookup, which is
/// the cost this index exists to remove. All three answer HTTP 200: the
/// status lives in one field, not split across the body and the HTTP layer.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AuxResponse {
    Found {
        tx_hash: String,
        era: String,
        chunk: u16,
        /// Auxiliary data CBOR, hex.
        aux_cbor: String,
    },
    NoMetadata {
        tx_hash: String,
        era: String,
        chunk: u16,
    },
    UnknownTx {
        tx_hash: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutRefRequest {
    pub tx_hash: String,
    pub index: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolveRequest {
    pub items: Vec<OutRefRequest>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResolveStatus {
    Found,
    /// The tx exists but has fewer outputs than the requested index.
    NoSuchOutput,
    /// No body with that hash in any completed chunk — volatile tip, or a
    /// hash that never landed.
    UnknownTx,
    /// The request itself was malformed (bad hex, wrong length).
    BadRequest,
    /// Lookup failed (I/O, decode) — the message says why.
    Error,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolveResult {
    pub tx_hash: String,
    pub index: u32,
    pub status: ResolveStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<ResolvedOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolveResponse {
    pub results: Vec<ResolveResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub uptime_secs: u64,
    pub base_first_chunk: Option<u16>,
    pub base_last_chunk: Option<u16>,
    pub base_entries: u64,
    pub tail_segments: usize,
    pub tail_entries: u64,
    /// Highest chunk any layer covers.
    pub newest_chunk: Option<u16>,
}
