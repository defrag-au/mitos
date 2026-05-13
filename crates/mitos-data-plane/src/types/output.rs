//! `TypedOutput`, `AssetEntry`, `TypedDatum`, `TypedScript` —
//! the structured query response shapes.
//!
//! Optional fields mean **"absent on chain"**, never "caller
//! didn't ask". The latter case is encoded in the request's
//! `DecodeLevel`, not in the response's `Option<T>`. Keeps the
//! response struct's contract honest.

use cardano_assets::PolicyId;
use pallas_primitives::{Hash, PlutusData};
use serde::{Deserialize, Serialize};

use crate::types::DecodeLevel;

/// A single asset entry on an output. Cardano outputs carry an
/// asset multiset; one `AssetEntry` per `(policy_id, asset_name)`
/// in that multiset.
///
/// `asset_name_hex` is the canonical hex form (matching
/// `cardano_assets::AssetId.asset_name_hex`). Quantity is `u64`
/// because we're surfacing current state — positive holdings.
/// Mints / burns (signed deltas) belong in event types, not
/// snapshot queries.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AssetEntry {
    pub policy_id: PolicyId,
    pub asset_name_hex: String,
    pub quantity: u64,
}

impl AssetEntry {
    pub fn new(policy_id: PolicyId, asset_name_hex: impl Into<String>, quantity: u64) -> Self {
        Self {
            policy_id,
            asset_name_hex: asset_name_hex.into(),
            quantity,
        }
    }
}

/// Server-resolved datum. Hash always present when any datum
/// exists on the output; payload populated whenever the plane
/// could resolve it (inline datum directly, or hash-referenced
/// via witness-set lookup); `original_cbor` available for
/// callers that prefer to skip the codec hop.
///
/// **Caller-blind resolution.** The plane handles inline-vs-hash
/// transparently — caller never sees the source distinction
/// unless they specifically inspect both `payload` and
/// `original_cbor`. If `payload.is_some()` you have decoded
/// PlutusData regardless of which on-chain shape the datum
/// actually had.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypedDatum {
    pub hash: Hash<32>,
    /// `Some` iff the plane successfully resolved + decoded the
    /// datum body. `None` for hash-references the plane couldn't
    /// resolve (witness-set absent — rare in current state but
    /// possible).
    pub payload: Option<PlutusData>,
    /// Raw CBOR bytes of the datum, when known. Always present
    /// when `payload` is `Some`; may be `None` if the plane
    /// couldn't resolve.
    pub original_cbor: Option<Vec<u8>>,
}

impl TypedDatum {
    /// Backfill `original_cbor` from a witness-set table when the
    /// plane couldn't resolve bytes directly. The witness slice is
    /// the consuming TX's plutus-data witnesses keyed by hash —
    /// jpg.store's metadata-encoded CO datums, for example, are
    /// only revealed in the cancel/accept TX's witness set, so
    /// hash-only `prior_datum` values from a `read_utxo` lookup
    /// need this fallback before dispatch reaches the module.
    ///
    /// No-op when `original_cbor` is already populated, or when no
    /// witness entry matches by hash. `payload` is left untouched —
    /// minicbor decode stays out of the dispatch hot path.
    pub fn fill_from_witness(&mut self, witnesses: &[(Hash<32>, Vec<u8>)]) {
        if self.original_cbor.is_some() {
            return;
        }
        if let Some((_, cbor)) = witnesses.iter().find(|(h, _)| *h == self.hash) {
            self.original_cbor = Some(cbor.clone());
        }
    }
}

/// On-chain script (reference scripts only — outputs that carry
/// a `script_ref`). Native script + Plutus V1/V2/V3 are
/// distinguished via `language`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedScript {
    pub hash: Hash<28>,
    pub language: ScriptLanguage,
    pub original_cbor: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ScriptLanguage {
    Native,
    PlutusV1,
    PlutusV2,
    PlutusV3,
}

/// Whether a `TypedOutput` carries real chain data or is a
/// dispatcher-emitted placeholder for an input whose prior
/// output the data plane couldn't resolve.
///
/// `Unresolved` shows up on `ConsumedEvent.prior_output` /
/// `ReferencedEvent.prior_output` when the input's `OutputRef`
/// names a UTxO outside the archive horizon (or otherwise
/// missing from current state + archive). The dispatcher emits
/// a placeholder so downstream filters can decide whether to
/// surface or skip — see
/// `docs/design/EVENT_DELIVERY_RESILIENCE.md` drop site #1.
///
/// `address`, `lovelace`, `assets` on an unresolved output are
/// empty / zero — only the OREF + (for Consumed) the redeemer
/// carry semantic content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum Resolution {
    /// Real chain output, fully populated to the requested
    /// `DecodeLevel`.
    #[default]
    Resolved,
    /// Placeholder: the OREF was valid but the data plane
    /// couldn't return chain data for it (archive horizon, etc).
    Unresolved,
}

/// What the data plane returns per output. Field optionality
/// rules:
///
/// - `address` — always present (every UTxO has one).
/// - `lovelace` — always present.
/// - `assets` — always present (empty `Vec` if pure-ADA output).
/// - `datum` — `None` iff the output has no datum on chain.
///   When present, content depends on `DecodeLevel` (see below).
/// - `script_ref` — `None` iff the output has no reference
///   script. When present, populated only at
///   `DecodeLevel::Full`.
/// - `original_cbor` — present only if the request asked for it
///   (via `DecodeLevel::Full` or `Lean`-with-raw flag — TBD).
///
/// `datum.payload` and `datum.original_cbor` populate per the
/// plane's resolution effort:
/// - `DecodeLevel::Lean` — `datum: None` regardless of on-chain
///   datum (skipped).
/// - `DecodeLevel::WithDatum` — `datum: Some(TypedDatum { hash,
///   payload, .. })` with `payload` resolved if the plane could.
/// - `DecodeLevel::Full` — same as `WithDatum`, plus `script_ref`
///   populated when present.
///
/// (Originally drafted as a struct with every field
/// `Option<T>`. That conflated "absent on chain" with "caller
/// didn't ask" — encoded the request shape in the response.
/// Switching to `DecodeLevel` request-side keeps the
/// `Option<T>` here meaning genuinely-absent, which the type
/// system can rely on.)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypedOutput {
    /// Always present. Bech32-encoded address (`addr1...` for
    /// Shelley, `stake1...` for stake, base58 for Byron).
    /// Stored as `String` so serde works without custom glue;
    /// callers needing the typed `pallas_addresses::Address`
    /// can `Address::from_bech32(&output.address)` cheaply.
    pub address: String,
    /// Lovelace held at this output.
    pub lovelace: u64,
    /// Native assets multiset. Empty for pure-ADA outputs.
    pub assets: Vec<AssetEntry>,
    /// Datum if any. `None` for outputs with no datum.
    /// Population per `DecodeLevel`.
    pub datum: Option<TypedDatum>,
    /// Reference script if any. `None` for outputs without one.
    /// Populated only at `DecodeLevel::Full`.
    pub script_ref: Option<TypedScript>,
    /// Original output CBOR. Populated when caller wants
    /// passthrough; useful for re-serialising into tx
    /// constructions without re-encoding.
    pub original_cbor: Option<Vec<u8>>,
    /// What level of decode the plane actually performed for
    /// this output — surfaced so callers can detect whether
    /// expected fields are populated. Mirror of the request's
    /// `DecodeLevel`.
    pub decoded_at: DecodeLevel,
    /// Whether this is real chain data or a dispatcher
    /// placeholder for an input whose prior output the data
    /// plane couldn't resolve. See [`Resolution`] for the full
    /// rationale.
    #[serde(default)]
    pub resolution: Resolution,
}

impl TypedOutput {
    /// Placeholder for an input whose prior output couldn't be
    /// resolved. Address is empty, lovelace is zero, asset
    /// multiset is empty — only the surrounding event's OREF
    /// (and, for Consumed, the redeemer) carry semantic content.
    /// Callers must check [`Self::is_resolved`] before relying
    /// on field values.
    ///
    /// Emitted by the dispatcher in place of the previous
    /// silent-drop when `read_utxos` misses a ref past the
    /// archive horizon. See
    /// `docs/design/EVENT_DELIVERY_RESILIENCE.md` drop site #1.
    pub fn unresolved() -> Self {
        Self {
            address: String::new(),
            lovelace: 0,
            assets: Vec::new(),
            datum: None,
            script_ref: None,
            original_cbor: None,
            decoded_at: DecodeLevel::Lean,
            resolution: Resolution::Unresolved,
        }
    }

    /// `true` iff this output is real chain data.
    pub fn is_resolved(&self) -> bool {
        matches!(self.resolution, Resolution::Resolved)
    }

    /// `true` iff this output is a dispatcher placeholder. The
    /// inverse of [`Self::is_resolved`], spelled out for clarity
    /// at call sites.
    pub fn is_unresolved(&self) -> bool {
        matches!(self.resolution, Resolution::Unresolved)
    }
}
