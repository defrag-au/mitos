//! Companion subscribe-call wire format.
//!
//! Per the design doc (`docs/strategy/MITOS_COMPANION_RUNTIME_V1.md`,
//! "Addressing & wake-up: mitos dials companions"), the companion
//! POSTs a single CBOR-encoded `SubscribeRequest` to the mitos host
//! on first DO wake. Mitos persists the registration, then dials
//! back to establish the WS for emission delivery.
//!
//! These types are **shared verbatim** by the host (mitos) and
//! the CF companion (cnft.dev-workers/types/mitos-companion); both
//! sides re-export them from this module to avoid drift.

use serde::{Deserialize, Serialize};

use crate::interest::Interest;
use crate::wire::ChainPoint;

/// Wire MIME type for both `SubscribeRequest` and
/// `SubscribeResponse`.
///
/// CBOR for both directions — same encoder + decoder pair on
/// each side, no encoding ambiguity. Errors (non-2xx HTTP
/// responses) stay JSON so operators can read the body straight
/// from `curl` output.
pub const SUBSCRIBE_MIME: &str = "application/cbor";

/// Request body for `POST /api/companions/subscribe`.
///
/// CBOR-encoded — host expects `application/cbor`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeRequest {
    /// Module name (matches the host-side indexer's `name()`).
    pub module_name: String,

    /// Companion key. dApp's choice — see the design doc's Q8
    /// resolution. Load-bearing in four places (DO addressing, host
    /// emission scoping, Worker URL `{key}` substitution,
    /// subscribe-call `companion_key` field).
    pub companion_key: String,

    /// Resume cursor. `None` for fresh companions; otherwise the
    /// last applied chain point from the companion's DO storage.
    #[serde(default)]
    pub resume_from: Option<ChainPoint>,

    /// Initial interest set. May be empty for v1 dApps that
    /// populate interest dynamically over the WS post-dial.
    #[serde(default)]
    pub interests: Vec<Interest>,

    /// Optional per-companion dial-back override. `None` = use the
    /// module's `mitos.toml [companion]` defaults.
    #[serde(default)]
    pub dial_back: Option<DialBackOverride>,
}

/// Per-companion override for the dial-back URL. Rare — intended
/// for multi-tenant SaaS with per-customer subdomains. Most
/// companions inherit the module-level defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DialBackOverride {
    pub url: Option<String>,
    pub auth_header: Option<String>,
    pub auth_value: Option<String>,
}

/// Response body from `POST /api/companions/subscribe`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeResponse {
    /// Status string. `"subscribed"` on success.
    pub status: String,

    /// Host's current emission_id counter for this module. PR 3
    /// populates this from the actual `module_emissions` log; PR 1
    /// stub returns 0.
    #[serde(default)]
    pub next_emission_id: u64,
}

// ============================================================================
// Wire codec — co-located with the types so both sides agree on the
// encoding without each independently picking ciborium / serde_json /
// axum::Json. CBOR for both directions; the host's `subscribe_handler`
// and the companion's `post_subscribe` call into these.
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum SubscribeWireError {
    #[error("encode: {0}")]
    Encode(String),
    #[error("decode: {0}")]
    Decode(String),
}

impl SubscribeRequest {
    /// CBOR-encode for the wire. Used by the companion's
    /// `post_subscribe` HTTPS call.
    pub fn encode(&self) -> Result<Vec<u8>, SubscribeWireError> {
        let mut buf = Vec::with_capacity(256);
        ciborium::ser::into_writer(self, &mut buf)
            .map_err(|e| SubscribeWireError::Encode(e.to_string()))?;
        Ok(buf)
    }

    /// CBOR-decode from the wire. Used by the host's
    /// `subscribe_handler`.
    pub fn decode(bytes: &[u8]) -> Result<Self, SubscribeWireError> {
        ciborium::de::from_reader(bytes).map_err(|e| SubscribeWireError::Decode(e.to_string()))
    }
}

impl SubscribeResponse {
    /// CBOR-encode for the wire. Used by the host's
    /// `subscribe_handler`. Errors (non-2xx) bypass this and
    /// return JSON for operator readability.
    pub fn encode(&self) -> Result<Vec<u8>, SubscribeWireError> {
        let mut buf = Vec::with_capacity(64);
        ciborium::ser::into_writer(self, &mut buf)
            .map_err(|e| SubscribeWireError::Encode(e.to_string()))?;
        Ok(buf)
    }

    /// CBOR-decode from the wire. Used by the companion's
    /// `post_subscribe` HTTPS call.
    pub fn decode(bytes: &[u8]) -> Result<Self, SubscribeWireError> {
        ciborium::de::from_reader(bytes).map_err(|e| SubscribeWireError::Decode(e.to_string()))
    }
}
