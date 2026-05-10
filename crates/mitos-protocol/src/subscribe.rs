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
    /// Sources the companion wants to receive events from. Each
    /// target is either a wasm module or an in-tree indexer.
    /// See `docs/design/UNIFIED_SUBSCRIBE.md`.
    ///
    /// v1 implementation opens one dial-back WS per target. Multi-
    /// target subscriptions in a single subscribe call ARE
    /// supported on the wire from day one (forward-compat), but
    /// the v1 host limits this to a single target until the
    /// per-target WS-multiplexing work lands.
    pub targets: Vec<SubscribeTarget>,

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

/// What a companion subscribes to: a wasm module or an in-tree
/// indexer. The host's `/api/companions/subscribe` handler
/// routes the dial-back differently per variant — module
/// targets go through `host_v2`'s emit stream; indexer targets
/// connect to the indexer's broadcast channel via the bridge
/// described in `docs/design/UNIFIED_SUBSCRIBE.md`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubscribeTarget {
    /// Wasm module identity (matches the module's
    /// `[module].id` in its manifest).
    Module { name: String },
    /// In-tree indexer name (e.g. `"mint-burn"`, `"marketplace"`).
    /// Indexers marked internal (`Indexer::is_internal() == true`)
    /// are not subscribable through this path — the host returns
    /// 400.
    Indexer { name: String },
}

impl SubscribeTarget {
    /// Convenience: the name field, regardless of variant.
    pub fn name(&self) -> &str {
        match self {
            SubscribeTarget::Module { name } | SubscribeTarget::Indexer { name } => name,
        }
    }
}

impl SubscribeRequest {
    /// Extract the single module target's name, for v1 callers
    /// that only support `[SubscribeTarget::Module { name }]`.
    /// Returns `None` if the request has zero or multiple targets
    /// or if the single target is an `Indexer` variant.
    ///
    /// Removed once multi-target + indexer-target support lands
    /// in the host (steps 3+ of the unified-subscribe migration).
    pub fn single_module_target(&self) -> Option<&str> {
        match self.targets.as_slice() {
            [SubscribeTarget::Module { name }] => Some(name.as_str()),
            _ => None,
        }
    }

    /// Transitional accessor: returns the first target's name,
    /// for code paths that haven't been refactored to handle
    /// multi-target subscriptions yet. v1 always populates exactly
    /// one target, so this is equivalent to the old
    /// `request.module_name` field access at every existing call
    /// site.
    ///
    /// Returns `"<no-target>"` if `targets` is empty — that should
    /// never happen for v1 requests; the placeholder is safer than
    /// panicking inside log fields.
    ///
    /// Removed once host-side code is refactored to handle each
    /// target explicitly (per-target dial loops, per-target
    /// storage paths) — see steps 3+ of the unified-subscribe
    /// migration.
    pub fn primary_target_name(&self) -> &str {
        self.targets
            .first()
            .map(|t| t.name())
            .unwrap_or("<no-target>")
    }
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
