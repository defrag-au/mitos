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
