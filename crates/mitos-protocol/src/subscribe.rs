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

use mitos_lifecycle::ids::{ModuleId, Revision, Sha256Hex};
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
    /// The dialer opens one drain task per target. Multi-target
    /// subscribes are supported end-to-end — each target's HTTP
    /// POSTs land at `/_internal/apply-<target>` /
    /// `/_internal/recapture-<target>` on the companion (per the
    /// `{target}` and `{op}` substitutions in `MITOS_REPLICATE_URL`)
    /// and the runtime routes by the URL channel suffix to the
    /// `MitosChannel` whose `NAME` matches.
    pub targets: Vec<SubscribeTarget>,

    /// Companion key. dApp's choice — see the design doc's Q8
    /// resolution. Load-bearing in four places (DO addressing, host
    /// emission scoping, Worker URL `{key}` substitution,
    /// subscribe-call `companion_key` field).
    pub companion_key: String,

    /// Client instance identifier. dApp-chosen, **required**, must
    /// be non-empty. Disambiguates two consumers that share the
    /// same `companion_key` (e.g. a dev worker and a prod worker
    /// both subscribing to the same policy_id). The companion
    /// store keys records by `(module_id, client_id,
    /// companion_key)` so distinct `client_id`s land in parallel
    /// records and each gets its own dial-back URL slot.
    ///
    /// The mitos-companion SDK derives this from the host portion
    /// of the dial-back URL by default. dApps may override via
    /// `MitosCompanion::client_id()`. See
    /// `docs/design/MULTI_CLIENT_COMPANIONS.md` — "Worker-side
    /// change".
    ///
    /// Validated server-side at `/api/companions/subscribe`:
    /// empty / whitespace-only is rejected with HTTP 400.
    pub client_id: String,

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

    /// The artifact(s) this worker was BUILT against — see
    /// [`AnnouncedModule`]. Empty for every companion that does not announce,
    /// which is all of them until an operator opts one in.
    ///
    /// On the REQUEST rather than on [`SubscribeTarget::Module`] because that
    /// variant is built by literal across five workers in another repo;
    /// `SubscribeRequest` is not. `skip_serializing_if` is load-bearing: a
    /// request that announces nothing must encode byte-identically to one
    /// from before this field existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub announced: Vec<AnnouncedModule>,
}

/// "This is the module I was built against."
///
/// **There is no URL**, on purpose. The host derives where to fetch from out
/// of the client's AUTHENTICATED identity — a client id is the worker's own
/// hostname, and an operator wrote it into the namespace registry. So the
/// question "may I fetch this URL" never arises. See
/// `mitos_lifecycle::artifact::ArtifactSource` for the full reasoning; do not
/// add one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnouncedModule {
    pub module: ModuleId,
    pub wasm: Sha256Hex,
    /// The MANIFEST's own digest. The wasm sha pins the code; the manifest
    /// carries the upgrade actions and the interest set, and is fetched from
    /// the same public place.
    pub manifest: Sha256Hex,
    pub revision: Revision,
    /// The module's `config.cbor`, INLINE — it is part of instance identity
    /// and is small, so it travels with the announcement rather than being
    /// fetched. `None` means the module has no config, which the host
    /// distinguishes from an empty one. At most 64 KiB.
    #[serde(default, with = "serde_bytes")]
    pub config: Option<Vec<u8>>,
}

/// A worker embedded one build's digests and a different build's config
/// bytes — so what it would announce does not describe what it is running.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "config.cbor does not match the build: built-module.json says {declared}, the embedded bytes \
     hash to {actual} — the two came from different builds"
)]
pub struct MismatchedConfig {
    pub declared: String,
    pub actual: String,
}

impl AnnouncedModule {
    /// Turn an embedded `built-module.json` into the announcement, with the
    /// config bytes the worker also embedded.
    ///
    /// REFUSES when those bytes do not hash to the digest the build recorded.
    /// A worker whose two embedded artifacts came from different builds
    /// should find that out at startup, where the message names both digests
    /// — not as a refused announcement whose reason is a sha mismatch inside
    /// the host.
    pub fn from_built(
        built: &mitos_lifecycle::artifact::BuiltModule,
        config: Option<Vec<u8>>,
    ) -> Result<Self, MismatchedConfig> {
        let actual = config.as_deref().map(sha256_hex);
        let declared = built.config.as_ref().map(|c| c.as_str().to_owned());
        if actual != declared {
            return Err(MismatchedConfig {
                declared: declared.unwrap_or_else(|| "no config".to_owned()),
                actual: actual.unwrap_or_else(|| "no config".to_owned()),
            });
        }
        Ok(Self {
            module: built.module.clone(),
            wasm: built.wasm.clone(),
            manifest: built.manifest.clone(),
            revision: built.revision,
            config,
        })
    }
}

/// Lowercase hex SHA-256. A local copy rather than a dependency on
/// `mitos-platform`: this crate compiles into every companion worker for
/// wasm32, and the host crate does not.
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// What the host made of one announcement. Purely informational: an
/// announcement NEVER fails the subscribe, so a refused one leaves the
/// companion registered and served exactly where it was.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnounceOutcome {
    pub module: ModuleId,
    pub result: AnnounceResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "result")]
pub enum AnnounceResult {
    /// The client is already on this exact artifact. Every re-announcement
    /// from a client's other companions lands here.
    AlreadyCurrent,
    /// A move toward it is pending.
    Accepted,
    Refused {
        reason: String,
    },
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

    /// Host's current emission_id counter for this module, read
    /// from the per-module `EmissionsStore`. Companions treat
    /// this as a sync point: anything below it is already in the
    /// emissions log (delivered or pending), anything from this
    /// id forward is "live" — covered by the dial-back stream
    /// rather than the resume scan.
    #[serde(default)]
    pub next_emission_id: u64,

    /// One outcome per [`SubscribeRequest::announced`] entry. Empty for a
    /// worker that announced nothing, so an older worker decoding this
    /// response sees exactly what it always did.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub announced: Vec<AnnounceOutcome>,
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

/// The compatibility guarantees, as tests.
///
/// A host and a worker are deployed independently and in either order, so all
/// four combinations of old/new on each side have to work. These pin the two
/// that adding a field could break.
#[cfg(test)]
mod announce_tests {
    use super::*;

    /// `SubscribeRequest` exactly as it was before `announced` existed —
    /// same fields, same order, same names. Encoding one of these is the
    /// baseline: it is what a worker built against the old crate puts on the
    /// wire, reproduced rather than pasted as a byte literal so it cannot
    /// silently stop corresponding to the real old shape.
    #[derive(Debug, Serialize, Deserialize)]
    struct PreWo05Request {
        targets: Vec<SubscribeTarget>,
        companion_key: String,
        client_id: String,
        #[serde(default)]
        resume_from: Option<ChainPoint>,
        #[serde(default)]
        interests: Vec<Interest>,
        #[serde(default)]
        dial_back: Option<DialBackOverride>,
    }

    #[derive(Debug, Deserialize)]
    struct PreWo05Response {
        status: String,
        #[serde(default)]
        next_emission_id: u64,
    }

    fn request() -> SubscribeRequest {
        SubscribeRequest {
            targets: vec![SubscribeTarget::Module {
                name: "ownership-indexer".into(),
            }],
            companion_key: "policy_a".into(),
            client_id: "ownership.cnft.dev".into(),
            resume_from: None,
            interests: vec![],
            dial_back: None,
            announced: Vec::new(),
        }
    }

    fn announcement() -> AnnouncedModule {
        AnnouncedModule {
            module: ModuleId::parse("ownership-indexer").unwrap(),
            wasm: Sha256Hex::parse(&"ab".repeat(32)).unwrap(),
            manifest: Sha256Hex::parse(&"cd".repeat(32)).unwrap(),
            revision: Revision(5),
            config: None,
        }
    }

    /// The `skip_serializing_if`, proven on the wire. If this fails, every
    /// companion that announces nothing starts sending a different request.
    #[test]
    fn a_request_that_announces_nothing_encodes_as_it_always_did() {
        let old = PreWo05Request {
            targets: vec![SubscribeTarget::Module {
                name: "ownership-indexer".into(),
            }],
            companion_key: "policy_a".into(),
            client_id: "ownership.cnft.dev".into(),
            resume_from: None,
            interests: vec![],
            dial_back: None,
        };
        let mut baseline = Vec::new();
        ciborium::ser::into_writer(&old, &mut baseline).unwrap();

        assert_eq!(request().encode().unwrap(), baseline);
    }

    /// A NEW worker against an OLD host: the host ignores the field and
    /// serves the subscribe as it always did.
    #[test]
    fn an_old_host_ignores_an_announcement() {
        let mut announcing = request();
        announcing.announced = vec![announcement()];
        let bytes = announcing.encode().unwrap();

        let old: PreWo05Request = ciborium::de::from_reader(&bytes[..])
            .expect("an older host must still decode this request");
        assert_eq!(old.client_id, "ownership.cnft.dev");
        assert_eq!(old.targets.len(), 1);
    }

    /// An OLD worker against a NEW host: the response it gets back is the
    /// one it understands.
    #[test]
    fn an_old_worker_gets_no_outcomes() {
        let response = SubscribeResponse {
            status: "subscribed".into(),
            next_emission_id: 42,
            announced: Vec::new(),
        };
        let bytes = response.encode().unwrap();
        let old: PreWo05Response = ciborium::de::from_reader(&bytes[..]).unwrap();
        assert_eq!(old.status, "subscribed");
        assert_eq!(old.next_emission_id, 42);

        // …and an outcome-bearing response is still readable by it.
        let with_outcome = SubscribeResponse {
            announced: vec![AnnounceOutcome {
                module: ModuleId::parse("ownership-indexer").unwrap(),
                result: AnnounceResult::Accepted,
            }],
            ..response
        };
        let bytes = with_outcome.encode().unwrap();
        let old: PreWo05Response = ciborium::de::from_reader(&bytes[..]).unwrap();
        assert_eq!(old.next_emission_id, 42);
    }

    /// The ids are validated newtypes, so a bad digest is refused by the
    /// DECODER — the existing 400 — rather than by a hand-written check in
    /// the handler that somebody could forget.
    #[test]
    fn a_malformed_digest_is_a_decode_error() {
        #[derive(Serialize)]
        struct Loose<'a> {
            module: &'a str,
            wasm: &'a str,
            manifest: &'a str,
            revision: u32,
            config: Option<Vec<u8>>,
        }
        let good_manifest = "cd".repeat(32);
        for (wasm, why) in [
            ("not-a-sha", "too short"),
            (
                "AB00000000000000000000000000000000000000000000000000000000000000",
                "uppercase is a second spelling of one digest",
            ),
        ] {
            let mut bytes = Vec::new();
            ciborium::ser::into_writer(
                &Loose {
                    module: "ownership-indexer",
                    wasm,
                    manifest: &good_manifest,
                    revision: 1,
                    config: None,
                },
                &mut bytes,
            )
            .unwrap();
            assert!(
                ciborium::de::from_reader::<AnnouncedModule, _>(&bytes[..]).is_err(),
                "{why}",
            );
        }
    }

    /// A worker that embeds one build's `built-module.json` and another
    /// build's `config.cbor` must find out at STARTUP, not as a refused
    /// announcement whose reason is a sha mismatch inside the host.
    #[test]
    fn from_built_refuses_config_bytes_that_do_not_match_the_build() {
        use mitos_lifecycle::artifact::BuiltModule;

        let config = b"\xa1\x64conf\x01".to_vec();
        let built = BuiltModule {
            module: ModuleId::parse("ownership-indexer").unwrap(),
            wasm: Sha256Hex::parse(&"ab".repeat(32)).unwrap(),
            manifest: Sha256Hex::parse(&"cd".repeat(32)).unwrap(),
            config: Some(Sha256Hex::parse(&sha256_hex(&config)).unwrap()),
            revision: Revision(5),
        };

        // The matching bytes are accepted.
        let announced = AnnouncedModule::from_built(&built, Some(config.clone())).unwrap();
        assert_eq!(announced.config, Some(config));
        assert_eq!(announced.revision, Revision(5));

        // Another build's bytes are not.
        let err =
            AnnouncedModule::from_built(&built, Some(b"a different build".to_vec())).unwrap_err();
        assert!(err.to_string().contains("different builds"), "{err}");

        // Nor is dropping them — "no config" is not the same as this config,
        // and the difference is part of instance identity.
        assert!(AnnouncedModule::from_built(&built, None).is_err());

        // A module with no config announces none.
        let configless = BuiltModule {
            config: None,
            ..built
        };
        assert!(AnnouncedModule::from_built(&configless, None).is_ok());
        assert!(AnnouncedModule::from_built(&configless, Some(vec![1])).is_err());
    }

    #[test]
    fn an_announcement_round_trips() {
        let mut announcing = request();
        announcing.announced = vec![AnnouncedModule {
            config: Some(vec![0xa1, 0x01, 0x02]),
            ..announcement()
        }];
        let bytes = announcing.encode().unwrap();
        let decoded = SubscribeRequest::decode(&bytes).unwrap();
        assert_eq!(decoded.announced, announcing.announced);
    }
}
