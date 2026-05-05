//! Ownership indexer — wasm-module port.
//!
//! Watches a configured set of policies; for each block,
//! enumerates produced outputs and emits an `OwnershipChange::Transfer`
//! event for every asset under a watched policy.
//!
//! Intentionally *behaviour-equivalent* with the host-side
//! `crates/collection-ownership-indexer/` so platform v1 can
//! be tested for observable equivalence: same emission for the
//! same blocks. CIP-14 fingerprint computation is deferred
//! host-side in v1 (the WIT exposes only address + lovelace +
//! assets; fingerprints could be added with an ABI bump).
//!
//! Watch-state model is simpler than the host-side version:
//! v1 modules don't have a subscription system, so the watch
//! set is *the* config — pinned at init from CBOR'd typed
//! config.

wit_bindgen::generate!({
    path: "../wit",
    world: "mitos-module",
});

use std::cell::RefCell;
use std::collections::HashSet;

use mitos_protocol::{Interest, watched_policies};
use serde::{Deserialize, Serialize};

use crate::mitos::platform::logging::LogLevel;
use crate::mitos::platform::state_kv;

/// `state-kv` key under which the persisted interest set lives.
/// Module-private namespace; the host scopes state-kv per
/// module so collisions with other modules' keys are impossible.
const INTEREST_STATE_KV_KEY: &str = "__mitos_interest_set";

/// CBOR'd typed config the host hands us in `init`. Mirror of
/// `mitos.toml` for this module — only `policies` for v1.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Config {
    /// Watched policy IDs as 56-char lowercase hex strings.
    /// Empty set = no-op (the module loads but emits nothing).
    policies: Vec<String>,
}

/// Mirror of the host-side `OwnershipChange` event shape. Same
/// field names + serde representation so a CF mirror that
/// already deserialises today's host-side events will accept
/// these without change.
///
/// `role` and `asset_fingerprint` are dropped from v1 module
/// emissions: the WIT doesn't expose them, and computing them
/// host-side adds CBOR boundary cost. Re-add via WIT extension
/// when we promote to v2.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum OwnershipChange {
    Transfer {
        policy_id: String,
        asset_name: String,
        new_owner: String,
        tx_hash: String,
        output_index: u32,
    },
}

thread_local! {
    /// Hot-path filter set: policy hex → emit. Derived from the
    /// canonical `INTERESTS` list whenever it changes; reading
    /// `WATCHED` per asset in `handle_event` is O(1).
    static WATCHED: RefCell<HashSet<String>> = RefCell::new(HashSet::new());

    /// Canonical interest set — the Vec the host hands us via
    /// `update_interest`, plus whatever was bootstrapped from
    /// `init` config. Persisted to state-kv on every mutation
    /// so a host restart without an attached companion can
    /// rehydrate the same filter on the next `init`.
    static INTERESTS: RefCell<Vec<Interest>> = const { RefCell::new(Vec::new()) };
}

/// Recompute `WATCHED` from `INTERESTS`. Called on every interest
/// mutation. `mitos_protocol::watched_policies` returns
/// `Some(Vec<PolicyId>)` when the union of selectors is policy-
/// bounded; `None` means at least one interest matches all assets
/// (treated here as "watch everything currently asserted in
/// other interests" — for v1 ownership we conservatively accept
/// that non-bounded interests degrade to no-op since we have no
/// bounded substitute).
fn rebuild_watched() {
    let policies = INTERESTS.with(|i| watched_policies(i.borrow().as_slice()));
    WATCHED.with(|w| {
        let mut set = w.borrow_mut();
        set.clear();
        if let Some(policy_ids) = policies {
            for p in policy_ids {
                // `PolicyId::as_str()` returns the canonical
                // 56-char lowercase hex — matches the format
                // `handle_event` builds via `hex::encode(&asset.asset.policy)`.
                set.insert(p.as_str().to_string());
            }
        }
    });
}

/// Persist `INTERESTS` to state-kv. CBOR-encoded
/// `Vec<Interest>` — same wire shape `update_interest` accepts,
/// so re-hydration on `init` is symmetrical.
fn persist_interests() -> Result<(), String> {
    let bytes = INTERESTS.with(|i| {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(i.borrow().as_slice(), &mut buf)
            .map(|_| buf)
            .map_err(|e| format!("encode interests: {e}"))
    })?;
    state_kv::set_value(INTEREST_STATE_KV_KEY, &bytes);
    Ok(())
}

/// CBOR-byte equality for two `Interest` values. The `Interest`
/// type doesn't impl `Eq` (its selectors carry types like
/// `EnumSet` / `Option<PolicyId>` whose equality is shape-aware
/// but not derived publicly), so for dedup we fall back to
/// re-encode-and-compare. Cheap enough for v1's expected
/// interest-set sizes (tens to thousands).
fn cbor_eq(a: &Interest, b: &Interest) -> bool {
    let mut buf_a = Vec::new();
    let mut buf_b = Vec::new();
    let _ = ciborium::ser::into_writer(a, &mut buf_a);
    let _ = ciborium::ser::into_writer(b, &mut buf_b);
    buf_a == buf_b
}

/// `Add` op dedup helper. Same shape as `cbor_eq` but takes a
/// slice + candidate so the borrow doesn't tangle with the
/// `RefMut` we're holding.
fn contains_equivalent(existing: &[Interest], item: &Interest) -> bool {
    existing.iter().any(|e| cbor_eq(e, item))
}

/// Restore `INTERESTS` from state-kv (if present). Idempotent;
/// safe to call from `init` without checking for prior state.
fn restore_interests() {
    let Some(bytes) = state_kv::get_value(INTEREST_STATE_KV_KEY) else {
        return;
    };
    let restored: Vec<Interest> = match ciborium::de::from_reader(bytes.as_slice()) {
        Ok(v) => v,
        Err(e) => {
            let _ = mitos::platform::logging::log(
                LogLevel::Warn,
                "ownership-indexer-module",
                &format!("restore_interests: decode failed, dropping persisted set: {e}"),
            );
            return;
        }
    };
    INTERESTS.with(|i| *i.borrow_mut() = restored);
    rebuild_watched();
}

struct Module;

impl Guest for Module {
    fn module_version() -> (u32, u32) {
        (1, 0)
    }

    fn trap_policy() -> (TrapStrategy, RetryPolicy) {
        // Idempotent: each block's emissions are deterministic
        // functions of the produced outputs, so replaying a
        // block produces the same events. Replay is safe.
        (
            TrapStrategy::Replay,
            RetryPolicy {
                max_retries: 3,
                backoff_cap_ms: 1_000,
            },
        )
    }

    fn init(config: Vec<u8>) {
        // Pull persisted interest set first — restores whatever
        // companion-driven `update_interest` calls have asserted
        // since the last cold start. This runs before the static
        // bootstrap below so an empty companion-driven set on
        // first deploy still falls through to `policies = [...]`.
        restore_interests();

        // Empty config bytes = no-op default. Useful for tests
        // that just want to validate the dispatch loop without
        // wiring real policies.
        if config.is_empty() {
            return;
        }
        let cfg: Config = match ciborium::de::from_reader(config.as_slice()) {
            Ok(c) => c,
            Err(e) => {
                let _ = mitos::platform::logging::log(
                    LogLevel::Error,
                    "ownership-indexer-module",
                    &format!("init: failed to decode CBOR config: {e}"),
                );
                return;
            }
        };
        // `mitos.toml`'s `policies = [...]` is a bootstrap-only
        // default. If the persisted interest set already has
        // entries (companion has driven interest at least once),
        // skip the bootstrap — the canonical source is whatever
        // the companion last asserted, not whatever was in the
        // module's deploy config.
        let persisted_empty = INTERESTS.with(|i| i.borrow().is_empty());
        if !persisted_empty {
            return;
        }
        WATCHED.with(|w| {
            let mut set = w.borrow_mut();
            for p in cfg.policies {
                set.insert(p);
            }
        });
    }

    fn update_interest(op: InterestOp, items_cbor: Vec<u8>) -> Result<(), String> {
        let incoming: Vec<Interest> = ciborium::de::from_reader(items_cbor.as_slice())
            .map_err(|e| format!("decode interest items: {e}"))?;

        match op {
            InterestOp::Replace => {
                INTERESTS.with(|i| *i.borrow_mut() = incoming);
            }
            InterestOp::Add => {
                INTERESTS.with(|i| {
                    let mut existing = i.borrow_mut();
                    for item in incoming {
                        // Deduplicate — Interest doesn't impl Eq
                        // (selectors carry typed enums); fall back
                        // to CBOR-byte comparison via re-encode.
                        if !contains_equivalent(&existing, &item) {
                            existing.push(item);
                        }
                    }
                });
            }
            InterestOp::Remove => {
                INTERESTS.with(|i| {
                    let mut existing = i.borrow_mut();
                    for item in &incoming {
                        existing.retain(|e| !cbor_eq(e, item));
                    }
                });
            }
        }

        rebuild_watched();
        persist_interests()?;

        let watched_count = WATCHED.with(|w| w.borrow().len());
        let _ = mitos::platform::logging::log(
            LogLevel::Info,
            "ownership-indexer-module",
            &format!(
                "update_interest op={op:?} watched_policies_now={watched_count}"
            ),
        );
        Ok(())
    }

    fn handle_event(_channel: u32, block: &ResolvedBlock) {
        let watched_empty = WATCHED.with(|w| w.borrow().is_empty());
        if watched_empty {
            return;
        }

        let tx_count = block.tx_count();
        for tx_idx in 0..tx_count {
            let tx_hash_bytes = block.tx_hash(tx_idx);
            let tx_hash_hex = hex::encode(&tx_hash_bytes);

            let output_count = block.output_count(tx_idx);
            for output_idx in 0..output_count {
                let output = block.get_output(tx_idx, output_idx);
                if output.assets.is_empty() {
                    continue;
                }
                for asset in &output.assets {
                    let policy_hex = hex::encode(&asset.asset.policy);
                    let watched = WATCHED.with(|w| w.borrow().contains(&policy_hex));
                    if !watched {
                        continue;
                    }
                    let asset_name_hex = hex::encode(&asset.asset.name);
                    let event = OwnershipChange::Transfer {
                        policy_id: policy_hex,
                        asset_name: asset_name_hex,
                        new_owner: output.address.clone(),
                        tx_hash: tx_hash_hex.clone(),
                        output_index: output_idx,
                    };
                    let mut buf = Vec::new();
                    if let Err(e) = ciborium::ser::into_writer(&event, &mut buf) {
                        let _ = mitos::platform::logging::log(
                            LogLevel::Warn,
                            "ownership-indexer-module",
                            &format!("emit serialise failed: {e}"),
                        );
                        continue;
                    }
                    // INFO-log every emission so it surfaces in the
                    // host's journal — visible during early-deploy
                    // observation. Modules wired to a real CF
                    // subscription will drop this once the consumer
                    // pipeline is in place.
                    if let OwnershipChange::Transfer {
                        policy_id,
                        asset_name,
                        new_owner,
                        ..
                    } = &event
                    {
                        let _ = mitos::platform::logging::log(
                            LogLevel::Info,
                            "ownership-indexer-module",
                            &format!(
                                "emit Transfer policy={} asset={} owner={}",
                                policy_id, asset_name, new_owner,
                            ),
                        );
                    }
                    mitos::platform::emit::emit_event(0, &buf);
                }
            }
        }
    }
}

export!(Module);
