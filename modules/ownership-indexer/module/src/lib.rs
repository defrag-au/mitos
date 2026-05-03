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

use serde::{Deserialize, Serialize};

use crate::mitos::platform::logging::LogLevel;

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
    static WATCHED: RefCell<HashSet<String>> = RefCell::new(HashSet::new());
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
        WATCHED.with(|w| {
            let mut set = w.borrow_mut();
            for p in cfg.policies {
                set.insert(p);
            }
        });
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
                    mitos::platform::emit::emit_event(0, &buf);
                }
            }
        }
    }
}

export!(Module);
