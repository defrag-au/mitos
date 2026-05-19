//! Test-fixture wasm module for `mitos-platform` integration
//! tests. Self-contained — no shared event-shape crate, only
//! crates.io deps — so the platform crate's tests can drive a
//! real wasm component without dragging cnft.dev-workers' wire-
//! format crates in.
//!
//! Behaviour: on every `Produced` event the host filter delivers
//! (one or more assets under a watched policy), emit one CBOR
//! event per asset on that output:
//!
//! ```ignore
//! { "policy_id": "<hex>", "tx_hash": "<hex>", "out": <u32> }
//! ```
//!
//! Tests assert the emission count + decoded shape against the
//! block fixture they hand the host. Per-policy filtering happens
//! host-side; the module just reports what it sees.

use serde::{Deserialize, Serialize};

use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::types::{ProducedEvent, UtxoEvent};

/// CBOR'd typed config. Tests pass an empty config (the watched
/// interest is declared on the manifest, not in init bytes).
#[derive(Debug, Clone, Default, Deserialize)]
struct Config {}

/// Shape the tests CBOR-decode + assert against. Field names
/// drive serde — keep them stable.
#[derive(Debug, Serialize)]
struct TestEvent {
    policy_id: String,
    tx_hash: String,
    out: u32,
}

struct Module;

impl Guest for Module {
    fn module_version() -> (u32, u32) {
        (2, 0)
    }

    fn trap_policy() -> (TrapStrategy, RetryPolicy) {
        (
            TrapStrategy::Replay,
            RetryPolicy {
                max_retries: 3,
                backoff_cap_ms: 1_000,
            },
        )
    }

    fn init(config: Vec<u8>) {
        // Decode for forward-compat with future test configs;
        // empty bytes parse cleanly to `Config::default()` so
        // tests that pass nothing don't error.
        if config.is_empty() {
            return;
        }
        let _: Config = match ciborium::de::from_reader(config.as_slice()) {
            Ok(c) => c,
            Err(_) => return,
        };
    }

    fn handle_events(events: Vec<DispatchEvent>) {
        for event in events {
            if let DispatchEvent::Utxo(UtxoEvent::Produced(p)) = event {
                handle_produced(&p);
            }
        }
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        // Manifest interest is the source of truth in tests; the
        // dynamic-update path isn't exercised here.
        Ok(())
    }

    fn rebootstrap() -> Result<u64, String> {
        // No-op: the test fixture's refill comes from the host's
        // manifest bootstrap. Present so the module satisfies the
        // `mitos-module-v2` world's `rebootstrap` export.
        Ok(0)
    }
}

fn handle_produced(p: &ProducedEvent) {
    for asset in &p.output.assets {
        let event = TestEvent {
            policy_id: hex::encode(&asset.asset.policy),
            tx_hash: hex::encode(&p.tx_hash),
            out: p.oref.index,
        };
        let mut buf = Vec::new();
        if ciborium::ser::into_writer(&event, &mut buf).is_err() {
            continue;
        }
        emit::emit_event(0, &buf);
    }
}

export!(Module);
