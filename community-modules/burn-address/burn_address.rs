//! Burn-address community module — emits an `AddressBurn`
//! event for every asset landing in an output at one of the
//! consumer's watched addresses.
//!
//! Interest is **dynamic addresses**. The consumer's companion
//! calls `/api/_interest/burn-address/subscribe` with
//! `kind = "address"` and the bech32 of each address it
//! considers a burn sink. The platform's `at-address` predicate
//! filters `Produced` events to the module; the module receives
//! only outputs at watched addresses + the bootstrap walk
//! synthesises events for current unspent UTxOs at each newly-
//! added address.
//!
//! One event per `(asset, output)` pair. An output that carries
//! 50 different assets to the burn address emits 50 events.
//!
//! See `mitos/docs/design/MINT_BURN_MODULES.md`.

use mitos_community_events::burn_address::AddressBurn;

use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
// `DispatchEvent`, `TrapStrategy`, `RetryPolicy`, `InterestOp`,
// and the `Guest` trait come from world-level `use ...` clauses.
use crate::mitos::platform_v2::types::{ProducedEvent, UtxoEvent};

const LOG_TARGET: &str = "burn-address-module";

fn emit_address_burn(event: &AddressBurn) {
    let mut buf = Vec::with_capacity(256);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode AddressBurn failed: {e}"),
        );
        return;
    }
    emit::emit_event(0, &buf);
}

fn handle_produced(p: &ProducedEvent) {
    // Platform pre-filters by `at-address` so we trust the
    // delivered event matches one of the watched addresses.
    // Emit one AddressBurn per asset entry in the output's
    // value (no lovelace event — lovelace is the native asset
    // and isn't "burned" by being sent to an address).
    if p.output.assets.is_empty() {
        // Pure-ADA output landed at a watched address. Nothing
        // to emit; log at debug for diagnosis.
        return;
    }
    let burn_address = p.output.address.clone();
    let tx_hash_hex = hex::encode(&p.tx_hash);
    for entry in &p.output.assets {
        emit_address_burn(&AddressBurn {
            policy: hex::encode(&entry.asset.policy),
            asset_name_hex: hex::encode(&entry.asset.name),
            tx_hash: tx_hash_hex.clone(),
            output_index: p.oref.index,
            quantity: entry.quantity,
            burn_address: burn_address.clone(),
        });
    }
}

// ============================================================
// v2 Guest impl
// ============================================================

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
        logging::log(
            LogLevel::Info,
            LOG_TARGET,
            &format!("init: enter (config_bytes={})", config.len()),
        );
        // No persistent config — interest set entirely dynamic.
    }

    fn handle_events(events: Vec<DispatchEvent>) {
        for event in events {
            if let DispatchEvent::Utxo(UtxoEvent::Produced(p)) = event {
                handle_produced(&p);
            }
            // Consumed at a burn address is informational only —
            // by definition a "burn address" output never gets
            // respent. If we ever see a Consumed for a watched
            // address it indicates the address wasn't actually
            // a burn sink (operator misconfiguration). Silently
            // ignore.
        }
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        // Filter application happens host-side; module is a pure
        // shape-transform from Produced → AddressBurn.
        Ok(())
    }
}

export!(Module);
