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

use std::cell::RefCell;
use std::collections::HashSet;

use mitos_community_events::burn_address::AddressBurn;
use serde::Deserialize;

use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
// `DispatchEvent`, `TrapStrategy`, `RetryPolicy`, `InterestOp`,
// and the `Guest` trait come from world-level `use ...` clauses.
use crate::mitos::platform_v2::types::{ProducedEvent, UtxoEvent};

const LOG_TARGET: &str = "burn-address-module";

// Watched-address set. The platform's v2 dispatch model
// filters TXs (any matching event qualifies → all events
// dispatched), NOT per-output. So a TX that touches *any*
// watched address also delivers Produced events for the
// other (change, fee) outputs in the same TX. We have to
// filter per-output ourselves to avoid emitting AddressBurn
// for the user's own wallet change.
//
// Wire format from `update-interest` mirrors what the platform
// host encodes: `ciborium`-serialised
// `Vec<mitos_data_plane::InterestPredicate>` with serde's
// default external tagging. We define a local minimal copy
// of the discriminator so we can decode without pulling in
// the full data-plane crate dep.
thread_local! {
    static WATCHED_ADDRS: RefCell<HashSet<String>> = RefCell::new(HashSet::new());
}

/// Minimal mirror of the on-wire `InterestPredicate` enum.
/// We only care about `AtAddress`; the other variants get
/// captured for shape-completeness so the deserialiser can
/// step over them without erroring on unrecognised tags.
#[derive(Debug, Deserialize)]
enum InterestPredicateWire {
    AtAddress(String),
    AtStakeCred(serde::de::IgnoredAny),
    HoldsPolicy(serde::de::IgnoredAny),
    HoldsAsset {
        #[allow(dead_code)]
        policy: serde::de::IgnoredAny,
        #[allow(dead_code)]
        asset_name: serde::de::IgnoredAny,
    },
    TickEvery(u32),
}

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
    // Per-output filter: platform dispatches every Produced
    // event in any TX that touched a watched address, NOT just
    // outputs at the watched address. We bounce non-matching
    // outputs here so change/fee outputs don't leak into the
    // emit channel as false-positive burns.
    let is_watched = WATCHED_ADDRS.with(|set| set.borrow().contains(&p.output.address));
    if !is_watched {
        return;
    }
    if p.output.assets.is_empty() {
        // Pure-ADA output landed at a watched address. Nothing
        // to emit (lovelace isn't "burned" by being sent to an
        // address).
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

/// Decode the CBOR-encoded interest predicates and update the
/// watched-address set per the op. Silent best-effort: unknown
/// variants and decode errors leave the set unchanged.
fn apply_interest_update(op: InterestOp, items_cbor: &[u8]) {
    let predicates: Vec<InterestPredicateWire> =
        match ciborium::de::from_reader(items_cbor) {
            Ok(v) => v,
            Err(e) => {
                logging::log(
                    LogLevel::Error,
                    LOG_TARGET,
                    &format!("decode interest predicates failed: {e}"),
                );
                return;
            }
        };
    let addresses: Vec<String> = predicates
        .into_iter()
        .filter_map(|p| match p {
            InterestPredicateWire::AtAddress(a) => Some(a),
            _ => None,
        })
        .collect();
    WATCHED_ADDRS.with(|set| {
        let mut set = set.borrow_mut();
        match op {
            InterestOp::Replace => {
                set.clear();
                set.extend(addresses);
            }
            InterestOp::Add => {
                set.extend(addresses);
            }
            InterestOp::Remove => {
                for a in &addresses {
                    set.remove(a);
                }
            }
        }
        logging::log(
            LogLevel::Info,
            LOG_TARGET,
            &format!("interest update applied; now watching {} address(es)", set.len()),
        );
    });
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

    fn update_interest(op: InterestOp, items_cbor: Vec<u8>) -> Result<(), String> {
        apply_interest_update(op, &items_cbor);
        Ok(())
    }
}

export!(Module);
