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
use mitos_module_kit::ReentrantRound;
use serde::Deserialize;

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::state_kv;
// `DispatchEvent`, `TrapStrategy`, `RetryPolicy`, `InterestOp`,
// and the `Guest` trait come from world-level `use ...` clauses.
use crate::mitos::platform_v2::types::{
    AssetEntry, OutputRef as WitOutputRef, ProducedEvent, UtxoEvent,
};

const LOG_TARGET: &str = "burn-address-module";
/// state-kv key under which we persist the resolved
/// watched-address set. Keyed by a single string so the whole
/// set round-trips as one CBOR-encoded list of strings —
/// cheaper than per-address keys for the small sets typical
/// of burn-sink consumers.
const KV_WATCHED_ADDRS: &str = "watched-addresses";

/// state-kv key for the re-entrant `rebootstrap` continuation
/// cursor — the index of the watched address currently being
/// re-scanned (8 BE bytes). Durable so a host restart mid-round
/// resumes at the right address; per-page progress is
/// thread-local + volatile (a trap/restart restarts the current
/// address from page 0, safe because burns are idempotent —
/// consumers dedup on `(tx_hash, output_index)`).
const KV_REBOOTSTRAP_CURSOR: &str = "rebootstrap-cursor";
/// Per-page hint for the cold-start scan. `utxos_by_address` is
/// paged (`WASM_BUDGET_CHUNKING.md`); this is a generous upper
/// bound — the host clamps each page to its own adaptive
/// per-call budget, so the walk never holds more than one
/// clamped page of refs at once.
const COLD_START_PAGE_HINT: u32 = 10_000;

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

    /// In-flight `rebootstrap` round. `None` between rounds.
    /// `ReentrantRound` (from `mitos-module-kit`) owns the
    /// address list, `predicate_idx`, and page cursor. The
    /// accumulator type is `()` — burn-address has no resident
    /// accumulator, each page emits its `AddressBurn`s directly.
    /// Resident across the host's re-entrant call loop; a trap or
    /// host restart discards it and the round resumes from the
    /// durable state-kv cursor (`predicate_idx`).
    static REBOOTSTRAP_STATE: RefCell<Option<ReentrantRound<String, ()>>> = RefCell::new(None);
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

/// Emit one `AddressBurn` per asset carried by an output that
/// landed at a watched address. Shared by the live `Produced`
/// path and the cold-start walk so both emit an identical wire
/// shape.
fn emit_output_burns(
    burn_address: &str,
    tx_hash_hex: &str,
    output_index: u32,
    assets: &[AssetEntry],
) {
    for entry in assets {
        emit_address_burn(&AddressBurn {
            policy: hex::encode(&entry.asset.policy),
            asset_name_hex: hex::encode(&entry.asset.name),
            tx_hash: tx_hash_hex.to_string(),
            output_index,
            quantity: entry.quantity,
            burn_address: burn_address.to_string(),
        });
    }
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
    emit_output_burns(
        &p.output.address,
        &hex::encode(&p.tx_hash),
        p.oref.index,
        &p.output.assets,
    );
}

/// Bootstrap walk for a newly-watched burn address. Enumerates
/// the address's current unspent set (`utxos_by_address`),
/// resolves each output, and emits one `AddressBurn` per
/// `(asset, output)`.
///
/// Burn sinks are never respent, so the current unspent set is
/// the all-time inflow set — a consumer summing the distinct
/// `(tx_hash, output_index)` contributions arrives at the full
/// historical burn balance without needing a dedicated snapshot
/// event.
///
/// Idempotent by design: re-running the walk (address re-added,
/// or a host recapture) re-emits the same `AddressBurn`s, so
/// consumers MUST dedup on `(tx_hash, output_index)` rather than
/// blindly accumulating.
fn process_address_page(addr: &str, refs: &[WitOutputRef]) -> Option<usize> {
    let outputs = chain_data::read_utxos(refs);
    if outputs.len() != refs.len() {
        // `read_utxos` is positionally parallel to its input; a
        // length mismatch means we can't safely zip refs to
        // outputs. Signal abort rather than emit misattributed
        // burns.
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!(
                "cold-start address={addr}: read_utxos returned {} output(s) for {} ref(s); bootstrap walk aborted",
                outputs.len(),
                refs.len()
            ),
        );
        return None;
    }
    let mut burn_events = 0usize;
    for (oref, output) in refs.iter().zip(outputs.iter()) {
        if output.assets.is_empty() {
            continue;
        }
        emit_output_burns(addr, &hex::encode(&oref.tx_hash), oref.index, &output.assets);
        burn_events += output.assets.len();
    }
    Some(burn_events)
}

fn cold_start_address(addr: &str) {
    // Paged walk (`WASM_BUDGET_CHUNKING.md`): each
    // `utxos_by_address` call returns one host-clamped page plus
    // a continuation token. Only one page of refs + its resolved
    // outputs is ever resident. Used by the live `update_interest`
    // add path; the re-entrant `rebootstrap` path spreads the
    // same walk across fuel-budgeted calls.
    let mut burn_events = 0usize;
    let mut total_utxos = 0usize;
    let mut after: Option<Vec<u8>> = None;

    loop {
        let page = chain_data::utxos_by_address(addr, after.as_deref(), COLD_START_PAGE_HINT);
        total_utxos += page.refs.len();
        match process_address_page(addr, &page.refs) {
            Some(n) => burn_events += n,
            None => return,
        }
        match page.next {
            Some(token) => after = Some(token),
            None => break,
        }
    }
    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        &format!(
            "cold-start address={addr}: {total_utxos} UTxO(s) → {burn_events} burn event(s)"
        ),
    );
}

/// Decode the CBOR-encoded interest predicates and update the
/// watched-address set per the op. Silent best-effort: unknown
/// variants and decode errors leave the set unchanged. Always
/// flushes the updated set to state-kv so a host process
/// restart (without an attached companion to re-send Interest
/// messages) keeps filtering correctly.
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
    // Track addresses *new* to the watched set so the bootstrap
    // walk runs only for those — re-asserting an already-watched
    // address (companion reconnect) must not re-scan.
    let mut added: Vec<String> = Vec::new();
    let final_size = WATCHED_ADDRS.with(|set| {
        let mut set = set.borrow_mut();
        match op {
            InterestOp::Replace => {
                let prev = std::mem::take(&mut *set);
                set.extend(addresses.iter().cloned());
                for a in set.iter() {
                    if !prev.contains(a) {
                        added.push(a.clone());
                    }
                }
            }
            InterestOp::Add => {
                for a in addresses {
                    if set.insert(a.clone()) {
                        added.push(a);
                    }
                }
            }
            InterestOp::Remove => {
                for a in &addresses {
                    set.remove(a);
                }
            }
        }
        set.len()
    });
    persist_watched_addrs();
    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        &format!("interest update applied; now watching {final_size} address(es)"),
    );

    // Bootstrap each newly-watched address: synthesise burn
    // events for its current unspent UTxOs so a fresh consumer
    // sees the full historical balance, not just burns from now
    // on.
    for addr in &added {
        cold_start_address(addr);
    }
}

/// Serialise the current watched-address set as CBOR and write
/// it to state-kv. Cheap (small payload, single key); skipped
/// silently on encode failure since the in-memory set is still
/// the source of truth for this process.
fn persist_watched_addrs() {
    let addrs: Vec<String> =
        WATCHED_ADDRS.with(|set| set.borrow().iter().cloned().collect());
    let mut buf = Vec::with_capacity(64 + addrs.len() * 64);
    if let Err(e) = ciborium::ser::into_writer(&addrs, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode watched addresses for state-kv: {e}"),
        );
        return;
    }
    state_kv::set_value(KV_WATCHED_ADDRS, &buf);
}

/// Restore the watched-address set from state-kv at init time.
/// No-op when the key is absent (first-ever start, or the
/// companion will send Interest before the first dispatch).
fn restore_watched_addrs_from_kv() {
    let Some(bytes) = state_kv::get_value(KV_WATCHED_ADDRS) else {
        return;
    };
    let addrs: Vec<String> = match ciborium::de::from_reader(bytes.as_slice()) {
        Ok(v) => v,
        Err(e) => {
            logging::log(
                LogLevel::Error,
                LOG_TARGET,
                &format!("decode persisted watched addresses: {e}"),
            );
            return;
        }
    };
    let n = addrs.len();
    WATCHED_ADDRS.with(|set| {
        let mut set = set.borrow_mut();
        set.clear();
        set.extend(addrs);
    });
    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        &format!("restored {n} watched address(es) from state-kv"),
    );
}

// ============================================================
// Rebootstrap continuation cursor (durable predicate index)
// ============================================================

fn save_rebootstrap_cursor(predicate_idx: usize) {
    state_kv::set_value(KV_REBOOTSTRAP_CURSOR, &(predicate_idx as u64).to_be_bytes());
}

fn load_rebootstrap_cursor() -> usize {
    state_kv::get_value(KV_REBOOTSTRAP_CURSOR)
        .and_then(|b| <[u8; 8]>::try_from(b.as_slice()).ok())
        .map(|b| u64::from_be_bytes(b) as usize)
        .unwrap_or(0)
}

fn clear_rebootstrap_cursor() {
    state_kv::delete_value(KV_REBOOTSTRAP_CURSOR);
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
        // Rehydrate the watched-address set in case the host
        // restarted without an attached companion to re-send
        // Interest messages.
        restore_watched_addrs_from_kv();
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

    /// Re-emit burn events for watched addresses — **one bounded
    /// page per call** (`WASM_BUDGET_CHUNKING.md`). The host
    /// loops, refuelling each call, until a step comes back
    /// `done`; a page of UTxOs fits one fuel budget, a whole
    /// busy address does not.
    ///
    /// Round state (address list + page cursor) is thread-local;
    /// the durable cursor in `state-kv` is only the
    /// `predicate_idx`, so a trap or host restart restarts the
    /// current address from page 0. Safe — burns are idempotent
    /// (consumers dedup on `(tx_hash, output_index)`), and burn
    /// sinks are never respent so re-walking re-emits the same
    /// set.
    ///
    /// `init` restores `WATCHED_ADDRS` from `state-kv`, so the
    /// module knows what it watches.
    fn rebootstrap() -> Result<RebootstrapStep, String> {
        REBOOTSTRAP_STATE.with(|cell| {
            let mut state = cell.borrow_mut();

            // First call of a round (or the thread-local was
            // wiped by a trap/restart) — rebuild round state. The
            // address list is sorted so the durable `predicate_idx`
            // cursor is stable across a host restart.
            if state.is_none() {
                let mut addresses: Vec<String> =
                    WATCHED_ADDRS.with(|s| s.borrow().iter().cloned().collect());
                addresses.sort_unstable();
                *state = Some(ReentrantRound::resume(addresses, load_rebootstrap_cursor()));
            }
            let round = state.as_mut().expect("round initialised above");

            // No addresses left — round done.
            let Some(addr) = round.current().cloned() else {
                clear_rebootstrap_cursor();
                *state = None;
                return Ok(RebootstrapStep {
                    done: true,
                    ingested: 0,
                });
            };

            // Process exactly one page of the current address.
            let page =
                chain_data::utxos_by_address(&addr, round.after(), COLD_START_PAGE_HINT);
            let ingested = page.refs.len() as u64;

            // A `read_utxos` length mismatch aborts this address —
            // skip the rest of its pages, advance to the next.
            let page_ok = process_address_page(&addr, &page.refs).is_some();

            match page.next {
                Some(token) if page_ok => {
                    // More pages for this address — keep the round.
                    round.page_more(ingested, token);
                    Ok(RebootstrapStep {
                        done: false,
                        ingested,
                    })
                }
                _ => {
                    // Address fully walked (or aborted) — advance
                    // the durable cursor to the next address.
                    round.page_last(ingested);
                    let adv = round.finish_predicate();
                    if adv.round_done {
                        clear_rebootstrap_cursor();
                        *state = None;
                    } else {
                        save_rebootstrap_cursor(adv.predicate_idx);
                    }
                    Ok(RebootstrapStep {
                        done: adv.round_done,
                        ingested,
                    })
                }
            }
        })
    }
}

export!(Module);
