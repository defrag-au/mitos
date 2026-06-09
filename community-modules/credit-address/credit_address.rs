//! Credit-address community module — emits an `AddressCredit`
//! event for every output (ADA + any native assets) landing at
//! one of the consumer's watched addresses.
//!
//! The mirror of `burn-address`: that reports assets sent to a
//! sink address; this reports the whole inbound credit (lovelace
//! + assets) at ANY watched address, and stays dumb about INTENT
//! — a credit may be a buyer payment, a fund top-up, change, or
//! junk; classifying it is the consumer's job.
//!
//! Interest is **dynamic addresses**. The consumer's companion
//! calls `/api/_interest/credit-address/subscribe` with
//! `kind = "address"` and the bech32 of each address it wants to
//! watch (e.g. a per-tenant receiving address created at runtime —
//! a mint engine's per-collection deposit address). The platform's
//! `at-address` predicate filters `Produced` events to the module;
//! the module receives only outputs at watched addresses + the
//! bootstrap walk synthesises events for current unspent UTxOs at
//! each newly-added address (the catch-up backfill).
//!
//! One event per OUTPUT landing at a watched address (NOT per
//! asset): the output's `lovelace` plus its list of native assets.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use mitos_community_events::credit_address::{AddressCredit, CreditedAsset};
use mitos_module_kit::ReentrantRound;
use serde::Deserialize;

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::state_kv;
// `DispatchEvent`, `TrapStrategy`, `RetryPolicy`, `InterestOp`,
// and the `Guest` trait come from world-level `use ...` clauses.
use crate::mitos::platform_v2::types::{
    AssetEntry, ChainPoint, OutputRef as WitOutputRef, ProducedEvent, UtxoEvent,
};

const LOG_TARGET: &str = "credit-address-module";
/// state-kv key under which we persist the resolved
/// watched-address set (one CBOR-encoded list of strings).
const KV_WATCHED_ADDRS: &str = "watched-addresses";
/// state-kv key for the re-entrant `rebootstrap` continuation
/// cursor (the index of the watched address being re-scanned).
const KV_REBOOTSTRAP_CURSOR: &str = "rebootstrap-cursor";
/// Per-page hint for the cold-start scan (`utxos_by_address` is
/// paged; the host clamps each page to its own budget).
const COLD_START_PAGE_HINT: u32 = 10_000;

// Watched-address set. The platform's v2 dispatch model filters
// TXs (any matching event qualifies → all events dispatched), NOT
// per-output. So a TX that touches *any* watched address also
// delivers Produced events for the other (change, fee) outputs in
// the same TX. We filter per-output ourselves to avoid emitting an
// `AddressCredit` for outputs that didn't land at a watched address.
//
// Wire format from `update-interest` mirrors what the platform host
// encodes: `ciborium`-serialised `Vec<InterestPredicate>` with
// serde's default external tagging. We define a local minimal copy
// of the discriminator so we can decode without the data-plane dep.
thread_local! {
    static WATCHED_ADDRS: RefCell<HashSet<String>> = RefCell::new(HashSet::new());

    /// In-flight `rebootstrap` round. `None` between rounds.
    /// `ReentrantRound` owns the address list, `predicate_idx`, and
    /// page cursor; accumulator `()` (each page emits directly).
    static REBOOTSTRAP_STATE: RefCell<Option<ReentrantRound<String, ()>>> = RefCell::new(None);
}

/// Minimal mirror of the on-wire `InterestPredicate` enum. We only
/// care about `AtAddress`; the others are captured for
/// shape-completeness so the deserialiser steps over them.
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

/// Slot carried by a dispatch cursor / tx record. `Origin` (genesis
/// pre-state) has no slot — reported as 0.
fn slot_of(cursor: &ChainPoint) -> u64 {
    match cursor {
        ChainPoint::SlotOnly(s) => *s,
        ChainPoint::Specific(p) => p.slot,
        ChainPoint::Origin => 0,
    }
}

/// The payer of a tx: the resolved address of its largest-lovelace
/// input. `prior_outputs` yields `(address, lovelace)` for each
/// consumed input; unresolved inputs (empty address — a prior output
/// pruned past the archive horizon) are skipped. `None` when no input
/// is resolvable, so the caller can fall back or skip rather than emit
/// a payer-less credit.
fn payer_of<'a>(prior_outputs: impl Iterator<Item = (&'a str, u64)>) -> Option<String> {
    prior_outputs
        .filter(|(addr, _)| !addr.is_empty())
        .max_by_key(|(_, lovelace)| *lovelace)
        .map(|(addr, _)| addr.to_string())
}

fn emit_address_credit(event: &AddressCredit) {
    let mut buf = Vec::with_capacity(256);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode AddressCredit failed: {e}"),
        );
        return;
    }
    emit::emit_event(0, &buf);
}

/// Emit one `AddressCredit` for an output that landed at a watched
/// address — its total lovelace plus every native asset it carries.
/// Shared by the live `Produced` path and the cold-start walk so
/// both emit an identical wire shape.
#[allow(clippy::too_many_arguments)]
fn emit_output_credit(
    address: &str,
    tx_hash_hex: &str,
    output_index: u32,
    lovelace: u64,
    from_address: &str,
    slot: u64,
    metadata: Option<Vec<u8>>,
    assets: &[AssetEntry],
) {
    let credited: Vec<CreditedAsset> = assets
        .iter()
        .map(|entry| CreditedAsset {
            policy: hex::encode(&entry.asset.policy),
            asset_name_hex: hex::encode(&entry.asset.name),
            quantity: entry.quantity,
        })
        .collect();
    emit_address_credit(&AddressCredit {
        address: address.to_string(),
        tx_hash: tx_hash_hex.to_string(),
        output_index,
        lovelace,
        from_address: from_address.to_string(),
        slot,
        metadata,
        assets: credited,
    });
}

fn handle_produced(p: &ProducedEvent, payer_by_tx: &HashMap<Vec<u8>, (u64, String)>) {
    // Per-output filter: the platform dispatches every Produced
    // event in any TX that touched a watched address, NOT just
    // outputs at the watched address. Bounce non-matching outputs
    // (change/fee/other-recipient) so they don't leak as credits.
    let is_watched = WATCHED_ADDRS.with(|set| set.borrow().contains(&p.output.address));
    if !is_watched {
        return;
    }
    // One `read_tx` for this tx — its source of the tx metadata (`aux_data`, read
    // from the block via dolos; NO Maestro) and the payer fallback if the
    // in-batch consumed inputs didn't resolve one. Prefer the dispatched consumed
    // inputs for the payer (no extra work); only consult the record's inputs when
    // they didn't resolve.
    let record = chain_data::read_tx(&p.tx_hash);
    let from_address = match payer_by_tx.get(&p.tx_hash) {
        Some((_, addr)) if !addr.is_empty() => addr.clone(),
        _ => {
            let resolved = record.as_ref().and_then(|r| {
                payer_of(
                    r.inputs
                        .iter()
                        .map(|i| (i.prior_output.address.as_str(), i.prior_output.lovelace)),
                )
            });
            match resolved {
                Some(addr) => addr,
                None => {
                    logging::log(
                        LogLevel::Warn,
                        LOG_TARGET,
                        &format!(
                            "credit at {} in {}: unresolvable payer — skipping (backstop will catch it)",
                            p.output.address,
                            hex::encode(&p.tx_hash)
                        ),
                    );
                    return;
                }
            }
        }
    };
    let metadata = record.and_then(|r| r.aux_data);
    // Emit EVERY credit, including pure-ADA — unlike burn-address we
    // do NOT skip asset-less outputs: a pure-ADA payment is the
    // common case here. The consumer classifies intent.
    emit_output_credit(
        &p.output.address,
        &hex::encode(&p.tx_hash),
        p.oref.index,
        p.output.lovelace,
        &from_address,
        slot_of(&p.cursor),
        metadata,
        &p.output.assets,
    );
}

/// Bootstrap walk for a newly-watched address: enumerate its
/// current unspent set (`utxos_by_address`), resolve each output,
/// and emit one `AddressCredit` per output.
///
/// For an address whose credits get respent (e.g. a deposit address
/// that's swept), the current unspent set is the **un-processed**
/// inflow — exactly the catch-up a fresh consumer wants (credits it
/// hasn't acted on yet). Idempotent: re-running re-emits the same
/// events, so consumers MUST dedup on `(tx_hash, output_index)`.
fn process_address_page(addr: &str, refs: &[WitOutputRef]) -> Option<usize> {
    let mut credit_events = 0usize;
    // `read_utxos` returns each output paired with its own ref — not
    // in `refs` order, so the ref comes from the tuple.
    for (oref, output) in chain_data::read_utxos(refs) {
        // The payer + metadata come from the CREATING tx (`oref.tx_hash` produced
        // this unspent deposit). No live block context here, so `read_tx` it from
        // dolos (the block carries the inputs + aux_data — no Maestro). Skip —
        // leaving it to the consumer's backstop — if the creating tx is unavailable
        // (pruned past horizon) or has no resolvable input.
        let Some(record) = chain_data::read_tx(&oref.tx_hash) else {
            logging::log(
                LogLevel::Warn,
                LOG_TARGET,
                &format!(
                    "cold-start {addr}: no tx record for {} — skipping",
                    hex::encode(&oref.tx_hash)
                ),
            );
            continue;
        };
        let Some(from_address) = payer_of(
            record
                .inputs
                .iter()
                .map(|i| (i.prior_output.address.as_str(), i.prior_output.lovelace)),
        ) else {
            logging::log(
                LogLevel::Warn,
                LOG_TARGET,
                &format!(
                    "cold-start {addr}: unresolvable payer for {} — skipping",
                    hex::encode(&oref.tx_hash)
                ),
            );
            continue;
        };
        let slot = slot_of(&record.cursor);
        let metadata = record.aux_data;
        emit_output_credit(
            addr,
            &hex::encode(&oref.tx_hash),
            oref.index,
            output.lovelace,
            &from_address,
            slot,
            metadata,
            &output.assets,
        );
        credit_events += 1;
    }
    Some(credit_events)
}

fn cold_start_address(addr: &str) {
    // Paged walk: each `utxos_by_address` call returns one
    // host-clamped page plus a continuation token. Only one page of
    // refs + its resolved outputs is ever resident.
    let mut credit_events = 0usize;
    let mut total_utxos = 0usize;
    let mut after: Option<Vec<u8>> = None;

    loop {
        let page = chain_data::utxos_by_address(addr, after.as_deref(), COLD_START_PAGE_HINT);
        total_utxos += page.refs.len();
        match process_address_page(addr, &page.refs) {
            Some(n) => credit_events += n,
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
        &format!("cold-start address={addr}: {total_utxos} UTxO(s) → {credit_events} credit event(s)"),
    );
}

/// Decode the CBOR-encoded interest predicates and update the
/// watched-address set per the op. Silent best-effort. Always
/// flushes the updated set to state-kv so a host restart (without
/// an attached companion to re-send Interest) keeps filtering.
fn apply_interest_update(op: InterestOp, items_cbor: &[u8]) {
    let predicates: Vec<InterestPredicateWire> = match ciborium::de::from_reader(items_cbor) {
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
    // Track addresses *new* to the watched set so the bootstrap walk
    // runs only for those — re-asserting an already-watched address
    // (companion reconnect) must not re-scan.
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

    // Bootstrap each newly-watched address: synthesise credit events
    // for its current unspent UTxOs so a fresh consumer catches up
    // on inflow it hasn't acted on yet.
    for addr in &added {
        cold_start_address(addr);
    }
}

/// Serialise the watched-address set as CBOR and write it to
/// state-kv. Cheap (single key); skipped silently on encode failure.
fn persist_watched_addrs() {
    let addrs: Vec<String> = WATCHED_ADDRS.with(|set| set.borrow().iter().cloned().collect());
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
/// No-op when absent (first start, or the companion will send
/// Interest before the first dispatch).
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
        // restarted without an attached companion to re-send Interest.
        restore_watched_addrs_from_kv();
    }

    fn handle_events(events: Vec<DispatchEvent>) {
        // Pass 1: per spending-TX, find the largest-lovelace consumed
        // input — the payer. Keyed on `consuming_tx_hash` so a
        // multi-TX batch resolves each TX's payer independently.
        // Reading Consumed here is for payer attribution only; we
        // still emit CREDITS for inbound outputs only (a bidirectional
        // "movement" module would also emit on the spend side).
        let mut payer_by_tx: HashMap<Vec<u8>, (u64, String)> = HashMap::new();
        for event in &events {
            if let DispatchEvent::Utxo(UtxoEvent::Consumed(c)) = event {
                if c.prior_output.address.is_empty() {
                    continue;
                }
                let best = payer_by_tx
                    .entry(c.consuming_tx_hash.clone())
                    .or_insert((0, String::new()));
                if c.prior_output.lovelace > best.0 {
                    *best = (c.prior_output.lovelace, c.prior_output.address.clone());
                }
            }
        }
        // Pass 2: emit a credit for each watched produced output,
        // tagged with its TX's payer + slot.
        for event in &events {
            if let DispatchEvent::Utxo(UtxoEvent::Produced(p)) = event {
                handle_produced(p, &payer_by_tx);
            }
        }
    }

    fn update_interest(op: InterestOp, items_cbor: Vec<u8>) -> Result<(), String> {
        apply_interest_update(op, &items_cbor);
        Ok(())
    }

    /// Re-emit credit events for watched addresses — **one bounded
    /// page per call**. The host loops, refuelling each call, until a
    /// step comes back `done`.
    ///
    /// Round state (address list + page cursor) is thread-local; the
    /// durable cursor in `state-kv` is only the `predicate_idx`, so a
    /// trap or host restart restarts the current address from page 0.
    /// Safe — credits are idempotent (consumers dedup on
    /// `(tx_hash, output_index)`), and re-walking the current unspent
    /// set re-emits the same events.
    ///
    /// `init` restores `WATCHED_ADDRS` from `state-kv`, so the module
    /// knows what it watches.
    fn rebootstrap(_mode: RebootstrapMode) -> Result<RebootstrapStep, String> {
        REBOOTSTRAP_STATE.with(|cell| {
            let mut state = cell.borrow_mut();

            // First call of a round (or the thread-local was wiped by
            // a trap/restart) — rebuild round state. The address list
            // is sorted so the durable `predicate_idx` cursor is
            // stable across a host restart.
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
            let page = chain_data::utxos_by_address(&addr, round.after(), COLD_START_PAGE_HINT);
            let ingested = page.refs.len() as u64;

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
                    // Address fully walked (or aborted) — advance the
                    // durable cursor to the next address.
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
