//! Standard-burn community module — detects token destruction
//! via negative `tx.mint` entries for watched policies.
//!
//! v2 ABI model: the platform filters `Minted` events host-side
//! against the module's `holds-policy` predicates and delivers
//! only matching events to `handle-events`. The module's job is
//! a simple shape transform: `MintedEvent { quantity_delta < 0 }`
//! → `standard_burn::Burn { quantity_burned }` (positive).
//!
//! Interest is **dynamic** — the module ships with an empty
//! interest set; the consumer's companion calls
//! `/api/_interest/standard-burn/subscribe` with `kind = "policy"`
//! to add each policy it wants to track. The platform's
//! `update-interest` path delivers the new predicate to the
//! running module + invokes `bootstrap_one_predicate` to
//! hydrate current state.
//!
//! **Bootstrap caveat:** bootstrap synthesises `Produced` events
//! for current UTxOs holding the watched policy — it does *not*
//! replay historical `Minted` events. A consumer subscribing
//! late only receives burn events from the moment of
//! subscription forward. Historical burns must be backfilled
//! out-of-band (data-plane query, external indexer).
//!
//! Architecture: see `mitos/docs/design/MINT_BURN_MODULES.md`.
//! v2 platform docs: `mitos/docs/strategy/MITOS_PLATFORM_V2.md`.

use mitos_community_events::standard_burn::Burn;

use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
// `DispatchEvent`, `TrapStrategy`, `RetryPolicy`, `InterestOp`,
// and the `Guest` trait come from the world-level `use ...`
// clauses and are addressable at the top of this module's scope
// without explicit imports.
use crate::mitos::platform_v2::types::{MintedEvent, UtxoEvent};

const LOG_TARGET: &str = "standard-burn-module";

/// Encode + emit a `Burn` event on channel 0 (single-channel
/// module). Failure to encode the typed event is treated as a
/// soft error — logged but not trapped, so a single bad row
/// doesn't quarantine the module.
fn emit_burn(event: &Burn) {
    let mut buf = Vec::with_capacity(128);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode burn event failed: {e}"),
        );
        return;
    }
    emit::emit_event(0, &buf);
}

fn handle_minted(m: &MintedEvent) {
    // Platform pre-filters by `holds-policy` so we trust the
    // delivered event matches a watched policy. We only emit
    // for negative deltas (= burns); positive deltas are
    // someone else's concern (e.g. `cip-25-mint` / `cip-68-mint`).
    if m.quantity_delta >= 0 {
        return;
    }
    let quantity_burned = match u64::try_from(m.quantity_delta.unsigned_abs()) {
        Ok(q) => q,
        Err(_) => {
            // i64::MIN edge case — unsigned_abs returns u64
            // so this branch is actually unreachable, but
            // surface it loudly if the WIT widens to i128.
            logging::log(
                LogLevel::Error,
                LOG_TARGET,
                &format!(
                    "burn quantity overflow: delta={} (skipping)",
                    m.quantity_delta
                ),
            );
            return;
        }
    };
    emit_burn(&Burn {
        policy: hex::encode(&m.policy),
        asset_name_hex: hex::encode(&m.asset_name),
        tx_hash: hex::encode(&m.tx_hash),
        quantity_burned,
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
        // No persistent config — empty interest set, populated
        // entirely via `update-interest` at runtime.
    }

    fn handle_events(events: Vec<DispatchEvent>) {
        for event in events {
            if let DispatchEvent::Utxo(UtxoEvent::Minted(m)) = event {
                handle_minted(&m);
            }
            // Other variants ignored — we only care about mint
            // entries with negative quantity for watched
            // policies. The platform delivers Produced / Consumed
            // for matching TXs too but we have no use for them.
        }
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        // Filter application happens host-side via the platform's
        // dispatch composer. The module is a pure shape-transform;
        // no module-side state is keyed by the interest set.
        Ok(())
    }

    /// No-op: event-driven modules are refilled host-side by
    /// `run_bootstrap` over the manifest `[interest]`. See the
    /// `rebootstrap` export in `wit-v2/world.wit`.
    fn rebootstrap() -> Result<(), String> {
        Ok(())
    }
}

export!(Module);
