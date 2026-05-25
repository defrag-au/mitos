//! CIP-25 mint community module — emits `Cip25Mint` events for
//! positive `tx.mint` entries on watched policies whose TX
//! carries label-721 metadata for the asset.
//!
//! ## Decode path
//!
//! 1. Receive `MintedEvent` from the platform's pre-filtered
//!    stream (filtered by `holds-policy` for the consumer's
//!    watched policies).
//! 2. If `quantity_delta <= 0`, skip — burns are
//!    `standard-burn`'s concern.
//! 3. Call `chain-data::tx-metadata(tx_hash)` once per matching
//!    TX to retrieve the aux-data CBOR bytes.
//! 4. Decode the asset's label-721 entry via the shared
//!    `cardano_assets::cip25` decoder — the same decoder the
//!    `collection-metadata` CIP-25 facade uses, so both modules
//!    emit byte-identical `metadata_json`.
//!
//! TX-metadata calls per TX are batched implicitly — multiple
//! `MintedEvent`s for the same `tx_hash` would each trigger one
//! call. For v1 we accept the redundant calls; the data plane
//! lookup is in-process redb so cost is bounded. Future
//! optimisation: cache the aux-data within a `handle-events`
//! invocation since events for one TX arrive together.
//!
//! ## Interest
//!
//! Empty default — companion declares policies via
//! `/api/_interest/cip-25-mint/subscribe` (kind = "policy").
//! Platform's bootstrap synthesises `Produced` events for
//! current UTxOs holding the policy; this module ignores them
//! (mint detection is `Minted`-event-driven, not state-driven —
//! see the bootstrap caveat in `MINT_BURN_MODULES.md`).
//!
//! See `mitos/docs/design/MINT_BURN_MODULES.md`.

use std::cell::RefCell;
use std::collections::HashMap;

use cardano_assets::cip25::cip25_metadata_json;
use mitos_community_events::cip25_mint::Cip25Mint;

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{MintedEvent, UtxoEvent};

const LOG_TARGET: &str = "cip-25-mint-module";

// Per-handler aux-data cache: `Minted` events for one TX
// arrive in one `handle-events` call, so caching by tx_hash
// across that call avoids redundant `tx-metadata` lookups
// when a TX mints multiple assets.
thread_local! {
    static AUX_CACHE: RefCell<HashMap<Vec<u8>, Option<Vec<u8>>>> = RefCell::new(HashMap::new());
}

fn lookup_aux_data(tx_hash: &[u8]) -> Option<Vec<u8>> {
    AUX_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        if let Some(cached) = c.get(tx_hash) {
            return cached.clone();
        }
        let fetched = chain_data::tx_metadata(&tx_hash.to_vec());
        c.insert(tx_hash.to_vec(), fetched.clone());
        fetched
    })
}

fn emit_cip25_mint(event: &Cip25Mint) {
    let mut buf = Vec::with_capacity(512);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode Cip25Mint failed: {e}"),
        );
        return;
    }
    // Key by policy hex so the host's SB6b interest filter fans this
    // mint only to companions watching this policy (a keyless emission
    // is broadcast to every companion — cross-policy pollution).
    emit::emit_event_keyed(0, event.policy.as_bytes(), &buf);
}

fn handle_minted(m: &MintedEvent) {
    // Skip burns (those are `standard-burn`'s concern).
    if m.quantity_delta <= 0 {
        return;
    }
    let quantity = match u64::try_from(m.quantity_delta) {
        Ok(q) => q,
        Err(_) => {
            logging::log(
                LogLevel::Error,
                LOG_TARGET,
                &format!("mint quantity overflow: delta={}", m.quantity_delta),
            );
            return;
        }
    };

    // Look up the TX's aux-data and decode label-721 for this asset
    // via the shared decoder. `None` means no metadata present — we
    // still emit the mint event with `metadata_json = None` so
    // consumers observe the mint and can choose whether to backfill
    // metadata out-of-band.
    let metadata_json = lookup_aux_data(&m.tx_hash)
        .as_deref()
        .and_then(|aux| cip25_metadata_json(aux, &m.policy, &m.asset_name));

    emit_cip25_mint(&Cip25Mint {
        policy: hex::encode(&m.policy),
        asset_name_hex: hex::encode(&m.asset_name),
        tx_hash: hex::encode(&m.tx_hash),
        quantity,
        metadata_json,
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
    }

    fn handle_events(events: Vec<DispatchEvent>) {
        // Clear the per-handler aux-data cache so we don't
        // retain entries across invocations (each call holds
        // one TX or a small batch).
        AUX_CACHE.with(|c| c.borrow_mut().clear());

        for event in events {
            if let DispatchEvent::Utxo(UtxoEvent::Minted(m)) = event {
                handle_minted(&m);
            }
        }
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        Ok(())
    }

    /// No-op: event-driven modules are refilled host-side by
    /// `run_bootstrap` over the manifest `[interest]`. See the
    /// `rebootstrap` export in `wit-v2/world.wit`. One call,
    /// immediately `done`.
    fn rebootstrap(_mode: RebootstrapMode) -> Result<RebootstrapStep, String> {
        Ok(RebootstrapStep {
            done: true,
            ingested: 0,
        })
    }
}

export!(Module);
