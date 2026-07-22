//! jpg.store listing lifecycle community module.
//!
//! Emits `JpgStoreListing::{Create, Update, Unlisting}` for the jpg.store sale
//! contracts (V1/V2/V3/V4). Sale events (asset moving to buyer + payment
//! outputs to seller) live in the sibling `jpg-store-sale` module.
//!
//! ## Thin wrapper over `mitos-marketplace-decode`
//!
//! The lifecycle classification (produced/consumed matching → Create / Update /
//! Unlisting, price math, the hash-only-create no-fallback boot-stall rule) now
//! lives in [`mitos_marketplace_decode::decode_jpg_listings`] — the single
//! source of truth the historical `market-ledger` walker shares. This module
//! only maps the platform's `Produced`/`Consumed` events into the neutral
//! [`DecodeTx`] and passes a `datum_by_hash` resolver.
//!
//! Datum resolution is asymmetric and preserved by the crate: a *create* reads
//! the produced output's inline payload only and NEVER calls the resolver (jpg
//! creates are hash-only by design; a fallback would stall bootstrap re-scans of
//! the stranded jpg book), while updates and unlistings fall back to
//! `chain_data::datum_by_hash`. To match that, we resolve a consumed listing's
//! datum host-side only for cancel-redeemer spends (the sale-domain Buy spends
//! never need it) and leave the produced side's hash unresolved — the crate
//! calls the resolver itself on the update path.

use mitos_community_events::jpg_store_listing::JpgStoreListing;
use mitos_marketplace_decode::{
    classify_jpg_address, decode_jpg_listings, AssetId, DecodeTx, OutputDatum, TxInput, TxOutput,
};

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{
    AssetEntry, ConsumedEvent, ProducedEvent, TypedDatum, UtxoEvent,
};

const LOG_TARGET: &str = "jpg-store-listing-module";

/// jpg.store / Wayup delist (cancel) redeemer: constructor 1, empty (`d87a80`).
/// Matched exactly (not by prefix) — the crate's listing decode uses the same
/// full-byte match, so a richer constructor-1 redeemer is not a delist.
fn is_delist_redeemer(bytes: &[u8]) -> bool {
    bytes == [0xd8, 0x7a, 0x80]
}

/// Resolve datum bytes for a consumed listing: inline `payload`, else the
/// hash via `chain_data::datum_by_hash`.
fn resolve_datum_bytes(d: Option<&TypedDatum>) -> Option<Vec<u8>> {
    let d = d?;
    if !d.payload.is_empty() {
        return Some(d.payload.clone());
    }
    chain_data::datum_by_hash(&d.hash)
}

fn to_asset_ids(assets: &[AssetEntry]) -> Vec<AssetId> {
    assets
        .iter()
        .map(|e| AssetId {
            policy: e.asset.policy.clone(),
            name: e.asset.name.clone(),
        })
        .collect()
}

/// Build a neutral `TxOutput`, carrying the produced UTxO's on-chain index and
/// datum (inline payload + hash, unresolved — the crate's create path is
/// payload-only and its update path resolves the hash itself).
fn build_output(p: &ProducedEvent) -> TxOutput {
    TxOutput {
        address: p.output.address.clone(),
        lovelace: p.output.lovelace,
        assets: to_asset_ids(&p.output.assets),
        index: p.oref.index,
        datum: p.datum.as_ref().map(|d| OutputDatum {
            payload: d.payload.clone(),
            hash: d.hash.clone(),
        }),
    }
}

/// Build a neutral `TxInput` from a consumed event. The prior listing's datum
/// (a host call) is resolved only for jpg-script consumes with the delist
/// redeemer — the only consumes listing decode reads — to avoid blanket
/// per-hash lookups.
fn build_input(c: &ConsumedEvent) -> TxInput {
    let at_venue = classify_jpg_address(&c.prior_output.address).is_some();
    let is_delist = c.redeemer.as_deref().map(is_delist_redeemer).unwrap_or(false);
    let datum = if at_venue && is_delist {
        resolve_datum_bytes(c.prior_datum.as_ref())
    } else {
        None
    };
    TxInput {
        address: c.prior_output.address.clone(),
        lovelace: c.prior_output.lovelace,
        assets: to_asset_ids(&c.prior_output.assets),
        datum,
        redeemer: c.redeemer.clone(),
        ..Default::default()
    }
}

fn emit_listing(event: &JpgStoreListing) {
    let mut buf = Vec::with_capacity(512);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode JpgStoreListing failed: {e}"),
        );
        return;
    }
    emit::emit_event(0, &buf);
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
        let mut tx = DecodeTx::default();
        for event in events {
            match event {
                DispatchEvent::Utxo(UtxoEvent::Produced(p)) => {
                    if tx.tx_hash.is_empty() {
                        tx.tx_hash = p.tx_hash.clone();
                    }
                    tx.outputs.push(build_output(&p));
                }
                DispatchEvent::Utxo(UtxoEvent::Consumed(c)) => {
                    if tx.tx_hash.is_empty() {
                        tx.tx_hash = c.consuming_tx_hash.clone();
                    }
                    tx.inputs.push(build_input(&c));
                }
                _ => {}
            }
        }
        if tx.tx_hash.is_empty() {
            return;
        }
        for listing in decode_jpg_listings(&tx, |h| chain_data::datum_by_hash(h)) {
            emit_listing(&listing);
        }
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        // Interest is fully static (declared in <name>.toml); no runtime updates.
        Ok(())
    }

    /// No-op: event-driven modules are refilled host-side by `run_bootstrap`
    /// over the manifest `[interest]`. One call, immediately `done`.
    fn rebootstrap(_mode: RebootstrapMode) -> Result<RebootstrapStep, String> {
        Ok(RebootstrapStep {
            done: true,
            ingested: 0,
        })
    }
}

export!(Module);
