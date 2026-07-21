//! jpg.store sales community module.
//!
//! Emits one `JpgStoreSale::Sale` per completed sale on the jpg.store sale
//! contracts (V1/V2/V3/V4). All decode/matching logic lives in the shared
//! `mitos-marketplace-decode` crate (the single source of truth reused by the
//! `cnft.dev-workers` historical backfill) — this module is a thin adapter that
//! maps the platform's dispatch events into the crate's neutral `DecodeTx`
//! (resolving datum hashes via the host) and emits what it returns.
//!
//! Listing lifecycle events live in the sibling `jpg-store-listing` module.

use mitos_community_events::jpg_store_sale::JpgStoreSale;
use mitos_marketplace_decode::{
    AssetId, DecodeTx, TxInput, TxOutput, classify_jpg_address, decode_jpg_sales, is_buy_redeemer,
};

use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{ConsumedEvent, ProducedEvent, TypedDatum, UtxoEvent};

const LOG_TARGET: &str = "jpg-store-sale-module";

/// Resolve a datum to its CBOR bytes: inline/witness payload when present,
/// else the host's hash lookup.
fn resolve_datum_bytes(d: Option<&TypedDatum>) -> Option<Vec<u8>> {
    let d = d?;
    if !d.payload.is_empty() {
        return Some(d.payload.clone());
    }
    crate::mitos::platform_v2::chain_data::datum_by_hash(&d.hash)
}

fn to_asset_ids(assets: &[crate::mitos::platform_v2::types::AssetEntry]) -> Vec<AssetId> {
    assets
        .iter()
        .map(|e| AssetId {
            policy: e.asset.policy.clone(),
            name: e.asset.name.clone(),
        })
        .collect()
}

/// Build a neutral `TxInput` from a consumed event. Datum resolution (a host
/// call) is done lazily — only for jpg-script consumes with a Buy redeemer —
/// to avoid the per-hash host lookups a blanket resolve would incur.
fn build_input(c: &ConsumedEvent) -> TxInput {
    let at_venue = classify_jpg_address(&c.prior_output.address).is_some();
    let is_buy = c.redeemer.as_deref().map(is_buy_redeemer).unwrap_or(false);
    let datum = if at_venue && is_buy {
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
    }
}

fn emit_sale(event: &JpgStoreSale) {
    let mut buf = Vec::with_capacity(512);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode JpgStoreSale failed: {e}"),
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
                DispatchEvent::Utxo(UtxoEvent::Consumed(c)) => {
                    if tx.tx_hash.is_empty() {
                        tx.tx_hash = c.consuming_tx_hash.clone();
                    }
                    tx.inputs.push(build_input(&c));
                }
                DispatchEvent::Utxo(UtxoEvent::Produced(p)) => {
                    if tx.tx_hash.is_empty() {
                        tx.tx_hash = p.tx_hash.clone();
                    }
                    tx.outputs.push(TxOutput {
                        address: p.output.address.clone(),
                        lovelace: p.output.lovelace,
                        assets: to_asset_ids(&p.output.assets),
                    });
                }
                _ => {}
            }
        }
        for sale in decode_jpg_sales(&tx) {
            emit_sale(&sale);
        }
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
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
