//! Wayup fixed-price sale community module.
//!
//! Emits `WayupStoreSale::Sale` when a Wayup listing is bought. All
//! decode/matching logic (including fee-waiver detection) lives in the shared
//! `mitos-marketplace-decode` crate — the single source of truth reused by the
//! `cnft.dev-workers` historical backfill. This module is a thin adapter: it
//! holds the sale/fee credentials from `init`, maps dispatch events into the
//! crate's neutral `DecodeTx` (resolving datum hashes via the host), and emits
//! what the crate returns.
//!
//! Listing lifecycle events live in `wayup-store-listing`; offer (bid) lifecycle
//! in `wayup-store-offer`, whose `Accept` is the bid-side sale.

use std::cell::RefCell;

use mitos_community_events::wayup_store_sale::WayupStoreSale;
use mitos_marketplace_decode::{
    AssetId, DecodeTx, TxInput, TxOutput, WayupSaleConfig, decode_wayup_sales, is_buy_redeemer,
};
use serde::Deserialize;

use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{ConsumedEvent, ProducedEvent, TypedDatum, UtxoEvent};

const LOG_TARGET: &str = "wayup-store-sale-module";

#[derive(Debug, Clone, Deserialize)]
struct Config {
    /// 56-char hex of the Wayup sale validator's payment credential.
    #[serde(default)]
    payment_cred: String,
    /// 56-char hex of Wayup's fee-address payment credential. A buy that pays
    /// no output here had its platform fee waived (see `fee_waived`).
    #[serde(default)]
    fee_payment_cred: String,
}

thread_local! {
    static SALE_CONFIG: RefCell<WayupSaleConfig> = RefCell::new(WayupSaleConfig::default());
}

/// Resolve a datum to its CBOR bytes: inline/witness payload when present, else
/// the host's hash lookup.
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
/// call) is lazy — only for sale-credential consumes with a Buy redeemer — to
/// avoid the per-hash host lookups a blanket resolve would incur.
fn build_input(c: &ConsumedEvent) -> TxInput {
    let at_venue = SALE_CONFIG.with(|cfg| cfg.borrow().is_listing_address(&c.prior_output.address));
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

fn emit_sale(event: &WayupStoreSale) {
    let mut buf = Vec::with_capacity(512);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode WayupStoreSale failed: {e}"),
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
        if config.is_empty() {
            return;
        }
        let cfg: Config = match ciborium::de::from_reader(config.as_slice()) {
            Ok(c) => c,
            Err(e) => {
                logging::log(
                    LogLevel::Error,
                    LOG_TARGET,
                    &format!("init: decode config failed: {e}"),
                );
                return;
            }
        };
        let sale_config = WayupSaleConfig::from_hex(&cfg.payment_cred, &cfg.fee_payment_cred);
        SALE_CONFIG.with(|c| *c.borrow_mut() = sale_config);
        logging::log(LogLevel::Info, LOG_TARGET, "init: sale config stored");
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
        let sales = SALE_CONFIG.with(|cfg| decode_wayup_sales(&tx, &cfg.borrow()));
        for sale in sales {
            emit_sale(&sale);
        }
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        // Interest is fully static (declared in <name>.toml).
        Ok(())
    }

    /// No-op: event-driven modules are refilled host-side by `run_bootstrap`
    /// over the manifest `[interest]`.
    fn rebootstrap(_mode: RebootstrapMode) -> Result<RebootstrapStep, String> {
        Ok(RebootstrapStep {
            done: true,
            ingested: 0,
        })
    }
}

export!(Module);
