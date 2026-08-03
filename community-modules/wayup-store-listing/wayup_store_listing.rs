//! Wayup listing lifecycle community module.
//!
//! Emits `WayupStoreListing::{Create, Update, Unlisting}` for the Wayup sale
//! validator. Completed sales live in the sibling `wayup-store-sale` module;
//! offers (bids) in `wayup-store-offer`.
//!
//! ## Thin wrapper over `mitos-marketplace-decode`
//!
//! Listing-lifecycle classification (produced/consumed matching → Create /
//! Update / Unlisting, price math) lives in
//! [`mitos_marketplace_decode::decode_wayup_listings`] — the single source of
//! truth the historical `market-ledger` walker shares. This module holds the
//! sale validator's payment credential from `init`, maps dispatch events into
//! the neutral [`DecodeTx`], and passes a `datum_by_hash` resolver.
//!
//! Wayup listing UTxOs sit at addresses sharing the sale validator's payment
//! credential (per-seller staking part), so classification is by payment
//! credential. Datums are hash-only; unlike jpg, a Wayup *create* falls back to
//! the resolver when its inline payload is absent (the crate applies this) —
//! host-side, we resolve a consumed listing's datum only for the cancel
//! redeemer (constructor 1, `d87a80`), the only consumes listing decode reads.

use std::cell::RefCell;

use mitos_community_events::wayup_store_listing::WayupStoreListing;
use mitos_marketplace_decode::{
    decode_wayup_listings, AssetId, DecodeTx, OutputDatum, TxInput, TxOutput, WayupSaleConfig,
};
use serde::Deserialize;

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{
    AssetEntry, ConsumedEvent, ProducedEvent, TypedDatum, UtxoEvent,
};

const LOG_TARGET: &str = "wayup-store-listing-module";

#[derive(Debug, Clone, Deserialize)]
struct Config {
    /// 56-char hex of the Wayup sale validator's payment credential. Listing
    /// UTxOs share this payment cred but vary in staking part per seller.
    #[serde(default)]
    payment_cred: String,
}

thread_local! {
    static LISTING_CONFIG: RefCell<WayupSaleConfig> = RefCell::new(WayupSaleConfig::default());
}

/// Wayup delist (cancel) redeemer: constructor 1, empty (`d87a80`). Matched
/// exactly (not by prefix), as the crate's listing decode does.
fn is_delist_redeemer(bytes: &[u8]) -> bool {
    bytes == [0xd8, 0x7a, 0x80]
}

/// Resolve datum bytes for a consumed listing: inline `payload`, else the host's
/// hash lookup.
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
/// datum (inline payload + hash, unresolved — the crate resolves the hash on the
/// create/update path itself).
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

/// Build a neutral `TxInput` from a consumed event. The prior listing's datum (a
/// host call) is resolved only for sale-credential consumes with the delist
/// redeemer.
fn build_input(c: &ConsumedEvent) -> TxInput {
    let at_venue =
        LISTING_CONFIG.with(|cfg| cfg.borrow().is_listing_address(&c.prior_output.address));
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

fn emit_listing(event: &WayupStoreListing) {
    let mut buf = Vec::with_capacity(512);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode WayupStoreListing failed: {e}"),
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
        let listing_config = WayupSaleConfig::from_hex(&cfg.payment_cred, "");
        LISTING_CONFIG.with(|c| *c.borrow_mut() = listing_config);
        logging::log(LogLevel::Info, LOG_TARGET, "init: listing config stored");
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
        let listings = LISTING_CONFIG
            .with(|cfg| decode_wayup_listings(&tx, &cfg.borrow(), |h| chain_data::datum_by_hash(h)));
        for listing in listings {
            emit_listing(&listing);
        }
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        // Interest is fully static (declared in <name>.toml).
        Ok(())
    }

    /// No-op: event-driven modules are refilled host-side by `run_bootstrap`
    /// over the manifest `[interest]` (`payment_credentials` → `utxos_by_payment_cred`).
    fn rebootstrap(_mode: RebootstrapMode) -> Result<RebootstrapStep, String> {
        Ok(RebootstrapStep {
            done: true,
            ingested: 0,
        })
    }
}

export!(Module);
