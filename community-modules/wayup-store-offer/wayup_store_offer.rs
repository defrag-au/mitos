//! Wayup offer lifecycle community module.
//!
//! Emits `WayupStoreOffer::{Create, Cancel, Accept, Update}` for the Wayup
//! collection-offer contract, watched by payment credential (each bidder sits at
//! a different full address sharing one payment script).
//!
//! ## Thin wrapper over `mitos-marketplace-decode`
//!
//! Lifecycle classification (Create/Cancel/Update/Accept correlation, the
//! per-bidder atomic-edit guard) lives in
//! [`mitos_marketplace_decode::decode_wayup_offer_lifecycle`] — the single
//! source of truth the historical `market-ledger` walker shares. This module
//! holds the offer contract's payment credential from `init`, resolves each
//! offer's hash-only datum, and maps the platform's events into the neutral
//! [`DecodeTx`].
//!
//! Accept-vs-cancel is redeemer-agnostic on Wayup (both spend with `d87a80`): an
//! accept delivers a target-policy asset to the bidder's own wallet AND the
//! bidder is not among the tx's required signers. That discrimination lives in
//! the crate's accept decode, so the module forwards `required_signers` (from
//! the tx-context event) into the `DecodeTx`.

use std::cell::RefCell;

use mitos_community_events::wayup_store_offer::WayupStoreOffer;
use mitos_marketplace_decode::{
    decode_wayup_offer_lifecycle, AssetId, DecodeTx, OutputDatum, TxInput, TxOutput,
    WayupOfferConfig,
};
use serde::Deserialize;

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{
    AssetEntry, ConsumedEvent, ProducedEvent, TxContextEvent, TypedDatum, UtxoEvent,
};

const LOG_TARGET: &str = "wayup-store-offer-module";

#[derive(Debug, Clone, Deserialize)]
struct Config {
    /// 56-char hex of the Wayup offer contract's payment credential. Offer UTxOs
    /// share this payment cred but vary in staking part per bidder.
    #[serde(default)]
    payment_cred: String,
}

thread_local! {
    static OFFER_CONFIG: RefCell<WayupOfferConfig> = RefCell::new(WayupOfferConfig::default());
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

/// Resolve datum bytes: host payload first (inline / witness-set), else the
/// `datum_by_hash` side-door. Wayup has no metadata-label fallback (unlike jpg).
fn resolve_datum_bytes(datum: &TypedDatum) -> Option<Vec<u8>> {
    if !datum.payload.is_empty() {
        return Some(datum.payload.clone());
    }
    chain_data::datum_by_hash(&datum.hash)
}

/// Map a produced event into a neutral `TxOutput`. Offer outputs carry the
/// resolved datum (in `payload`); non-offer outputs are accept-delivery
/// candidates (assets only).
fn build_output(p: &ProducedEvent, cfg: &WayupOfferConfig) -> TxOutput {
    let datum = if cfg.is_offer_address(&p.output.address) {
        p.datum
            .as_ref()
            .and_then(resolve_datum_bytes)
            .map(|bytes| OutputDatum {
                payload: bytes,
                hash: Vec::new(),
            })
    } else {
        None
    };
    TxOutput {
        address: p.output.address.clone(),
        lovelace: p.output.lovelace,
        assets: to_asset_ids(&p.output.assets),
        index: p.oref.index,
        datum,
    }
}

/// Map a consumed event into a neutral `TxInput`. Non-offer consumes are dropped
/// (offer lifecycle only reads offer inputs).
fn build_input(c: &ConsumedEvent, cfg: &WayupOfferConfig) -> Option<TxInput> {
    if !cfg.is_offer_address(&c.prior_output.address) {
        return None;
    }
    let datum = c.prior_datum.as_ref().and_then(resolve_datum_bytes);
    Some(TxInput {
        address: c.prior_output.address.clone(),
        lovelace: c.prior_output.lovelace,
        assets: Vec::new(),
        datum,
        redeemer: c.redeemer.clone(),
        oref_tx_hash: c.oref.tx_hash.clone(),
        oref_index: c.oref.index,
    })
}

fn emit_offer(event: &WayupStoreOffer) {
    let mut buf = Vec::new();
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            &format!("emit serialize failed: {e}"),
        );
        return;
    }
    let key = partition_key_for_offer(event);
    emit::emit_event_keyed(0, &key, &buf);
}

fn partition_key_for_offer(event: &WayupStoreOffer) -> Vec<u8> {
    let policy_hex: Option<&str> = match event {
        WayupStoreOffer::Create(c) => c.target_policy.as_deref(),
        WayupStoreOffer::Cancel(c) => c.target_policy.as_deref(),
        WayupStoreOffer::Update(u) => u.target_policy.as_deref(),
        WayupStoreOffer::Accept(a) if !a.policy.is_empty() => Some(a.policy.as_str()),
        WayupStoreOffer::Accept(_) => None,
    };
    policy_hex.map(|s| s.as_bytes().to_vec()).unwrap_or_default()
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
        OFFER_CONFIG.with(|c| *c.borrow_mut() = WayupOfferConfig::from_hex(&cfg.payment_cred));
        logging::log(LogLevel::Info, LOG_TARGET, "init: offer config stored");
    }

    fn handle_events(events: Vec<DispatchEvent>) {
        let cfg = OFFER_CONFIG.with(|c| c.borrow().clone());
        let mut tx = DecodeTx::default();
        let mut required_signers = Vec::new();
        for event in events {
            match event {
                DispatchEvent::Utxo(UtxoEvent::TxContext(t)) => {
                    if tx.tx_hash.is_empty() {
                        tx.tx_hash = t.tx_hash.clone();
                    }
                    required_signers = t.required_signers.clone();
                }
                DispatchEvent::Utxo(UtxoEvent::Produced(p)) => {
                    if tx.tx_hash.is_empty() {
                        tx.tx_hash = p.tx_hash.clone();
                    }
                    tx.outputs.push(build_output(&p, &cfg));
                }
                DispatchEvent::Utxo(UtxoEvent::Consumed(c)) => {
                    if tx.tx_hash.is_empty() {
                        tx.tx_hash = c.consuming_tx_hash.clone();
                    }
                    if let Some(input) = build_input(&c, &cfg) {
                        tx.inputs.push(input);
                    }
                }
                _ => {}
            }
        }
        tx.required_signers = required_signers;
        if tx.tx_hash.is_empty() {
            return;
        }
        for offer in decode_wayup_offer_lifecycle(&tx, &cfg) {
            emit_offer(&offer);
        }
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
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
