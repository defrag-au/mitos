//! jpg.store offer lifecycle community module.
//!
//! Emits `JpgStoreOffer::{Create, Cancel, Accept, Update}` for the jpg.store
//! collection-offer (CO) contract.
//!
//! ## Thin wrapper over `mitos-marketplace-decode`
//!
//! The lifecycle classification (Create/Cancel/Update/Accept correlation, the
//! per-bidder atomic-edit guard, the partial-accept fallback) now lives in
//! [`mitos_marketplace_decode::decode_jpg_offer_lifecycle`] — the single source
//! of truth the historical `market-ledger` walker shares. Address classification
//! is the crate's [`classify_jpg_offer_address`] (the CO V2 script); this module
//! only maps the platform's events into the neutral [`DecodeTx`] and resolves
//! each offer's hash-only datum.
//!
//! ## Datum recovery (the caller-side resolution seam)
//!
//! jpg.store offer outputs commit a hash-only datum whose bytes are published in
//! the create tx's metadata via the labels-50..=63 convention: hex chunks,
//! reassembled and blake2b-verified against the output's datum hash. Produced
//! offers resolve against the current (create) tx; consumed offers resolve
//! against their origin tx (`oref.tx_hash`). The resolved CBOR rides into the
//! `DecodeTx` — consumed datums in `TxInput::datum`, produced datums in
//! `TxOutput::datum.payload` — and the crate decodes from there.

use mitos_community_events::jpg_store_offer::JpgStoreOffer;
use mitos_marketplace_decode::recover_datum_from_metadata;
use mitos_marketplace_decode::{
    classify_jpg_offer_address, decode_jpg_offer_lifecycle, AssetId, DecodeTx, OutputDatum, TxInput,
    TxOutput,
};

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{AssetEntry, ConsumedEvent, ProducedEvent, UtxoEvent};

const LOG_TARGET: &str = "jpg-store-offer-module";

fn to_asset_ids(assets: &[AssetEntry]) -> Vec<AssetId> {
    assets
        .iter()
        .map(|e| AssetId {
            policy: e.asset.policy.clone(),
            name: e.asset.name.clone(),
        })
        .collect()
}

/// Resolve datum bytes for an offer. Host populates `payload` when it could
/// resolve (inline / witness-set); when empty, fall back to jpg.store's
/// labels-50+ aux-data convention and hash-verify candidate reconstructions.
/// `tx_hash` is the tx whose metadata carries the preimage — the current tx for
/// a produced offer, the origin (`oref`) tx for a consumed one.
///
/// The recovery itself now lives in `mitos_marketplace_decode::metadata_datum`,
/// shared with the listing module — it is the same convention and must not
/// drift between the bid and ask books.
fn resolve_datum_bytes(tx_hash: &[u8], datum_hash: &[u8], payload: &[u8]) -> Option<Vec<u8>> {
    if !payload.is_empty() {
        return Some(payload.to_vec());
    }
    let aux = chain_data::tx_metadata(tx_hash)?;
    recover_datum_from_metadata(&aux, datum_hash)
}


/// Map a produced event into a neutral `TxOutput`. Offer outputs carry the
/// resolved datum (in `payload`); non-offer outputs are accept-delivery
/// candidates (assets only). Both carry the on-chain output index.
fn build_output(p: &ProducedEvent) -> TxOutput {
    let datum = if classify_jpg_offer_address(&p.output.address).is_some() {
        p.datum
            .as_ref()
            .and_then(|d| resolve_datum_bytes(&p.tx_hash, &d.hash, &d.payload))
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

/// Map a consumed event into a neutral `TxInput`, resolving the prior offer's
/// datum against its origin tx. Non-offer consumes are dropped (offer lifecycle
/// only reads offer inputs).
fn build_input(c: &ConsumedEvent) -> Option<TxInput> {
    classify_jpg_offer_address(&c.prior_output.address)?;
    let datum = c
        .prior_datum
        .as_ref()
        .and_then(|d| resolve_datum_bytes(&c.oref.tx_hash, &d.hash, &d.payload));
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

fn emit_offer(event: &JpgStoreOffer) {
    let mut buf = Vec::new();
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            &format!("emit serialize failed: {e}"),
        );
        return;
    }
    // Per-policy dialer lane key: same-policy events serialise (preserving
    // per-policy aggregate ordering on the companion side), different-policy
    // events parallelise across lanes. Empty (global lane) for partial accepts /
    // the rare datum-with-no-policy case.
    let key = partition_key_for_offer(event);
    emit::emit_event_keyed(0, &key, &buf);
}

/// Hex bytes of the offer's target policy, or empty when none.
fn partition_key_for_offer(event: &JpgStoreOffer) -> Vec<u8> {
    let policy_hex: Option<&str> = match event {
        JpgStoreOffer::Create(c) => c.target_policy.as_deref(),
        JpgStoreOffer::Cancel(c) => c.target_policy.as_deref(),
        JpgStoreOffer::Update(u) => u.target_policy.as_deref(),
        JpgStoreOffer::Accept(a) if !a.policy.is_empty() => Some(a.policy.as_str()),
        JpgStoreOffer::Accept(_) => None,
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
        // Address classification is the crate's `classify_jpg_offer_address`
        // (CO V2 script) — no per-module config needed.
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
                    if let Some(input) = build_input(&c) {
                        tx.inputs.push(input);
                    }
                }
                _ => {}
            }
        }
        if tx.tx_hash.is_empty() {
            return;
        }
        for offer in decode_jpg_offer_lifecycle(&tx) {
            emit_offer(&offer);
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
