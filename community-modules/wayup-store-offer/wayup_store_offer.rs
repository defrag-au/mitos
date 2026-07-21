//! Wayup offer lifecycle community module.
//!
//! Watches the Wayup offer contract by **payment credential**
//! and emits typed `WayupStoreOffer::{Create, Cancel, Accept,
//! Update}` events per offer lifecycle stage. Sibling of
//! `jpg-store-offer`; the decoded datum has the same shape, so
//! the event surface mirrors it. The four wayup-specific
//! differences (see `docs/design/WAYUP_STORE_OFFER_MODULE.md`):
//!
//! 1. **Watch by payment credential.** Each offer UTxO's address
//!    embeds the *bidder's own stake key* as its staking part, so
//!    bidders sit at different full addresses sharing one payment
//!    script. Interest is declared as `payment_credentials` in
//!    the manifest; the module identifies offer outputs by
//!    decoding each address's payment credential and comparing to
//!    the configured `payment_cred`.
//!
//! 2. **Hash-only datum, no metadata.** Wayup commits a
//!    hash-only datum and does NOT publish the bytes in TX
//!    metadata (jpg.store's labels-50+ trick). Bytes are resolved
//!    from the host payload (witness set / `DATUM_NS`) with a
//!    `chain_data::datum_by_hash` fallback — same resolver shape
//!    as `collection-metadata` / `cip-68-mint`.
//!
//! 3. **Non-positional target payout.** The NFT (buyer) payout is
//!    not last in the payout list (order is fee / buyer /
//!    royalty); the decoder scans all payouts for the one whose
//!    value carries a non-ADA policy.
//!
//! 4. **Redeemer-agnostic accept/cancel.** Accept and cancel both
//!    spend with redeemer `d87a80`, so the redeemer carries no
//!    signal. Discrimination: an **Accept** delivers an asset
//!    under the offer's target policy to a non-offer output AND
//!    the bidder's owner key is NOT among the TX's required
//!    signers; otherwise it's a **Cancel** (the bidder signs to
//!    reclaim their locked lovelace).

use std::cell::RefCell;
use std::collections::HashMap;

use mitos_community_events::wayup_store_offer::{
    OfferAccept, OfferCancel, OfferCreate, OfferUpdate, WayupStoreOffer, WayupStoreOfferVersion,
};
use mitos_marketplace_decode::{
    decode_wayup_offer_accepts, decode_wayup_offer_datum, AssetId, DecodeTx, DecodedOffer, TxInput,
    TxOutput, WayupOfferConfig,
};
use pallas_addresses::{Address, ShelleyPaymentPart};
use serde::Deserialize;

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{
    ConsumedEvent, ProducedEvent, TxContextEvent, TypedDatum, UtxoEvent,
};

const LOG_TARGET: &str = "wayup-store-offer-module";

#[derive(Debug, Clone, Deserialize)]
struct Config {
    /// 56-char hex of the Wayup offer contract's payment
    /// credential (payment-script hash). Offer UTxOs share this
    /// payment cred but vary in staking part per bidder.
    #[serde(default)]
    payment_cred: String,
}

// Configured at init from the manifest's colocated config.
// RefCell since the wasm world is single-threaded.
thread_local! {
    static OFFER_PAYMENT_CRED: RefCell<Option<[u8; 28]>> = const { RefCell::new(None) };
}

/// Extract a Shelley address's 28-byte payment credential
/// (payment-key or payment-script hash). `None` for non-Shelley
/// shapes or unparseable bech32.
fn address_payment_cred(addr: &str) -> Option<[u8; 28]> {
    let Ok(Address::Shelley(s)) = Address::from_bech32(addr) else {
        return None;
    };
    let cred: [u8; 28] = match s.payment() {
        ShelleyPaymentPart::Key(h) => **h,
        ShelleyPaymentPart::Script(h) => **h,
    };
    Some(cred)
}

/// Is this output at the watched Wayup offer payment credential?
fn is_offer_output(addr: &str) -> bool {
    OFFER_PAYMENT_CRED.with(|c| match *c.borrow() {
        Some(cred) => address_payment_cred(addr) == Some(cred),
        None => false,
    })
}

/// The Wayup CO datum decode (`Constr0[bidder_key, [payout...]]`, scan-all-payouts
/// target extraction + recipient credential) lives in the shared
/// `mitos-marketplace-decode` crate — the single source of truth reused by the
/// `cnft.dev-workers` firehose — as [`decode_wayup_offer_datum`], returning
/// [`DecodedOffer`]. This module resolves the datum bytes and calls it.

/// Resolve datum bytes: host payload first (inline / witness-set
/// / `DATUM_NS`), else the `datum_by_hash` side-door. Wayup has
/// no metadata-label fallback (unlike jpg.store).
fn resolve_datum_bytes(datum: &TypedDatum) -> Option<Vec<u8>> {
    if !datum.payload.is_empty() {
        return Some(datum.payload.clone());
    }
    chain_data::datum_by_hash(&datum.hash)
}

// ============================================================
// In-batch correlation buffer (one TX per `handle_events`)
// ============================================================

#[derive(Clone)]
struct ConsumedOffer {
    /// The consumed offer UTxO's address + raw datum — carried so `flush_buffer`
    /// can rebuild the neutral `DecodeTx` the crate's accept decode consumes.
    address: String,
    prior_datum_bytes: Vec<u8>,
    prior_tx_hash: Vec<u8>,
    prior_output_index: u32,
    prior_lovelace: u64,
    decoded: DecodedOffer,
}

#[derive(Clone)]
struct ProducedOffer {
    tx_hash: Vec<u8>,
    output_index: u32,
    lovelace: u64,
    datum_bytes: Vec<u8>,
    decoded: DecodedOffer,
}

/// Produced output at a non-offer address — a potential
/// accept-delivery target (the bidder receiving their NFT).
#[derive(Clone)]
struct ProducedNonScript {
    address: String,
    assets: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Default)]
struct TxBuffer {
    consumed_offers: Vec<ConsumedOffer>,
    produced_offers: Vec<ProducedOffer>,
    produced_other: Vec<ProducedNonScript>,
    /// 28-byte key hashes from the TX's `required_signers` field.
    /// A Wayup cancel declares the bidder's owner key here; an
    /// accept does not.
    required_signers: Vec<Vec<u8>>,
    tx_hash: Option<Vec<u8>>,
}

fn handle_txcontext(tx: &TxContextEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(tx.tx_hash.clone());
    }
    buf.required_signers = tx.required_signers.clone();
}

fn handle_produced(p: &ProducedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(p.tx_hash.clone());
    }
    if is_offer_output(&p.output.address) {
        let Some(datum) = p.datum.as_ref() else {
            return;
        };
        let Some(datum_bytes) = resolve_datum_bytes(datum) else {
            return;
        };
        let Some(decoded) = decode_wayup_offer_datum(&datum_bytes) else {
            return;
        };
        buf.produced_offers.push(ProducedOffer {
            tx_hash: p.tx_hash.clone(),
            output_index: p.oref.index,
            lovelace: p.output.lovelace,
            datum_bytes,
            decoded,
        });
        return;
    }
    let assets: Vec<(Vec<u8>, Vec<u8>)> = p
        .output
        .assets
        .iter()
        .map(|a| (a.asset.policy.clone(), a.asset.name.clone()))
        .collect();
    if !assets.is_empty() {
        buf.produced_other.push(ProducedNonScript {
            address: p.output.address.clone(),
            assets,
        });
    }
}

fn handle_consumed(c: &ConsumedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(c.consuming_tx_hash.clone());
    }
    if !is_offer_output(&c.prior_output.address) {
        return;
    }
    let Some(prior_datum) = c.prior_datum.as_ref() else {
        return;
    };
    let Some(datum_bytes) = resolve_datum_bytes(prior_datum) else {
        return;
    };
    let Some(decoded) = decode_wayup_offer_datum(&datum_bytes) else {
        return;
    };
    buf.consumed_offers.push(ConsumedOffer {
        address: c.prior_output.address.clone(),
        prior_datum_bytes: datum_bytes,
        prior_tx_hash: c.oref.tx_hash.clone(),
        prior_output_index: c.oref.index,
        prior_lovelace: c.prior_output.lovelace,
        decoded,
    });
}

fn flush_buffer(mut buf: TxBuffer) {
    let Some(tx_hash) = buf.tx_hash.take() else {
        return;
    };
    let tx_hash_hex = hex::encode(&tx_hash);

    let consumed = std::mem::take(&mut buf.consumed_offers);
    let mut produced_slots: Vec<Option<ProducedOffer>> = std::mem::take(&mut buf.produced_offers)
        .into_iter()
        .map(Some)
        .collect();

    // Update detection fires only for a bidder with exactly 1
    // consume + 1 produce in this TX (atomic edit). Multi-offer
    // batches emit independent Cancel/Accept + Create — pairing N
    // consumes with N produces has no reliable correspondence.
    // (Same guard + rationale as `jpg-store-offer`.)
    let mut consume_count: HashMap<String, usize> = HashMap::new();
    for c in &consumed {
        *consume_count
            .entry(c.decoded.bidder_pkh.clone())
            .or_default() += 1;
    }
    let mut produce_count: HashMap<String, usize> = HashMap::new();
    for slot in produced_slots.iter().flatten() {
        *produce_count
            .entry(slot.decoded.bidder_pkh.clone())
            .or_default() += 1;
    }
    let is_update_pair = |pkh: &str| {
        consume_count.get(pkh).copied().unwrap_or(0) == 1
            && produce_count.get(pkh).copied().unwrap_or(0) == 1
    };

    // Decode the tx's offer-accepts with the shared crate — the single source of
    // truth (also used by the `cnft.dev-workers` firehose) for the accept-vs-
    // cancel discrimination (bidder-signed ⇒ cancel; else asset delivered to the
    // bidder's own wallet ⇒ accept). We rebuild the neutral `DecodeTx` from the
    // buffered consumes, the non-offer produced outputs, and the tx's required
    // signers, then index the accepts by the consumed offer's oref. A consume
    // with no crate accept (and not an Update) is a Cancel.
    let offer_cfg = OFFER_PAYMENT_CRED.with(|c| {
        c.borrow()
            .map(|cred| WayupOfferConfig::from_hex(&hex::encode(cred)))
            .unwrap_or_default()
    });
    let accept_tx = DecodeTx {
        tx_hash: tx_hash.clone(),
        inputs: consumed
            .iter()
            .map(|c| TxInput {
                address: c.address.clone(),
                lovelace: c.prior_lovelace,
                assets: Vec::new(),
                datum: Some(c.prior_datum_bytes.clone()),
                redeemer: None,
                oref_tx_hash: c.prior_tx_hash.clone(),
                oref_index: c.prior_output_index,
            })
            .collect(),
        outputs: buf
            .produced_other
            .iter()
            .map(|o| TxOutput {
                address: o.address.clone(),
                lovelace: 0,
                assets: o
                    .assets
                    .iter()
                    .map(|(policy, name)| AssetId {
                        policy: policy.clone(),
                        name: name.clone(),
                    })
                    .collect(),
            })
            .collect(),
        required_signers: buf.required_signers.clone(),
    };
    let mut crate_accepts: HashMap<(String, u32), OfferAccept> =
        decode_wayup_offer_accepts(&accept_tx, &offer_cfg)
            .into_iter()
            .map(|a| ((a.prior_tx_hash.clone(), a.prior_output_index), a))
            .collect();

    // Step 1: each consumed offer → Update (paired), Accept
    // (asset delivered + bidder didn't sign), or Cancel.
    for consume in consumed {
        let bidder_pkh = consume.decoded.bidder_pkh.clone();

        if is_update_pair(&bidder_pkh)
            && let Some(slot) = produced_slots
                .iter_mut()
                .find(|s| s.as_ref().is_some_and(|p| p.decoded.bidder_pkh == bidder_pkh))
            && let Some(produced) = slot.take()
        {
            emit_offer(&WayupStoreOffer::Update(OfferUpdate {
                bidder_pkh,
                tx_hash: tx_hash_hex.clone(),
                prior_tx_hash: hex::encode(&consume.prior_tx_hash),
                prior_output_index: consume.prior_output_index,
                new_output_index: produced.output_index,
                previous_lovelace: consume.prior_lovelace,
                new_lovelace: produced.lovelace,
                datum_cbor: produced.datum_bytes,
                target_policy: produced.decoded.target_policy,
                target_asset_names: produced.decoded.target_asset_names,
                co_version: WayupStoreOfferVersion::V1,
            }));
            continue;
        }

        // Accept vs Cancel: the shared crate discriminated them (bidder-signed ⇒
        // cancel; else delivery to the bidder's wallet ⇒ accept). A crate accept
        // keyed on this consume's oref ⇒ emit it; otherwise it's a Cancel.
        let key = (hex::encode(&consume.prior_tx_hash), consume.prior_output_index);
        if let Some(accept) = crate_accepts.remove(&key) {
            emit_offer(&WayupStoreOffer::Accept(accept));
        } else {
            emit_offer(&WayupStoreOffer::Cancel(OfferCancel {
                bidder_pkh,
                tx_hash: tx_hash_hex.clone(),
                prior_tx_hash: hex::encode(&consume.prior_tx_hash),
                prior_output_index: consume.prior_output_index,
                target_policy: consume.decoded.target_policy,
                co_version: WayupStoreOfferVersion::V1,
            }));
        }
    }

    // Step 2: unpaired produced offers → fresh Create, in
    // on-chain output order.
    for p in produced_slots.into_iter().flatten() {
        emit_offer(&WayupStoreOffer::Create(OfferCreate {
            bidder_pkh: p.decoded.bidder_pkh,
            tx_hash: hex::encode(&p.tx_hash),
            output_index: p.output_index,
            lovelace: p.lovelace,
            datum_cbor: p.datum_bytes,
            target_policy: p.decoded.target_policy,
            target_asset_names: p.decoded.target_asset_names,
            co_version: WayupStoreOfferVersion::V1,
        }));
    }
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
    // Per-policy partition key (same rationale as jpg-store-offer):
    // same-policy events serialise; different-policy events
    // parallelise across dialer lanes.
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
    policy_hex
        .map(|s| s.as_bytes().to_vec())
        .unwrap_or_default()
}

// ============================================================
// v2 Guest impl
// ============================================================

struct Module;

impl Guest for Module {
    fn module_version() -> (u32, u32) {
        // ABI/platform world version (v2), not the contract
        // version — the latter is `WayupStoreOfferVersion`.
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
        match hex::decode(&cfg.payment_cred) {
            Ok(bytes) if bytes.len() == 28 => {
                let mut cred = [0u8; 28];
                cred.copy_from_slice(&bytes);
                OFFER_PAYMENT_CRED.with(|c| *c.borrow_mut() = Some(cred));
                logging::log(LogLevel::Info, LOG_TARGET, "init: payment cred stored");
            }
            _ => {
                logging::log(
                    LogLevel::Error,
                    LOG_TARGET,
                    &format!("init: invalid payment_cred `{}`", cfg.payment_cred),
                );
            }
        }
    }

    fn handle_events(events: Vec<DispatchEvent>) {
        let mut buf = TxBuffer::default();
        for event in events {
            match event {
                DispatchEvent::Utxo(UtxoEvent::TxContext(tx)) => handle_txcontext(&tx, &mut buf),
                DispatchEvent::Utxo(UtxoEvent::Produced(p)) => handle_produced(&p, &mut buf),
                DispatchEvent::Utxo(UtxoEvent::Consumed(c)) => handle_consumed(&c, &mut buf),
                _ => {}
            }
        }
        flush_buffer(buf);
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        Ok(())
    }

    /// No-op: event-driven modules are refilled host-side by
    /// `run_bootstrap` over the manifest `[interest]`
    /// (`payment_credentials` → `utxos_by_payment_cred`). One
    /// call, immediately `done`.
    fn rebootstrap(_mode: RebootstrapMode) -> Result<RebootstrapStep, String> {
        Ok(RebootstrapStep {
            done: true,
            ingested: 0,
        })
    }
}

export!(Module);
