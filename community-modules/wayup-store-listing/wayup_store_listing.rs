//! Wayup listing lifecycle community module.
//!
//! Emits `WayupStoreListing::{Create, Update, Unlisting}` for the
//! Wayup sale validator. Completed sales live in the sibling
//! `wayup-store-sale` module; offers (bids) in `wayup-store-offer`.
//!
//! ## Detection
//!
//! Wayup listing UTxOs sit at addresses sharing the sale
//! validator's payment credential with a per-seller staking part,
//! so classification is by payment credential (like
//! `wayup-store-offer`), not exact address match (unlike the jpg
//! modules).
//!
//! Within one `handle_events` call (= one TX's events):
//!
//! - `Produced` at the sale credential with a listing datum →
//!   record in `produced` keyed by `(policy, asset_name)`.
//! - `Consumed` at the sale credential with the cancel redeemer
//!   (constructor 1, `d87a80`) → record in `consumed`. Buy spends
//!   (constructor 0, `d8799f<idx>ff` — the field is an input
//!   index) are the sale module's domain and are skipped here.
//!
//! At flush time:
//!
//! - `(consumed, produced)` for the same asset → `ListingUpdate`
//!   (Wayup price edits are cancel+recreate in a single TX,
//!   observed as multi-asset batches, e.g. `383d83e2…` updated
//!   11 listings in one TX)
//! - `produced` alone → `ListingCreate`
//! - `consumed` alone → `Unlisting` (asset returns to the seller)
//!
//! ## Datum shape
//!
//! Identical structure to jpg.store V1-V3 listings:
//! `Constr 0 [ List<Payout>, Bytes(owner) ]`, payout =
//! `Constr 0 [ Address, Int ]`, price = sum of payout lovelace.
//! **The owner field is the seller's STAKE credential** (jpg uses
//! a payment pkh there). Datums are hash-only; resolution falls
//! back to `chain_data::datum_by_hash` and, as with jpg, a create
//! whose preimage isn't yet revealed emits with empty payouts
//! rather than being dropped.

use std::cell::RefCell;
use std::collections::BTreeMap;

use mitos_community_events::wayup_store_listing::{
    ListingCreate, ListingPayout, ListingUpdate, Unlisting, WayupStoreContractVersion,
    WayupStoreListing,
};
use pallas_addresses::{Address, ShelleyPaymentPart};
use pallas_primitives::PlutusData;
use serde::Deserialize;

use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{
    ConsumedEvent, ProducedEvent, TypedDatum, TypedOutput, UtxoEvent,
};

const LOG_TARGET: &str = "wayup-store-listing-module";

#[derive(Debug, Clone, Deserialize)]
struct Config {
    /// 56-char hex of the Wayup sale validator's payment
    /// credential. Listing UTxOs share this payment cred but vary
    /// in staking part per seller.
    #[serde(default)]
    payment_cred: String,
}

// Configured at init from the manifest's colocated config.
// RefCell since the wasm world is single-threaded.
thread_local! {
    static SALE_PAYMENT_CRED: RefCell<Option<[u8; 28]>> = const { RefCell::new(None) };
}

/// Extract a Shelley address's 28-byte payment credential.
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

/// Is this output at the watched Wayup sale payment credential?
fn is_listing_output(addr: &str) -> bool {
    SALE_PAYMENT_CRED.with(|c| match *c.borrow() {
        Some(cred) => address_payment_cred(addr) == Some(cred),
        None => false,
    })
}

#[derive(Clone)]
struct ProducedListing {
    output_index: u32,
    output: TypedOutput,
    datum: Option<TypedDatum>,
}

#[derive(Clone)]
struct ConsumedListing {
    prior_output: TypedOutput,
    prior_datum: Option<TypedDatum>,
}

#[derive(Default)]
struct TxBuffer {
    /// Produced listings keyed by `(policy, asset_name)`.
    produced: BTreeMap<(Vec<u8>, Vec<u8>), ProducedListing>,
    /// Cancel-redeemer consumed listings keyed the same way.
    consumed: BTreeMap<(Vec<u8>, Vec<u8>), ConsumedListing>,
    tx_hash: Option<Vec<u8>>,
}

fn handle_produced(p: &ProducedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(p.tx_hash.clone());
    }
    if !is_listing_output(&p.output.address) {
        return;
    }
    for entry in &p.output.assets {
        buf.produced.insert(
            (entry.asset.policy.clone(), entry.asset.name.clone()),
            ProducedListing {
                output_index: p.oref.index,
                output: p.output.clone(),
                datum: p.datum.clone(),
            },
        );
    }
}

fn handle_consumed(c: &ConsumedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(c.consuming_tx_hash.clone());
    }
    if !is_listing_output(&c.prior_output.address) {
        return;
    }
    let Some(redeemer_bytes) = c.redeemer.as_ref() else {
        return;
    };
    if !is_cancel_redeemer(redeemer_bytes) {
        return;
    }
    for entry in &c.prior_output.assets {
        buf.consumed.insert(
            (entry.asset.policy.clone(), entry.asset.name.clone()),
            ConsumedListing {
                prior_output: c.prior_output.clone(),
                prior_datum: c.prior_datum.clone(),
            },
        );
    }
}

/// Wayup cancel/delist redeemer: constructor 1 with empty fields
/// (`d87a80`) — verified against mainnet delists (`d1e39e43…`,
/// `81469028…`: asset returned to the datum owner's stake) and
/// batch price-updates (`383d83e2…`: 11 cancel spends + 11
/// re-produced listings in one TX). Buy spends use constructor 0.
fn is_cancel_redeemer(bytes: &[u8]) -> bool {
    bytes == [0xd8, 0x7a, 0x80]
}

fn flush_buffer(buf: TxBuffer) {
    let Some(tx_hash) = buf.tx_hash else {
        return;
    };
    let tx_hash_hex = hex::encode(&tx_hash);

    let mut consumed = buf.consumed;

    for ((policy, asset_name), produced) in buf.produced {
        let policy_hex = hex::encode(&policy);
        let asset_name_hex = hex::encode(&asset_name);

        // Wayup listing datums are hash-only; at create time the
        // preimage is usually revealed in the same TX's witness
        // set (resolvable via datum_by_hash), but emit
        // honest-about-unknowns like jpg when it isn't.
        let decoded = resolve_datum_bytes(produced.datum.as_ref())
            .and_then(|b| decode_listing_datum(&b))
            .unwrap_or(DecodedListing {
                payouts: Vec::new(),
                seller_stake_pkh: String::new(),
            });
        let new_price = decoded.payouts.iter().map(|p| p.lovelace).sum::<u64>();

        if let Some(prior) = consumed.remove(&(policy.clone(), asset_name.clone())) {
            let previous_price = resolve_datum_bytes(prior.prior_datum.as_ref())
                .and_then(|b| decode_listing_datum(&b))
                .map(|d| d.payouts.iter().map(|p| p.lovelace).sum::<u64>())
                .unwrap_or(0);
            emit_listing(&WayupStoreListing::Update(ListingUpdate {
                policy: policy_hex.clone(),
                asset_name_hex: asset_name_hex.clone(),
                tx_hash: tx_hash_hex.clone(),
                output_index: produced.output_index,
                previous_price_lovelace: previous_price,
                new_price_lovelace: new_price,
                seller_stake_pkh: decoded.seller_stake_pkh.clone(),
                payouts: decoded.payouts.clone(),
                contract_version: WayupStoreContractVersion::V1,
            }));
            let _ = prior;
        } else {
            emit_listing(&WayupStoreListing::Create(ListingCreate {
                policy: policy_hex.clone(),
                asset_name_hex: asset_name_hex.clone(),
                tx_hash: tx_hash_hex.clone(),
                output_index: produced.output_index,
                price_lovelace: new_price,
                seller_stake_pkh: decoded.seller_stake_pkh.clone(),
                payouts: decoded.payouts.clone(),
                contract_version: WayupStoreContractVersion::V1,
            }));
        }
        let _ = produced.output;
    }

    // Remaining consumed entries (no paired produced) are unlistings.
    for ((policy, asset_name), prior) in consumed {
        let policy_hex = hex::encode(&policy);
        let asset_name_hex = hex::encode(&asset_name);
        let seller_stake_pkh = resolve_datum_bytes(prior.prior_datum.as_ref())
            .and_then(|b| decode_listing_datum(&b))
            .map(|d| d.seller_stake_pkh)
            .unwrap_or_default();
        emit_listing(&WayupStoreListing::Unlisting(Unlisting {
            policy: policy_hex,
            asset_name_hex,
            tx_hash: tx_hash_hex.clone(),
            seller_stake_pkh,
            contract_version: WayupStoreContractVersion::V1,
        }));
        let _ = prior.prior_output;
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

/// Resolve the on-chain datum bytes for an output. Inline datums
/// carry `payload` directly; hash-only datums fall back to
/// `chain_data::datum_by_hash`.
fn resolve_datum_bytes(d: Option<&TypedDatum>) -> Option<Vec<u8>> {
    let d = d?;
    if !d.payload.is_empty() {
        return Some(d.payload.clone());
    }
    crate::mitos::platform_v2::chain_data::datum_by_hash(&d.hash)
}

/// Result of decoding a Wayup listing (ask) datum.
struct DecodedListing {
    payouts: Vec<ListingPayout>,
    /// Hex of the seller's STAKE credential (28 bytes).
    seller_stake_pkh: String,
}

/// Decode the Wayup listing datum:
/// `Constr 0 [ List<Payout>, Bytes(owner_stake_cred) ]` — the
/// same shape as jpg.store V1-V3 asks, except the owner bytes are
/// the seller's stake credential.
fn decode_listing_datum(cbor: &[u8]) -> Option<DecodedListing> {
    let pd: PlutusData = pallas_codec::minicbor::decode(cbor).ok()?;
    let outer = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    let fields: Vec<PlutusData> = outer.fields.into();
    if fields.len() < 2 {
        return None;
    }
    let payouts = match &fields[0] {
        PlutusData::Array(items) => items
            .iter()
            .filter_map(decode_payout)
            .collect::<Vec<ListingPayout>>(),
        _ => return None,
    };
    let seller_stake_pkh = match &fields[1] {
        PlutusData::BoundedBytes(b) => hex::encode(&**b),
        _ => String::new(),
    };
    Some(DecodedListing {
        payouts,
        seller_stake_pkh,
    })
}

/// One payout entry: `Constr 0 [ Address, Int ]` where Address is
/// `Constr 0 [ payment_cred, stake_cred ]` and payment_cred is
/// `Constr 0 [ Bytes(pkh) ]`.
fn decode_payout(pd: &PlutusData) -> Option<ListingPayout> {
    let constr = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    let fields: Vec<PlutusData> = constr.fields.clone().into();
    if fields.len() < 2 {
        return None;
    }
    let (payment_pkh, stake_pkh) = match &fields[0] {
        PlutusData::Constr(addr) => {
            let addr_fields: Vec<PlutusData> = addr.fields.clone().into();
            let payment = decode_credential_bytes(addr_fields.first())?;
            let stake = addr_fields.get(1).and_then(decode_maybe_stake);
            (payment, stake)
        }
        _ => return None,
    };
    let lovelace = match &fields[1] {
        PlutusData::BigInt(i) => decode_bigint_u64(i)?,
        _ => return None,
    };
    Some(ListingPayout {
        payment_pkh: hex::encode(payment_pkh),
        stake_pkh: stake_pkh.map(hex::encode),
        lovelace,
    })
}

/// Plutus address-credential extractor: `Constr 0 [ Bytes ]`.
fn decode_credential_bytes(pd: Option<&PlutusData>) -> Option<Vec<u8>> {
    match pd? {
        PlutusData::Constr(c) => {
            let fields: Vec<PlutusData> = c.fields.clone().into();
            match fields.first()? {
                PlutusData::BoundedBytes(b) => Some((**b).to_vec()),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Optional stake credential: `Constr 0 [ Constr 0 [ Constr 0 [ Bytes ] ] ]`
/// for `Just (StakingHash (KeyHash bytes))`; `None` for `Nothing`.
fn decode_maybe_stake(pd: &PlutusData) -> Option<Vec<u8>> {
    let outer = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    if outer.any_constructor.unwrap_or(0) != 0 {
        return None;
    }
    let outer_fields: Vec<PlutusData> = outer.fields.clone().into();
    let mut cur = outer_fields.into_iter().next()?;
    for _ in 0..3 {
        match cur {
            PlutusData::Constr(c) => {
                let f: Vec<PlutusData> = c.fields.into();
                cur = f.into_iter().next()?;
            }
            PlutusData::BoundedBytes(b) => return Some((*b).to_vec()),
            _ => return None,
        }
    }
    None
}

/// Big-int → u64 (positive only).
fn decode_bigint_u64(i: &pallas_primitives::BigInt) -> Option<u64> {
    match i {
        pallas_primitives::BigInt::Int(n) => {
            let v = i128::from(*n);
            if v < 0 {
                None
            } else {
                u64::try_from(v).ok()
            }
        }
        pallas_primitives::BigInt::BigUInt(b) => {
            let bytes: &[u8] = &**b;
            if bytes.len() > 8 {
                return None;
            }
            let mut buf = [0u8; 8];
            buf[8 - bytes.len()..].copy_from_slice(bytes);
            Some(u64::from_be_bytes(buf))
        }
        pallas_primitives::BigInt::BigNInt(_) => None,
    }
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
        match hex::decode(&cfg.payment_cred) {
            Ok(bytes) if bytes.len() == 28 => {
                let mut cred = [0u8; 28];
                cred.copy_from_slice(&bytes);
                SALE_PAYMENT_CRED.with(|c| *c.borrow_mut() = Some(cred));
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
                DispatchEvent::Utxo(UtxoEvent::Produced(p)) => handle_produced(&p, &mut buf),
                DispatchEvent::Utxo(UtxoEvent::Consumed(c)) => handle_consumed(&c, &mut buf),
                _ => {}
            }
        }
        flush_buffer(buf);
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        // Interest is fully static (declared in <name>.toml).
        Ok(())
    }

    /// No-op: event-driven modules are refilled host-side by
    /// `run_bootstrap` over the manifest `[interest]`
    /// (`payment_credentials` → `utxos_by_payment_cred`).
    fn rebootstrap(_mode: RebootstrapMode) -> Result<RebootstrapStep, String> {
        Ok(RebootstrapStep {
            done: true,
            ingested: 0,
        })
    }
}

export!(Module);
