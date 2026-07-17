//! Wayup fixed-price sale community module.
//!
//! Emits `WayupStoreSale::Sale` when a Wayup listing is bought.
//! Listing lifecycle events (create/update/unlisting) live in the
//! sibling `wayup-store-listing` module; offer (bid) lifecycle in
//! `wayup-store-offer`, whose `Accept` is the bid-side sale.
//!
//! ## Detection
//!
//! Wayup listing UTxOs sit at addresses sharing the sale
//! validator's payment credential (`a76f0fb8…`) with a per-seller
//! staking part, so classification is by payment credential.
//!
//! - `Consumed` at the sale credential with a Buy redeemer —
//!   constructor 0 with one uint field that varies per spend (an
//!   input index: `d8799f00ff`, `d8799f09ff`, … observed in
//!   mainnet sweep `4edc97fd…` buying 4 listings with fields
//!   0/3/6/9). Matching is on the `d879` constructor prefix,
//!   never the full bytes. Cancel spends (constructor 1,
//!   `d87a80`) are the listing module's domain.
//! - The consumed listing's datum
//!   (`Constr 0 [ payouts, owner_stake_cred ]`, hash-only —
//!   revealed in the sale TX's witness set) gives the agreed
//!   payouts; price = their sum. Wayup's platform fee (2%,
//!   1-10 ADA) is paid on top by the buyer and is NOT part of
//!   the payouts.
//! - The asset moves to a non-listing output — the buyer. Offer
//!   outputs created in the same TX (mixed buy+bid TXs exist)
//!   carry no assets and never match.

use std::cell::RefCell;
use std::collections::BTreeMap;

use mitos_community_events::wayup_store_sale::{
    ListingPayout, Sale, WayupStoreContractVersion, WayupStoreSale,
};
use pallas_addresses::{Address, ShelleyPaymentPart};
use pallas_primitives::PlutusData;
use serde::Deserialize;

use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{ConsumedEvent, ProducedEvent, TypedDatum, UtxoEvent};

const LOG_TARGET: &str = "wayup-store-sale-module";

#[derive(Debug, Clone, Deserialize)]
struct Config {
    /// 56-char hex of the Wayup sale validator's payment
    /// credential.
    #[serde(default)]
    payment_cred: String,
}

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

/// Sale-side state for one consumed listing UTxO.
#[derive(Clone)]
struct PendingSale {
    seller_stake_pkh: String,
    payouts: Vec<ListingPayout>,
    price_lovelace: u64,
}

#[derive(Default)]
struct TxBuffer {
    /// `(policy, asset_name)` → PendingSale, populated from
    /// Buy-redeemer consumes at the sale credential.
    pending: BTreeMap<(Vec<u8>, Vec<u8>), PendingSale>,
    /// Produced outputs buffered for buyer matching at flush.
    produced: Vec<(String, Vec<(Vec<u8>, Vec<u8>)>)>,
    tx_hash: Option<Vec<u8>>,
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
    if !is_buy_redeemer(redeemer_bytes) {
        return;
    }
    let Some(datum_bytes) = resolve_datum_bytes(c.prior_datum.as_ref()) else {
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            "consumed listing has no resolvable datum; skipping sale emit",
        );
        return;
    };
    let Some(decoded) = decode_listing_datum(&datum_bytes) else {
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            "consumed listing datum didn't match expected shape; skipping",
        );
        return;
    };
    let price_lovelace = decoded.payouts.iter().map(|p| p.lovelace).sum::<u64>();
    for entry in &c.prior_output.assets {
        buf.pending.insert(
            (entry.asset.policy.clone(), entry.asset.name.clone()),
            PendingSale {
                seller_stake_pkh: decoded.seller_stake_pkh.clone(),
                payouts: decoded.payouts.clone(),
                price_lovelace,
            },
        );
    }
}

fn handle_produced(p: &ProducedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(p.tx_hash.clone());
    }
    // Skip outputs back at the sale credential — those are listing
    // updates (the listing module's domain). Sales deliver the
    // asset to a non-listing address (the buyer).
    if is_listing_output(&p.output.address) {
        return;
    }
    let assets: Vec<(Vec<u8>, Vec<u8>)> = p
        .output
        .assets
        .iter()
        .map(|a| (a.asset.policy.clone(), a.asset.name.clone()))
        .collect();
    if !assets.is_empty() {
        buf.produced.push((p.output.address.clone(), assets));
    }
}

fn flush_buffer(buf: TxBuffer) {
    let Some(tx_hash) = buf.tx_hash else {
        return;
    };
    let tx_hash_hex = hex::encode(&tx_hash);
    let mut pending = buf.pending;

    for (buyer_address, assets) in buf.produced {
        for asset_key in assets {
            let Some(sale) = pending.remove(&asset_key) else {
                continue;
            };
            let (policy, asset_name) = asset_key;
            emit_sale(&WayupStoreSale::Sale(Sale {
                policy: hex::encode(&policy),
                asset_name_hex: hex::encode(&asset_name),
                tx_hash: tx_hash_hex.clone(),
                seller_stake_pkh: sale.seller_stake_pkh,
                buyer_address: buyer_address.clone(),
                payouts: sale.payouts,
                price_lovelace: sale.price_lovelace,
                contract_version: WayupStoreContractVersion::V1,
            }));
        }
    }

    for ((policy, asset_name), _) in pending {
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            &format!(
                "consumed listing {}/{} had Buy redeemer but no produced output matched as buyer",
                hex::encode(&policy),
                hex::encode(&asset_name)
            ),
        );
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

/// Wayup Buy redeemer: constructor 0 with one uint field whose
/// value varies per spend (an input index — `d8799f00ff`,
/// `d8799f03ff`, `d8799f09ff` all observed on mainnet). Match on
/// the constructor prefix only.
fn is_buy_redeemer(bytes: &[u8]) -> bool {
    bytes.starts_with(&[0xd8, 0x79])
}

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
/// `Constr 0 [ List<Payout>, Bytes(owner_stake_cred) ]`.
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

/// One payout entry: `Constr 0 [ Address, Int ]`.
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

/// Optional stake credential (`Just (StakingHash (KeyHash b))`).
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
    /// `run_bootstrap` over the manifest `[interest]`.
    fn rebootstrap(_mode: RebootstrapMode) -> Result<RebootstrapStep, String> {
        Ok(RebootstrapStep {
            done: true,
            ingested: 0,
        })
    }
}

export!(Module);
