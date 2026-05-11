//! jpg.store listing lifecycle community module.
//!
//! Emits `JpgStoreListing::{Create, Update, Unlisting}` for
//! the jpg.store sale contracts (V1/V2/V3/V4). Sale events
//! (asset moving to buyer + payment outputs to seller) live in
//! the sibling `jpg-store-sale` module to keep concerns
//! separable on the consumer side.
//!
//! ## Detection
//!
//! Within one `handle_events` call (= one TX's events) we
//! observe what touched the jpg.store sale scripts:
//!
//! - `Produced` at a jpg-script with a listing datum → record
//!   it in `produced_listings` keyed by `(policy, asset_name)`.
//! - `Consumed` at a jpg-script → record in `consumed_listings`
//!   keyed the same way. Redeemer constructor distinguishes
//!   action: 0 = Buy/Sale (skipped — jpg-store-sale's domain),
//!   1 = Cancel (Unlisting if no paired Produced for same asset
//!   in this TX; Update if paired).
//!
//! At flush time:
//!
//! - `(consumed, produced)` for same asset → `ListingUpdate`
//! - `produced` alone → `ListingCreate`
//! - `consumed` alone (cancel redeemer) → `Unlisting`
//!
//! ## Datum shape (V1/V2/V3 share the same payouts+owner_pkh
//! structure; V4 is documented to use a different datum but
//! production TXs to validate the shape against haven't been
//! sampled yet — falls through to "unknown shape" emission
//! with `payouts: []` until a V4 fixture confirms).

use std::collections::BTreeMap;

use mitos_community_events::jpg_store_listing::{
    JpgStoreContractVersion, JpgStoreListing, ListingCreate, ListingPayout, ListingUpdate,
    Unlisting,
};
use pallas_primitives::PlutusData;

use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{
    ConsumedEvent, ProducedEvent, TypedDatum, TypedOutput, UtxoEvent,
};

const LOG_TARGET: &str = "jpg-store-listing-module";

const JPG_V1_ADDR: &str = "addr1zxgx3far7qygq0k6epa0zcvcvrevmn0ypsnfsue94nsn3tvpw288a4x0xf8pxgcntelxmyclq83s0ykeehchz2wtspks905plm";
const JPG_V2_ADDR: &str = "addr1x8rjw3pawl0kelu4mj3c8x20fsczf5pl744s9mxz9v8n7efvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8ekstg4qrx";
const JPG_V3_ADDR: &str = "addr1w8rjw3pawl0kelu4mj3c8x20fsczf5pl744s9mxz9v8n7efvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8ekstg4qrx";
const JPG_V4_ADDR: &str = "addr1w999n67e47he8y0v36hjtzluargwu25zw94f6lqnm82aqqsg4xkcp";

fn classify_address(addr: &str) -> Option<JpgStoreContractVersion> {
    match addr {
        JPG_V1_ADDR => Some(JpgStoreContractVersion::V1),
        JPG_V2_ADDR => Some(JpgStoreContractVersion::V2),
        JPG_V3_ADDR => Some(JpgStoreContractVersion::V3),
        JPG_V4_ADDR => Some(JpgStoreContractVersion::V4),
        _ => None,
    }
}

/// One Produced output at a jpg-script that we've recorded. We
/// snapshot the bits the listing event shape needs — full TypedOutput
/// is kept around for the asset-multiset walk during flush.
#[derive(Clone)]
struct ProducedListing {
    output_index: u32,
    output: TypedOutput,
    datum: Option<TypedDatum>,
    contract_version: JpgStoreContractVersion,
}

#[derive(Clone)]
struct ConsumedListing {
    prior_output: TypedOutput,
    prior_datum: Option<TypedDatum>,
    redeemer: Option<Vec<u8>>,
    contract_version: JpgStoreContractVersion,
}

#[derive(Default)]
struct TxBuffer {
    /// Produced listings keyed by `(policy, asset_name)`. Multi-
    /// asset bundles get one entry per asset — most jpg.store
    /// listings are single-asset, so this is typically 0 or 1
    /// entries.
    produced: BTreeMap<(Vec<u8>, Vec<u8>), ProducedListing>,
    /// Consumed listings keyed the same way.
    consumed: BTreeMap<(Vec<u8>, Vec<u8>), ConsumedListing>,
    /// tx_hash from the first event seen in this batch.
    tx_hash: Option<Vec<u8>>,
}

fn handle_produced(p: &ProducedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(p.tx_hash.clone());
    }
    let Some(version) = classify_address(&p.output.address) else {
        return;
    };
    // Index every native asset in the output. Real jpg.store
    // listings are usually single-asset but multi-asset bundles
    // exist; emit one event per asset under the same listing UTxO.
    for entry in &p.output.assets {
        buf.produced.insert(
            (entry.asset.policy.clone(), entry.asset.name.clone()),
            ProducedListing {
                output_index: p.oref.index,
                output: p.output.clone(),
                datum: p.datum.clone(),
                contract_version: version,
            },
        );
    }
}

fn handle_consumed(c: &ConsumedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(c.consuming_tx_hash.clone());
    }
    let Some(version) = classify_address(&c.prior_output.address) else {
        return;
    };
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
                redeemer: c.redeemer.clone(),
                contract_version: version,
            },
        );
    }
}

/// jpg.store V1/V2/V3 use a unit-constructor redeemer where
/// Buy = constructor 0 and Cancel = constructor 1, both with
/// empty fields. The on-wire CBOR for Cancel is `d87a80`
/// (tag 122 = alternative 1 empty fields).
fn is_cancel_redeemer(bytes: &[u8]) -> bool {
    // Minimal sniff: Cancel = `d87a80`. Anything else (Buy = `d87980`,
    // or richer redeemers V4 might introduce) → not our concern.
    bytes == [0xd8, 0x7a, 0x80]
}

fn flush_buffer(buf: TxBuffer) {
    let Some(tx_hash) = buf.tx_hash else {
        return;
    };
    let tx_hash_hex = hex::encode(&tx_hash);

    // Consume `consumed` into a working map so we can pop pairs
    // out as we walk produced.
    let mut consumed = buf.consumed;

    for ((policy, asset_name), produced) in buf.produced {
        let policy_hex = hex::encode(&policy);
        let asset_name_hex = hex::encode(&asset_name);

        // Decode the produced listing's datum into payouts +
        // owner. jpg.store V1/V2/V3 listings use hash-only
        // datums — the datum bytes don't exist on-chain until
        // the listing is later consumed (cancel or sale), at
        // which point they're revealed in that TX's witness set.
        // So at listing time the resolution often returns None;
        // we emit with empty payouts + empty seller_pkh, and
        // downstream consumers either backfill from the
        // subsequent Unlisting/Sale event or use an off-chain
        // source. Honest-about-unknowns is preferable to
        // skipping the create event entirely.
        let decoded = resolve_datum_bytes(produced.datum.as_ref())
            .and_then(|b| decode_listing_datum(&b))
            .unwrap_or(DecodedListing {
                payouts: Vec::new(),
                seller_pkh: String::new(),
            });
        let new_price = decoded.payouts.iter().map(|p| p.lovelace).sum::<u64>();

        // Is there a matching Consumed for the same asset?
        if let Some(prior) = consumed.remove(&(policy.clone(), asset_name.clone())) {
            // Listing update: extract previous price from the
            // prior datum (best-effort; if decode fails we emit
            // ListingUpdate with `previous_price_lovelace = 0`).
            let previous_price = resolve_datum_bytes(prior.prior_datum.as_ref())
                .and_then(|b| decode_listing_datum(&b))
                .map(|d| d.payouts.iter().map(|p| p.lovelace).sum::<u64>())
                .unwrap_or(0);
            emit_listing(&JpgStoreListing::Update(ListingUpdate {
                policy: policy_hex.clone(),
                asset_name_hex: asset_name_hex.clone(),
                tx_hash: tx_hash_hex.clone(),
                output_index: produced.output_index,
                previous_price_lovelace: previous_price,
                new_price_lovelace: new_price,
                seller_pkh: decoded.seller_pkh.clone(),
                payouts: decoded.payouts.clone(),
                contract_version: produced.contract_version,
            }));
            let _ = prior;
        } else {
            emit_listing(&JpgStoreListing::Create(ListingCreate {
                policy: policy_hex.clone(),
                asset_name_hex: asset_name_hex.clone(),
                tx_hash: tx_hash_hex.clone(),
                output_index: produced.output_index,
                price_lovelace: new_price,
                seller_pkh: decoded.seller_pkh.clone(),
                payouts: decoded.payouts.clone(),
                contract_version: produced.contract_version,
            }));
        }
        // `produced.output` consumed implicitly via the loop.
        let _ = produced.output;
    }

    // Remaining consumed entries (no paired produced for the
    // asset) are unlistings.
    for ((policy, asset_name), prior) in consumed {
        let policy_hex = hex::encode(&policy);
        let asset_name_hex = hex::encode(&asset_name);
        let seller_pkh = resolve_datum_bytes(prior.prior_datum.as_ref())
            .and_then(|b| decode_listing_datum(&b))
            .map(|d| d.seller_pkh)
            .unwrap_or_default();
        emit_listing(&JpgStoreListing::Unlisting(Unlisting {
            policy: policy_hex,
            asset_name_hex,
            tx_hash: tx_hash_hex.clone(),
            seller_pkh,
            contract_version: prior.contract_version,
        }));
        let _ = prior.prior_output;
        let _ = prior.redeemer;
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

/// Resolve the on-chain datum bytes for an output. Inline
/// datums carry `payload` directly; hash-only datums fall back
/// to `chain_data::datum_by_hash`.
fn resolve_datum_bytes(d: Option<&TypedDatum>) -> Option<Vec<u8>> {
    let d = d?;
    if !d.payload.is_empty() {
        return Some(d.payload.clone());
    }
    crate::mitos::platform_v2::chain_data::datum_by_hash(&d.hash)
}

/// Result of decoding a jpg.store V1/V2/V3 listing datum.
struct DecodedListing {
    payouts: Vec<ListingPayout>,
    /// Hex of the seller's payment-credential pkh (28 bytes).
    seller_pkh: String,
}

/// Decode V1/V2/V3 listing datum:
/// `Constr 0 [ List<Payout>, Bytes(owner_pkh) ]`.
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
    let seller_pkh = match &fields[1] {
        PlutusData::BoundedBytes(b) => hex::encode(&**b),
        _ => String::new(),
    };
    Some(DecodedListing {
        payouts,
        seller_pkh,
    })
}

/// One payout entry: `Constr 0 [ Address, Int ]` where Address
/// is `Constr 0 [ payment_cred, stake_cred ]` and payment_cred
/// is `Constr 0 [ Bytes(pkh) ]`. Stake credential is a nested
/// Maybe of the same shape; we extract bytes when present,
/// fall through to `None` when not.
fn decode_payout(pd: &PlutusData) -> Option<ListingPayout> {
    let constr = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    let fields: Vec<PlutusData> = constr.fields.clone().into();
    if fields.len() < 2 {
        return None;
    }
    // Address constructor 0 [payment_cred, stake_cred]
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
/// for `Just (StakingHash (KeyHash bytes))`. Returns `None` for
/// `Nothing` (constructor 1) or shape mismatch.
fn decode_maybe_stake(pd: &PlutusData) -> Option<Vec<u8>> {
    let outer = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    // `Just` constructor (0) wraps `StakingHash (KeyHash bytes)`.
    // We unwrap one constr level at a time, taking the first
    // field each time, until we hit a BoundedBytes.
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

/// Big-int → u64 (positive only — payouts are always positive
/// lovelace amounts; negative inputs return `None`).
fn decode_bigint_u64(i: &pallas_primitives::BigInt) -> Option<u64> {
    match i {
        pallas_primitives::BigInt::Int(n) => {
            let v = i128::from(*n);
            if v < 0 { None } else { u64::try_from(v).ok() }
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
        // Interest is fully static (declared in <name>.toml); no
        // runtime updates needed.
        Ok(())
    }
}

export!(Module);
