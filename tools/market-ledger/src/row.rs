//! Decoder events → `market_events` rows.
//!
//! Row shape mirrors D1 `0005-market-events.sql` (plus a `venue` column) so the
//! ledger, the D1 upload, and Parquet share one schema. `buyer_price_lovelace`
//! folds the venue buyer fee into the settlement (jpg on-top fee; Wayup
//! `price/49` min 1 ADA unless waived; offers add no fee) — the same convention
//! the firehose uses.
//!
//! Stake fields are best-effort here (the credential the event carries, or the
//! delegation part of an address); exact parity with the firehose's stake
//! extraction is a validation-slice concern.

use cardano_assets::AssetId;
use mitos_community_events::jpg_store_listing::JpgStoreListing;
use mitos_community_events::jpg_store_offer::JpgStoreOffer;
use mitos_community_events::jpg_store_sale::JpgStoreSale;
use mitos_community_events::wayup_store_listing::WayupStoreListing;
use mitos_community_events::wayup_store_offer::WayupStoreOffer;
use mitos_community_events::wayup_store_sale::WayupStoreSale;
use pallas_addresses::{Address, ShelleyDelegationPart};

/// Chain position for the tx being decoded.
#[derive(Clone, Copy)]
pub struct BlockCtx {
    pub slot: u64,
    pub height: Option<u64>,
    pub time: u64,
}

/// One `market_events` row (≈ D1 0005 + `venue`). `source` is always `walk`.
pub struct MarketEventRow {
    pub tx_hash: String,
    pub policy_id: String,
    pub asset_name_hex: String,
    pub fingerprint: Option<String>,
    pub kind: String,
    pub price_lovelace: Option<u64>,
    pub buyer_price_lovelace: Option<u64>,
    pub seller_stake: Option<String>,
    pub buyer_stake: Option<String>,
    pub marketplace: String,
    pub bundle_size: Option<u32>,
    pub output_index: Option<u32>,
    pub fee_waived: bool,
    pub slot: u64,
    pub block_height: Option<u64>,
    pub block_time: u64,
    pub venue: String,
}

const JPG_MARKETPLACE: &str = "jpg.store";
const WAYUP_MARKETPLACE: &str = "wayup";

/// Wayup buyer fee: `price/49`, minimum 1 ADA — enforced in-script.
fn wayup_fee(price: u64) -> u64 {
    (price / 49).max(1_000_000)
}

/// CIP-14 fingerprint (`asset1…`); `None` if the hex is malformed.
fn fingerprint(policy_id: &str, asset_name_hex: &str) -> Option<String> {
    AssetId::new(policy_id.to_string(), asset_name_hex.to_string())
        .ok()
        .and_then(|a| a.fingerprint().ok())
}

/// Delegation (stake) credential hex of a Shelley address, if it has one.
fn stake_of(bech32: &str) -> Option<String> {
    let Ok(Address::Shelley(s)) = Address::from_bech32(bech32) else {
        return None;
    };
    match s.delegation() {
        ShelleyDelegationPart::Key(h) => Some(hex::encode(h.as_ref())),
        ShelleyDelegationPart::Script(h) => Some(hex::encode(h.as_ref())),
        _ => None,
    }
}

fn some_nonempty(s: &str) -> Option<String> {
    (!s.is_empty()).then(|| s.to_string())
}

// ============================================================
// sales
// ============================================================

pub fn from_jpg_sale(e: &JpgStoreSale, ctx: &BlockCtx, venue: &str) -> MarketEventRow {
    let JpgStoreSale::Sale(s) = e;
    MarketEventRow {
        tx_hash: s.tx_hash.clone(),
        policy_id: s.policy.clone(),
        asset_name_hex: s.asset_name_hex.clone(),
        fingerprint: fingerprint(&s.policy, &s.asset_name_hex),
        kind: "sold".into(),
        price_lovelace: Some(s.price_lovelace),
        buyer_price_lovelace: Some(s.price_lovelace + s.on_top_fee_lovelace),
        seller_stake: some_nonempty(&s.seller_pkh),
        buyer_stake: stake_of(&s.buyer_address),
        marketplace: JPG_MARKETPLACE.into(),
        bundle_size: s.bundle_size,
        output_index: None,
        fee_waived: false,
        slot: ctx.slot,
        block_height: ctx.height,
        block_time: ctx.time,
        venue: venue.into(),
    }
}

pub fn from_wayup_sale(e: &WayupStoreSale, ctx: &BlockCtx, venue: &str) -> MarketEventRow {
    let WayupStoreSale::Sale(s) = e;
    let buyer_price = if s.fee_waived {
        s.price_lovelace
    } else {
        s.price_lovelace + wayup_fee(s.price_lovelace)
    };
    MarketEventRow {
        tx_hash: s.tx_hash.clone(),
        policy_id: s.policy.clone(),
        asset_name_hex: s.asset_name_hex.clone(),
        fingerprint: fingerprint(&s.policy, &s.asset_name_hex),
        kind: "sold".into(),
        price_lovelace: Some(s.price_lovelace),
        buyer_price_lovelace: Some(buyer_price),
        seller_stake: some_nonempty(&s.seller_stake_pkh),
        buyer_stake: stake_of(&s.buyer_address),
        marketplace: WAYUP_MARKETPLACE.into(),
        bundle_size: s.bundle_size,
        output_index: None,
        fee_waived: s.fee_waived,
        slot: ctx.slot,
        block_height: ctx.height,
        block_time: ctx.time,
        venue: venue.into(),
    }
}

// ============================================================
// listings
// ============================================================

pub fn from_jpg_listing(e: &JpgStoreListing, ctx: &BlockCtx, venue: &str) -> MarketEventRow {
    let base = |policy: &str, asset: &str, kind: &str| MarketEventRow {
        tx_hash: String::new(),
        policy_id: policy.into(),
        asset_name_hex: asset.into(),
        fingerprint: fingerprint(policy, asset),
        kind: kind.into(),
        price_lovelace: None,
        buyer_price_lovelace: None,
        seller_stake: None,
        buyer_stake: None,
        marketplace: JPG_MARKETPLACE.into(),
        bundle_size: None,
        output_index: None,
        fee_waived: false,
        slot: ctx.slot,
        block_height: ctx.height,
        block_time: ctx.time,
        venue: venue.into(),
    };
    match e {
        JpgStoreListing::Create(c) => MarketEventRow {
            tx_hash: c.tx_hash.clone(),
            price_lovelace: Some(c.price_lovelace),
            seller_stake: some_nonempty(&c.seller_pkh),
            bundle_size: c.bundle_size,
            output_index: Some(c.output_index),
            ..base(&c.policy, &c.asset_name_hex, "listed")
        },
        JpgStoreListing::Update(u) => MarketEventRow {
            tx_hash: u.tx_hash.clone(),
            price_lovelace: Some(u.new_price_lovelace),
            seller_stake: some_nonempty(&u.seller_pkh),
            bundle_size: u.bundle_size,
            output_index: Some(u.output_index),
            ..base(&u.policy, &u.asset_name_hex, "price_change")
        },
        JpgStoreListing::Unlisting(d) => MarketEventRow {
            tx_hash: d.tx_hash.clone(),
            seller_stake: some_nonempty(&d.seller_pkh),
            bundle_size: d.bundle_size,
            ..base(&d.policy, &d.asset_name_hex, "delisted")
        },
    }
}

pub fn from_wayup_listing(e: &WayupStoreListing, ctx: &BlockCtx, venue: &str) -> MarketEventRow {
    let base = |policy: &str, asset: &str, kind: &str| MarketEventRow {
        tx_hash: String::new(),
        policy_id: policy.into(),
        asset_name_hex: asset.into(),
        fingerprint: fingerprint(policy, asset),
        kind: kind.into(),
        price_lovelace: None,
        buyer_price_lovelace: None,
        seller_stake: None,
        buyer_stake: None,
        marketplace: WAYUP_MARKETPLACE.into(),
        bundle_size: None,
        output_index: None,
        fee_waived: false,
        slot: ctx.slot,
        block_height: ctx.height,
        block_time: ctx.time,
        venue: venue.into(),
    };
    match e {
        WayupStoreListing::Create(c) => MarketEventRow {
            tx_hash: c.tx_hash.clone(),
            price_lovelace: Some(c.price_lovelace),
            seller_stake: some_nonempty(&c.seller_stake_pkh),
            bundle_size: c.bundle_size,
            output_index: Some(c.output_index),
            ..base(&c.policy, &c.asset_name_hex, "listed")
        },
        WayupStoreListing::Update(u) => MarketEventRow {
            tx_hash: u.tx_hash.clone(),
            price_lovelace: Some(u.new_price_lovelace),
            seller_stake: some_nonempty(&u.seller_stake_pkh),
            bundle_size: u.bundle_size,
            output_index: Some(u.output_index),
            ..base(&u.policy, &u.asset_name_hex, "price_change")
        },
        WayupStoreListing::Unlisting(d) => MarketEventRow {
            tx_hash: d.tx_hash.clone(),
            seller_stake: some_nonempty(&d.seller_stake_pkh),
            bundle_size: d.bundle_size,
            ..base(&d.policy, &d.asset_name_hex, "delisted")
        },
    }
}

// ============================================================
// offers (accepts → fills; create/cancel/update → book events)
// ============================================================

pub fn from_jpg_offer(e: &JpgStoreOffer, ctx: &BlockCtx, venue: &str) -> MarketEventRow {
    match e {
        JpgStoreOffer::Accept(a) => MarketEventRow {
            tx_hash: a.tx_hash.clone(),
            policy_id: a.policy.clone(),
            asset_name_hex: a.asset_name_hex.clone(),
            fingerprint: fingerprint(&a.policy, &a.asset_name_hex),
            kind: accept_kind(a.collection_offer).into(),
            price_lovelace: Some(a.price_lovelace),
            buyer_price_lovelace: Some(a.price_lovelace),
            seller_stake: stake_of(&a.seller_address),
            buyer_stake: some_nonempty(&a.bidder_pkh),
            marketplace: JPG_MARKETPLACE.into(),
            bundle_size: None,
            output_index: Some(a.prior_output_index),
            fee_waived: false,
            slot: ctx.slot,
            block_height: ctx.height,
            block_time: ctx.time,
            venue: venue.into(),
        },
        JpgStoreOffer::Create(c) => offer_book_row(
            &c.tx_hash,
            &c.bidder_pkh,
            c.target_policy.as_deref(),
            c.target_asset_names.first().map(String::as_str),
            Some(c.lovelace),
            Some(c.output_index),
            "offer_created",
            JPG_MARKETPLACE,
            ctx,
            venue,
        ),
        JpgStoreOffer::Update(u) => offer_book_row(
            &u.tx_hash,
            &u.bidder_pkh,
            u.target_policy.as_deref(),
            u.target_asset_names.first().map(String::as_str),
            Some(u.new_lovelace),
            Some(u.new_output_index),
            "offer_updated",
            JPG_MARKETPLACE,
            ctx,
            venue,
        ),
        JpgStoreOffer::Cancel(c) => offer_book_row(
            &c.tx_hash,
            &c.bidder_pkh,
            c.target_policy.as_deref(),
            None,
            None,
            None,
            "offer_cancelled",
            JPG_MARKETPLACE,
            ctx,
            venue,
        ),
    }
}

pub fn from_wayup_offer(e: &WayupStoreOffer, ctx: &BlockCtx, venue: &str) -> MarketEventRow {
    match e {
        WayupStoreOffer::Accept(a) => MarketEventRow {
            tx_hash: a.tx_hash.clone(),
            policy_id: a.policy.clone(),
            asset_name_hex: a.asset_name_hex.clone(),
            fingerprint: fingerprint(&a.policy, &a.asset_name_hex),
            kind: accept_kind(a.collection_offer).into(),
            price_lovelace: Some(a.price_lovelace),
            buyer_price_lovelace: Some(a.price_lovelace),
            seller_stake: stake_of(&a.seller_address),
            buyer_stake: some_nonempty(&a.bidder_pkh),
            marketplace: WAYUP_MARKETPLACE.into(),
            bundle_size: None,
            output_index: Some(a.prior_output_index),
            fee_waived: false,
            slot: ctx.slot,
            block_height: ctx.height,
            block_time: ctx.time,
            venue: venue.into(),
        },
        WayupStoreOffer::Create(c) => offer_book_row(
            &c.tx_hash,
            &c.bidder_pkh,
            c.target_policy.as_deref(),
            c.target_asset_names.first().map(String::as_str),
            Some(c.lovelace),
            Some(c.output_index),
            "offer_created",
            WAYUP_MARKETPLACE,
            ctx,
            venue,
        ),
        WayupStoreOffer::Update(u) => offer_book_row(
            &u.tx_hash,
            &u.bidder_pkh,
            u.target_policy.as_deref(),
            u.target_asset_names.first().map(String::as_str),
            Some(u.new_lovelace),
            Some(u.new_output_index),
            "offer_updated",
            WAYUP_MARKETPLACE,
            ctx,
            venue,
        ),
        WayupStoreOffer::Cancel(c) => offer_book_row(
            &c.tx_hash,
            &c.bidder_pkh,
            c.target_policy.as_deref(),
            None,
            None,
            None,
            "offer_cancelled",
            WAYUP_MARKETPLACE,
            ctx,
            venue,
        ),
    }
}

fn accept_kind(collection_offer: bool) -> &'static str {
    if collection_offer {
        "collection_offer_accepted"
    } else {
        "offer_accepted"
    }
}

#[allow(clippy::too_many_arguments)]
fn offer_book_row(
    tx_hash: &str,
    bidder_pkh: &str,
    target_policy: Option<&str>,
    target_asset: Option<&str>,
    lovelace: Option<u64>,
    output_index: Option<u32>,
    kind: &str,
    marketplace: &str,
    ctx: &BlockCtx,
    venue: &str,
) -> MarketEventRow {
    let policy = target_policy.unwrap_or_default();
    let asset = target_asset.unwrap_or_default();
    MarketEventRow {
        tx_hash: tx_hash.into(),
        policy_id: policy.into(),
        asset_name_hex: asset.into(),
        fingerprint: (!asset.is_empty())
            .then(|| fingerprint(policy, asset))
            .flatten(),
        kind: kind.into(),
        price_lovelace: lovelace,
        buyer_price_lovelace: None,
        seller_stake: None,
        buyer_stake: some_nonempty(bidder_pkh),
        marketplace: marketplace.into(),
        bundle_size: None,
        output_index,
        fee_waived: false,
        slot: ctx.slot,
        block_height: ctx.height,
        block_time: ctx.time,
        venue: venue.into(),
    }
}
