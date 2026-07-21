//! Sale assembly: match consumed listings to the produced output that received
//! the asset (the buyer), and project to the venue wire events.
//!
//! The matching is venue-agnostic ([`collect_sales`]); the two public
//! entrypoints supply the venue's address classification and map the result to
//! `mitos_community_events`' `JpgStoreSale` / `WayupStoreSale`.

use std::collections::BTreeMap;

use mitos_community_events::jpg_store_listing::JpgStoreContractVersion;
use mitos_community_events::jpg_store_sale::{JpgStoreSale, Sale as JpgSale};
use mitos_community_events::marketplace::ListingPayout;
use mitos_community_events::wayup_store_listing::WayupStoreContractVersion;
use mitos_community_events::wayup_store_sale::{Sale as WayupSale, WayupStoreSale};
use pallas_addresses::{Address, ShelleyPaymentPart};

use crate::DecodeTx;
use crate::datum::{decode_listing_datum, is_buy_redeemer};

/// A matched sale before venue projection: one asset, the buyer that received
/// it, and the decoded listing terms. `tag` carries any venue-specific datum
/// the classifier attached to the listing address (e.g. jpg contract version).
#[derive(Debug, Clone)]
pub struct MatchedSale<T> {
    pub policy: Vec<u8>,
    pub asset_name: Vec<u8>,
    pub tx_hash: Vec<u8>,
    pub buyer_address: String,
    /// Owner credential hex from the listing datum (jpg: seller payment pkh;
    /// Wayup: seller stake credential).
    pub cred_hex: String,
    pub payouts: Vec<ListingPayout>,
    pub price_lovelace: u64,
    pub bundle_size: Option<u32>,
    pub tag: T,
}

struct Pending<T> {
    cred_hex: String,
    payouts: Vec<ListingPayout>,
    price_lovelace: u64,
    bundle_size: Option<u32>,
    tag: T,
}

/// Match Buy-redeemer listing consumes to their buyer outputs.
///
/// `classify` returns `Some(tag)` for an address that belongs to the venue's
/// sale contract (and `None` otherwise); it gates both which inputs are
/// listings and which outputs to skip (assets sent back to the venue are
/// listing updates, not sales). A listing input is a sale iff it is at the
/// venue, spent with a Buy redeemer, and its datum decodes to a payout list.
pub fn collect_sales<T: Clone>(
    tx: &DecodeTx,
    classify: impl Fn(&str) -> Option<T>,
) -> Vec<MatchedSale<T>> {
    let mut pending: BTreeMap<(Vec<u8>, Vec<u8>), Pending<T>> = BTreeMap::new();

    for input in &tx.inputs {
        let Some(tag) = classify(&input.address) else {
            continue;
        };
        let Some(redeemer) = input.redeemer.as_ref() else {
            continue;
        };
        if !is_buy_redeemer(redeemer) {
            continue;
        }
        let Some(datum) = input.datum.as_ref() else {
            continue;
        };
        let Some(decoded) = decode_listing_datum(datum) else {
            continue;
        };
        let price_lovelace = decoded.payouts.iter().map(|p| p.lovelace).sum::<u64>();
        let bundle_size = (input.assets.len() > 1).then_some(input.assets.len() as u32);
        for asset in &input.assets {
            pending.insert(
                (asset.policy.clone(), asset.name.clone()),
                Pending {
                    cred_hex: decoded.cred_hex.clone(),
                    payouts: decoded.payouts.clone(),
                    price_lovelace,
                    bundle_size,
                    tag: tag.clone(),
                },
            );
        }
    }

    let mut sales = Vec::new();
    for output in &tx.outputs {
        // Assets sent back to the venue are listing updates, not a buyer.
        if classify(&output.address).is_some() {
            continue;
        }
        for asset in &output.assets {
            let Some(p) = pending.remove(&(asset.policy.clone(), asset.name.clone())) else {
                continue;
            };
            sales.push(MatchedSale {
                policy: asset.policy.clone(),
                asset_name: asset.name.clone(),
                tx_hash: tx.tx_hash.clone(),
                buyer_address: output.address.clone(),
                cred_hex: p.cred_hex,
                payouts: p.payouts,
                price_lovelace: p.price_lovelace,
                bundle_size: p.bundle_size,
                tag: p.tag,
            });
        }
    }
    sales
}

// ============================================================
// jpg.store
// ============================================================

const JPG_V1_ADDR: &str = "addr1zxgx3far7qygq0k6epa0zcvcvrevmn0ypsnfsue94nsn3tvpw288a4x0xf8pxgcntelxmyclq83s0ykeehchz2wtspks905plm";
const JPG_V2_ADDR: &str = "addr1x8rjw3pawl0kelu4mj3c8x20fsczf5pl744s9mxz9v8n7efvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8ekstg4qrx";
const JPG_V3_ADDR: &str = "addr1w8rjw3pawl0kelu4mj3c8x20fsczf5pl744s9mxz9v8n7efvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8ekstg4qrx";
const JPG_V4_ADDR: &str = "addr1w999n67e47he8y0v36hjtzluargwu25zw94f6lqnm82aqqsg4xkcp";

/// Classify a jpg.store sale-contract address to its version, or `None`.
pub fn classify_jpg_address(addr: &str) -> Option<JpgStoreContractVersion> {
    match addr {
        JPG_V1_ADDR => Some(JpgStoreContractVersion::V1),
        JPG_V2_ADDR => Some(JpgStoreContractVersion::V2),
        JPG_V3_ADDR => Some(JpgStoreContractVersion::V3),
        JPG_V4_ADDR => Some(JpgStoreContractVersion::V4),
        _ => None,
    }
}

/// Decode every completed jpg.store sale in a transaction.
///
/// Datum-authoritative (price = sum of the listing datum's payouts). jpg V4
/// listings carry no payout datum, so they are skipped here — matching the
/// live module's current behaviour; V4 ADA-flow decode is a separate concern.
pub fn decode_jpg_sales(tx: &DecodeTx) -> Vec<JpgStoreSale> {
    collect_sales(tx, classify_jpg_address)
        .into_iter()
        .map(|s| {
            JpgStoreSale::Sale(JpgSale {
                policy: hex::encode(&s.policy),
                asset_name_hex: hex::encode(&s.asset_name),
                tx_hash: hex::encode(&s.tx_hash),
                seller_pkh: s.cred_hex,
                buyer_address: s.buyer_address,
                payouts: s.payouts,
                price_lovelace: s.price_lovelace,
                contract_version: s.tag,
                bundle_size: s.bundle_size,
            })
        })
        .collect()
}

// ============================================================
// Wayup
// ============================================================

/// Static configuration for the Wayup sale decode — the sale validator's
/// payment credential (listings sit at addresses sharing it, staking part
/// varying per seller) and, optionally, Wayup's fee-address payment credential
/// for fee-waiver detection.
#[derive(Debug, Clone, Default)]
pub struct WayupSaleConfig {
    sale_payment_cred: Option<[u8; 28]>,
    fee_payment_cred: Option<[u8; 28]>,
}

impl WayupSaleConfig {
    /// Build from 56-char hex credentials. An empty/invalid `fee_cred_hex`
    /// leaves waiver detection off (`fee_waived` stays `false`), which is the
    /// safe default — an unconfigured fee cred can't distinguish a waiver from
    /// a missed output.
    pub fn from_hex(sale_cred_hex: &str, fee_cred_hex: &str) -> Self {
        Self {
            sale_payment_cred: parse_cred(sale_cred_hex),
            fee_payment_cred: parse_cred(fee_cred_hex),
        }
    }

    /// Whether an address is a Wayup sale listing (shares the sale validator's
    /// payment credential). Exposed so callers can gate expensive per-input
    /// work (e.g. host datum resolution) to listing consumes only.
    pub fn is_listing_address(&self, addr: &str) -> bool {
        match self.sale_payment_cred {
            Some(cred) => address_payment_cred(addr) == Some(cred),
            None => false,
        }
    }

    fn is_fee_output(&self, addr: &str) -> bool {
        match self.fee_payment_cred {
            Some(cred) => address_payment_cred(addr) == Some(cred),
            None => false,
        }
    }
}

/// Decode every completed Wayup fixed-price sale in a transaction.
///
/// Fee-waiver detection is per-TX (Wayup's co-signature waiver is all-or-nothing
/// for a transaction): a sale is `fee_waived` when the fee cred is configured
/// and no output pays it.
pub fn decode_wayup_sales(tx: &DecodeTx, cfg: &WayupSaleConfig) -> Vec<WayupStoreSale> {
    let fee_waived =
        cfg.fee_payment_cred.is_some() && !tx.outputs.iter().any(|o| cfg.is_fee_output(&o.address));

    collect_sales(tx, |addr| cfg.is_listing_address(addr).then_some(()))
        .into_iter()
        .map(|s| {
            WayupStoreSale::Sale(WayupSale {
                policy: hex::encode(&s.policy),
                asset_name_hex: hex::encode(&s.asset_name),
                tx_hash: hex::encode(&s.tx_hash),
                seller_stake_pkh: s.cred_hex,
                buyer_address: s.buyer_address,
                payouts: s.payouts,
                price_lovelace: s.price_lovelace,
                contract_version: WayupStoreContractVersion::V1,
                bundle_size: s.bundle_size,
                fee_waived,
            })
        })
        .collect()
}

/// Extract a Shelley address's 28-byte payment credential.
pub fn address_payment_cred(addr: &str) -> Option<[u8; 28]> {
    let Ok(Address::Shelley(s)) = Address::from_bech32(addr) else {
        return None;
    };
    Some(match s.payment() {
        ShelleyPaymentPart::Key(h) => **h,
        ShelleyPaymentPart::Script(h) => **h,
    })
}

fn parse_cred(hex_str: &str) -> Option<[u8; 28]> {
    if hex_str.is_empty() {
        return None;
    }
    let bytes = hex::decode(hex_str).ok()?;
    if bytes.len() != 28 {
        return None;
    }
    let mut cred = [0u8; 28];
    cred.copy_from_slice(&bytes);
    Some(cred)
}

#[cfg(test)]
mod tests {
    use super::*;

    // A real jpg.store payment-script address, used here only as a stable
    // Shelley address to exercise the credential extraction/match path.
    const SHELLEY_ADDR: &str = "addr1x8rjw3pawl0kelu4mj3c8x20fsczf5pl744s9mxz9v8n7efvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8ekstg4qrx";

    #[test]
    fn cred_parsing_rejects_wrong_length_and_empty() {
        assert!(parse_cred("").is_none());
        assert!(parse_cred("ab").is_none()); // 1 byte, not 28
        assert!(parse_cred(&"aa".repeat(28)).is_some());
        assert!(parse_cred("zz").is_none()); // not hex
    }

    #[test]
    fn wayup_config_matches_own_derived_cred() {
        let cred = address_payment_cred(SHELLEY_ADDR).expect("shelley addr has a cred");
        let cfg = WayupSaleConfig::from_hex(&hex::encode(cred), "");
        assert!(cfg.is_listing_address(SHELLEY_ADDR));
        // Unconfigured fee cred → waiver detection off.
        assert!(cfg.fee_payment_cred.is_none());
        // A byte-shaped-but-different address must not match.
        assert!(!cfg.is_listing_address("addr1qxy"));
    }

    #[test]
    fn empty_config_never_classifies() {
        let cfg = WayupSaleConfig::default();
        assert!(!cfg.is_listing_address(SHELLEY_ADDR));
    }

    #[test]
    fn classify_jpg_versions() {
        assert_eq!(
            classify_jpg_address(JPG_V1_ADDR),
            Some(JpgStoreContractVersion::V1)
        );
        assert_eq!(
            classify_jpg_address(JPG_V4_ADDR),
            Some(JpgStoreContractVersion::V4)
        );
        assert!(classify_jpg_address("addr1notjpg").is_none());
    }
}
