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
use pallas_addresses::{Address, ShelleyDelegationPart, ShelleyPaymentPart};

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
/// sale contract (and `None` otherwise); it gates which inputs are listings and
/// carries the venue tag. A listing input is a sale iff it is at the venue,
/// spent with a Buy redeemer, and its datum decodes to a payout list.
///
/// `is_marketplace_escrow` returns `true` for *any* known marketplace sale
/// escrow (not just this venue's). An asset produced back to such an address is
/// a listing update (same venue) or a cross-venue migration (a different
/// venue) — never a buyer delivery — so it is skipped. Without this,
/// re-listing an NFT from one marketplace's escrow to another's is
/// mis-booked as a sale to the receiving contract.
///
/// Two spend actions share the Buy redeemer's `d879` constructor prefix, so a
/// consumed listing whose NFT goes *back to the lister* — a reclaim to their own
/// wallet, or a self-directed move — is not a sale either. Such an output is
/// dropped when its payment or stake credential matches the listing's owner
/// credential ([`address_bears_cred`]). Without this, ~71% of jpg "sales" are
/// phantom (the owner reclaiming/relisting their own NFT), because the receiving
/// wallet is not a marketplace escrow and so escapes the check above.
pub fn collect_sales<T: Clone>(
    tx: &DecodeTx,
    classify: impl Fn(&str) -> Option<T>,
    is_marketplace_escrow: impl Fn(&str) -> bool,
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
        // Assets re-escrowed at a marketplace sale contract are not a buyer
        // delivery — whether the same venue (a listing / price update) or a
        // different one (a cross-venue migration, e.g. jpg → Wayup).
        if classify(&output.address).is_some() || is_marketplace_escrow(&output.address) {
            continue;
        }
        for asset in &output.assets {
            let Some(p) = pending.remove(&(asset.policy.clone(), asset.name.clone())) else {
                continue;
            };
            // The NFT returned to the lister's own credential (jpg: owner payment
            // pkh; Wayup: owner stake cred) → a reclaim or self-directed move, not
            // a buyer. (Re-lists onto a marketplace escrow are caught above; this
            // catches a return to the owner's own wallet.)
            if address_bears_cred(&output.address, &p.cred_hex) {
                continue;
            }
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

/// Wayup's mainnet sale-validator payment credential. Wayup listings sit at
/// addresses sharing this cred (the staking part varies per seller). Baked in —
/// alongside the hardcoded jpg addresses above — so [`is_marketplace_escrow`]
/// can recognise a re-listing onto Wayup without every caller threading Wayup's
/// config through. (Confirmed on-chain 2026-08-02, tx `084bf145…`: a seller
/// migrated two jpg V1 listings to this cred in one tx.)
const WAYUP_SALE_CRED: [u8; 28] = [
    0xa7, 0x6f, 0x0f, 0xb8, 0x01, 0xa2, 0x9f, 0x59, 0x1e, 0x98, 0x71, 0x57, 0x65, 0x08, 0xd8, 0x5b,
    0x0b, 0x5f, 0x3c, 0x38, 0x77, 0x4f, 0x65, 0x03, 0x2f, 0x58, 0xfd, 0xad,
];

/// Whether `addr` is any known marketplace **sale** escrow — jpg V1–V4 (exact
/// address) or Wayup (payment credential). The sale matcher uses this to tell a
/// buyer delivery (asset leaves the marketplace to a wallet) from a re-listing
/// (asset re-escrowed at some marketplace, same venue or another). See
/// [`collect_sales`].
pub fn is_marketplace_escrow(addr: &str) -> bool {
    classify_jpg_address(addr).is_some() || address_payment_cred(addr) == Some(WAYUP_SALE_CRED)
}

/// Payment credential of jpg.store's fee-collection address. jpg-frontend-era
/// listings carried the platform fee INSIDE the datum payouts (paid to this
/// credential); WayUp, settling the surviving jpg book, charges its fee as an
/// extra contract-enforced output to the SAME address that is NOT a datum
/// payout. `outputs here − payouts here` is therefore the buyer-side on-top
/// fee, correct for both generations. (Verified on-chain 2026-07-22, tx
/// `1abb0f60…`: payouts 950+30=980 plus a 20 ADA fee output → buyer paid
/// 1000, matching WayUp's displayed price.)
const JPG_FEE_CRED_HEX: &str = "84cc25ea4c29951d40b443b95bbc5676bc425470f96376d1984af9ab";

/// Decode every completed jpg.store sale in a transaction.
///
/// Datum-authoritative (price = sum of the listing datum's payouts). jpg V4
/// listings carry no payout datum, so they are skipped here — matching the
/// live module's current behaviour; V4 ADA-flow decode is a separate concern.
///
/// Each sale also carries its `on_top_fee_lovelace` share (see
/// [`JPG_FEE_CRED_HEX`]): the tx-level on-top fee attributed pro-rata by
/// settlement across the tx's listings. Bundle members repeat their listing's
/// whole share, mirroring how they repeat the whole-bundle price.
pub fn decode_jpg_sales(tx: &DecodeTx) -> Vec<JpgStoreSale> {
    let matched = collect_sales(tx, classify_jpg_address, is_marketplace_escrow);
    if matched.is_empty() {
        return Vec::new();
    }

    let fee_cred = parse_cred(JPG_FEE_CRED_HEX);
    let outputs_to_fees: u64 = tx
        .outputs
        .iter()
        .filter(|o| address_payment_cred(&o.address) == fee_cred)
        .map(|o| o.lovelace)
        .sum();
    // Listing-level totals. `matched` repeats a bundle listing per member, so
    // re-derive from the listing inputs (same gate as `collect_sales`).
    let mut listings_total: u64 = 0;
    let mut payouts_to_fees: u64 = 0;
    for input in &tx.inputs {
        if classify_jpg_address(&input.address).is_none() {
            continue;
        }
        let Some(redeemer) = input.redeemer.as_ref() else {
            continue;
        };
        if !is_buy_redeemer(redeemer) {
            continue;
        }
        let Some(decoded) = input.datum.as_deref().and_then(decode_listing_datum) else {
            continue;
        };
        listings_total += decoded.payouts.iter().map(|p| p.lovelace).sum::<u64>();
        payouts_to_fees += decoded
            .payouts
            .iter()
            .filter(|p| p.payment_pkh == JPG_FEE_CRED_HEX)
            .map(|p| p.lovelace)
            .sum::<u64>();
    }
    let on_top_total = outputs_to_fees.saturating_sub(payouts_to_fees);

    matched
        .into_iter()
        .map(|s| {
            let on_top_fee_lovelace = if on_top_total == 0 || listings_total == 0 {
                0
            } else {
                (u128::from(on_top_total) * u128::from(s.price_lovelace)
                    / u128::from(listings_total)) as u64
            };
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
                on_top_fee_lovelace,
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

    collect_sales(
        tx,
        |addr| cfg.is_listing_address(addr).then_some(()),
        is_marketplace_escrow,
    )
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

/// Whether `addr`'s payment OR stake credential equals `cred_hex` (56-char hex).
/// The listing owner credential is a payment pkh for jpg and a stake credential
/// for Wayup, so checking both parts makes the "returned to the lister" test
/// venue-agnostic. Empty/undecodable `cred_hex` never matches — leaving those
/// rows to the escrow check alone, which is the safe (no-drop) default.
fn address_bears_cred(addr: &str, cred_hex: &str) -> bool {
    if cred_hex.is_empty() {
        return false;
    }
    let Ok(Address::Shelley(s)) = Address::from_bech32(addr) else {
        return false;
    };
    let payment = match s.payment() {
        ShelleyPaymentPart::Key(h) | ShelleyPaymentPart::Script(h) => hex::encode(**h),
    };
    if payment == cred_hex {
        return true;
    }
    match s.delegation() {
        ShelleyDelegationPart::Key(h) | ShelleyDelegationPart::Script(h) => {
            hex::encode(**h) == cred_hex
        }
        _ => false,
    }
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

    use crate::{AssetId, TxInput, TxOutput};

    /// jpg.store's real fee address — payment cred [`JPG_FEE_CRED_HEX`].
    const JPG_FEE_ADDR: &str = "addr1xxzvcf02fs5e282qk3pmjkau2emtcsj5wrukxak3np90n2evjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8eksg6pw3p";

    /// Build a listing datum: `Constr 0 [ [payout…], Bytes(seller_pkh) ]`,
    /// each payout `Constr 0 [ Addr(Constr0[Constr0[pkh]], no-stake), amount ]`.
    /// Amounts are pre-encoded CBOR uints (e.g. `1a389fd980` = 950 ADA).
    fn listing_datum(seller_pkh: &str, payouts: &[(&str, &str)]) -> Vec<u8> {
        let mut s = String::from("d8799f9f");
        for (pkh, amount_cbor) in payouts {
            s.push_str(&format!(
                "d8799fd8799fd8799f581c{pkh}ffd87a80ff{amount_cbor}ff"
            ));
        }
        s.push_str(&format!("ff581c{seller_pkh}ff"));
        hex::decode(s).expect("valid hex")
    }

    fn sale_tx(datum: Vec<u8>, extra_outputs: Vec<TxOutput>) -> DecodeTx {
        let asset = AssetId {
            policy: vec![1; 28],
            name: b"Bud".to_vec(),
        };
        let mut outputs = vec![TxOutput {
            address: "addr1buyer".into(),
            lovelace: 1_315_000,
            assets: vec![asset.clone()],
            ..Default::default()
        }];
        outputs.extend(extra_outputs);
        DecodeTx {
            tx_hash: vec![0xab; 32],
            inputs: vec![TxInput {
                address: JPG_V2_ADDR.into(),
                assets: vec![asset],
                datum: Some(datum),
                redeemer: Some(vec![0xd8, 0x79, 0x9f, 0x00, 0xff]),
                ..Default::default()
            }],
            outputs,
            ..Default::default()
        }
    }

    /// The observed WayUp-settled shape (tx `1abb0f60…`): payouts 950 + 30,
    /// plus a 20 ADA fee output NOT in the payouts → on-top fee 20.
    #[test]
    fn wayup_settled_jpg_sale_carries_on_top_fee() {
        let seller = "bb".repeat(28);
        let royalty = "cc".repeat(28);
        let tx = sale_tx(
            listing_datum(
                &seller,
                &[(&seller, "1a389fd980"), (&royalty, "1a01c9c380")],
            ),
            vec![TxOutput {
                address: JPG_FEE_ADDR.into(),
                lovelace: 20_000_000,
                assets: Vec::new(),
                ..Default::default()
            }],
        );
        let sales = decode_jpg_sales(&tx);
        assert_eq!(sales.len(), 1);
        let JpgStoreSale::Sale(s) = &sales[0];
        assert_eq!(s.price_lovelace, 980_000_000);
        assert_eq!(s.on_top_fee_lovelace, 20_000_000);
    }

    /// jpg-frontend-era shape: the fee is a datum payout to the fee cred, and
    /// the matching fee output is NOT on-top — buyer pays the settlement.
    #[test]
    fn jpg_era_fee_in_payouts_yields_zero_on_top() {
        let seller = "bb".repeat(28);
        let tx = sale_tx(
            listing_datum(
                &seller,
                &[(&seller, "1a389fd980"), (JPG_FEE_CRED_HEX, "1a01312d00")],
            ),
            vec![TxOutput {
                address: JPG_FEE_ADDR.into(),
                lovelace: 20_000_000,
                assets: Vec::new(),
                ..Default::default()
            }],
        );
        let sales = decode_jpg_sales(&tx);
        assert_eq!(sales.len(), 1);
        let JpgStoreSale::Sale(s) = &sales[0];
        assert_eq!(s.price_lovelace, 970_000_000); // 950 + 20 in-datum fee
        assert_eq!(s.on_top_fee_lovelace, 0);
    }

    /// No fee output at all → no on-top fee.
    #[test]
    fn jpg_sale_without_fee_output_has_zero_on_top() {
        let seller = "bb".repeat(28);
        let tx = sale_tx(
            listing_datum(&seller, &[(&seller, "1a389fd980")]),
            Vec::new(),
        );
        let sales = decode_jpg_sales(&tx);
        let JpgStoreSale::Sale(s) = &sales[0];
        assert_eq!(s.on_top_fee_lovelace, 0);
    }

    /// The real re-list address from tx `084bf145…`: a Wayup sale escrow (its
    /// payment cred is [`WAYUP_SALE_CRED`]), staking part per-seller.
    const WAYUP_RELIST_ADDR: &str = "addr1zxnk7racqx3f7kg7npc4weggmpdskheu8pm57egr9av0mtfmstyj2cxlcts698uyn5zvqtsq9gryxwzavykdk62yetpqn2fucd";

    /// Regression for tx `084bf145…`: a seller spends a jpg V1 listing (Buy-shaped
    /// redeemer) and re-lists the NFT onto Wayup in one tx. The asset lands at a
    /// Wayup sale escrow, not a buyer — so it must NOT be booked as a jpg sale.
    #[test]
    fn cross_venue_migration_is_not_a_sale() {
        let seller = "aa".repeat(28);
        let asset = AssetId {
            policy: vec![7; 28],
            name: b"Naru09878".to_vec(),
        };
        let tx = DecodeTx {
            tx_hash: vec![0x08; 32],
            inputs: vec![TxInput {
                address: JPG_V1_ADDR.into(),
                assets: vec![asset.clone()],
                datum: Some(listing_datum(&seller, &[(&seller, "1a389fd980")])),
                redeemer: Some(vec![0xd8, 0x79, 0x9f, 0x00, 0xff]),
                ..Default::default()
            }],
            // NFT re-escrowed at the Wayup sale contract (the migration), plus
            // the seller's own change.
            outputs: vec![
                TxOutput {
                    address: WAYUP_RELIST_ADDR.into(),
                    lovelace: 1_305_930,
                    assets: vec![asset],
                    ..Default::default()
                },
                TxOutput {
                    address: "addr1seller_change".into(),
                    lovelace: 1_963_000_000,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert!(
            decode_jpg_sales(&tx).is_empty(),
            "re-listing a jpg NFT onto Wayup must not be booked as a jpg sale"
        );
    }

    /// Control: a genuine sale (NFT delivered to a plain buyer wallet, not a
    /// marketplace escrow) is still recorded after the migration guard.
    #[test]
    fn genuine_sale_to_wallet_still_records() {
        let seller = "bb".repeat(28);
        let tx = sale_tx(
            listing_datum(&seller, &[(&seller, "1a389fd980")]),
            Vec::new(),
        );
        let sales = decode_jpg_sales(&tx);
        assert_eq!(sales.len(), 1);
    }

    /// The owner wallet + its listing owner pkh from tx `e00cbce7…` — the owner
    /// reclaimed Wave1Flame129 from a jpg V1 listing back to this wallet (payment
    /// cred == the listing datum's owner pkh) in the same tx that migrated other
    /// assets to Wayup.
    const RECLAIM_OWNER_ADDR: &str = "addr1q9hjep309lgmezdt9suwg666xc3xddmtfwjqd29rtxmm59sgl4kqa40pa07acf8rm4lpey7pnkp0vn0mfe63tyg5aagq8wz5th";
    const RECLAIM_OWNER_PKH: &str = "6f2c862f2fd1bc89ab2c38e46b5a362266b76b4ba406a8a359b7ba16";

    /// Regression for the ~71% buyer==seller phantom class: a jpg listing spent
    /// with a Buy-shaped redeemer whose NFT is reclaimed to the owner's OWN wallet
    /// (not a marketplace escrow) must NOT be booked as a sale.
    #[test]
    fn reclaim_to_owner_wallet_is_not_a_sale() {
        let asset = AssetId {
            policy: vec![9; 28],
            name: b"Wave1Flame129".to_vec(),
        };
        let tx = DecodeTx {
            tx_hash: vec![0xe0; 32],
            inputs: vec![TxInput {
                address: JPG_V1_ADDR.into(),
                assets: vec![asset.clone()],
                // owner-first listing datum: owner pkh == the reclaim wallet's pay cred.
                datum: Some(listing_datum(
                    RECLAIM_OWNER_PKH,
                    &[(RECLAIM_OWNER_PKH, "1a0939c880")],
                )),
                redeemer: Some(vec![0xd8, 0x79, 0x9f, 0x00, 0xff]),
                ..Default::default()
            }],
            outputs: vec![TxOutput {
                address: RECLAIM_OWNER_ADDR.into(),
                lovelace: 1_323_170,
                assets: vec![asset],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(
            decode_jpg_sales(&tx).is_empty(),
            "an NFT reclaimed to the lister's own wallet must not be a sale"
        );
    }

    #[test]
    fn address_bears_cred_matches_payment_and_rejects_others() {
        assert!(address_bears_cred(RECLAIM_OWNER_ADDR, RECLAIM_OWNER_PKH));
        assert!(!address_bears_cred(RECLAIM_OWNER_ADDR, &"ab".repeat(28)));
        assert!(!address_bears_cred(RECLAIM_OWNER_ADDR, "")); // empty never matches
        assert!(!address_bears_cred("not-an-address", RECLAIM_OWNER_PKH));
    }

    #[test]
    fn is_marketplace_escrow_recognises_jpg_and_wayup() {
        assert!(is_marketplace_escrow(JPG_V1_ADDR));
        assert!(is_marketplace_escrow(JPG_V2_ADDR));
        assert!(is_marketplace_escrow(WAYUP_RELIST_ADDR));
        assert!(!is_marketplace_escrow("addr1buyer"));
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
