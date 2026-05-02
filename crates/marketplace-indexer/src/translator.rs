//! Translate `TxClassification` (the upstream classifier output)
//! into `Vec<ProtocolEvent>` (mitos's typed wire format).
//!
//! One `ProtocolEvent` per `(policy_id, marketplace_event)` pair —
//! a tx that touches N policies emits N records, with brand
//! resolved per-event from the contract address. Multi-asset
//! bundles within a single policy share one event (the asset list
//! lives in the payload).
//!
//! Phase 2 deliverable: this function. Phase 3 swaps the indexer's
//! `MarketplaceIndexer::handle_event` to use it. Until then the
//! indexer continues emitting the existing `MarketplaceEvent`
//! shape and this translator is exercised purely by unit tests.
//!
//! Out of scope here: cancel-payload redeemer/script-ref decoding.
//! `OfferCancelPayload` carries `redeemer: None, script_ref: None`
//! for known brands until a future enhancement extracts them from
//! raw tx bytes. The brand identity, policy, and bidder are all
//! correctly populated.

use std::collections::HashMap;

use cardano_assets::{AssetId, PolicyId};
use mitos_protocol::{
    Domain, ListingPayload, Marketplace, MarketplaceBrand, OfferAcceptPayload, OfferCancelPayload,
    OfferCreatePayload, OfferUpdatePayload, ProtocolEvent, SalePayload, UnlistingPayload,
};
use pipeline_types::PricedAsset;
use tx_classifier::{TxClassification, TxType};

use crate::brand_resolver::marketplace_brand_from_address;

/// Convert a `TxClassification` into the protocol-event records
/// mitos emits. `slot` comes from the block point — the classifier
/// itself doesn't track it.
pub fn classification_to_events(
    classification: &TxClassification,
    slot: u64,
) -> Vec<ProtocolEvent> {
    let tx_hash = &classification.tx_hash;
    let mut out = Vec::new();
    for ty in &classification.tx_types {
        translate_tx_type(ty, tx_hash, slot, &mut out);
    }
    out
}

fn translate_tx_type(ty: &TxType, tx_hash: &str, slot: u64, out: &mut Vec<ProtocolEvent>) {
    match ty {
        TxType::Sale {
            asset,
            seller,
            buyer,
            marketplace,
            breakdown: _,
        } => {
            // breakdown.total_lovelace is recoverable from
            // `asset.price_lovelace`; royalty is not yet surfaced
            // by the classifier, so `royalty_lovelace = None` for
            // now. Tracked under the upstream "Royalty resolution"
            // line item in MARKETPLACE_INDEXER.md.
            let brand = marketplace
                .as_deref()
                .map(marketplace_brand_from_address)
                .unwrap_or(MarketplaceBrand::Unknown);
            let Some(policy_id) = parse_policy_id(&asset.asset.policy_id) else {
                return;
            };
            out.push(ProtocolEvent {
                policy_id,
                asset_name_hex: Some(asset.asset.asset_name_hex.clone()),
                tx_hash: tx_hash.to_string(),
                slot,
                domain: Domain::Marketplace(Marketplace::Sale(SalePayload {
                    brand,
                    asset: asset.clone(),
                    seller: seller.clone(),
                    buyer: buyer.clone(),
                    royalty_lovelace: None,
                })),
            });
        }
        TxType::OfferAccept {
            asset,
            offer_lovelace,
            seller,
            buyer,
        } => {
            let Some(policy_id) = parse_policy_id(&asset.policy_id) else {
                return;
            };
            // OfferAccept's `TxType` doesn't carry the marketplace
            // contract address, so brand resolution falls back to
            // Unknown until classifier surfaces it. Tracked under
            // Phase 2.5 / classifier follow-up.
            out.push(ProtocolEvent {
                policy_id,
                asset_name_hex: Some(asset.asset_name_hex.clone()),
                tx_hash: tx_hash.to_string(),
                slot,
                domain: Domain::Marketplace(Marketplace::OfferAccept(OfferAcceptPayload {
                    brand: MarketplaceBrand::Unknown,
                    asset: asset.clone(),
                    price_lovelace: *offer_lovelace,
                    bidder: buyer.clone(),
                    seller: seller.clone(),
                })),
            });
        }
        TxType::CreateOffer {
            policy_id,
            encoded_asset_name,
            offer_lovelace,
            bidder,
            marketplace,
            ..
        } => {
            let brand = marketplace_brand_from_address(marketplace);
            let Some(typed_policy) = parse_policy_id(policy_id) else {
                return;
            };
            out.push(ProtocolEvent {
                policy_id: typed_policy.clone(),
                asset_name_hex: encoded_asset_name.clone(),
                tx_hash: tx_hash.to_string(),
                slot,
                domain: Domain::Marketplace(Marketplace::OfferCreate(OfferCreatePayload {
                    brand,
                    policy_id: typed_policy,
                    asset_name_hex: encoded_asset_name.clone(),
                    price_lovelace: *offer_lovelace,
                    bidder: bidder.clone(),
                })),
            });
        }
        TxType::OfferUpdate {
            policy_id,
            encoded_asset_name,
            original_amount,
            updated_amount,
            delta_amount,
            bidder,
            marketplace,
            ..
        } => {
            let brand = marketplace_brand_from_address(marketplace);
            let Some(typed_policy) = parse_policy_id(policy_id) else {
                return;
            };
            // Classifier tracks original + updated; surface the
            // updated price as the canonical "price now" plus the
            // signed delta. Original is recoverable as
            // `new_price - delta` if needed.
            let _ = original_amount;
            out.push(ProtocolEvent {
                policy_id: typed_policy.clone(),
                asset_name_hex: encoded_asset_name.clone(),
                tx_hash: tx_hash.to_string(),
                slot,
                domain: Domain::Marketplace(Marketplace::OfferUpdate(OfferUpdatePayload {
                    brand,
                    policy_id: typed_policy,
                    asset_name_hex: encoded_asset_name.clone(),
                    new_price_lovelace: *updated_amount,
                    delta_lovelace: *delta_amount,
                    bidder: bidder.clone(),
                })),
            });
        }
        TxType::OfferCancel {
            policy_id,
            encoded_asset_name,
            bidder,
            marketplace,
            ..
        } => {
            let Some(typed_policy) = parse_policy_id(policy_id) else {
                return;
            };
            let payload = match marketplace_brand_from_address(marketplace) {
                MarketplaceBrand::JpgStore => OfferCancelPayload::JpgStore {
                    policy_id: typed_policy.clone(),
                    asset_name_hex: encoded_asset_name.clone(),
                    bidder: bidder.clone(),
                    redeemer: None,
                    script_ref: None,
                },
                MarketplaceBrand::Wayup => OfferCancelPayload::Wayup {
                    policy_id: typed_policy.clone(),
                    asset_name_hex: encoded_asset_name.clone(),
                    bidder: bidder.clone(),
                    redeemer: None,
                },
                _ => OfferCancelPayload::Unknown {
                    brand_script: marketplace.clone(),
                    policy_id: typed_policy.clone(),
                    raw: None,
                },
            };
            out.push(ProtocolEvent {
                policy_id: typed_policy,
                asset_name_hex: encoded_asset_name.clone(),
                tx_hash: tx_hash.to_string(),
                slot,
                domain: Domain::Marketplace(Marketplace::OfferCancel(payload)),
            });
        }
        TxType::ListingCreate {
            assets,
            seller,
            marketplace,
            ..
        } => emit_listing(
            assets,
            seller,
            marketplace,
            tx_hash,
            slot,
            out,
            ListingKind::Create,
        ),
        TxType::ListingUpdate {
            assets,
            seller,
            marketplace,
            ..
        } => emit_listing(
            assets,
            seller,
            marketplace,
            tx_hash,
            slot,
            out,
            ListingKind::Update,
        ),
        TxType::Unlisting {
            assets,
            seller,
            marketplace,
            ..
        } => {
            let brand = marketplace_brand_from_address(marketplace);
            // Group assets by policy — one event per affected
            // policy carries the full asset list within that policy.
            let mut grouped: HashMap<PolicyId, Vec<AssetId>> = HashMap::new();
            for a in assets {
                if let Some(p) = parse_policy_id(&a.policy_id) {
                    grouped.entry(p).or_default().push(a.clone());
                }
            }
            for (policy, policy_assets) in grouped {
                out.push(ProtocolEvent {
                    policy_id: policy.clone(),
                    asset_name_hex: single_asset_name(&policy_assets),
                    tx_hash: tx_hash.to_string(),
                    slot,
                    domain: Domain::Marketplace(Marketplace::Unlisting(UnlistingPayload {
                        brand,
                        assets: policy_assets,
                        seller: seller.clone(),
                    })),
                });
            }
        }
        // Non-marketplace variants pass through silently — Mint,
        // Burn, Transfer, AssetTransfer, SmartContract, Staking,
        // Vesting, Dex*, MarketplaceBundle, Offer (legacy),
        // Unknown. The marketplace indexer only cares about the
        // typed marketplace events above. A future dex-indexer
        // crate will translate the Dex* variants similarly.
        _ => {}
    }
}

#[derive(Copy, Clone)]
enum ListingKind {
    Create,
    Update,
}

fn emit_listing(
    assets: &[PricedAsset],
    seller: &str,
    marketplace: &str,
    tx_hash: &str,
    slot: u64,
    out: &mut Vec<ProtocolEvent>,
    kind: ListingKind,
) {
    let brand = marketplace_brand_from_address(marketplace);
    let mut grouped: HashMap<PolicyId, Vec<PricedAsset>> = HashMap::new();
    for a in assets {
        if let Some(p) = parse_policy_id(&a.asset.policy_id) {
            grouped.entry(p).or_default().push(a.clone());
        }
    }
    for (policy, policy_assets) in grouped {
        let asset_name = single_priced_asset_name(&policy_assets);
        let payload = ListingPayload {
            brand,
            assets: policy_assets,
            seller: seller.to_string(),
        };
        let domain = match kind {
            ListingKind::Create => Domain::Marketplace(Marketplace::ListingCreate(payload)),
            ListingKind::Update => Domain::Marketplace(Marketplace::ListingUpdate(payload)),
        };
        out.push(ProtocolEvent {
            policy_id: policy,
            asset_name_hex: asset_name,
            tx_hash: tx_hash.to_string(),
            slot,
            domain,
        });
    }
}

fn parse_policy_id(s: &str) -> Option<PolicyId> {
    PolicyId::new(s).ok()
}

/// For a list of assets within a single policy, surface the asset
/// name only when there's exactly one — otherwise the event is a
/// multi-asset bundle and selectors should match it via `Policy`,
/// not `Asset`.
fn single_asset_name(assets: &[AssetId]) -> Option<String> {
    if assets.len() == 1 {
        Some(assets[0].asset_name_hex.clone())
    } else {
        None
    }
}

fn single_priced_asset_name(assets: &[PricedAsset]) -> Option<String> {
    if assets.len() == 1 {
        Some(assets[0].asset.asset_name_hex.clone())
    } else {
        None
    }
}
