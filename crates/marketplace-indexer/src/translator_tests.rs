//! Fixture-based tests for the `TxClassification -> Vec<ProtocolEvent>`
//! translator. Hand-built classifications cover every marketplace
//! `TxType` variant the translator handles, plus brand resolution
//! across the known JpgStore and Wayup contract addresses.

use cardano_assets::AssetId;
use mitos_protocol::{
    Domain, Marketplace, MarketplaceBrand, MarketplaceEventKind, OfferCancelPayload,
};
use pipeline_types::PricedAsset;
use tx_classifier::{AdaFlows, Confidence, SaleBreakdown, TxClassification, TxContext, TxType};

use crate::classification_to_events;

const JPG_STORE_V2_SALE: &str = "addr1x8rjw3pawl0kelu4mj3c8x20fsczf5pl744s9mxz9v8n7efvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8ekstg4qrx";
const JPG_STORE_V4_SALE: &str = "addr1w999n67e47he8y0v36hjtzluargwu25zw94f6lqnm82aqqsg4xkcp";
const WAYUP_SALE: &str = "addr1zxnk7racqx3f7kg7npc4weggmpdskheu8pm57egr9av0mtvasazx8r5xwqtnfjsfrnat3h6yrycd2hfm9qpg7d0hf50s7x4y79";

const POLICY_BLACKFLAG: &str = "b3dab69f7e6100849434fb1781e34bd12a916557f6231b8d2629b6f6";
const POLICY_OTHER: &str = "4523c5e21d409b81c95b45b0aea275b8ea1406e6cafea5583b9f8a5f";
const ASSET_NAME_A: &str = "426c61636b466c6167303031";
const ASSET_NAME_B: &str = "426c61636b466c6167303032";

fn asset(policy: &str, name: &str) -> AssetId {
    AssetId::new(policy.to_string(), name.to_string()).unwrap()
}

fn priced(policy: &str, name: &str, price: u64) -> PricedAsset {
    PricedAsset::with_price(asset(policy, name), price)
}

fn classify(tx_types: Vec<TxType>) -> TxClassification {
    TxClassification {
        tx_hash: "ab".repeat(32),
        tx_types,
        tags: vec![],
        confidence: Confidence::High,
        score: 1.0,
        context: TxContext {
            block_height: None,
            timestamp: None,
            fee: None,
            size: None,
            metadata: None,
            scripts: vec![],
            notes: vec![],
        },
        assets: vec![],
        ada_amounts: AdaFlows {
            total_input: 0,
            total_output: 0,
            fee: 0,
            collateral_input: 0,
            collateral_output: 0,
            largest_transfer: None,
            flows: vec![],
            collateral_flows: vec![],
        },
        distributions: vec![],
    }
}

#[test]
fn sale_resolves_jpgstore_brand_from_v2_contract() {
    let c = classify(vec![TxType::Sale {
        asset: priced(POLICY_BLACKFLAG, ASSET_NAME_A, 50_000_000),
        breakdown: SaleBreakdown {
            total_lovelace: 50_000_000,
        },
        seller: "addr1seller".into(),
        buyer: "addr1buyer".into(),
        marketplace: Some(JPG_STORE_V2_SALE.into()),
    }]);

    let events = classification_to_events(&c, 100);
    assert_eq!(events.len(), 1);
    let ev = &events[0];
    assert_eq!(ev.policy_id.as_str(), POLICY_BLACKFLAG);
    assert_eq!(ev.asset_name_hex.as_deref(), Some(ASSET_NAME_A));
    assert_eq!(ev.slot, 100);
    let Domain::Marketplace(Marketplace::Sale(ref s)) = ev.domain else {
        panic!("expected Sale, got {:?}", ev.domain);
    };
    assert_eq!(s.brand, MarketplaceBrand::JpgStore);
    assert_eq!(s.asset.price_lovelace, Some(50_000_000));
}

#[test]
fn sale_resolves_jpgstore_brand_from_v4_contract() {
    let c = classify(vec![TxType::Sale {
        asset: priced(POLICY_BLACKFLAG, ASSET_NAME_A, 75_000_000),
        breakdown: SaleBreakdown {
            total_lovelace: 75_000_000,
        },
        seller: "addr1seller".into(),
        buyer: "addr1buyer".into(),
        marketplace: Some(JPG_STORE_V4_SALE.into()),
    }]);

    let events = classification_to_events(&c, 200);
    assert_eq!(events.len(), 1);
    let Domain::Marketplace(Marketplace::Sale(ref s)) = events[0].domain else {
        panic!();
    };
    assert_eq!(s.brand, MarketplaceBrand::JpgStore);
}

#[test]
fn sale_resolves_wayup_brand() {
    let c = classify(vec![TxType::Sale {
        asset: priced(POLICY_BLACKFLAG, ASSET_NAME_A, 25_000_000),
        breakdown: SaleBreakdown {
            total_lovelace: 25_000_000,
        },
        seller: "addr1seller".into(),
        buyer: "addr1buyer".into(),
        marketplace: Some(WAYUP_SALE.into()),
    }]);

    let events = classification_to_events(&c, 300);
    let Domain::Marketplace(Marketplace::Sale(ref s)) = events[0].domain else {
        panic!();
    };
    assert_eq!(s.brand, MarketplaceBrand::Wayup);
}

#[test]
fn sale_falls_back_to_unknown_when_marketplace_field_missing() {
    let c = classify(vec![TxType::Sale {
        asset: priced(POLICY_BLACKFLAG, ASSET_NAME_A, 25_000_000),
        breakdown: SaleBreakdown {
            total_lovelace: 25_000_000,
        },
        seller: "addr1seller".into(),
        buyer: "addr1buyer".into(),
        marketplace: None,
    }]);

    let events = classification_to_events(&c, 0);
    let Domain::Marketplace(Marketplace::Sale(ref s)) = events[0].domain else {
        panic!();
    };
    assert_eq!(s.brand, MarketplaceBrand::Unknown);
}

#[test]
fn create_offer_per_asset_carries_asset_name_hex() {
    let c = classify(vec![TxType::CreateOffer {
        policy_id: POLICY_BLACKFLAG.into(),
        encoded_asset_name: Some(ASSET_NAME_A.into()),
        offer_count: 1,
        offer_lovelace: 100_000_000,
        total_lovelace: 100_000_000,
        bidder: "addr1bidder".into(),
        marketplace: JPG_STORE_V2_SALE.into(),
    }]);

    let events = classification_to_events(&c, 1);
    assert_eq!(events.len(), 1);
    let ev = &events[0];
    assert_eq!(ev.policy_id.as_str(), POLICY_BLACKFLAG);
    assert_eq!(ev.asset_name_hex.as_deref(), Some(ASSET_NAME_A));
    let Domain::Marketplace(ref m) = ev.domain else {
        panic!();
    };
    assert_eq!(m.kind(), MarketplaceEventKind::OfferCreate);
}

#[test]
fn create_offer_collection_wide_has_no_asset_name() {
    let c = classify(vec![TxType::CreateOffer {
        policy_id: POLICY_BLACKFLAG.into(),
        encoded_asset_name: None, // collection offer
        offer_count: 1,
        offer_lovelace: 100_000_000,
        total_lovelace: 100_000_000,
        bidder: "addr1bidder".into(),
        marketplace: JPG_STORE_V2_SALE.into(),
    }]);

    let events = classification_to_events(&c, 1);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].asset_name_hex, None);
}

#[test]
fn offer_cancel_jpgstore_emits_typed_payload_with_none_redeemer() {
    let c = classify(vec![TxType::OfferCancel {
        policy_id: POLICY_BLACKFLAG.into(),
        encoded_asset_name: Some(ASSET_NAME_A.into()),
        offer_count: 1,
        total_cancelled_lovelace: 100_000_000,
        bidder: "addr1bidder".into(),
        marketplace: JPG_STORE_V2_SALE.into(),
    }]);

    let events = classification_to_events(&c, 1);
    let Domain::Marketplace(Marketplace::OfferCancel(ref payload)) = events[0].domain else {
        panic!();
    };
    let OfferCancelPayload::JpgStore {
        redeemer,
        script_ref,
        ..
    } = payload
    else {
        panic!("expected JpgStore variant");
    };
    assert!(redeemer.is_none());
    assert!(script_ref.is_none());
}

#[test]
fn offer_cancel_wayup_emits_typed_payload() {
    let c = classify(vec![TxType::OfferCancel {
        policy_id: POLICY_BLACKFLAG.into(),
        encoded_asset_name: None,
        offer_count: 1,
        total_cancelled_lovelace: 100_000_000,
        bidder: "addr1bidder".into(),
        marketplace: WAYUP_SALE.into(),
    }]);

    let events = classification_to_events(&c, 1);
    let Domain::Marketplace(Marketplace::OfferCancel(ref payload)) = events[0].domain else {
        panic!();
    };
    assert!(matches!(payload, OfferCancelPayload::Wayup { .. }));
}

#[test]
fn offer_cancel_unknown_marketplace_preserves_script_string() {
    let unknown_addr = "addr1z9unknown_marketplace_address_used_in_test";
    let c = classify(vec![TxType::OfferCancel {
        policy_id: POLICY_BLACKFLAG.into(),
        encoded_asset_name: None,
        offer_count: 1,
        total_cancelled_lovelace: 100_000_000,
        bidder: "addr1bidder".into(),
        marketplace: unknown_addr.into(),
    }]);

    let events = classification_to_events(&c, 1);
    let Domain::Marketplace(Marketplace::OfferCancel(ref payload)) = events[0].domain else {
        panic!();
    };
    let OfferCancelPayload::Unknown { brand_script, .. } = payload else {
        panic!("expected Unknown variant for unrecognised marketplace");
    };
    assert_eq!(brand_script, unknown_addr);
}

#[test]
fn listing_create_groups_assets_by_policy() {
    // Multi-asset listing across two policies — translator should
    // emit one event per policy with the policy's asset list.
    let c = classify(vec![TxType::ListingCreate {
        assets: vec![
            priced(POLICY_BLACKFLAG, ASSET_NAME_A, 50_000_000),
            priced(POLICY_BLACKFLAG, ASSET_NAME_B, 60_000_000),
            priced(POLICY_OTHER, ASSET_NAME_A, 70_000_000),
        ],
        total_listing_count: 3,
        seller: "addr1seller".into(),
        marketplace: JPG_STORE_V2_SALE.into(),
    }]);

    let events = classification_to_events(&c, 1);
    assert_eq!(events.len(), 2);

    let mut by_policy: std::collections::HashMap<String, &mitos_protocol::ProtocolEvent> =
        std::collections::HashMap::new();
    for ev in &events {
        by_policy.insert(ev.policy_id.as_str().to_string(), ev);
    }

    let bf = by_policy.get(POLICY_BLACKFLAG).unwrap();
    let Domain::Marketplace(Marketplace::ListingCreate(ref l)) = bf.domain else {
        panic!();
    };
    assert_eq!(l.assets.len(), 2);
    // Multi-asset bundle within a policy => no single asset name.
    assert_eq!(bf.asset_name_hex, None);

    let other = by_policy.get(POLICY_OTHER).unwrap();
    let Domain::Marketplace(Marketplace::ListingCreate(ref l)) = other.domain else {
        panic!();
    };
    assert_eq!(l.assets.len(), 1);
    // Single asset within this policy => the asset name surfaces.
    assert_eq!(other.asset_name_hex.as_deref(), Some(ASSET_NAME_A));
}

#[test]
fn unlisting_groups_assets_by_policy() {
    let c = classify(vec![TxType::Unlisting {
        assets: vec![
            asset(POLICY_BLACKFLAG, ASSET_NAME_A),
            asset(POLICY_BLACKFLAG, ASSET_NAME_B),
        ],
        total_unlisting_count: 2,
        seller: "addr1seller".into(),
        marketplace: WAYUP_SALE.into(),
    }]);

    let events = classification_to_events(&c, 1);
    assert_eq!(events.len(), 1);
    let Domain::Marketplace(Marketplace::Unlisting(ref u)) = events[0].domain else {
        panic!();
    };
    assert_eq!(u.brand, MarketplaceBrand::Wayup);
    assert_eq!(u.assets.len(), 2);
    assert_eq!(events[0].asset_name_hex, None);
}

#[test]
fn offer_update_carries_signed_delta() {
    let c = classify(vec![TxType::OfferUpdate {
        policy_id: POLICY_BLACKFLAG.into(),
        encoded_asset_name: Some(ASSET_NAME_A.into()),
        original_amount: 100_000_000,
        updated_amount: 75_000_000,
        delta_amount: -25_000_000,
        bidder: "addr1bidder".into(),
        marketplace: JPG_STORE_V2_SALE.into(),
        offer_index: 0,
    }]);

    let events = classification_to_events(&c, 1);
    let Domain::Marketplace(Marketplace::OfferUpdate(ref u)) = events[0].domain else {
        panic!();
    };
    assert_eq!(u.brand, MarketplaceBrand::JpgStore);
    assert_eq!(u.new_price_lovelace, 75_000_000);
    assert_eq!(u.delta_lovelace, -25_000_000);
}

#[test]
fn offer_accept_brand_currently_unknown() {
    // Classifier's TxType::OfferAccept doesn't currently carry the
    // marketplace contract address, so the translator emits
    // Unknown. This test pins that contract — when the upstream
    // change lands, this test will fail and the translator gets
    // a real brand.
    let c = classify(vec![TxType::OfferAccept {
        asset: asset(POLICY_BLACKFLAG, ASSET_NAME_A),
        offer_lovelace: 100_000_000,
        seller: "addr1seller".into(),
        buyer: "addr1bidder".into(),
    }]);

    let events = classification_to_events(&c, 1);
    let Domain::Marketplace(Marketplace::OfferAccept(ref a)) = events[0].domain else {
        panic!();
    };
    assert_eq!(a.brand, MarketplaceBrand::Unknown);
    assert_eq!(a.price_lovelace, 100_000_000);
}

#[test]
fn non_marketplace_tx_types_emit_nothing() {
    // Mint / Transfer / DexSwap etc. should pass through silently.
    let c = classify(vec![
        TxType::Transfer {
            assets: vec![asset(POLICY_BLACKFLAG, ASSET_NAME_A)],
        },
        TxType::Burn {
            policy_id: POLICY_BLACKFLAG.into(),
            asset_count: 1,
        },
        TxType::Unknown,
    ]);

    let events = classification_to_events(&c, 1);
    assert!(events.is_empty());
}

#[test]
fn multiple_marketplace_events_in_one_classification_emit_per_event() {
    let c = classify(vec![
        TxType::Sale {
            asset: priced(POLICY_BLACKFLAG, ASSET_NAME_A, 50_000_000),
            breakdown: SaleBreakdown {
                total_lovelace: 50_000_000,
            },
            seller: "addr1seller".into(),
            buyer: "addr1buyer".into(),
            marketplace: Some(JPG_STORE_V2_SALE.into()),
        },
        TxType::OfferCancel {
            policy_id: POLICY_OTHER.into(),
            encoded_asset_name: Some(ASSET_NAME_A.into()),
            offer_count: 1,
            total_cancelled_lovelace: 75_000_000,
            bidder: "addr1bidder".into(),
            marketplace: WAYUP_SALE.into(),
        },
    ]);

    let events = classification_to_events(&c, 1);
    assert_eq!(events.len(), 2);
    assert!(
        events
            .iter()
            .any(|e| matches!(e.domain, Domain::Marketplace(Marketplace::Sale(_))))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e.domain, Domain::Marketplace(Marketplace::OfferCancel(_))))
    );
}
