//! Phase 1 verification: serde roundtrip + matching across every
//! axis of the `Interest` model.
//!
//! These tests don't touch the `Domain` data plane or the
//! replication wire — they're pure type-level checks that the
//! selector tree compiles, serialises, and matches as the design
//! says it does.

use cardano_assets::{AssetId, PolicyId};
use enumset::enum_set;
use pipeline_types::PricedAsset;

use crate::interest::{
    AssetSelector, DexSelector, DomainSelector, Interest, LendingSelector, MarketplaceSelector,
    ValueFilter,
};
use crate::protocol::{
    Address, BorrowPayload, Dex, DexBrand, DexEventKind, Domain, Lending, LendingBrand,
    LendingEventKind, ListingPayload, Marketplace, MarketplaceBrand, MarketplaceEventKind,
    OfferCancelPayload, OfferCreatePayload, OutputRef, PlutusBytes, ProtocolEvent, SalePayload,
    SwapPayload,
};

const POLICY_BLACKFLAG: &str = "b3dab69f7e6100849434fb1781e34bd12a916557f6231b8d2629b6f6";
const POLICY_OTHER: &str = "4523c5e21d409b81c95b45b0aea275b8ea1406e6cafea5583b9f8a5f";
const ASSET_NAME_HEX: &str = "426c61636b466c6167303031";

fn policy_blackflag() -> PolicyId {
    PolicyId::new(POLICY_BLACKFLAG).unwrap()
}

fn policy_other() -> PolicyId {
    PolicyId::new(POLICY_OTHER).unwrap()
}

fn asset_blackflag() -> AssetId {
    AssetId::new(POLICY_BLACKFLAG.to_string(), ASSET_NAME_HEX.to_string()).unwrap()
}

fn asset_other() -> AssetId {
    AssetId::new(POLICY_OTHER.to_string(), ASSET_NAME_HEX.to_string()).unwrap()
}

fn sale_event(brand: MarketplaceBrand, asset: AssetId) -> ProtocolEvent {
    ProtocolEvent {
        policy_id: PolicyId::new(asset.policy_id.clone()).unwrap(),
        asset_name_hex: Some(asset.asset_name_hex.clone()),
        tx_hash: "deadbeef".repeat(8),
        slot: 100_000_000,
        domain: Domain::Marketplace(Marketplace::Sale(SalePayload {
            brand,
            asset: PricedAsset::with_price(asset, 50_000_000),
            seller: Address::from("addr1seller"),
            buyer: Address::from("addr1buyer"),
            royalty_lovelace: Some(2_500_000),
        })),
    }
}

fn offer_cancel_event(asset: AssetId, payload: OfferCancelPayload) -> ProtocolEvent {
    ProtocolEvent {
        policy_id: PolicyId::new(asset.policy_id.clone()).unwrap(),
        asset_name_hex: Some(asset.asset_name_hex.clone()),
        tx_hash: "feedface".repeat(8),
        slot: 100_000_001,
        domain: Domain::Marketplace(Marketplace::OfferCancel(payload)),
    }
}

#[test]
fn marketplace_kind_projection_matches_variants() {
    let cases: &[(Marketplace, MarketplaceEventKind)] = &[
        (
            Marketplace::Sale(SalePayload {
                brand: MarketplaceBrand::JpgStore,
                asset: PricedAsset::new(asset_blackflag()),
                seller: "x".into(),
                buyer: "y".into(),
                royalty_lovelace: None,
            }),
            MarketplaceEventKind::Sale,
        ),
        (
            Marketplace::ListingCreate(ListingPayload {
                brand: MarketplaceBrand::Wayup,
                assets: vec![],
                seller: "x".into(),
            }),
            MarketplaceEventKind::ListingCreate,
        ),
        (
            Marketplace::OfferCancel(OfferCancelPayload::Wayup {
                policy_id: policy_blackflag(),
                asset_name_hex: None,
                bidder: "x".into(),
                redeemer: Some(PlutusBytes::new(vec![1, 2, 3])),
            }),
            MarketplaceEventKind::OfferCancel,
        ),
    ];
    for (event, expected_kind) in cases {
        assert_eq!(event.kind(), *expected_kind);
    }
}

#[test]
fn marketplace_brand_dispatch_handles_field_and_variant_layouts() {
    // Shared-shape variant: brand is a struct field.
    let sale = Marketplace::Sale(SalePayload {
        brand: MarketplaceBrand::JpgStore,
        asset: PricedAsset::new(asset_blackflag()),
        seller: "x".into(),
        buyer: "y".into(),
        royalty_lovelace: None,
    });
    assert_eq!(sale.brand(), MarketplaceBrand::JpgStore);

    // Divergent-shape variant: brand is the inner enum's identity.
    let cancel = Marketplace::OfferCancel(OfferCancelPayload::JpgStore {
        policy_id: policy_blackflag(),
        asset_name_hex: None,
        bidder: "x".into(),
        redeemer: Some(PlutusBytes::new(vec![])),
        script_ref: Some(OutputRef::new("aa".repeat(32), 0)),
    });
    assert_eq!(cancel.brand(), MarketplaceBrand::JpgStore);

    let unknown_cancel = Marketplace::OfferCancel(OfferCancelPayload::Unknown {
        brand_script: "addr1unknown".into(),
        policy_id: policy_blackflag(),
        raw: Some(PlutusBytes::new(vec![])),
    });
    assert_eq!(unknown_cancel.brand(), MarketplaceBrand::Unknown);
}

#[test]
fn interest_any_matches_everything() {
    let event = sale_event(MarketplaceBrand::JpgStore, asset_blackflag());
    assert!(Interest::any().matches(&event));
}

#[test]
fn asset_selector_policy_isolates_by_policy() {
    let interest = Interest {
        asset: AssetSelector::Policy(policy_blackflag()),
        domain: DomainSelector::Any,
        value: ValueFilter::Any,
    };
    let blackflag_event = sale_event(MarketplaceBrand::JpgStore, asset_blackflag());
    let other_event = sale_event(MarketplaceBrand::JpgStore, asset_other());
    assert!(interest.matches(&blackflag_event));
    assert!(!interest.matches(&other_event));
}

#[test]
fn asset_selector_specific_asset_matches_only_exact_pair() {
    let interest = Interest {
        asset: AssetSelector::Asset {
            policy: policy_blackflag(),
            name_hex: ASSET_NAME_HEX.into(),
        },
        domain: DomainSelector::Any,
        value: ValueFilter::Any,
    };
    let exact = sale_event(MarketplaceBrand::JpgStore, asset_blackflag());
    let other = sale_event(MarketplaceBrand::JpgStore, asset_other());
    let same_policy_other_name = ProtocolEvent {
        asset_name_hex: Some("deadbeef".into()),
        ..exact.clone()
    };
    assert!(interest.matches(&exact));
    assert!(!interest.matches(&other));
    assert!(!interest.matches(&same_policy_other_name));
}

#[test]
fn asset_selector_trait_is_inert() {
    let interest = Interest {
        asset: AssetSelector::Trait {
            policy: policy_blackflag(),
            key: "background".into(),
            value: "blue".into(),
        },
        domain: DomainSelector::Any,
        value: ValueFilter::Any,
    };
    let event = sale_event(MarketplaceBrand::JpgStore, asset_blackflag());
    assert!(!interest.matches(&event));
}

#[test]
fn marketplace_selector_filters_orthogonal_axes() {
    let interest = Interest {
        asset: AssetSelector::Any,
        domain: DomainSelector::Marketplace(MarketplaceSelector::Filter {
            brands: enum_set!(MarketplaceBrand::JpgStore | MarketplaceBrand::Wayup),
            kinds: enum_set!(MarketplaceEventKind::Sale | MarketplaceEventKind::OfferCancel),
        }),
        value: ValueFilter::Any,
    };

    // Both axes pass.
    assert!(interest.matches(&sale_event(MarketplaceBrand::JpgStore, asset_blackflag())));
    assert!(interest.matches(&sale_event(MarketplaceBrand::Wayup, asset_blackflag())));
    assert!(interest.matches(&offer_cancel_event(
        asset_blackflag(),
        OfferCancelPayload::JpgStore {
            policy_id: policy_blackflag(),
            asset_name_hex: None,
            bidder: "x".into(),
            redeemer: Some(PlutusBytes::new(vec![])),
            script_ref: Some(OutputRef::new("aa".repeat(32), 0)),
        }
    )));

    // Brand axis fails.
    assert!(!interest.matches(&sale_event(MarketplaceBrand::Dropspot, asset_blackflag())));

    // Kind axis fails.
    let listing_event = ProtocolEvent {
        domain: Domain::Marketplace(Marketplace::ListingCreate(ListingPayload {
            brand: MarketplaceBrand::JpgStore,
            assets: vec![PricedAsset::with_price(asset_blackflag(), 100_000_000)],
            seller: "x".into(),
        })),
        ..sale_event(MarketplaceBrand::JpgStore, asset_blackflag())
    };
    assert!(!interest.matches(&listing_event));
}

#[test]
fn marketplace_selector_any_matches_all_kinds_and_brands() {
    let interest = Interest {
        asset: AssetSelector::Any,
        domain: DomainSelector::Marketplace(MarketplaceSelector::Any),
        value: ValueFilter::Any,
    };
    for brand in [
        MarketplaceBrand::JpgStore,
        MarketplaceBrand::Wayup,
        MarketplaceBrand::Dropspot,
        MarketplaceBrand::SpaceBudzBidBoard,
        MarketplaceBrand::Unknown,
    ] {
        assert!(interest.matches(&sale_event(brand, asset_blackflag())));
    }
}

#[test]
fn cross_domain_mismatch_does_not_match() {
    let marketplace_only = Interest {
        asset: AssetSelector::Any,
        domain: DomainSelector::Marketplace(MarketplaceSelector::Any),
        value: ValueFilter::Any,
    };
    let dex_event = ProtocolEvent {
        policy_id: policy_blackflag(),
        asset_name_hex: Some(ASSET_NAME_HEX.into()),
        tx_hash: "ab".repeat(32),
        slot: 1,
        domain: Domain::Dex(Dex::Swap(SwapPayload {
            brand: DexBrand::Splash,
            asset_in: asset_blackflag(),
            amount_in: 1_000,
            asset_out: asset_other(),
            amount_out: 2_000,
            trader: "addr1".into(),
        })),
    };
    assert!(!marketplace_only.matches(&dex_event));

    let dex_only = Interest {
        asset: AssetSelector::Any,
        domain: DomainSelector::Dex(DexSelector::Any),
        value: ValueFilter::Any,
    };
    assert!(dex_only.matches(&dex_event));
    assert!(!dex_only.matches(&sale_event(MarketplaceBrand::JpgStore, asset_blackflag())));
}

#[test]
fn dex_and_lending_filters_compile_and_match() {
    let dex_swap = Interest {
        asset: AssetSelector::Any,
        domain: DomainSelector::Dex(DexSelector::Filter {
            brands: enum_set!(DexBrand::Splash | DexBrand::Cswap),
            kinds: enum_set!(DexEventKind::Swap),
        }),
        value: ValueFilter::Any,
    };
    let splash_swap = ProtocolEvent {
        policy_id: policy_blackflag(),
        asset_name_hex: Some(ASSET_NAME_HEX.into()),
        tx_hash: "ab".repeat(32),
        slot: 1,
        domain: Domain::Dex(Dex::Swap(SwapPayload {
            brand: DexBrand::Splash,
            asset_in: asset_blackflag(),
            amount_in: 1_000,
            asset_out: asset_other(),
            amount_out: 2_000,
            trader: "addr1".into(),
        })),
    };
    assert!(dex_swap.matches(&splash_swap));

    let lending_borrow = Interest {
        asset: AssetSelector::Any,
        domain: DomainSelector::Lending(LendingSelector::Filter {
            brands: enum_set!(LendingBrand::Liqwid),
            kinds: enum_set!(LendingEventKind::Borrow),
        }),
        value: ValueFilter::Any,
    };
    let borrow_event = ProtocolEvent {
        policy_id: policy_blackflag(),
        asset_name_hex: Some(ASSET_NAME_HEX.into()),
        tx_hash: "ab".repeat(32),
        slot: 1,
        domain: Domain::Lending(Lending::Borrow(BorrowPayload {
            brand: LendingBrand::Liqwid,
            collateral_policy: policy_blackflag(),
            collateral_asset: asset_blackflag(),
            borrowed_lovelace: 100_000_000,
            borrower: "addr1borrower".into(),
        })),
    };
    assert!(lending_borrow.matches(&borrow_event));
}

#[test]
fn vec_of_interests_ors_at_subscription_level() {
    // "Sales for any asset, OR offer-cancels on a specific policy"
    let subscription = [
        Interest {
            asset: AssetSelector::Any,
            domain: DomainSelector::Marketplace(MarketplaceSelector::Filter {
                brands: enum_set!(MarketplaceBrand::JpgStore | MarketplaceBrand::Wayup),
                kinds: enum_set!(MarketplaceEventKind::Sale),
            }),
            value: ValueFilter::Any,
        },
        Interest {
            asset: AssetSelector::Policy(policy_blackflag()),
            domain: DomainSelector::Marketplace(MarketplaceSelector::Filter {
                brands: enumset::EnumSet::all(),
                kinds: enum_set!(MarketplaceEventKind::OfferCancel),
            }),
            value: ValueFilter::Any,
        },
    ];

    let matches_any = |ev: &ProtocolEvent| subscription.iter().any(|i| i.matches(ev));

    // Sale of an unrelated asset on JpgStore — first interest matches.
    assert!(matches_any(&sale_event(
        MarketplaceBrand::JpgStore,
        asset_other()
    )));

    // OfferCancel on the watched policy under a brand the first
    // interest doesn't cover — second interest matches.
    let cancel = offer_cancel_event(
        asset_blackflag(),
        OfferCancelPayload::Unknown {
            brand_script: "addr1unknown".into(),
            policy_id: policy_blackflag(),
            raw: Some(PlutusBytes::new(vec![])),
        },
    );
    assert!(matches_any(&cancel));

    // OfferCancel on an unrelated policy — neither interest matches.
    let off_policy_cancel = offer_cancel_event(
        asset_other(),
        OfferCancelPayload::Wayup {
            policy_id: policy_other(),
            asset_name_hex: None,
            bidder: "x".into(),
            redeemer: Some(PlutusBytes::new(vec![])),
        },
    );
    assert!(!matches_any(&off_policy_cancel));
}

#[test]
fn offer_create_create_payload_is_addressable() {
    // Sanity check: every payload struct constructs cleanly. Catches
    // any field rename in payload structs that would otherwise only
    // surface in Phase 2.
    let payload = OfferCreatePayload {
        brand: MarketplaceBrand::JpgStore,
        policy_id: policy_blackflag(),
        asset_name_hex: Some(ASSET_NAME_HEX.into()),
        price_lovelace: 100_000_000,
        bidder: "addr1bidder".into(),
    };
    let event = ProtocolEvent {
        policy_id: policy_blackflag(),
        asset_name_hex: Some(ASSET_NAME_HEX.into()),
        tx_hash: "ab".repeat(32),
        slot: 1,
        domain: Domain::Marketplace(Marketplace::OfferCreate(payload)),
    };
    assert_eq!(
        event.domain,
        Domain::Marketplace(Marketplace::OfferCreate(OfferCreatePayload {
            brand: MarketplaceBrand::JpgStore,
            policy_id: policy_blackflag(),
            asset_name_hex: Some(ASSET_NAME_HEX.into()),
            price_lovelace: 100_000_000,
            bidder: "addr1bidder".into(),
        }))
    );
}

#[test]
fn interest_serde_json_roundtrip() {
    let interest = Interest {
        asset: AssetSelector::Policy(policy_blackflag()),
        domain: DomainSelector::Marketplace(MarketplaceSelector::Filter {
            brands: enum_set!(MarketplaceBrand::JpgStore | MarketplaceBrand::Wayup),
            kinds: enum_set!(MarketplaceEventKind::Sale | MarketplaceEventKind::OfferCancel),
        }),
        value: ValueFilter::Any,
    };
    let encoded = serde_json::to_string(&interest).unwrap();
    let decoded: Interest = serde_json::from_str(&encoded).unwrap();
    assert_eq!(interest, decoded);
}

#[test]
fn protocol_event_serde_json_roundtrip() {
    let event = sale_event(MarketplaceBrand::JpgStore, asset_blackflag());
    let encoded = serde_json::to_string(&event).unwrap();
    let decoded: ProtocolEvent = serde_json::from_str(&encoded).unwrap();
    assert_eq!(event, decoded);

    let cancel = offer_cancel_event(
        asset_blackflag(),
        OfferCancelPayload::JpgStore {
            policy_id: policy_blackflag(),
            asset_name_hex: None,
            bidder: "addr1bidder".into(),
            redeemer: Some(PlutusBytes::new(vec![1, 2, 3, 4])),
            script_ref: Some(OutputRef::new("aa".repeat(32), 7)),
        },
    );
    let encoded = serde_json::to_string(&cancel).unwrap();
    let decoded: ProtocolEvent = serde_json::from_str(&encoded).unwrap();
    assert_eq!(cancel, decoded);
}

#[test]
fn interest_serde_cbor_roundtrip() {
    // Wire format is CBOR over the replication channel, so verify
    // that path too — not just JSON.
    let interest = Interest {
        asset: AssetSelector::Asset {
            policy: policy_blackflag(),
            name_hex: ASSET_NAME_HEX.into(),
        },
        domain: DomainSelector::Dex(DexSelector::Filter {
            brands: enum_set!(DexBrand::Splash),
            kinds: enum_set!(DexEventKind::Swap),
        }),
        value: ValueFilter::Any,
    };
    let mut buf = Vec::new();
    ciborium::into_writer(&interest, &mut buf).unwrap();
    let decoded: Interest = ciborium::from_reader(buf.as_slice()).unwrap();
    assert_eq!(interest, decoded);
}
