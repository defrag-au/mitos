//! Phase A unit tests: predicate type construction + serde
//! round-trips. Tests that need a live `Domain` are deferred —
//! the LocalDataPlane is exercised by the OwnershipIndexer
//! backfill spike instead.

use cardano_assets::PolicyId;
use pallas_primitives::Hash;

use crate::types::{
    AssetPattern, DecodeLevel, OutputRef, OutputRefPattern, PageRequest, UtxoPattern, UtxoPredicate,
};

const POLICY_BLACKFLAG: &str = "b3dab69f7e6100849434fb1781e34bd12a916557f6231b8d2629b6f6";

fn policy_blackflag() -> PolicyId {
    PolicyId::new(POLICY_BLACKFLAG).unwrap()
}

#[test]
fn output_ref_display_formats_as_txhash_hash_index() {
    let r = OutputRef::from_bytes([0xAB; 32], 7);
    let s = format!("{r}");
    assert!(s.ends_with("#7"));
    assert!(s.starts_with(&"ab".repeat(32)));
}

#[test]
fn output_ref_round_trips_to_dolos_txo_ref() {
    let r = OutputRef::from_bytes([0xCD; 32], 3);
    let txo: dolos_core::TxoRef = r.into();
    let back: OutputRef = txo.into();
    assert_eq!(r, back);
}

#[test]
fn predicate_convenience_constructors() {
    let any = UtxoPredicate::any();
    let none = UtxoPredicate::none();
    assert!(matches!(any, UtxoPredicate::AllOf(ref v) if v.is_empty()));
    assert!(matches!(none, UtxoPredicate::AnyOf(ref v) if v.is_empty()));
}

#[test]
fn pattern_helpers_build_expected_shape() {
    let by_policy = UtxoPattern::under_policy(policy_blackflag());
    assert!(matches!(by_policy.asset, Some(AssetPattern::Policy(_))));
    assert!(by_policy.address.is_none());
    assert!(by_policy.output_ref.is_none());
}

#[test]
fn output_ref_pattern_default_is_open() {
    let p = OutputRefPattern::default();
    assert!(p.tx_hash.is_none());
    assert!(p.index.is_none());
}

#[test]
fn decode_level_inclusion_helpers() {
    assert!(!DecodeLevel::Lean.includes_datum());
    assert!(!DecodeLevel::Lean.includes_script());
    assert!(DecodeLevel::WithDatum.includes_datum());
    assert!(!DecodeLevel::WithDatum.includes_script());
    assert!(DecodeLevel::Full.includes_datum());
    assert!(DecodeLevel::Full.includes_script());
}

#[test]
fn page_request_default_sane() {
    let p = PageRequest::default();
    assert_eq!(p.max_items, 100);
    assert!(p.start_token.is_none());
}

#[test]
fn predicate_serde_round_trip_json() {
    let pred = UtxoPredicate::AnyOf(vec![
        UtxoPredicate::Match(UtxoPattern::under_policy(policy_blackflag())),
        UtxoPredicate::Not(Box::new(UtxoPredicate::Match(UtxoPattern {
            output_ref: Some(OutputRefPattern {
                tx_hash: Some(Hash::from([0xEF; 32])),
                index: Some(2),
            }),
            ..Default::default()
        }))),
    ]);
    let encoded = serde_json::to_string(&pred).unwrap();
    let decoded: UtxoPredicate = serde_json::from_str(&encoded).unwrap();
    assert_eq!(pred, decoded);
}

#[test]
fn predicate_serde_round_trip_cbor() {
    let pred = UtxoPredicate::AllOf(vec![UtxoPredicate::Match(UtxoPattern::under_policy(
        policy_blackflag(),
    ))]);
    let mut buf = Vec::new();
    ciborium::into_writer(&pred, &mut buf).unwrap();
    let decoded: UtxoPredicate = ciborium::from_reader(buf.as_slice()).unwrap();
    assert_eq!(pred, decoded);
}
