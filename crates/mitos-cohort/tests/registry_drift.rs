//! mitos and the address registry must agree — in both directions.
//!
//! # Why
//!
//! The same contracts are known in two places: mitos's decode crates — the DEX
//! venue registry (`mitos_dex_decode::venue::SITES`), the launchpad
//! (`mitos_launchpad_decode`) and the lock platforms (`mitos_vesting_decode`) —
//! and the hosted registry, which every other consumer names scripts by. Nothing
//! kept them in step, and they drifted: on 2026-10-23 snek.fun's order and lock
//! contracts were known to mitos and missing from the registry, so any consumer
//! naming $ALIENS's scripts through the registry still called them unknown.
//!
//! Two checks, because a category can be wrong in both directions:
//!
//! 1. **Every contract mitos names is in the registry, under the category
//!    mitos would have given it.** A contract added to a decoder fails here
//!    until the registry has it.
//! 2. **Every datum the registry publishes an example for is one mitos can
//!    decode, in the shape the registry declares.** A shape added to the
//!    registry fails here until mitos grows a decoder for it.
//!
//! It compares CATEGORIES, never labels: the two spell venues differently on
//! purpose (`minswap-v2` here, `Minswap` there; see `mitos_dex_decode::venue`'s
//! header), and joining them by string would be the bug that header warns about.
//!
//! The registry used here is the crate's pinned copy (`data/`), so the test is
//! offline and the pin is loud: refresh it with the registry rev, and this test
//! says whether mitos still agrees.

use mitos_dex_decode::venue::{SITES, SiteKey, SiteRole};
use mitos_registry::{Category, Registry, Shape};

/// The category a contract must carry in the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Want {
    /// A DEX pool or order contract — and a launchpad's ORDER contract, which
    /// is an order book like any other.
    Exchange,
    /// A launchpad's bonding curve. Deliberately not `Exchange`: see
    /// `Category::Launchpad`.
    Launchpad,
    /// A lock platform.
    Vesting,
    /// One project's staking contract: read like a lock (its datum names the
    /// owner), named as what it is.
    Staking,
}

impl Want {
    fn matches(self, category: Category) -> bool {
        matches!(
            (self, category),
            (Want::Exchange, Category::Exchange)
                | (Want::Launchpad, Category::Launchpad)
                | (Want::Vesting, Category::Vesting)
                | (Want::Staking, Category::Staking)
        )
    }
}

/// Every contract mitos names, by payment credential, with what it is.
fn mitos_contracts() -> Vec<(String, [u8; 28], Want)> {
    let mut out = Vec::new();
    for site in SITES {
        let cred = match site.key {
            SiteKey::Cred(c) => c,
            SiteKey::Address(a) => mitos_cohort::payment_cred(a)
                .unwrap_or_else(|| panic!("{} site address {a} has no credential", site.venue)),
            // A venue known by its stake has no one payment credential to
            // check; its stake is the registry's stake table's to name.
            SiteKey::Stake(_) => continue,
        };
        let role = match site.role {
            SiteRole::Pool => "pool",
            SiteRole::Order => "order",
            SiteRole::Book => "order book",
            SiteRole::Escrow => "escrow",
        };
        out.push((format!("{} {role}", site.venue), cred, Want::Exchange));
    }
    out.push((
        "snek.fun bonding curve".into(),
        mitos_launchpad_decode::BONDING_CURVE_CRED,
        Want::Launchpad,
    ));
    out.push((
        "snek.fun order".into(),
        mitos_launchpad_decode::ORDER_CRED,
        Want::Exchange,
    ));
    out.push((
        "CrowdLock (the cnft.tools lock)".into(),
        mitos_vesting_decode::crowd_lock::PAYMENT_CRED,
        Want::Vesting,
    ));
    out.push((
        "snek.fun lock".into(),
        mitos_vesting_decode::snek_fun::PAYMENT_CRED,
        Want::Vesting,
    ));
    out.push((
        "the $PERP staking site".into(),
        mitos_vesting_decode::perps_staking::PAYMENT_CRED,
        Want::Staking,
    ));
    out
}

/// Every one, so a run names all the gaps at once rather than the first.
#[test]
fn every_contract_mitos_names_is_in_the_address_registry() {
    let registry = Registry::pinned();
    let mut drift = Vec::new();
    for (what, cred, want) in mitos_contracts() {
        let hex = hex::encode(cred);
        let uses = registry.uses(&hex);
        if uses.is_empty() {
            drift.push(format!("{what} ({hex}): missing from the registry"));
        } else if !uses.iter().any(|use_of| want.matches(use_of.category)) {
            let said: Vec<&str> = uses
                .iter()
                .map(|use_of| category_name(use_of.category))
                .collect();
            drift.push(format!(
                "{what} ({hex}): registry says {said:?}, expected {want:?}"
            ));
        }
    }
    assert!(
        drift.is_empty(),
        "mitos and address-registry disagree on {} contract(s):\n  {}",
        drift.len(),
        drift.join("\n  ")
    );
}

/// A `datum_example` is a claim the registry makes about bytes. If mitos reads
/// the shape, it must read the example — and the datum's own layout must agree
/// with the shape named.
#[test]
fn every_published_datum_example_decodes_to_the_shape_it_declares() {
    let registry = Registry::pinned();
    let mut failures = Vec::new();
    let mut checked = 0;
    for (credential, uses) in &registry.behaviour().contracts {
        for use_of in uses {
            let (Some(shape), Some(example)) = (use_of.shape, &use_of.datum_example) else {
                continue;
            };
            checked += 1;
            if !matches!(shape, Shape::TimestampAndOwner | Shape::TimestampAndOwners) {
                failures.push(format!(
                    "{credential}: declares {shape:?} and mitos has no decoder for it"
                ));
                continue;
            }
            let Ok(bytes) = hex::decode(example) else {
                failures.push(format!("{credential}: datum_example is not hex"));
                continue;
            };
            let Some(datum) = mitos_vesting_decode::decode_vesting_datum(&bytes) else {
                failures.push(format!("{credential}: {shape:?} example does not decode"));
                continue;
            };
            // `timestamp_and_owners` wraps the owner in a one-element list and
            // the decoder flattens it, so the two shapes are told apart by the
            // raw datum rather than by the decode result.
            let owners = owner_is_list(&bytes);
            let expected = shape == Shape::TimestampAndOwners;
            if owners != expected {
                failures.push(format!(
                    "{credential}: declares {shape:?} and the datum's owner is {}",
                    if owners { "a list" } else { "bare" }
                ));
            }
            if datum.owner_pkh_hex.len() != 56 {
                failures.push(format!(
                    "{credential}: a decoded owner that is not 28 bytes"
                ));
            }
        }
    }
    assert!(checked > 0, "the registry publishes no datum examples");
    assert!(
        failures.is_empty(),
        "{} published datum example(s) mitos cannot read as declared:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

/// Whether a lock datum's second field is an array (CrowdLock) or a bare hash
/// (snek.fun, $PERP). Parsed here rather than inferred from the decoder, which
/// deliberately accepts both.
fn owner_is_list(cbor: &[u8]) -> bool {
    use pallas_primitives::PlutusData;
    let Ok(PlutusData::Constr(constr)) = pallas_codec::minicbor::decode::<PlutusData>(cbor) else {
        return false;
    };
    let fields: Vec<PlutusData> = constr.fields.into();
    matches!(fields.get(1), Some(PlutusData::Array(_)))
}

fn category_name(category: Category) -> &'static str {
    match category {
        Category::Exchange => "exchange",
        Category::Marketplace => "marketplace",
        Category::Launchpad => "launchpad",
        Category::Burn => "burn",
        Category::Airdropper => "airdropper",
        Category::Minter => "minter",
        Category::Defi => "defi",
        Category::Staking => "staking",
        Category::Vesting => "vesting",
        Category::Lock => "lock",
        Category::Wallet => "wallet",
    }
}
