//! Every contract mitos hard-codes must also be in `address-registry`, under
//! the matching category.
//!
//! # Why
//!
//! The same contracts are known in two places: mitos's decode crates — the DEX
//! venue registry (`mitos_dex_decode::venue::SITES`), the launchpad
//! (`mitos_launchpad_decode`) and the lock platforms (`mitos_vesting_decode`) —
//! and `address-registry` in shared-crates, which every other consumer names
//! scripts by. Nothing kept them in step, and they drifted: on 2026-10-23
//! snek.fun's order and lock contracts were known to mitos and missing from
//! the registry, so any consumer naming $ALIENS's scripts through the registry
//! still called them unknown.
//!
//! This test makes a contract added on one side fail the build until the
//! other side has it — the same guard `venue.rs` keeps between its decoders
//! and the trade fold. It compares CATEGORIES, never labels: the two spell
//! venues differently on purpose (`minswap-v2` here, `Minswap` there; see
//! `mitos_dex_decode::venue`'s header), and joining them by string would be
//! the bug that header warns about.
//!
//! Until the two are unified into one source, this is what keeps them one.

use address_registry::{AddressCategory, ScriptCategory, lookup_payment_credential};
use mitos_dex_decode::venue::{SITES, SiteKey, SiteRole};

/// The category a contract must carry in the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Want {
    /// A DEX pool or order contract — and a launchpad's ORDER contract, which
    /// is an order book like any other.
    Exchange,
    /// A launchpad's bonding curve. Deliberately not `Exchange`: see
    /// `ScriptCategory::Launchpad`.
    Launchpad,
    /// A lock platform.
    Vesting,
}

impl Want {
    fn matches(self, category: &AddressCategory) -> bool {
        matches!(
            (self, category),
            (
                Want::Exchange,
                AddressCategory::Script(ScriptCategory::Exchange { .. })
            ) | (
                Want::Launchpad,
                AddressCategory::Script(ScriptCategory::Launchpad { .. })
            ) | (
                Want::Vesting,
                AddressCategory::Script(ScriptCategory::Vesting { .. })
            )
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
    out
}

/// Every one, so a run names all the gaps at once rather than the first.
#[test]
fn every_contract_mitos_names_is_in_the_address_registry() {
    let mut drift = Vec::new();
    for (what, cred, want) in mitos_contracts() {
        let hex = hex::encode(cred);
        match lookup_payment_credential(&hex) {
            None => drift.push(format!("{what} ({hex}): missing from the registry")),
            Some(entry) if !want.matches(&entry.category) => drift.push(format!(
                "{what} ({hex}): registry says {:?}, expected {want:?}",
                entry.category
            )),
            Some(_) => {}
        }
    }
    assert!(
        drift.is_empty(),
        "mitos and address-registry disagree on {} contract(s):\n  {}",
        drift.len(),
        drift.join("\n  ")
    );
}
