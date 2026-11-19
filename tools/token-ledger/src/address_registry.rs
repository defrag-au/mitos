//! How token-ledger gets the address registry.
//!
//! The DATA is hosted: `theyexist/address-registry` authors two artefacts and
//! this binary reads them at a rev-pinned URL, so a new contract or a corrected
//! sink needs no rebuild. What is pinned here is the schema crate; the stamp
//! recorded with each archive says which bytes classified it.
//!
//! `ADDRESS_REGISTRY_DIR` names a local `resolved/` directory — a checkout
//! or a cache — and keeps a run offline. Without it the two files are fetched.

use anyhow::{Context, Result};
use mitos_registry::source::{Loaded, Origin};

pub use mitos_registry::source::Stamp;

/// The registry this run classifies with, and the stamp for its bytes.
pub fn load() -> Result<Loaded> {
    let loaded = mitos_registry::source::load().context("loading the address registry")?;
    note(&loaded);
    Ok(loaded)
}

/// Say how the bytes arrived, when it is not the plain fetch.
///
/// A CACHED answer is a real one and must be loud: the contracts are the same
/// revision this build pins, but anything added to the registry since the cache
/// was written is invisible, and an unnamed DEX reads as income. A cache that
/// cannot be written is the same failure one outage earlier.
fn note(loaded: &Loaded) {
    match loaded.origin {
        Origin::Fetched => {}
        Origin::Dir => tracing::debug!(
            rev = loaded.stamp.rev,
            "address registry: read from ADDRESS_REGISTRY_DIR"
        ),
        Origin::Cache => tracing::warn!(
            rev = loaded.stamp.rev,
            generated_at = %loaded.stamp.generated_at,
            "address registry: FETCH FAILED — classifying with the cached copy. A contract \
             added to the registry since it was written will read as unnamed"
        ),
    }
    if let Some(reason) = &loaded.cache_note {
        tracing::warn!(
            reason,
            "address registry: fetched, but the cache was NOT written — the next failed \
             fetch has no fallback"
        );
    }
}

/// A use's display label: its own name, else the category. The old compiled
/// table rendered a marketplace as `"{marketplace} {purpose}"`, and the authored
/// rows carry exactly that string, so a reader sees no change.
pub fn label(use_of: &mitos_registry::Contract) -> String {
    match &use_of.label {
        Some(label) => label.clone(),
        None => category_label(use_of.category).to_string(),
    }
}

pub fn category_label(category: mitos_registry::Category) -> &'static str {
    use mitos_registry::Category;
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
