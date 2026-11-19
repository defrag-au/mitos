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
use mitos_registry::source::Loaded;

pub use mitos_registry::source::Stamp;

/// The registry this run classifies with, and the stamp for its bytes.
pub fn load() -> Result<Loaded> {
    mitos_registry::source::load().context("loading the address registry")
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
