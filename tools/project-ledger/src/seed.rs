//! `seed` — establish the walk floor and the initial frontier, before any chunk
//! is read.
//!
//! The floor is SEEDED from an indexer and PROVEN by the walk: `policy_asset_info`
//! gives every asset's `creation_time`; the earliest is the first mint. That is
//! recorded as `floor_basis = asserted`, the walk starts one immutable file
//! (21,600 slots) below it as margin, and at the end of the walk the distinct
//! assets minted are reconciled against the same list — equal flips the basis
//! to `observed`, short says so. `--floor` overrides (recorded `declared`).
//!
//! Seeds: the registry's `[[wallet]]`s (asserted, labelled, sourced). The
//! policy signer and the CIP-27 royalty address are found DURING the walk, from
//! the mint txs, and join the frontier then.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use chain_ledger::{Frontier, Role};
use mitos_chain_walk::mithril::CHUNK_SLOTS;

use crate::activity::Activity;
use crate::koios::Koios;
use crate::party::stake_party;
use crate::registry::Registry;
use crate::state::{Buffer, Holders, WalkState};
use crate::store::Ledger;

pub const META_POLICY: &str = "policy_id";
pub const META_POLICY_LABEL: &str = "policy_label";
pub const META_PROJECT: &str = "project";
pub const META_FLOOR_SLOT: &str = "floor_slot";
pub const META_FLOOR_SOURCE: &str = "floor_source";
pub const META_FLOOR_BASIS: &str = "floor_basis";
pub const META_WALK_START: &str = "walk_start_slot";
pub const META_EXPECTED_ASSETS: &str = "expected_assets";
pub const META_CEILING_SLOT: &str = "ceiling_slot";
pub const META_CEILING_SOURCE: &str = "ceiling_source";
pub const META_ROYALTY_ADDR: &str = "royalty_addr";
pub const META_ROYALTY_RATE: &str = "royalty_rate";
pub const META_SIGNER_CREDS: &str = "signer_creds";
pub const META_SEEDED_UNIX: &str = "seeded_unix";
pub const META_LAST_MINT_SLOT: &str = "last_mint_slot";
pub const META_MINTED_ASSETS: &str = "minted_assets";

#[derive(clap::Args, Debug)]
pub struct SeedArgs {
    /// Registry TOML.
    #[arg(long, default_value = "registry.toml")]
    registry: PathBuf,

    /// Ledger sqlite path (created if missing; refuses to re-seed a walked ledger).
    #[arg(long, default_value = "project-ledger.db")]
    db: PathBuf,

    /// Floor override (absolute slot). Recorded as `floor_source = declared`.
    #[arg(long)]
    floor: Option<u64>,

    /// Don't call Koios; requires `--floor` or a `floor` in the registry.
    #[arg(long)]
    offline: bool,

    /// Koios base URL.
    #[arg(long, env = "KOIOS_BASE")]
    koios_base: Option<String>,

    /// Koios bearer token (optional; free tier works for these two calls).
    #[arg(long, env = "KOIOS_TOKEN")]
    koios_token: Option<String>,

    /// Slots of margin below the floor to start walking (default: one immutable
    /// file). Cheap insurance against an indexer's `creation_time` being late.
    #[arg(long, default_value_t = CHUNK_SLOTS)]
    margin: u64,
}

pub fn run(args: SeedArgs) -> Result<()> {
    let registry = Registry::load(&args.registry)?;
    let ledger = Ledger::open(&args.db)?;
    if ledger.cursor()?.is_some() {
        bail!(
            "{} is already seeded/walked — `reset` first if you mean to start over",
            args.db.display()
        );
    }
    let policy = registry.policy();

    // --- floor -----------------------------------------------------------------
    let (floor, source, expected): (u64, &str, Option<usize>) = if let Some(f) = args.floor {
        (f, "declared", None)
    } else if let Some(f) = policy.floor {
        (f, "declared", None)
    } else if args.offline {
        bail!("--offline needs --floor (or `floor` in the registry)");
    } else {
        let koios = Koios::new(args.koios_base.clone(), args.koios_token.clone())?;
        let assets = koios
            .policy_asset_info(&policy.id)
            .context("seeding floor from koios policy_asset_info")?;
        if assets.is_empty() {
            bail!("koios knows no assets under policy {}", policy.id);
        }
        let earliest = assets
            .iter()
            .filter_map(|a| a.creation_time)
            .min()
            .context("no creation_time in koios rows")?;
        let slot = unix_to_slot(earliest as u64)
            .context("first mint predates Shelley — that's not an NFT policy")?;
        (slot, "koios", Some(assets.len()))
    };
    let walk_start = floor.saturating_sub(args.margin);

    // --- frontier --------------------------------------------------------------
    let mut frontier = Frontier::new(registry.thresholds(), registry.declared_terminal());
    for w in &registry.wallets {
        frontier
            .seed(stake_party(&w.stake), Role::Declared, walk_start)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    // --- persist ---------------------------------------------------------------
    let mut ledger = ledger;
    let state = WalkState {
        frontier,
        buffer: Buffer::default(),
        activity: Activity::default(),
        holders: Holders::default(),
    };
    // Cursor at the walk start with an EMPTY hash = "not started": the walk
    // streams from genesis and fast-skips to this slot.
    ledger.checkpoint(&state, walk_start, &[])?;
    for w in &registry.wallets {
        ledger.label_party(&w.stake, &w.label, &w.source)?;
    }
    ledger.meta_set(META_POLICY, &policy.id)?;
    ledger.meta_set(META_POLICY_LABEL, &policy.label)?;
    ledger.meta_set(META_PROJECT, &registry.project)?;
    ledger.meta_set(META_FLOOR_SLOT, &floor.to_string())?;
    ledger.meta_set(META_FLOOR_SOURCE, source)?;
    ledger.meta_set(META_FLOOR_BASIS, "asserted")?;
    ledger.meta_set(META_WALK_START, &walk_start.to_string())?;
    if let Some(n) = expected {
        ledger.meta_set(META_EXPECTED_ASSETS, &n.to_string())?;
    }
    ledger.meta_set(
        META_SEEDED_UNIX,
        &mitos_chain_walk::checkpoint::now_unix().to_string(),
    )?;

    tracing::info!(
        project = %registry.project,
        policy = %policy.id,
        floor,
        source,
        walk_start,
        expected_assets = ?expected,
        seeds = registry.wallets.len(),
        declared_terminal = registry.terminal.parties.len(),
        db = %args.db.display(),
        "seed: complete"
    );
    Ok(())
}

/// Mainnet unix seconds → slot (Shelley era only; `None` before it).
pub fn unix_to_slot(unix: u64) -> Option<u64> {
    const SHELLEY_START_SLOT: u64 = 4_492_800;
    const SHELLEY_START_UNIX: u64 = 1_596_059_091;
    (unix >= SHELLEY_START_UNIX).then(|| SHELLEY_START_SLOT + (unix - SHELLEY_START_UNIX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_slot_inverse() {
        let slot = 100_000_000u64;
        let unix = mitos_chain_walk::slot_to_unix(slot);
        assert_eq!(unix_to_slot(unix), Some(slot));
        assert_eq!(unix_to_slot(0), None);
    }
}
