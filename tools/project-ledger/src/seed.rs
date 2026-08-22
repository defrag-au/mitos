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
use crate::state::{Buffer, Holders, Relays, WalkState};
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
/// Holder-facing minted assets (CIP-68 reference tokens + labelled fungibles
/// excluded) — the collection's real supply.
pub const META_MINTED_HOLDINGS: &str = "minted_holdings";
pub const META_SUPPLY: &str = "supply";

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
    let mut ledger = Ledger::open(&args.db)?;
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
    // Declared terminals are seated HERE, not left to be recorded on contact.
    //
    // `on_movement` records the receiver of a movement, so a wallet that only
    // ever PAYS the watch set is never seated by contact — and its own funding,
    // which is usually the reason it was declared, is never recorded at all. On
    // Mekka, two wallets supplying the treasury with 38,870 ADA: one was seated
    // only by accident a third of the way in, the other never.
    //
    // Terminal, so this cannot recruit: seeding those two as ordinary wallets
    // instead took the frontier from 180 parties to 6,424 and unresolved inputs
    // from 0.06% to 16.5%.
    for t in &registry.terminal.parties {
        frontier.seed_terminal(stake_party(&t.stake), walk_start);
    }

    // Holders a PREVIOUS walk discovered (the kept `discovered_holder` table)
    // are seated FROM THE FLOOR. Discovered mid-walk they were seated too late
    // to book their earlier transactions — and on a queued mint the payment
    // precedes the fulfilment, so the purchase leg is precisely what the first
    // pass missed. Watched-never-expanding, so this cannot recruit.
    let holders = ledger.discovered_holders()?;
    let holders_seated = holders.len();
    for h in &holders {
        frontier.seed_holder(stake_party(h), walk_start);
    }
    if holders_seated > 0 {
        tracing::info!(
            holders_seated,
            "seed: collection holders from a previous walk seated from the floor"
        );
    }

    // Handle sightings the genesis scan harvested — re-emitted as aliases,
    // because `reset` cleared `party_alias` and the walk window alone cannot
    // name a wallet whose handle never moves inside it.
    let handles = ledger.discovered_handles()?;
    if !handles.is_empty() {
        let rows: Vec<crate::store::AliasRow> = handles
            .into_iter()
            .map(|(party, handle, slot)| crate::store::AliasRow {
                party,
                kind: chain_ledger::AliasKind::Handle,
                value: handle,
                slot,
            })
            .collect();
        let n = ledger.insert_aliases(&rows)?;
        tracing::info!(
            aliases = n,
            "seed: handle aliases re-emitted from the genesis scan's sightings"
        );
    }

    // --- persist ---------------------------------------------------------------
    let state = WalkState {
        frontier,
        buffer: Buffer::default(),
        activity: Activity::default(),
        holders: Holders::default(),
        relays: Relays::default(),
    };
    // Cursor at the walk start with an EMPTY hash = "not started": the walk
    // streams from genesis and fast-skips to this slot.
    ledger.checkpoint(&state, walk_start, &[])?;
    for w in &registry.wallets {
        ledger.label_party(&w.stake, &w.label, &w.source)?;
    }
    // Terminals carry their label and source too — a declared terminal is an
    // ASSERTION ("this wallet is custodial-scale, never expand it") and an
    // unsourced one is exactly the laundering-of-guesses this tool refuses
    // elsewhere.
    for t in &registry.terminal.parties {
        ledger.label_party(&t.stake, &t.label, &t.source)?;
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
