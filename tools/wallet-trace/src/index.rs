//! `index` — the chain-wide pass. One forward walk, no input resolution.
//!
//! Writes co-signing groups and credential pairs, and nothing else. It keeps no
//! frontier, resolves no outrefs, and holds no per-party state — the properties
//! that let `project-ledger`'s expensive machinery be skipped entirely here (see
//! [`crate::witness`]).
//!
//! Built once, queried by every investigation. That is the whole reason this is
//! an index rather than a per-case walker: the co-signing graph is the same
//! graph for every question you might ask of it.

use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use mitos_chain_walk::open_blocks;
use pallas_traverse::MultiEraBlock;

use crate::creds::cred_pair;
use crate::store::Index;
use crate::witness::{KeyHash, signer_keys};

/// Recently-written credential pairs, to skip redundant `INSERT OR IGNORE`
/// round trips. Addresses repeat heavily inside any window, so this removes
/// most of the write traffic; cleared wholesale when full, since correctness
/// never depends on it (sqlite still dedupes).
const PAIR_CACHE_MAX: usize = 2_000_000;

#[derive(clap::Args, Debug)]
pub struct IndexArgs {
    #[arg(long, default_value = "wallet-trace.db")]
    pub db: PathBuf,

    /// Snapshot root; the walk reads `<data-dir>/immutable`.
    #[arg(long)]
    pub data_dir: PathBuf,

    /// Slot to start at. Omitted = genesis.
    #[arg(long)]
    pub from_slot: Option<u64>,

    /// Stop after this slot.
    #[arg(long)]
    pub to_slot: Option<u64>,

    /// Resume from the last slot this index recorded, ignoring `--from-slot`.
    #[arg(long)]
    pub resume: bool,

    /// Group-size cap. A transaction with more distinct signers than this is an
    /// operator batch, not a person, and contributes nothing.
    ///
    /// MEASURED: raising this past 8 buys very little (a 2→32 move added ~2% of
    /// transactions) while admitting the worst false-merge sources.
    #[arg(long, default_value_t = 8)]
    pub max_group: usize,

    /// Commit every N transactions.
    #[arg(long, default_value_t = 20_000)]
    pub flush_every: u64,

    /// Skip credential pairs. Halves the artifact, at the cost of `trace` being
    /// unable to name a cluster's wallets — it would emit bare key hashes.
    #[arg(long)]
    pub no_cred_pairs: bool,
}

pub fn index(args: &IndexArgs) -> Result<()> {
    if args.max_group < 2 {
        bail!("--max-group must be at least 2; a group of one joins nothing");
    }
    let mut ix = Index::open(&args.db)?;
    let mut group_id = ix.max_group_id()?;

    let from_slot = if args.resume {
        match ix.get_meta("last_slot")? {
            Some(s) => {
                let slot: u64 = s.parse().context("meta.last_slot")?;
                tracing::info!(slot, "index: resuming");
                Some(slot)
            }
            None => {
                tracing::warn!("index: --resume but no last_slot recorded; starting from scratch");
                args.from_slot
            }
        }
    } else {
        args.from_slot
    };

    let immutable_dir = args.data_dir.join("immutable");
    // An EMPTY hash is a slot-only FUZZY seek — it binary-searches the chunk
    // list instead of decoding everything below the floor.
    let blocks = open_blocks(&immutable_dir, from_slot.map(|s| (s, Vec::new())))
        .context("opening the immutable DB for an index")?;

    let mut groups: Vec<(i64, [u8; 32], u64)> = Vec::new();
    let mut members: Vec<(i64, KeyHash)> = Vec::new();
    let mut pairs: Vec<(KeyHash, KeyHash, bool, u64)> = Vec::new();
    let mut pair_cache: HashSet<(KeyHash, KeyHash)> = HashSet::new();

    let mut blocks_seen = 0u64;
    let mut txs = 0u64;
    let mut since_flush = 0u64;
    let mut last_slot = 0u64;
    let mut first_slot: Option<u64> = None;

    for block in blocks {
        let bytes = block.map_err(|e| anyhow::anyhow!("reading block: {e:?}"))?;
        let blk = MultiEraBlock::decode(&bytes)
            .map_err(|e| anyhow::anyhow!("decoding block at ~#{blocks_seen}: {e:?}"))?;
        let slot = blk.slot();
        if let Some(t) = args.to_slot
            && slot > t
        {
            tracing::info!(to_slot = t, "index: reached --to-slot");
            break;
        }
        blocks_seen += 1;
        first_slot.get_or_insert(slot);
        last_slot = slot;

        for tx in blk.txs() {
            txs += 1;
            since_flush += 1;

            let signers = signer_keys(&tx);
            if signers.is_group() && signers.len() <= args.max_group {
                group_id += 1;
                let mut hash = [0u8; 32];
                hash.copy_from_slice(tx.hash().as_ref());
                groups.push((group_id, hash, slot));
                for k in &signers.keys {
                    members.push((group_id, *k));
                }
            }

            if !args.no_cred_pairs {
                for out in tx.outputs() {
                    let Ok(addr) = out.address() else { continue };
                    let Some(p) = cred_pair(&addr) else { continue };
                    if pair_cache.len() >= PAIR_CACHE_MAX {
                        pair_cache.clear();
                    }
                    if pair_cache.insert((p.payment, p.stake)) {
                        pairs.push((p.payment, p.stake, p.stake_is_script, slot));
                    }
                }
            }
        }

        if since_flush >= args.flush_every {
            ix.write_batch(&groups, &members, &pairs)?;
            ix.set_meta("last_slot", &last_slot.to_string())?;
            groups.clear();
            members.clear();
            pairs.clear();
            since_flush = 0;
            tracing::info!(
                blocks = blocks_seen,
                txs,
                slot,
                groups = group_id,
                "index: walking"
            );
        }
    }

    ix.write_batch(&groups, &members, &pairs)?;
    ix.set_meta("last_slot", &last_slot.to_string())?;
    if let Some(f) = first_slot
        && ix.get_meta("first_slot")?.is_none()
    {
        ix.set_meta("first_slot", &f.to_string())?;
    }
    ix.set_meta("max_group", &args.max_group.to_string())?;

    let (g, m, c, _) = ix.counts()?;
    tracing::info!(
        blocks = blocks_seen,
        txs,
        slots = ?(first_slot, last_slot),
        groups = g,
        cosign_rows = m,
        cred_pairs = c,
        "index: done"
    );
    println!(
        "\nindexed {blocks_seen} blocks / {txs} txs  slots {}..{}\n  \
         {g} groups, {m} cosign rows, {c} cred pairs\n  \
         next: `wallet-trace suppress --db {}`",
        first_slot.unwrap_or(0),
        last_slot,
        args.db.display()
    );
    Ok(())
}
