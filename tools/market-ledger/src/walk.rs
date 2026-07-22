//! `walk` — iterate certified immutable-DB history, decode each block, and
//! (this slice) count the marketplace-touching txs per venue above the fast-skip
//! floor. The outref buffer + `DecodeTx` assembly + sqlite ingest land in the
//! next slice; this proves the chunk-read → block-decode → venue-classify
//! pipeline end to end.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Result, bail};
use pallas_hardano::storage::immutable;
use pallas_traverse::{MultiEraBlock, MultiEraTx};

use crate::venue::VenueRegistry;

#[derive(clap::Args, Debug)]
pub struct WalkArgs {
    /// Data dir holding the immutable DB (expects `<data-dir>/immutable`).
    #[arg(long)]
    data_dir: PathBuf,

    /// Venue registry TOML.
    #[arg(long, default_value = "venues.toml")]
    venues: PathBuf,

    /// Comma-separated venues to enable (default: every venue in the registry).
    #[arg(long, value_delimiter = ',')]
    venue: Vec<String>,

    /// Start slot (default: the lowest enabled venue's `earliest_slot`).
    #[arg(long)]
    from_slot: Option<u64>,

    /// Stop after this many in-range blocks (0 = no limit) — for smoke tests.
    #[arg(long, default_value_t = 0)]
    max_blocks: u64,
}

pub fn run(args: WalkArgs) -> Result<()> {
    let registry = VenueRegistry::load(&args.venues, &args.venue)?;
    let floor = args
        .from_slot
        .unwrap_or_else(|| registry.min_earliest_slot());
    let immutable_dir = args.data_dir.join("immutable");
    if !immutable_dir.is_dir() {
        bail!(
            "immutable DB not found at {} — run `market-ledger bootstrap` first",
            immutable_dir.display()
        );
    }

    let venues: Vec<&str> = registry.venue_names().collect();
    tracing::info!(?venues, floor, dir = %immutable_dir.display(), "walk: starting");

    // NOTE: `read_blocks` streams from genesis; blocks below `floor` are decoded
    // and skipped. Chunk-level seeking (skip whole pre-floor chunks) and
    // resume-from-cursor (`read_blocks_from_point`) are follow-up optimizations —
    // see the outref-buffer slice.
    let blocks = immutable::read_blocks(&immutable_dir).map_err(|e| {
        anyhow::anyhow!("opening immutable DB at {}: {e:?}", immutable_dir.display())
    })?;

    let mut scanned: u64 = 0;
    let mut in_range: u64 = 0;
    let mut marketplace_txs: u64 = 0;
    let mut per_venue: BTreeMap<String, u64> = BTreeMap::new();
    let mut last_slot: u64 = 0;

    for block in blocks {
        let bytes = block.map_err(|e| anyhow::anyhow!("reading block from chunk: {e:?}"))?;
        let blk = MultiEraBlock::decode(&bytes)
            .map_err(|e| anyhow::anyhow!("decoding block at ~#{scanned}: {e:?}"))?;
        scanned += 1;
        let slot = blk.slot();
        last_slot = slot;

        if slot < floor {
            if scanned.is_multiple_of(500_000) {
                tracing::info!(scanned, slot, "walk: skipping toward floor");
            }
            continue;
        }
        in_range += 1;

        for tx in blk.txs() {
            if let Some(venue) = tx_venue(&tx, &registry) {
                marketplace_txs += 1;
                *per_venue.entry(venue).or_default() += 1;
            }
        }

        if in_range.is_multiple_of(100_000) {
            tracing::info!(scanned, in_range, slot, marketplace_txs, "walk: progress");
        }
        if args.max_blocks != 0 && in_range >= args.max_blocks {
            tracing::info!(max_blocks = args.max_blocks, "walk: max-blocks reached");
            break;
        }
    }

    tracing::info!(
        scanned,
        in_range,
        last_slot,
        marketplace_txs,
        ?per_venue,
        "walk: complete"
    );
    Ok(())
}

/// The venue whose watched site this tx touches (by any produced output address),
/// if any. Slice 2 classifies on produced outputs only; input resolution (the
/// outref buffer) arrives next.
fn tx_venue(tx: &MultiEraTx, registry: &VenueRegistry) -> Option<String> {
    for out in tx.outputs() {
        let Ok(addr) = out.address() else {
            continue;
        };
        let Ok(bech32) = addr.to_bech32() else {
            continue;
        };
        if let Some(w) = registry.watch_for(&bech32) {
            return Some(w.venue.clone());
        }
    }
    None
}
