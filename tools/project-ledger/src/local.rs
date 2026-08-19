//! The LOCAL rung of the input-resolution ladder: resolve refs by reading them
//! back out of the Mithril snapshot.
//!
//! ## Why this exists
//!
//! The ladder was `buffer → outref_cache → Koios`. The buffer only ever holds
//! **watched parties' own** outputs, so it resolves a tracked wallet spending
//! something the walk already saw — and nothing else. Every genuine inbound
//! payment comes from a stranger whose UTxO the walk never buffered, so it fell
//! straight through to the remote rung.
//!
//! On the first Mekka walk that rung was `--remote offline`, so nothing
//! resolved at all: **415,508 outputs were booked as receipts from an unnamed
//! payer**, and 96.4% of transactions had at least one unresolved input.
//!
//! That is not just missing attribution. `book_unit_flows` recognises change by
//! asking whether an output goes back to a wallet that FUNDED the transaction —
//! and with no resolved inputs there are no known funders, so a wallet's own
//! change coming back is indistinguishable from money arriving. Gross
//! throughput gets recorded as income. On the Mekka ledger one script address
//! shows 115M ₳ "received" against a net of 305k.
//!
//! ## Why LOCAL, and not Koios
//!
//! **The snapshot already contains every one of these outputs.** An input
//! references an output created in an earlier block of the same certified
//! immutable DB — 8,940 chunks, ~215 GB, genesis to tip. Reaching over the
//! network for data sitting on the disk is the wrong rung: it is slower per
//! ref, rate-limited, and it makes a rebootstrap re-fetch what it already had.
//!
//! It also restores a decision already taken for `market-ledger` in July:
//! resolution is local-first, with a cached remote only as a rare fallback.
//! That principle never made it into this walker.
//!
//! ## How it runs
//!
//! One extra pass, driven by the walk's own shopping list:
//!
//! 1. a walk records every ref it could not resolve into `wanted_outref`;
//! 2. `resolve-local` scans the snapshot and, for any transaction whose hash is
//!    on that list, writes its outputs into `outref_cache`;
//! 3. re-walk — the ladder now finds them, and the change rule works.
//!
//! Matching is by TRANSACTION HASH, not by ref: one tx commonly holds several
//! wanted outputs, and all of its outputs are cached when it is found, since
//! having decoded the transaction the marginal cost of keeping the rest is a
//! row each and it spares a future pass.
//!
//! ## The one thing to get right: where to start
//!
//! An input points at an output made EARLIER than the transaction that spends
//! it, by an unknown margin — a wallet may sit on a UTxO for years. So there is
//! no start slot that is provably far enough back short of genesis, and
//! `--from-slot` is a cost/coverage dial rather than a correctness one. The run
//! reports exactly how much of the list it closed, so widening it is an
//! informed decision instead of a guess.

use std::path::PathBuf;

use anyhow::{Context, Result};
use mitos_chain_walk::{decode_tx, open_blocks};
use pallas_traverse::MultiEraBlock;

use crate::store::{CachedOutput, Ledger};

#[derive(clap::Args, Debug)]
pub struct LocalArgs {
    #[arg(long, default_value = "project-ledger.db")]
    pub db: PathBuf,

    /// Snapshot root; the scan reads `<data-dir>/immutable`.
    #[arg(long)]
    pub data_dir: PathBuf,

    /// Slot to start scanning from. Omitted = genesis, which is the only
    /// setting that can close the list completely.
    ///
    /// A smaller range is much cheaper and will resolve the majority — most
    /// spent UTxOs are recent — so this is the dial for "measure before
    /// committing to a full pass".
    #[arg(long)]
    pub from_slot: Option<u64>,

    /// Stop after this slot. Pointless for a real run; useful for timing one.
    #[arg(long)]
    pub to_slot: Option<u64>,

    /// Write to the cache every N found transactions.
    #[arg(long, default_value_t = 2_000)]
    pub flush_every: u64,
}

/// Scan the snapshot for wanted refs and cache what it finds.
pub fn resolve_local(args: &LocalArgs) -> Result<()> {
    let mut ledger = Ledger::open(&args.db)?;
    let wanted = ledger.wanted_tx_hashes()?;
    let (before_wanted, before_have) = ledger.wanted_progress()?;
    if wanted.is_empty() {
        tracing::info!(
            "resolve-local: nothing wanted — run a walk first, or the list is already closed"
        );
        return Ok(());
    }
    // The earliest slot anything was wanted AT is a ceiling on where to start:
    // the output was created strictly before the tx that spent it, so a scan
    // beginning here is guaranteed to find nothing. Printed because the useful
    // question is "how much earlier than this?", and the operator can only ask
    // it if the number is on screen.
    let wanted_from = ledger.wanted_first_slot()?;
    tracing::info!(
        txs_wanted = wanted.len(),
        refs_wanted = before_wanted,
        already_cached = before_have,
        from_slot = ?args.from_slot,
        earliest_want_at_slot = ?wanted_from,
        "resolve-local: scanning the snapshot"
    );
    if let (Some(start), Some(want)) = (args.from_slot, wanted_from)
        && start >= want
    {
        tracing::warn!(
            start,
            want,
            "resolve-local: --from-slot is at or after the earliest slot that WANTED a ref. \
             Every wanted output was created before that, so this scan will resolve nothing — \
             start earlier."
        );
    }

    let immutable_dir = args.data_dir.join("immutable");
    // An EMPTY hash is a slot-only fuzzy seek: it binary-searches the chunk
    // list instead of decoding everything below the start, which is over an
    // hour of CPU on mainnet.
    let blocks = open_blocks(&immutable_dir, args.from_slot.map(|s| (s, Vec::new())))
        .context("opening the immutable DB for a local resolve")?;

    let mut scanned_blocks = 0u64;
    let mut found_txs = 0u64;
    let mut cached_rows = 0u64;
    let mut pending: Vec<(mitos_chain_walk::OutRef, CachedOutput)> = Vec::new();
    let mut last_slot = 0u64;

    for block in blocks {
        let bytes = block.map_err(|e| anyhow::anyhow!("reading block: {e:?}"))?;
        let blk = MultiEraBlock::decode(&bytes)
            .map_err(|e| anyhow::anyhow!("decoding block at ~#{scanned_blocks}: {e:?}"))?;
        scanned_blocks += 1;
        last_slot = blk.slot();
        if let Some(t) = args.to_slot
            && last_slot > t
        {
            tracing::info!(to_slot = t, "resolve-local: reached --to-slot");
            break;
        }

        for tx in blk.txs() {
            // The hot path is this single lookup, run once per transaction on
            // mainnet — everything else is behind it.
            if !wanted.contains(tx.hash().as_ref()) {
                continue;
            }
            found_txs += 1;
            let d = decode_tx(&tx);
            for o in &d.outputs {
                pending.push((
                    (d.tx_hash, o.index),
                    CachedOutput {
                        address: o.address.clone(),
                        lovelace: o.lovelace,
                        assets: o
                            .assets
                            .iter()
                            .map(|a| (a.policy.clone(), a.name.clone()))
                            .collect(),
                    },
                ));
            }
            if found_txs.is_multiple_of(args.flush_every.max(1)) {
                cached_rows += ledger.cache_put(&pending)? as u64;
                pending.clear();
                let (w, g) = ledger.wanted_progress()?;
                tracing::info!(
                    slot = last_slot,
                    scanned_blocks,
                    found_txs,
                    cached_rows,
                    closed = format!("{g}/{w}"),
                    "resolve-local: progress"
                );
            }
        }
        if scanned_blocks.is_multiple_of(500_000) {
            tracing::info!(
                slot = last_slot,
                scanned_blocks,
                found_txs,
                "resolve-local: scanning"
            );
        }
    }
    cached_rows += ledger.cache_put(&pending)? as u64;

    let (wanted_n, have_n) = ledger.wanted_progress()?;
    let pct = if wanted_n == 0 {
        100.0
    } else {
        have_n as f64 * 100.0 / wanted_n as f64
    };
    // SAY WHAT IS STILL MISSING. A partial resolve that reports only its hits
    // reads as a completed job, and the next walk would book the remainder as
    // receipts again with nobody the wiser.
    tracing::info!(
        scanned_blocks,
        last_slot,
        found_txs,
        cached_rows,
        closed = format!("{have_n}/{wanted_n} ({pct:.1}%)"),
        "resolve-local: done"
    );
    if have_n < wanted_n {
        tracing::warn!(
            still_wanted = wanted_n - have_n,
            "resolve-local: refs remain unresolved — they point at outputs created BEFORE the \
             scanned range. Re-run with an earlier --from-slot (omit it for genesis); until then \
             a walk still cannot tell those receipts from change."
        );
    }
    Ok(())
}
