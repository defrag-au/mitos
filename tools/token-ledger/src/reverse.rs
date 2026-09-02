//! `reverse` — walk history BACKWARD from the tip, emitting newest first.
//!
//! The forward walk in [`crate::walk`] is the complete one: it starts at or
//! below the policy's first mint, so its outref buffer is complete by
//! construction and every projection over it — float, holder count,
//! concentration, market cap — is exact. It is also the wrong shape for a feed.
//! A feed wants the newest row FIRST, and a forward walk produces the newest row
//! LAST, so nothing can be shown until the whole window is done.
//!
//! This mode inverts that: chunk-descending, newest first, rows available in
//! seconds and improving as it goes.
//!
//! # The buffer inverts too
//!
//! Forward, the buffer holds outputs awaiting their spender: a tx spends outref
//! X, `buffer.take(X)` says who held it, and the negative delta is known
//! immediately.
//!
//! Backward, that is impossible — the output that created X sits at a LOWER
//! slot, which is ground this walk has not covered yet. So the state inverts:
//! [`Pending`] holds outrefs already spent by transactions we have already
//! written, waiting for their creating output to come into view. Every
//! transaction is therefore written in two stages:
//!
//! - its **positive** deltas immediately, from its own outputs
//! - its **negative** deltas later, on reaching the transactions further back
//!   that created what it spent — via [`crate::store::Ledger::add_deltas`]
//!
//! That is the same progressive-enrichment shape the wallet view already speaks
//! over its socket (`FlowDelta::RowsUpdated`): a row lands readable and is
//! corrected in place rather than withheld until perfect.
//!
//! # What bounds it
//!
//! `Pending` grows with unresolved inputs and shrinks as they resolve, so it is
//! bounded by the transactions still missing a source rather than by the
//! policy's supply. That is the property that makes this safe on a collection a
//! completeness-first walker cannot touch at all: cost follows ACTIVITY in the
//! window, never the size of the holder set.
//!
//! # The sieve gate stays complete
//!
//! A watched input's creating output necessarily carried the policy id, so its
//! block — and its chunk — is a sieve hit. Skipping non-hit chunks therefore
//! cannot lose a resolution. Non-watched inputs (the ADA that funds a fee) may
//! never be visited at all, which costs nothing: they were never owed a delta.
//! Conservation is what distinguishes the two, and it needs no help from here —
//! see [`crate::walk::BreachKind`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use mitos_chain_walk::decode::decode_tx;
use mitos_chain_walk::mithril::CHUNK_SLOTS;
use mitos_chain_walk::{open_blocks, slot_to_unix};
use pallas_primitives::Hash;
use pallas_traverse::MultiEraBlock;

use crate::registry;
use crate::store::{DeltaRow, Ledger, TxRow};
use crate::walk::{Watched, stake_of, units_in_output};

/// Slots per day on Cardano — one slot per second.
const SLOTS_PER_DAY: u64 = 86_400;

#[derive(clap::Args, Debug)]
pub struct ReverseArgs {
    /// Data dir holding the immutable DB (expects `<data-dir>/immutable`).
    #[arg(long)]
    pub data_dir: PathBuf,

    /// Token registry TOML.
    #[arg(long, default_value = "tokens.toml")]
    pub tokens: PathBuf,

    /// Registered name, `<policy>.<name_hex>` unit, or a bare 56-hex POLICY.
    #[arg(long)]
    pub token: String,

    /// Ledger sqlite path (default: `<token>.db`).
    #[arg(long)]
    pub db: Option<PathBuf>,

    /// How much further back to reach, in days, measured from the ledger's
    /// current floor (or from the tip on a cold ledger).
    ///
    /// This is the WINDOW, and it is the whole cost control: a pass reads the
    /// chain in that window and nothing else, whatever the policy's supply. Run
    /// it again to go deeper — each pass extends coverage downward and resolves
    /// sources for rows already written.
    #[arg(long, default_value_t = 30)]
    pub days: u64,

    /// Absolute floor to stop at, overriding `--days`.
    #[arg(long)]
    pub to_slot: Option<u64>,

    /// Skip the memmem gate and decode every block. Slower by roughly the ratio
    /// of hit blocks to all blocks; only useful for isolating a suspected gate
    /// bug, since the gate is complete by the ledger's balance rule.
    #[arg(long)]
    pub no_sieve: bool,
}

/// Run one reverse pass, extending the ledger's coverage downward.
pub fn run(args: ReverseArgs) -> Result<()> {
    let token = registry::load_or_unit(&args.tokens, &args.token)?;
    let policy = token.policy_bytes()?;
    let watched = match token.asset_name_bytes()? {
        Some(name) => Watched::Unit(name),
        None => Watched::Policy,
    };

    let immutable = args.data_dir.join("immutable");
    if !immutable.is_dir() {
        bail!(
            "immutable DB not found at {} — point --data-dir at a bootstrapped snapshot",
            immutable.display()
        );
    }

    let db_path = args
        .db
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("{}.db", token.name)));
    let mut ledger = Ledger::open(&db_path)?;
    ledger.put_meta(
        &token.name,
        &policy,
        token.asset_name_bytes()?.as_deref(),
        token.resolved_decimals(),
    )?;

    let all_chunks = chain_sieve::list_chunks(&immutable, 0)?;
    let tip_slot = all_chunks
        .last()
        .map(|c| (c + 1) * CHUNK_SLOTS)
        .unwrap_or(0);

    // A pass runs from the existing floor DOWNWARD, so coverage stays
    // contiguous. On a cold ledger there is no floor yet and the tip is the
    // ceiling — the first pass is the one that establishes it.
    let ceiling = ledger.walked_from()?.unwrap_or(tip_slot);
    let floor = match args.to_slot {
        Some(s) => s,
        None => ceiling.saturating_sub(args.days * SLOTS_PER_DAY),
    };
    // Never dig below the policy's own first mint when we know it: there is
    // nothing there, and reading it is pure cost.
    let floor = token.floor_slot.map_or(floor, |first| floor.max(first));

    if floor >= ceiling {
        println!(
            "nothing to do — coverage already reaches slot {ceiling} \
             and the requested floor is {floor}"
        );
        return Ok(());
    }

    tracing::info!(
        token = %token.name,
        policy = %token.policy,
        policy_wide = watched_is_policy(&watched),
        floor,
        ceiling,
        days = args.days,
        "reverse: pass starting"
    );

    let mut pending = Pending::default();
    let outcome = pass(
        &mut ledger,
        &immutable,
        &all_chunks,
        &policy,
        &watched,
        floor,
        ceiling,
        &mut pending,
        !args.no_sieve,
    )?;

    println!("covered down to slot        = {}", outcome.floor);
    println!("transactions written        = {}", outcome.written);
    println!("delta rows backfilled       = {}", outcome.backfilled);
    println!("sources still below floor   = {}", outcome.unresolved);
    if outcome.unresolved > 0 {
        println!(
            "  run again to reach deeper — each pass resolves sources for rows \
             already written"
        );
    }
    Ok(())
}

fn watched_is_policy(w: &Watched) -> bool {
    matches!(w, Watched::Policy)
}

/// An outref spent by a transaction we have already written, whose creating
/// output we have not reached yet.
///
/// The reverse counterpart of a buffered output. Keyed by outref because that
/// is what a later (earlier-in-slot) transaction will present when it creates
/// the thing — the same join the forward buffer makes, in the other direction.
#[derive(Default)]
pub struct Pending {
    /// outref → the transactions that spent it.
    ///
    /// A `Vec` because a single outref can only be spent once on chain, but a
    /// resumed run can legitimately re-record the same waiter, and collapsing
    /// that to a single slot would drop the second half of a re-walk.
    waiting: HashMap<(Hash<32>, u32), Vec<Hash<32>>>,
}

impl Pending {
    fn want(&mut self, oref: (Hash<32>, u32), spender: Hash<32>) {
        self.waiting.entry(oref).or_default().push(spender);
    }

    /// Transactions waiting on this outref, removing it from the open set.
    fn resolve(&mut self, oref: &(Hash<32>, u32)) -> Option<Vec<Hash<32>>> {
        self.waiting.remove(oref)
    }

    pub fn len(&self) -> usize {
        self.waiting.len()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.waiting.is_empty()
    }
}

/// How far a reverse pass got, and what it cost.
pub struct Outcome {
    /// Lowest slot now covered — the ledger's new floor.
    pub floor: u64,
    /// Transactions written by this pass.
    pub written: u64,
    /// Delta rows backfilled onto transactions written earlier, here or by a
    /// previous pass. The measure of the walk resolving its own history.
    pub backfilled: usize,
    /// Outrefs still waiting on a source below the floor. The honest gap, and
    /// what a deeper pass would close.
    pub unresolved: usize,
}

/// Chunk numbers to visit, highest first, covering `[floor, ceiling)`.
///
/// Descending is the whole point, but the chunk LIST still has to come from the
/// directory rather than from arithmetic: chunk files are dense in practice and
/// sparse in principle, and inventing numbers that are not on disk turns a gap
/// into a read error halfway through a pass.
pub fn chunks_descending(chunks: &[u64], floor: u64, ceiling: u64) -> Vec<u64> {
    let lo = floor / CHUNK_SLOTS;
    let hi = ceiling.div_ceil(CHUNK_SLOTS);
    let mut wanted: Vec<u64> = chunks
        .iter()
        .copied()
        .filter(|c| *c >= lo && *c <= hi)
        .collect();
    wanted.sort_unstable_by(|a, b| b.cmp(a));
    wanted
}

/// One chunk's transactions, newest first.
///
/// The immutable DB only reads FORWARD — `open_blocks` seeks to a point and
/// streams upward — so "reverse" is chunk-descending with each chunk read
/// forward and flipped. A chunk is 21,600 slots (~6h), which is fine grain for a
/// feed and a trivial amount to hold: only blocks that pass the sieve gate are
/// decoded, and only their watched transactions are kept.
fn chunk_txs_newest_first(
    immutable: &Path,
    chunk: u64,
    needles: Option<&chain_sieve::Needles<'_>>,
) -> Result<Vec<(u64, Vec<u8>)>> {
    let start = chunk * CHUNK_SLOTS;
    let end = (chunk + 1) * CHUNK_SLOTS;
    let blocks = open_blocks(immutable, Some((start, Vec::new())))
        .with_context(|| format!("seeking chunk {chunk}"))?;
    let mut out = Vec::new();
    for raw in blocks {
        let raw = raw.map_err(|e| anyhow::anyhow!("reading block in chunk {chunk}: {e:?}"))?;
        // Cheap slot peek before any gate: decoding is the cost this whole
        // design exists to avoid, but we must decode to learn the slot. The
        // sieve check first is therefore strictly cheaper on a miss.
        if let Some(n) = needles
            && !n.hit(&raw)
        {
            continue;
        }
        let slot = match MultiEraBlock::decode(&raw) {
            Ok(b) => b.slot(),
            Err(e) => return Err(anyhow::anyhow!("decoding block in chunk {chunk}: {e:?}")),
        };
        if slot >= end {
            break;
        }
        if slot < start {
            continue;
        }
        out.push((slot, raw));
    }
    out.reverse();
    Ok(out)
}

/// Walk `[floor, ceiling)` backward, writing rows newest-first and resolving
/// sources as they come into view.
///
/// `ceiling` is exclusive and is normally the ledger's existing floor, so a
/// pass extends coverage downward contiguously — which is what lets the caller
/// lower `walked_from` for every chunk that commits.
#[allow(clippy::too_many_arguments)]
pub fn pass(
    ledger: &mut Ledger,
    immutable: &Path,
    chunks: &[u64],
    policy: &[u8],
    watched: &Watched,
    floor: u64,
    ceiling: u64,
    pending: &mut Pending,
    sieve: bool,
) -> Result<Outcome> {
    let needles = sieve
        .then(|| chain_sieve::Needles::new(std::slice::from_ref(&policy.to_vec())))
        .transpose()?;

    let mut written = 0u64;
    let mut backfilled = 0usize;
    let mut lowest = ceiling;

    for chunk in chunks_descending(chunks, floor, ceiling) {
        let blocks = chunk_txs_newest_first(immutable, chunk, needles.as_ref())?;
        let mut rows: Vec<TxRow> = Vec::new();
        // Backfills discovered in this chunk, applied after its rows are
        // written: a source and its spender can sit in the SAME chunk, and
        // applying the backfill first would find no parent transaction.
        let mut resolved: Vec<(Hash<32>, DeltaRow)> = Vec::new();

        for (slot, raw) in blocks {
            if slot < floor || slot >= ceiling {
                continue;
            }
            lowest = lowest.min(slot);
            let blk = MultiEraBlock::decode(&raw)
                .map_err(|e| anyhow::anyhow!("decoding block at slot {slot}: {e:?}"))?;
            let block_time = slot_to_unix(slot);

            // Transactions within a block are reversed too, so the emitted
            // order is a strict newest-first total order rather than one that
            // is newest-first between blocks and oldest-first inside them.
            let txs: Vec<_> = blk.txs();
            for tx in txs.iter().rev() {
                let dtx = decode_tx(tx);

                let mut net_mint: HashMap<Vec<u8>, i64> = HashMap::new();
                for pa in tx.mints().iter() {
                    if pa.policy().as_ref() != policy {
                        continue;
                    }
                    for a in pa.assets().iter() {
                        if !watched.matches(a.name()) {
                            continue;
                        }
                        *net_mint.entry(a.name().to_vec()).or_insert(0) +=
                            a.mint_coin().unwrap_or(0);
                    }
                }

                // OUTPUTS: known now, and they are two things at once — this
                // transaction's own positive deltas, AND the resolution of
                // whatever spent them later.
                let mut deltas: Vec<DeltaRow> = Vec::new();
                for out in &dtx.outputs {
                    let units = units_in_output(tx, out, policy, watched);
                    if units.is_empty() {
                        continue;
                    }
                    let stake = stake_of(&out.address);
                    for (name, qty) in &units {
                        deltas.push(DeltaRow {
                            address: out.address.clone(),
                            stake: stake.clone(),
                            name: name.clone(),
                            amount: *qty,
                        });
                    }
                    // Anyone waiting on this outref now learns who held it and
                    // how much — their MISSING NEGATIVE.
                    if let Some(spenders) = pending.resolve(&(dtx.tx_hash, out.index)) {
                        for spender in spenders {
                            for (name, qty) in &units {
                                resolved.push((
                                    spender,
                                    DeltaRow {
                                        address: out.address.clone(),
                                        stake: stake.clone(),
                                        name: name.clone(),
                                        amount: -qty,
                                    },
                                ));
                            }
                        }
                    }
                }

                // A transaction with nothing of ours in its outputs and nothing
                // in its mint field is only interesting if it SPENT something
                // of ours — which we cannot know yet. Recording its inputs as
                // pending is how that is discovered later.
                let touches_us = !deltas.is_empty() || !net_mint.is_empty();
                if !touches_us {
                    continue;
                }

                // INPUTS: unknown now. Register interest and move on.
                for inp in &dtx.inputs {
                    pending.want(inp.oref, dtx.tx_hash);
                }

                rows.push(TxRow {
                    tx_hash: dtx.tx_hash,
                    slot,
                    block_time,
                    net_mint: net_mint.into_iter().collect(),
                    deltas,
                    // Pool reserves and lock lifetimes are COMPLETENESS-shaped
                    // projections: both are read off the live open set, which a
                    // reverse pass does not have. They stay the forward walk's
                    // job, and leaving them empty here is what stops a partial
                    // pass publishing a half-populated curve.
                    pools: Vec::new(),
                    locks_created: Vec::new(),
                    locks_spent: Vec::new(),
                });
            }
        }

        if rows.is_empty() && resolved.is_empty() {
            continue;
        }
        written += rows.len() as u64;
        // The buffer is not this mode's state, so nothing is persisted through
        // it; the cursor written here is the forward frontier, which a reverse
        // pass must not move. `commit_rows` exists for exactly that.
        ledger.commit_rows(&rows)?;

        let mut by_tx: HashMap<Hash<32>, Vec<DeltaRow>> = HashMap::new();
        for (spender, delta) in resolved {
            by_tx.entry(spender).or_default().push(delta);
        }
        for (spender, deltas) in by_tx {
            backfilled += ledger.add_deltas(&spender, &deltas)?;
        }

        // Coverage extends downward one chunk at a time, so the floor moves
        // with committed work rather than at the end of the pass. A pass killed
        // halfway leaves a ledger that knows exactly how deep it got.
        ledger.set_walked_from(chunk * CHUNK_SLOTS)?;
    }

    Ok(Outcome {
        floor: lowest,
        written,
        backfilled,
        unresolved: pending.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(b: u8) -> Hash<32> {
        Hash::from([b; 32])
    }

    /// Descending, and only chunks that exist on disk. Inventing a number that
    /// is not there turns a sparse range into a read error mid-pass.
    #[test]
    fn chunks_come_back_highest_first_and_only_if_present() {
        let on_disk = [10u64, 11, 12, 14, 15];
        let got = chunks_descending(&on_disk, 11 * CHUNK_SLOTS, 15 * CHUNK_SLOTS);
        assert_eq!(got, vec![15, 14, 12, 11]);
    }

    /// The range is inclusive of the chunk holding the floor — a floor mid-chunk
    /// still needs that chunk read, because the slots above it inside the chunk
    /// are in range.
    #[test]
    fn the_chunk_holding_the_floor_is_included() {
        let on_disk = [10u64, 11, 12];
        let got = chunks_descending(&on_disk, 11 * CHUNK_SLOTS + 5, 12 * CHUNK_SLOTS);
        assert!(got.contains(&11), "{got:?}");
    }

    /// An outref is resolved once and then gone: leaving it in the open set
    /// would re-apply its negative delta on the next pass that saw the same
    /// source, which the `INSERT OR IGNORE` in `add_deltas` would mask rather
    /// than fix.
    #[test]
    fn resolving_an_outref_removes_it_from_the_open_set() {
        let mut p = Pending::default();
        p.want((h(1), 0), h(9));
        assert_eq!(p.len(), 1);

        let waiters = p.resolve(&(h(1), 0)).expect("a waiter");
        assert_eq!(waiters, vec![h(9)]);
        assert!(p.is_empty());
        assert!(p.resolve(&(h(1), 0)).is_none(), "resolved only once");
    }

    /// Two transactions can wait on different outputs of the same transaction,
    /// and both must be served when it comes into view.
    #[test]
    fn separate_outputs_of_one_source_serve_separate_spenders() {
        let mut p = Pending::default();
        p.want((h(1), 0), h(8));
        p.want((h(1), 1), h(9));
        assert_eq!(p.resolve(&(h(1), 0)).unwrap(), vec![h(8)]);
        assert_eq!(p.resolve(&(h(1), 1)).unwrap(), vec![h(9)]);
    }

    /// Nothing waiting means nothing to serve — the common case, since most
    /// inputs of a watched transaction never carried the asset.
    #[test]
    fn an_unwanted_outref_resolves_to_nothing() {
        let mut p = Pending::default();
        assert!(p.resolve(&(h(1), 0)).is_none());
    }
}
