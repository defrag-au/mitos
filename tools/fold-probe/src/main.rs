//! fold-probe — what a UTxO fold over the Mithril chunk log actually costs.
//!
//! The question behind `docs/design/MITHRIL_PHONEBOOK.md` §3 and
//! `docs/design/FOOTPRINT_AND_MINIMUM_SPEC.md` §7 — "can the live set be built
//! by a purpose-built folder, and what does it cost" — has four answers, and a
//! probe that reports one number cannot separate them:
//!
//! 1. **decode vs apply wall time.** A fold is a pure decode phase (CBOR split,
//!    `MultiEraBlock::decode`, tx hash, value/datum shape) feeding a stateful
//!    apply phase (resolve each spend, tombstone it, insert each output). Only
//!    the apply phase is serial, so this split is the ceiling on every parallel
//!    answer below it.
//! 2. **the same-chunk create→spend rate.** The fraction of spends whose source
//!    was created inside the same chunk. It decides whether speculative parallel
//!    apply is viable: high means a redo loop that eats the win.
//! 3. **live-set entries and shape.** The sizing input for a packed or mmap'd
//!    layout, measured from real outputs rather than assumed per-entry bytes.
//! 4. **peak anon RSS.** `FOOTPRINT` §4.1's finding is that user-space caches
//!    are anonymous memory the kernel cannot evict, so this is the number a
//!    cgroup limit kills the process on.
//!
//! # Two modes, because a window cannot answer (3)
//!
//! `fold` walks a contiguous window with a real set and reports all four. But a
//! window starts from an EMPTY set, so what it ends with is the residue of the
//! window and NOT mainnet's live set — no extrapolation of that is honest.
//! `count` walks every complete chunk with **no set at all**, only counters
//! (`created − spent` = live), which makes it embarrassingly parallel and
//! answers the sizing question exactly.
//!
//! # What this deliberately does not do
//!
//! - **No addresses.** A folder matching module interest pays `address()` per
//!   output; this does not, so the decode figure is a floor — add the interest
//!   match separately.
//! - **No datum hashing.** Inline datum length is recorded; the blake2b over it
//!   is not, because a set can hold the datum on the containing output.
//! - **No collateral handling.** A phase-2 failure consumes its collateral and
//!   creates its collateral return; both are skipped with the invalid
//!   transaction. Reported as a count, so the size of the omission is visible
//!   rather than assumed small.
//! - **No entity graph.** UTxO only. `AssetState` and the rest are separate
//!   projections with their own cost.
//! - **No I/O tuning.** One `fs::read` per chunk on the folding thread; read
//!   time is reported separately so a prefetch stage can be justified rather
//!   than assumed.
//!
//! The report goes to stdout as data rather than through `tracing`: it is the
//! deliverable, and it is meant to be pasted whole.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use mitos_chain_walk::mithril::CHUNK_SLOTS;
use pallas_codec::minicbor::Decoder;
use pallas_primitives::Hash;
use pallas_primitives::conway::DatumOption;
use pallas_traverse::{Era, MultiEraBlock};
use tx_index::extract::list_chunks;

/// An outref: the key of the live set, and the thing a spend names.
type OutRef = (Hash<32>, u32);

#[derive(Parser)]
#[command(about = "Measure what a UTxO fold over Mithril chunks costs")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Fold a contiguous window with a real live set (serial), reporting all four numbers.
    Fold(FoldArgs),
    /// Count created/spent over every complete chunk with no set (parallel) — the exact live-set size.
    Count(CountArgs),
}

#[derive(Args)]
struct FoldArgs {
    /// The Mithril immutable directory (holds `NNNNN.chunk`).
    #[arg(long)]
    immutable: PathBuf,
    /// First chunk of the window. Default: the newest `--chunks` complete chunks.
    #[arg(long)]
    start: Option<u16>,
    /// Chunks to walk; 0 = every complete chunk (the full serial fold — hours).
    #[arg(long, default_value_t = 10)]
    chunks: usize,
    /// A line per block, so a pathologically slow chunk names the block in it.
    #[arg(long)]
    verbose: bool,
}

#[derive(Args)]
struct CountArgs {
    /// The Mithril immutable directory (holds `NNNNN.chunk`).
    #[arg(long)]
    immutable: PathBuf,
    /// Worker threads; 0 = one per available core.
    #[arg(long, default_value_t = 0)]
    threads: usize,
    /// Stop after this many chunks (0 = all). For a bounded run on a busy box.
    #[arg(long, default_value_t = 0)]
    max_chunks: usize,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Fold(a) => fold(a),
        Cmd::Count(a) => count(a),
    }
}

// ---------------------------------------------------------------- the set

/// Key width: 32-byte tx hash + 4-byte output index, packed.
const KEY_BYTES: usize = 36;

/// The value of the live set, sized as a *packed* row: what a layout that
/// stores a summary per output (rather than the output's CBOR) would pay.
///
/// `size_of::<Entry>()` is reported in the output so the reader can recompute
/// the packed floor for a different value shape — this is a stand-in, not a
/// decision, and the numbers to design against are the entries and the shape
/// histogram, which are measured.
#[derive(Clone, Copy, Default)]
struct Entry {
    /// Which chunk created this output. Only used to measure the same-chunk
    /// spend rate; a production layout would not carry it.
    created_chunk: u16,
    lovelace: u64,
    n_assets: u16,
    /// Σ (28-byte policy + asset-name length) over this output's assets.
    asset_bytes: u32,
    /// 32 for a hash-only datum, else the inline datum's CBOR length, else 0.
    datum_bytes: u32,
}

/// One block's work, laid out as the boundary a pipelined folder would hand
/// across threads: the decoder fills it, the applier drains it.
///
/// Flat vecs plus per-tx spans rather than a vec per tx, so a block costs three
/// allocations and not one per transaction. Intra-block order is preserved by
/// the spans: Cardano allows a later tx in a block to spend an earlier tx's
/// output, so applying all spends before all creates would be wrong.
#[derive(Default)]
struct BlockPlan {
    spends: Vec<OutRef>,
    creates: Vec<(OutRef, Entry)>,
    /// `(spend_start, spend_end, create_start, create_end)` per tx.
    spans: Vec<(u32, u32, u32, u32)>,
}

impl BlockPlan {
    fn clear(&mut self) {
        self.spends.clear();
        self.creates.clear();
        self.spans.clear();
    }
}

#[derive(Clone, Copy, Default)]
struct Shape {
    /// The four buckets are a PARTITION of the outputs, not overlapping flags:
    /// a reader adds them up and expects `out`. `bare` means no assets and no
    /// datum; the payload byte totals below are separate sums.
    bare: u64,
    assets_only: u64,
    datum_only: u64,
    both: u64,
    asset_bytes: u64,
    inline_datum_bytes: u64,
}

#[derive(Clone, Default)]
struct DecodeStats {
    secs: f64,
    blocks: u64,
    txs: u64,
    invalid_txs: u64,
    inputs: u64,
    outputs: u64,
    /// Chunk bytes decoded. Reported because chunk size varies by an order of
    /// magnitude across the chain, so a per-chunk rate does not extrapolate and
    /// a per-byte one very nearly does.
    bytes: u64,
    /// Σ lovelace over every output created in the window. **u128, and not for
    /// safety margin**: a whole-chain fold of 355 M outputs sums past
    /// `u64::MAX` lovelace, and a `saturating_add` on u64 reported
    /// 18,446,744,073,709 ₳ as if it were a measurement on 2026-09-22. A
    /// saturated accumulator is a fabricated number.
    lovelace_created: u128,
    shape: Shape,
    eras: Vec<Era>,
}

#[derive(Clone, Copy, Default)]
struct ApplyStats {
    secs: f64,
    spends: u64,
    hit: u64,
    same_chunk: u64,
    miss: u64,
    inserted: u64,
}

// ------------------------------------------------------------- the decode

/// Split a chunk file into its top-level CBOR items — the block bytes.
///
/// Deliberately not the `.secondary` sidecar: `tx-index`'s extractor splits
/// CBOR itself so every offset it records is a fact about bytes it saw, and a
/// probe that measured a different decode path than the index would not be
/// measuring the thing it claims to.
fn split_blocks(bytes: &[u8]) -> Result<Vec<&[u8]>> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos < bytes.len() {
        let mut d = Decoder::new(&bytes[pos..]);
        d.skip()
            .with_context(|| format!("splitting block at byte {pos}"))?;
        let len = d.position();
        if len == 0 {
            bail!("zero-length CBOR item at byte {pos}");
        }
        out.push(&bytes[pos..pos + len]);
        pos += len;
    }
    Ok(out)
}

/// Everything the applier needs from a block, extracted with no reference to
/// the set — which is what makes this phase the parallelisable one.
fn plan_block(block: &MultiEraBlock<'_>, chunk: u16, plan: &mut BlockPlan, st: &mut DecodeStats) {
    plan.clear();
    if !st.eras.contains(&block.era()) {
        st.eras.push(block.era());
    }

    for tx in block.txs() {
        // PHASE-2 FAILURE — the guard every walker in this workspace carries.
        // The block declares the tx invalid, so nothing it produces exists and
        // nothing it names was spent.
        if !tx.is_valid() {
            st.invalid_txs += 1;
            continue;
        }
        let hash = tx.hash();

        let s0 = plan.spends.len() as u32;
        for inp in tx.consumes() {
            plan.spends.push((*inp.hash(), inp.index() as u32));
        }
        let s1 = plan.spends.len() as u32;

        let c0 = plan.creates.len() as u32;
        for (i, o) in tx.outputs().iter().enumerate() {
            let value = o.value();
            let mut n_assets = 0u16;
            let mut asset_bytes = 0u32;
            for pa in value.assets().iter() {
                let policy_len = pa.policy().as_slice().len() as u32;
                for a in pa.assets().iter() {
                    n_assets = n_assets.saturating_add(1);
                    asset_bytes = asset_bytes
                        .saturating_add(policy_len)
                        .saturating_add(a.name().len() as u32);
                }
            }
            let datum_bytes = match o.datum() {
                Some(DatumOption::Hash(_)) => 32u32,
                Some(DatumOption::Data(w)) => {
                    let len = w.0.raw_cbor().len() as u32;
                    st.shape.inline_datum_bytes += u64::from(len);
                    len
                }
                None => 0,
            };
            if n_assets > 0 {
                st.shape.asset_bytes += u64::from(asset_bytes);
            }
            match (n_assets > 0, datum_bytes > 0) {
                (false, false) => st.shape.bare += 1,
                (true, false) => st.shape.assets_only += 1,
                (false, true) => st.shape.datum_only += 1,
                (true, true) => st.shape.both += 1,
            }

            st.lovelace_created = st.lovelace_created.saturating_add(u128::from(value.coin()));
            plan.creates.push((
                (hash, i as u32),
                Entry {
                    created_chunk: chunk,
                    lovelace: value.coin(),
                    n_assets,
                    asset_bytes,
                    datum_bytes,
                },
            ));
        }
        let c1 = plan.creates.len() as u32;

        plan.spans.push((s0, s1, c0, c1));
        st.txs += 1;
        st.inputs += u64::from(s1 - s0);
        st.outputs += u64::from(c1 - c0);
    }
    st.blocks += 1;
}

/// The serial part: resolve every spend against the set, then insert every
/// output. Spends before creates *within a tx*, in tx order — see `BlockPlan`.
fn apply_plan(set: &mut HashMap<OutRef, Entry>, plan: &BlockPlan, chunk: u16, st: &mut ApplyStats) {
    for &(s0, s1, c0, c1) in &plan.spans {
        for r in &plan.spends[s0 as usize..s1 as usize] {
            st.spends += 1;
            match set.remove(r) {
                Some(e) => {
                    st.hit += 1;
                    if e.created_chunk == chunk {
                        st.same_chunk += 1;
                    }
                }
                None => st.miss += 1,
            }
        }
        for (r, e) in &plan.creates[c0 as usize..c1 as usize] {
            set.insert(*r, *e);
            st.inserted += 1;
        }
    }
}

// ------------------------------------------------------------- the driver

fn chunk_path(immutable: &std::path::Path, chunk: u16) -> PathBuf {
    immutable.join(format!("{chunk:05}.chunk"))
}

/// Apparent bytes of every `NNNNN.chunk` under `immutable`, excluding the
/// sidecars — they are 1.5 GB against 230 GB of chunk on mainnet, and including
/// them would inflate a byte-weighted projection. A `metadata()` per chunk, so
/// about 9k stats: once per run, not per chunk.
fn chunk_bytes_total(immutable: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(immutable) else {
        return 0;
    };
    entries
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".chunk"))
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum()
}

fn fold(a: FoldArgs) -> Result<()> {
    let all = list_chunks(&a.immutable)?;
    let window = pick_window(&all, a.start, a.chunks)?;

    println!("fold-probe fold");
    println!("  immutable   {}", a.immutable.display());
    println!(
        "  window      chunks {}..={}  slots {}..={}  ({:.1} d)",
        window[0],
        window[window.len() - 1],
        u64::from(window[0]) * CHUNK_SLOTS,
        (u64::from(window[window.len() - 1]) + 1) * CHUNK_SLOTS,
        (window.len() as f64 * CHUNK_SLOTS as f64) / 86_400.0,
    );
    println!("  chunks      {}", window.len());
    println!();
    println!(
        "{:>5}  {:>7}  {:>9}  {:>6}  {:>8}  {:>8}  {:>9}  {:>8}  {:>9}  {:>6}  {:>6}  {:>7}  {:>8}",
        "chunk",
        "blocks",
        "txs",
        "inval",
        "in",
        "out",
        "decode_s",
        "apply_s",
        "entries",
        "hit%",
        "same%",
        // Not decoration: a miss is a spend naming an output the log never
        // created, and `entries` is `created − spent + misses`. Without this
        // column the drift between a fold and a created−spent count is
        // invisible, and neither can be reconciled with the other.
        "miss",
        "anonMB"
    );

    let mut set: HashMap<OutRef, Entry> = HashMap::new();
    let mut plan = BlockPlan::default();
    let mut dec_acc = DecodeStats::default();
    let mut app_acc = ApplyStats::default();
    let mut read_secs = 0.0f64;
    let mut split_secs = 0.0f64;
    let mut peak_anon_kb = 0u64;
    let mut peak_rss_kb_seen = 0u64;
    let mut max_entries = 0u64;
    let mut rss_available = false;
    let started = Instant::now();

    for &chunk in &window {
        let mut dec = DecodeStats::default();
        let mut app = ApplyStats::default();

        let t = Instant::now();
        let bytes = std::fs::read(chunk_path(&a.immutable, chunk))
            .with_context(|| format!("reading chunk {chunk}"))?;
        read_secs += t.elapsed().as_secs_f64();

        let t = Instant::now();
        let raw_blocks = split_blocks(&bytes)?;
        split_secs += t.elapsed().as_secs_f64();
        let mut chunk_peak_anon = 0u64;
        dec.bytes = bytes.len() as u64;
        for (i, raw) in raw_blocks.iter().enumerate() {
            let t = Instant::now();
            let block = MultiEraBlock::decode(raw)
                .with_context(|| format!("chunk {chunk}: decoding block {i}"))?;
            plan_block(&block, chunk, &mut plan, &mut dec);
            dec.secs += t.elapsed().as_secs_f64();

            let t = Instant::now();
            apply_plan(&mut set, &plan, chunk, &mut app);
            app.secs += t.elapsed().as_secs_f64();

            if i.is_multiple_of(RSS_SAMPLE_BLOCKS) {
                max_entries = max_entries.max(set.len() as u64);
                if let Some((_, anon_kb)) = rss_now() {
                    rss_available = true;
                    chunk_peak_anon = chunk_peak_anon.max(anon_kb);
                    peak_anon_kb = peak_anon_kb.max(anon_kb);
                }
            }

            if a.verbose {
                eprintln!(
                    "    chunk {chunk} block {i} era {:?} txs {} spends {} creates {}",
                    block.era(),
                    plan.spans.len(),
                    plan.spends.len(),
                    plan.creates.len()
                );
            }
        }
        drop(bytes);
        if let Some(hwm) = peak_rss_kb() {
            rss_available = true;
            peak_rss_kb_seen = peak_rss_kb_seen.max(hwm);
        }

        // `n/a` rather than a zero on a platform where this cannot be read: a
        // printed 0 would be indistinguishable from a genuinely tiny footprint.
        let anon_col = if rss_available {
            format!("{:.0}", chunk_peak_anon as f64 / 1024.0)
        } else {
            "n/a".to_string()
        };

        println!(
            "{:>5}  {:>7}  {:>9}  {:>6}  {:>8}  {:>8}  {:>9.2}  {:>8.2}  {:>9}  {:>6.1}  {:>6.1}  {:>7}  {:>8}",
            chunk,
            dec.blocks,
            dec.txs,
            dec.invalid_txs,
            dec.inputs,
            dec.outputs,
            dec.secs,
            app.secs,
            set.len(),
            share(app.hit, app.spends),
            share(app.same_chunk, app.spends),
            app.miss,
            anon_col,
        );

        merge_decode(&mut dec_acc, &dec);
        accumulate_apply(&mut app_acc, app);
    }

    let n = window.len() as f64;
    let chunks_total = all.len() as f64;
    let mb = dec_acc.bytes as f64 / 1e6;
    let mb_per_s = mb / dec_acc.secs.max(1e-9);
    let store_bytes = chunk_bytes_total(&a.immutable);
    println!();
    println!("decode — parallelisable");
    println!(
        "  decode     {:>8.2} s/chunk · mean chunk {:.1} MB · {mb_per_s:.0} MB/s",
        dec_acc.secs / n,
        mb / n,
    );
    println!(
        "  by chunks  {:.0} × {:.2} s  →  {}",
        chunks_total,
        dec_acc.secs / n,
        human_secs(dec_acc.secs / n * chunks_total),
    );
    // Chunk size ranges over an order of magnitude here — 4.9 MB at 09000,
    // 9.7 MB at 09180, 58.5 MB at 04500 — so a count-based projection is only
    // right if the window's chunks are typical, which the newest ones are not.
    // A per-byte rate misses less, though decode time does not track bytes
    // exactly either (blocks per chunk vary by era). Both are shown.
    let projected_decode = if store_bytes > 0 {
        let t = store_bytes as f64 / 1e6 / mb_per_s.max(1e-9);
        println!(
            "  by bytes   {} across the store at {mb_per_s:.0} MB/s  →  {}",
            human_bytes(store_bytes),
            human_secs(t),
        );
        println!("             likelier of the two; still approximate, and only a whole-store");
        println!("             run settles it — `count`, or `fold --chunks 0`.");
        t
    } else {
        dec_acc.secs / n * chunks_total
    };
    println!(
        "  split      {:>8.2} s/chunk · {:.0} MB/s — the CBOR frame walk, untimed until\n\
     \
             2026-09-22 and 38% of a whole-chain fold's wall clock",
        split_secs / n,
        mb / split_secs.max(1e-9),
    );
    println!(
        "  read       {:>8.2} s/chunk ({} total, page-cache dependent)",
        read_secs / n,
        human_secs(read_secs)
    );
    println!(
        "  ⚠ these stages are timed separately and do NOT sum to the wall — the\n\
     \
     remainder is RSS sampling, printing, and whatever this list omits.",
    );
    println!();
    println!("apply — ONE CORE, and NOT extrapolable from a window");
    println!(
        "  apply      {:>8.2} s/chunk, sampled peak set size {max_entries} entries",
        app_acc.secs / n,
    );
    println!("             a whole-chain set is orders of magnitude larger, so lookups and");
    println!("             inserts get slower as it grows. Read the apply_s column: flat means");
    println!("             the set is not the bottleneck YET; climbing within the window does.");
    println!(
        "  serial     {:>8.2} s/chunk in a window — a floor, see above",
        (dec_acc.secs + app_acc.secs) / n,
    );
    println!();
    println!("pipelined — the applier is the floor, so extra cores only help while decode > apply");
    let apply_total = app_acc.secs / n * chunks_total;
    for p in [4u32, 8, 16] {
        let dec_share = projected_decode / f64::from(p - 1);
        println!(
            "  P={p:<2}        {}   (blocked on {})",
            human_secs(dec_share.max(apply_total)),
            if dec_share > apply_total {
                "decode"
            } else {
                "apply"
            },
        );
    }
    println!("  ⚠ every P= figure assumes a window-sized set on the applying core, so");
    println!("    they are optimistic by whatever the real set costs. `count` is the check.");
    println!(
        "  wall       {} for this window",
        human_secs(started.elapsed().as_secs_f64())
    );

    // Summed over the surviving set rather than accumulated on insert: a spent
    // output must not still be paying for its assets in the totals.
    let entries = set.len() as u64;
    let mut live_lovelace = 0u128;
    let mut live_assets = 0u64;
    let mut live_payload = 0u64;
    for e in set.values() {
        live_lovelace = live_lovelace.saturating_add(u128::from(e.lovelace));
        live_assets += u64::from(e.n_assets);
        live_payload += u64::from(e.asset_bytes) + u64::from(e.datum_bytes);
    }
    let packed = entries * (KEY_BYTES + std::mem::size_of::<Entry>()) as u64;
    println!();
    println!("live set after the window (the WINDOW's residue, not mainnet's live set)");
    println!(
        "  entries    {entries}  ({live_assets} asset entries, {live_payload} B of names+datums)"
    );
    println!(
        "  lovelace   {} ₳ held of {} ₳ created in the window",
        live_lovelace / 1_000_000,
        dec_acc.lovelace_created / 1_000_000,
    );
    println!(
        "             the held figure is the one to sanity-check: mainnet's live\n\
     \
             UTxO set should hold essentially all of the circulating supply.",
    );
    println!(
        "  packed     {} at {}+{} B/row — a stand-in for a chosen layout, not a measurement",
        human_bytes(packed),
        KEY_BYTES,
        std::mem::size_of::<Entry>(),
    );
    println!(
        "  shape      bare {}  assets-only {}  datum-only {}  both {}  (these sum to out)",
        dec_acc.shape.bare, dec_acc.shape.assets_only, dec_acc.shape.datum_only, dec_acc.shape.both,
    );
    println!(
        "  payload    {} of asset keys/names + {} of inline datum CBOR",
        human_bytes(dec_acc.shape.asset_bytes),
        human_bytes(dec_acc.shape.inline_datum_bytes),
    );
    println!(
        "  spends     {} total, {} resolved locally ({:.1}%), {} same-chunk ({:.1}%)",
        app_acc.spends,
        app_acc.hit,
        share(app_acc.hit, app_acc.spends),
        app_acc.same_chunk,
        share(app_acc.same_chunk, app_acc.spends),
    );
    println!(
        "  misresolved {} spends name an output the log never created",
        app_acc.miss
    );
    println!("             (Byron's genesis distribution and redeem-style inputs). entries =");
    println!("             created − spent + this, so a `count` of created−spent is LOW by it.");
    println!(
        "  x-chunk    {:.1}% of spends need an output created by a DIFFERENT chunk",
        share(app_acc.spends - app_acc.same_chunk, app_acc.spends),
    );
    println!("             (resolved from elsewhere in the window, or not at all). That is the");
    println!("             speculative-apply conflict rate — the number the two-ceilings");
    println!("             argument turns on, measured rather than asserted.");
    println!(
        "  rss        {}",
        if rss_available {
            format!(
                "peak rss {} (kernel VmHWM) · peak anon {} (sampled every {RSS_SAMPLE_BLOCKS} blocks)",
                human_bytes(peak_rss_kb_seen * 1024),
                human_bytes(peak_anon_kb * 1024),
            )
        } else {
            "n/a — /proc is Linux only; read the unit's memory.stat".to_string()
        }
    );
    println!();
    println!(
        "  eras       {}",
        dec_acc
            .eras
            .iter()
            .map(|e| format!("{e:?}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "  txs        {} ({} invalid — collateral skipped with them)",
        dec_acc.txs, dec_acc.invalid_txs
    );
    Ok(())
}

/// Every complete chunk, no set, counters only. `created − spent` is the live
/// set's entry count: every output a valid tx creates, minus every output a
/// valid tx spends, with no ordering requirement anywhere in the pass — which
/// is what makes it a thread pool rather than a fold.
fn count(a: CountArgs) -> Result<()> {
    let all = list_chunks(&a.immutable)?;
    let mut todo = all.clone();
    if a.max_chunks > 0 {
        todo.truncate(a.max_chunks);
    }
    if todo.is_empty() {
        bail!("no complete chunks to count");
    }
    let threads = if a.threads == 0 {
        std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
    } else {
        a.threads
    };

    println!("fold-probe count");
    println!("  immutable   {}", a.immutable.display());
    println!("  chunks      {} of {} complete", todo.len(), all.len());
    println!("  threads     {threads}");
    println!();

    let queue: Mutex<VecDeque<u16>> = Mutex::new(todo.iter().copied().collect());
    let done = AtomicU64::new(0);
    let blocks = AtomicU64::new(0);
    let txs = AtomicU64::new(0);
    let created = AtomicU64::new(0);
    let spent = AtomicU64::new(0);
    let invalid = AtomicU64::new(0);
    let bytes_read = AtomicU64::new(0);
    let hash_xor = AtomicU64::new(0);
    let total = todo.len() as u64;
    let started = Instant::now();
    // Progress every 50 chunks is a rate statement, not a per-chunk log: a
    // whole-chain pass on the ARM box is hours, and the ETA is the point.
    let progress_every = 50u64;

    std::thread::scope(|s| -> Result<()> {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                s.spawn(|| -> Result<()> {
                    let mut local_blocks = 0u64;
                    let mut local_txs = 0u64;
                    let mut local_created = 0u64;
                    let mut local_spent = 0u64;
                    let mut local_invalid = 0u64;
                    let mut local_bytes = 0u64;
                    let mut local_xor = 0u64;
                    loop {
                        let chunk = { queue.lock().expect("queue").pop_front() };
                        let Some(chunk) = chunk else { break };
                        let raw_bytes = std::fs::read(chunk_path(&a.immutable, chunk))
                            .with_context(|| format!("reading chunk {chunk}"))?;
                        local_bytes += raw_bytes.len() as u64;
                        for (i, raw) in split_blocks(&raw_bytes)?.iter().enumerate() {
                            let block = MultiEraBlock::decode(raw)
                                .with_context(|| format!("chunk {chunk}: decoding block {i}"))?;
                            local_blocks += 1;
                            for tx in block.txs() {
                                if !tx.is_valid() {
                                    local_invalid += 1;
                                    continue;
                                }
                                // The hash is not needed to COUNT, but every
                                // real pass pays it (an outref needs the tx
                                // hash), so it is paid here too — otherwise this
                                // figure would flatter a pass that cannot exist.
                                local_xor ^= u64::from(tx.hash().as_ref()[0]);
                                local_txs += 1;
                                local_created += tx.outputs().len() as u64;
                                local_spent += tx.consumes().len() as u64;
                            }
                        }
                        let d = done.fetch_add(1, Ordering::Relaxed) + 1;
                        blocks.fetch_add(local_blocks, Ordering::Relaxed);
                        txs.fetch_add(local_txs, Ordering::Relaxed);
                        created.fetch_add(local_created, Ordering::Relaxed);
                        spent.fetch_add(local_spent, Ordering::Relaxed);
                        invalid.fetch_add(local_invalid, Ordering::Relaxed);
                        bytes_read.fetch_add(local_bytes, Ordering::Relaxed);
                        hash_xor.fetch_add(local_xor, Ordering::Relaxed);
                        local_blocks = 0;
                        local_txs = 0;
                        local_created = 0;
                        local_spent = 0;
                        local_invalid = 0;
                        local_bytes = 0;
                        local_xor = 0;
                        if d.is_multiple_of(progress_every) || d == total {
                            let secs = started.elapsed().as_secs_f64();
                            let rate = d as f64 / secs.max(1e-9);
                            let left = (total - d) as f64 / rate.max(1e-9);
                            eprintln!(
                                "  {d}/{total} chunks  {:.1} chunks/s  {:.0} MB/s  eta {}",
                                rate,
                                bytes_read.load(Ordering::Relaxed) as f64 / 1e6 / secs.max(1e-9),
                                human_secs(left),
                            );
                        }
                    }
                    Ok(())
                })
            })
            .collect();
        for h in handles {
            h.join().expect("count worker panicked")?;
        }
        Ok(())
    })?;

    let created = created.load(Ordering::Relaxed);
    let spent = spent.load(Ordering::Relaxed);
    let secs = started.elapsed().as_secs_f64();
    println!("  wall        {} ({secs:.1} s)", human_secs(secs));
    println!(
        "  read        {} ({:.0} MB/s)",
        human_bytes(bytes_read.load(Ordering::Relaxed)),
        bytes_read.load(Ordering::Relaxed) as f64 / 1e6 / secs.max(1e-9),
    );
    println!("  blocks      {}", blocks.load(Ordering::Relaxed));
    println!(
        "  txs         {} ({} invalid)",
        txs.load(Ordering::Relaxed),
        invalid.load(Ordering::Relaxed)
    );
    println!("  created     {created}");
    println!("  spent       {spent}");
    let live = created as i64 - spent as i64;
    println!("  live        {live}  ← created − spent");
    if live < 0 {
        println!();
        println!("  ⚠ NEGATIVE: more spends than creations means this window does not reach");
        println!("  genesis, so `live` is not a live-set count. Drop --max-chunks for that.");
    }
    println!(
        "  hash-xor    0x{:02x}  (proof the tx hashes were computed, and a run-to-run check)",
        hash_xor.load(Ordering::Relaxed) & 0xff,
    );
    println!();
    println!("  live is an exact entry count only if no output is spent twice, and no");
    println!("  collateral output is involved — see the module docs. It is also LOW by");
    println!("  every spend that names an output never created in the log (Byron's");
    println!("  genesis distribution, redeem-style inputs): `fold` reports that count as");
    println!("  `misresolved`, and `entries = created − spent + misresolved`.");
    Ok(())
}

// ------------------------------------------------------------- utilities

fn pick_window(all: &[u16], start: Option<u16>, chunks: usize) -> Result<Vec<u16>> {
    let Some(&first) = all.first() else {
        bail!("no complete chunks");
    };
    if chunks == 0 {
        return Ok(all.to_vec());
    }
    let window = match start {
        None => {
            let take = chunks.min(all.len());
            all[all.len() - take..].to_vec()
        }
        Some(s) => {
            if s < first {
                bail!("--start {s} is below the first complete chunk {first}");
            }
            all.iter()
                .copied()
                .skip_while(|c| *c < s)
                .take(chunks)
                .collect()
        }
    };
    if window.is_empty() {
        bail!("window is empty — --start {start:?} past the last complete chunk?");
    }
    Ok(window)
}

fn share(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        0.0
    } else {
        part as f64 * 100.0 / whole as f64
    }
}

fn accumulate_apply(acc: &mut ApplyStats, one: ApplyStats) {
    acc.secs += one.secs;
    acc.spends += one.spends;
    acc.hit += one.hit;
    acc.same_chunk += one.same_chunk;
    acc.miss += one.miss;
    acc.inserted += one.inserted;
}

fn merge_decode(acc: &mut DecodeStats, one: &DecodeStats) {
    acc.secs += one.secs;
    acc.blocks += one.blocks;
    acc.txs += one.txs;
    acc.invalid_txs += one.invalid_txs;
    acc.inputs += one.inputs;
    acc.outputs += one.outputs;
    acc.bytes += one.bytes;
    acc.lovelace_created = acc.lovelace_created.saturating_add(one.lovelace_created);
    acc.shape.bare += one.shape.bare;
    acc.shape.assets_only += one.shape.assets_only;
    acc.shape.datum_only += one.shape.datum_only;
    acc.shape.both += one.shape.both;
    acc.shape.asset_bytes += one.shape.asset_bytes;
    acc.shape.inline_datum_bytes += one.shape.inline_datum_bytes;
    for e in &one.eras {
        if !acc.eras.contains(e) {
            acc.eras.push(*e);
        }
    }
}

fn human_secs(v: f64) -> String {
    if v >= 3600.0 {
        format!("{:.2} h", v / 3600.0)
    } else if v >= 60.0 {
        format!("{:.1} m", v / 60.0)
    } else {
        format!("{v:.1} s")
    }
}

fn human_bytes(v: u64) -> String {
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    let v = v as f64;
    if v >= GIB {
        format!("{:.2} GiB", v / GIB)
    } else if v >= MIB {
        format!("{:.0} MiB", v / MIB)
    } else {
        format!("{:.0} KiB", v / 1024.0)
    }
}

/// `Some((rss_kb, anon_kb))` on a platform where the split is readable.
///
/// Anonymous is the number that matters: file-backed pages are reclaimable
/// under pressure and anonymous ones are what a cgroup limit OOMs on
/// (`FOOTPRINT` §4.1). A `None` is reported as `n/a`, never as a zero.
///
/// ⚠️ Costs a page-table walk, so it is SAMPLED rather than called per block.
#[cfg(target_os = "linux")]
fn rss_now() -> Option<(u64, u64)> {
    let rollup = std::fs::read_to_string("/proc/self/smaps_rollup").ok()?;
    Some((kb_field(&rollup, "Rss:")?, kb_field(&rollup, "Anonymous:")?))
}

#[cfg(not(target_os = "linux"))]
fn rss_now() -> Option<(u64, u64)> {
    // Linux-only by construction: `/proc/<pid>/smaps_rollup` is the split the
    // footprint doc prescribes, and no equivalent is read here.
    None
}

/// True peak RSS, kernel-tracked. `/proc/self/status` is one small read with no
/// page-table walk, so unlike `rss_now` this is free to call per chunk — and it
/// is the only honest source for a "peak": sampling `Rss` after a chunk reports
/// the footprint of the moment, with the chunk buffer already freed.
#[cfg(target_os = "linux")]
fn peak_rss_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    kb_field(&status, "VmHWM:")
}

#[cfg(not(target_os = "linux"))]
fn peak_rss_kb() -> Option<u64> {
    None
}

/// How often `rss_now` is called while folding. 1024-ish blocks per chunk means
/// about eight samples: enough to catch the chunk buffer and the decoded plan
/// coexisting, cheap enough not to show up in `decode_s`/`apply_s`.
const RSS_SAMPLE_BLOCKS: usize = 128;

#[cfg(any(target_os = "linux", test))]
fn kb_field(text: &str, key: &str) -> Option<u64> {
    text.lines().find_map(|l| {
        l.strip_prefix(key)?
            .trim()
            .strip_suffix("kB")?
            .trim()
            .parse()
            .ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_concatenated_cbor_items() {
        // Two one-byte items: 0x01 (1), 0x02 (2).
        let items = split_blocks(&[0x01, 0x02]).expect("splits");
        assert_eq!(items.len(), 2);
        assert_eq!(items[0], &[0x01]);
        assert_eq!(items[1], &[0x02]);
    }

    #[test]
    fn splits_a_multi_byte_item_as_one_block() {
        // 0x42 0xAA 0xBB is a 2-byte byte-string: one item, not three.
        let items = split_blocks(&[0x42, 0xAA, 0xBB]).expect("splits");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].len(), 3);
    }

    #[test]
    fn reads_a_smaps_rollup_field() {
        let smaps = "Rss:                   12345 kB\n\
                     Pss:                    9999 kB\n\
                     Anonymous:              6789 kB\n";
        assert_eq!(kb_field(smaps, "Rss:"), Some(12345));
        assert_eq!(kb_field(smaps, "Anonymous:"), Some(6789));
        assert_eq!(kb_field(smaps, "Swap:"), None);
    }

    #[test]
    fn window_defaults_to_the_newest_chunks() {
        let all: Vec<u16> = (0..20).collect();
        let w = pick_window(&all, None, 5).expect("window");
        assert_eq!(w, vec![15, 16, 17, 18, 19]);
    }

    #[test]
    fn window_from_a_start_chunk_is_contiguous() {
        let all: Vec<u16> = vec![10, 11, 12, 13, 14];
        let w = pick_window(&all, Some(12), 2).expect("window");
        assert_eq!(w, vec![12, 13]);
    }

    #[test]
    fn window_refuses_a_start_below_the_store() {
        let all: Vec<u16> = vec![100, 101];
        assert!(pick_window(&all, Some(9), 1).is_err());
    }

    #[test]
    fn share_is_zero_not_nan_on_an_empty_denominator() {
        assert_eq!(share(0, 0), 0.0);
        assert_eq!(share(1, 4), 25.0);
    }
}
