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
use std::hash::BuildHasher;
use std::hash::BuildHasherDefault;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
// Through the walker's re-export rather than a new `chain-decode` dependency:
// it is the same symbol, and a probe should not be a reason to edit a manifest.
// The alias is what `chain-decode` expects to lose once call sites point there.
use mitos_chain_walk::decode::decode_tx;
use mitos_chain_walk::mithril::CHUNK_SLOTS;
use pallas_codec::minicbor::Decoder;
use pallas_primitives::conway::DatumOption;
use pallas_primitives::{Hash, alonzo, babbage, conway};
use pallas_traverse::{Era, MultiEraBlock, MultiEraOutput, MultiEraValue, OriginalHash};
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
    /// Also run the shared eager projection (`chain-decode`'s `decode_tx`) over
    /// every tx of the window, to price it against the fold's lean pull on the
    /// SAME bytes. In this mode the lean decode is `decode` MINUS `chain`.
    #[arg(long, conflicts_with = "primitives")]
    chain_decode: bool,
    /// Take the lean pull straight off the decoded primitives instead of through
    /// `MultiEraBlock::txs()`/`MultiEraTx`. Identical accounting, a different
    /// walk; the two must produce identical counters. Byron falls back.
    #[arg(long, conflicts_with = "chain_decode")]
    primitives: bool,
    /// Find block boundaries by DECODING each block off a cursor and reading its
    /// position, instead of walking the chunk with `Decoder::skip` first and
    /// decoding it again. One pass instead of two; implies `--primitives` for the
    /// Shelley+ eras, and falls back per Byron block. `split` reads 0 in this
    /// mode because there is no separate frame walk.
    #[arg(long, conflicts_with = "chain_decode")]
    cursor: bool,
    /// Which hasher the live set uses. `fx` is a hand-rolled FxHash, `std` is
    /// SipHash. Same run, one flag, so the difference is the hasher.
    #[arg(long, value_enum, default_value = "fx")]
    hasher: Hasher,
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
    /// Σ `n_assets` over every output in the window — the count the eager
    /// projection is reconciled against, since it builds one `Asset` each.
    asset_entries: u64,
    asset_bytes: u64,
    inline_datum_bytes: u64,
}

#[derive(Clone, Default)]
struct DecodeStats {
    secs: f64,
    /// The part of `secs` spent in `MultiEraBlock::txs()` — a sub-measurement,
    /// not a sibling. See `plan_block` for why it is worth its own line.
    txs_secs: f64,
    /// Σ seconds spent in `original_hash()` over the primitives walk's bodies.
    /// Named because blake2b over every tx body is not obviously small next to
    /// a decode that now runs at 160–260 MB/s, and pallas hashes through
    /// `cryptoxide` — a portable implementation, not a SIMD one.
    hash_secs: f64,
    /// Blocks the primitives walk could not take and handed back to
    /// `plan_block` (Byron, whose spends are self-contained, and any era pallas
    /// adds that this walk has not learned). Printed so a mixed run cannot be
    /// read as a clean one.
    fallback_blocks: u64,
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

/// What the eager, fixed-policy projection materialises when the caller wanted
/// only the fold's five facts (`chain-decode`'s `decode_tx`). Counted per field
/// so a slower run can name the field it paid for instead of blaming "the
/// shape", and timed so a delta can be read against the lean pull on identical
/// bytes.
#[derive(Clone, Default)]
struct ChainStats {
    secs: f64,
    txs: u64,
    outputs: u64,
    /// `Asset` entries built — two `Vec<u8>` allocations each.
    assets: u64,
    /// Total bech32 characters produced. The address a consumer gets is a
    /// `String` it re-parses; this is the size of that round trip.
    address_chars: u64,
    /// Outputs whose `datum()` was taken — each one CLONES the `KeepRaw`, and
    /// for an inline datum that clone is the whole `PlutusData` tree.
    datum_options: u64,
    inline_datum_bytes: u64,
    witness_datums: u64,
    witness_datum_bytes: u64,
    redeemers: u64,
    /// Outputs that re-encoded their own CBOR to price `min_utxo`.
    min_utxo_priced: u64,
    required_signers: u64,
    aux_bytes: u64,
    /// Summed over the payload so nothing above can be optimised away, and
    /// printed so a reader can see this pass did real work rather than none.
    checksum: u64,
}

impl ChainStats {
    fn absorb(&mut self, secs: f64, d: &mitos_chain_walk::decode::DecodedTx) {
        self.secs += secs;
        self.txs += 1;
        self.outputs += d.outputs.len() as u64;
        self.required_signers += d.required_signers.len() as u64;
        self.witness_datums += d.witness_datums.len() as u64;
        self.aux_bytes += d.aux_data.as_ref().map_or(0, |a| a.len() as u64);
        self.redeemers += d.inputs.iter().filter(|i| i.redeemer.is_some()).count() as u64;
        for o in &d.outputs {
            self.assets += o.assets.len() as u64;
            self.address_chars += o.address.len() as u64;
            if o.datum_hash.is_some() {
                self.datum_options += 1;
            }
            self.inline_datum_bytes += o.inline_datum.as_ref().map_or(0, |b| b.len() as u64);
            if !o.assets.is_empty() {
                self.min_utxo_priced += 1;
            }
            self.checksum = self
                .checksum
                .wrapping_add(o.lovelace)
                .wrapping_add(o.assets.len() as u64)
                .wrapping_add(o.inline_datum.as_ref().map_or(0, |b| b.len() as u64));
        }
        for b in d.witness_datums.values() {
            self.witness_datum_bytes += b.len() as u64;
        }
        self.checksum = self
            .checksum
            .wrapping_add(u64::from(d.tx_hash[0]))
            .wrapping_add(d.inputs.len() as u64);
    }
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
///
/// `chain`, when on, additionally runs the shared eager projection over every
/// valid tx so the two pulls can be compared on identical bytes. That extra work
/// is timed into `ChainStats`, never into `DecodeStats` — but it does sit inside
/// the caller's `decode` timer, so a `--chain-decode` run reads the lean figure
/// as `decode − chain`.
fn plan_block(
    block: &MultiEraBlock<'_>,
    chunk: u16,
    plan: &mut BlockPlan,
    st: &mut DecodeStats,
    chain: Option<&mut ChainStats>,
) {
    plan.clear();
    if !st.eras.contains(&block.era()) {
        st.eras.push(block.era());
    }

    // ⚠ `txs()` does NOT borrow the block. Pallas CLONES every transaction body
    // and every witness set out of it (`support::clone_conway_txs`), so this one
    // line is a second full materialisation of the block's contents on top of
    // whatever `MultiEraBlock::decode` already built — and for a `KeepRaw` clone
    // that includes every `PlutusData` tree in the block. Timed separately
    // because it is invisible in a per-chunk rate, avoidable by a decoder that
    // hands out borrowed txs, and large enough to move the whole-chain figure.
    let t = Instant::now();
    let txs = block.txs();
    st.txs_secs += t.elapsed().as_secs_f64();

    let mut chain = chain;
    for tx in &txs {
        // PHASE-2 FAILURE — the guard every walker in this workspace carries.
        // The block declares the tx invalid, so nothing it produces exists and
        // nothing it names was spent.
        if !tx.is_valid() {
            st.invalid_txs += 1;
            continue;
        }
        let hash = tx.hash();

        if let Some(c) = chain.as_deref_mut() {
            let t = Instant::now();
            let d = decode_tx(tx);
            c.absorb(t.elapsed().as_secs_f64(), &d);
        }

        let s0 = plan.spends.len() as u32;
        for inp in tx.consumes() {
            plan.spends.push((*inp.hash(), inp.index() as u32));
        }
        let s1 = plan.spends.len() as u32;

        let c0 = plan.creates.len() as u32;
        for (i, o) in tx.outputs().iter().enumerate() {
            let f = classic_facts(&o.value(), o);
            note_output(st, plan, chunk, hash, i as u32, f);
        }
        let c1 = plan.creates.len() as u32;

        plan.spans.push((s0, s1, c0, c1));
        st.txs += 1;
        st.inputs += u64::from(s1 - s0);
        st.outputs += u64::from(c1 - c0);
    }
    st.blocks += 1;
}

// ------------------------------------------------- the primitives walk

/// One output's facts, in the form BOTH walks record them.
///
/// Shared on purpose: the A/B between `plan_block` and `plan_block_primitives`
/// is only evidence if the Shape buckets, the byte totals and `lovelace_created`
/// come from the same arithmetic. A mismatch then means the two walks disagree
/// about the *facts*, which is the thing being tested.
struct OutFacts {
    lovelace: u64,
    n_assets: u16,
    asset_bytes: u32,
    /// 32 for a hash-only datum, the inline CBOR's length otherwise.
    datum_bytes: u32,
    /// The inline CBOR's length alone — 0 for a hash-only datum, which is why
    /// this is not just `datum_bytes`.
    inline_bytes: u32,
}

fn note_output(
    st: &mut DecodeStats,
    plan: &mut BlockPlan,
    chunk: u16,
    hash: Hash<32>,
    idx: u32,
    f: OutFacts,
) {
    if f.n_assets > 0 {
        st.shape.asset_bytes += u64::from(f.asset_bytes);
    }
    st.shape.asset_entries += u64::from(f.n_assets);
    st.shape.inline_datum_bytes += u64::from(f.inline_bytes);
    match (f.n_assets > 0, f.datum_bytes > 0) {
        (false, false) => st.shape.bare += 1,
        (true, false) => st.shape.assets_only += 1,
        (false, true) => st.shape.datum_only += 1,
        (true, true) => st.shape.both += 1,
    }
    st.lovelace_created = st.lovelace_created.saturating_add(u128::from(f.lovelace));
    plan.creates.push((
        (hash, idx),
        Entry {
            created_chunk: chunk,
            lovelace: f.lovelace,
            n_assets: f.n_assets,
            asset_bytes: f.asset_bytes,
            datum_bytes: f.datum_bytes,
        },
    ));
}

/// Facts through `MultiEraOutput` — the path that pays pallas's price.
fn classic_facts(value: &MultiEraValue<'_>, o: &MultiEraOutput<'_>) -> OutFacts {
    let mut n_assets = 0u16;
    let mut asset_bytes = 0u32;
    // `assets()` ALLOCATES a `Vec<MultiEraPolicyAssets>` per output — 355 M of
    // them over the store. Kept as-is: this function exists to be the control.
    for pa in value.assets().iter() {
        let policy_len = pa.policy().as_slice().len() as u32;
        for a in pa.assets().iter() {
            n_assets = n_assets.saturating_add(1);
            asset_bytes = asset_bytes
                .saturating_add(policy_len)
                .saturating_add(a.name().len() as u32);
        }
    }
    // `datum()` CLONES the `KeepRaw` — the whole `PlutusData` tree when inline.
    let (datum_bytes, inline_bytes) = match o.datum() {
        Some(DatumOption::Hash(_)) => (32u32, 0u32),
        Some(DatumOption::Data(w)) => {
            let len = w.0.raw_cbor().len() as u32;
            (len, len)
        }
        None => (0, 0),
    };
    OutFacts {
        lovelace: value.coin(),
        n_assets,
        asset_bytes,
        datum_bytes,
        inline_bytes,
    }
}

/// `(lovelace, asset entries, policy + name bytes)` from an Alonzo/Babbage value.
fn value_facts_alonzo(v: &alonzo::Value) -> (u64, u16, u32) {
    match v {
        alonzo::Value::Coin(c) => (*c, 0, 0),
        alonzo::Value::Multiasset(c, m) => {
            let mut n = 0u16;
            let mut bytes = 0u32;
            for (policy, assets) in m.iter() {
                let pl = policy.as_slice().len() as u32;
                for name in assets.keys() {
                    n = n.saturating_add(1);
                    bytes = bytes.saturating_add(pl).saturating_add(name.len() as u32);
                }
            }
            (*c, n, bytes)
        }
    }
}

/// The Conway value differs only in its quantity type.
fn value_facts_conway(v: &conway::Value) -> (u64, u16, u32) {
    match v {
        conway::Value::Coin(c) => (*c, 0, 0),
        conway::Value::Multiasset(c, m) => {
            let mut n = 0u16;
            let mut bytes = 0u32;
            for (policy, assets) in m.iter() {
                let pl = policy.as_slice().len() as u32;
                for name in assets.keys() {
                    n = n.saturating_add(1);
                    bytes = bytes.saturating_add(pl).saturating_add(name.len() as u32);
                }
            }
            (*c, n, bytes)
        }
    }
}

/// A legacy (pre-Babbage-shape) output: no inline datum, just a hash.
fn legacy_facts(o: &alonzo::TransactionOutput) -> OutFacts {
    let (lovelace, n_assets, asset_bytes) = value_facts_alonzo(&o.amount);
    OutFacts {
        lovelace,
        n_assets,
        asset_bytes,
        datum_bytes: if o.datum_hash.is_some() { 32 } else { 0 },
        inline_bytes: 0,
    }
}

/// A post-Alonzo output's datum, borrowed — `(datum_bytes, inline_bytes)`.
///
/// `MultiEraOutput::datum()` returns this same information by CLONING the
/// `KeepRaw`, which for an inline datum materialises a second `PlutusData` tree.
/// This walk only ever wants the length.
fn datum_facts(datum: Option<&DatumOption<'_>>) -> (u32, u32) {
    match datum {
        Some(DatumOption::Hash(_)) => (32, 0),
        Some(DatumOption::Data(w)) => {
            let len = w.0.raw_cbor().len() as u32;
            (len, len)
        }
        None => (0, 0),
    }
}

/// Whether the block's `invalid_transactions` list names this tx index.
fn tx_failed(list: Option<&Vec<u32>>, idx: usize) -> bool {
    list.is_some_and(|l| l.contains(&(idx as u32)))
}

/// Hash every body in the block into a reusable buffer, so the cost of
/// `original_hash()` is measurable on its own rather than smeared through the
/// walk — and so the walk's inner loop is not interrupted by it.
///
/// Hashes invalid transactions too, which the walk then skips (245 store-wide);
/// `hash_secs` is therefore a slight over-estimate of what the fold needs, which
/// is the right direction for a figure used to decide whether to chase it.
///
/// blake2b here goes through `cryptoxide`, a portable implementation. If this
/// line is a large share of `decode`, a SIMD blake2b is a bigger lever than
/// anything left in the walk — and `tx-index`'s 123.6 M recorded hashes are the
/// oracle that says whether a replacement hashes the same bytes the same way.
fn hash_bodies<T>(bodies: &[T], out: &mut Vec<Hash<32>>)
where
    T: OriginalHash<32>,
{
    out.clear();
    for b in bodies {
        out.push(b.original_hash());
    }
}

/// A Shelley-or-later block's parts, borrowed — the shape BOTH the cursor walk
/// and `MultiEraBlock` can supply, so there is one plan implementation.
#[derive(Clone, Copy)]
enum ShelleyBlock<'b> {
    Alonzo(&'b alonzo::Block<'b>, Era),
    Babbage(&'b babbage::Block<'b>),
    Conway(&'b conway::Block<'b>),
}

impl ShelleyBlock<'_> {
    fn era(self) -> Era {
        match self {
            ShelleyBlock::Alonzo(_, e) => e,
            ShelleyBlock::Babbage(_) => Era::Babbage,
            ShelleyBlock::Conway(_) => Era::Conway,
        }
    }
}

fn shelley_block_of<'a, 'b>(mb: &'a MultiEraBlock<'b>) -> Option<ShelleyBlock<'a>> {
    match mb {
        MultiEraBlock::AlonzoCompatible(b, era) => Some(ShelleyBlock::Alonzo(b, *era)),
        MultiEraBlock::Babbage(b) => Some(ShelleyBlock::Babbage(b)),
        MultiEraBlock::Conway(b) => Some(ShelleyBlock::Conway(b)),
        _ => None,
    }
}

/// What a cursor found at a block boundary — OWNED, because a decode produces a
/// value while `ShelleyBlock` borrows one (the difference the two callers have).
///
/// Byron and the epoch boundary are not decoded: this walk falls them back, and
/// `skip` finds the end of a ~1 KB Byron block for nothing.
enum DecodedBlock<'b> {
    Alonzo(alonzo::Block<'b>, Era),
    Babbage(babbage::Block<'b>),
    Conway(conway::Block<'b>),
    /// Byron, the epoch boundary, or an era this walk does not know.
    Fallback,
}

impl<'b> DecodedBlock<'b> {
    fn shelley(&self) -> Option<ShelleyBlock<'_>> {
        match self {
            DecodedBlock::Alonzo(b, e) => Some(ShelleyBlock::Alonzo(b, *e)),
            DecodedBlock::Babbage(b) => Some(ShelleyBlock::Babbage(b)),
            DecodedBlock::Conway(b) => Some(ShelleyBlock::Conway(b)),
            DecodedBlock::Fallback => None,
        }
    }
}

/// Decode ONE block from the front of `bytes`, and report how long it was.
///
/// This is the answer to the re-walk. `split_blocks` walks every byte with
/// `Decoder::skip` to find block boundaries, and then the decode walks the same
/// bytes again structurally — measured at ~20% of the decode stage on three eras
/// (`DECODE_SHAPE.md` §2.7). A `minicbor::Decoder` knows where it stopped, so
/// decoding the block and reading `position()` gives the boundary for free:
/// **one pass instead of two**, no `.secondary` sidecar to parse or trust, and
/// nothing that a streamed snapshot lacks.
///
/// It is also the skeleton of the era-dispatched decoder §5.3 argues for: the
/// tag is read once per block, and the era's type is decoded monomorphically.
fn decode_block_at(bytes: &[u8]) -> Result<(DecodedBlock<'_>, usize)> {
    let mut d = Decoder::new(bytes);
    let items = d.array()?;
    if items != Some(2) {
        bail!("block wrapper is not a 2-item array ({items:?})");
    }
    let tag: u16 = d.decode()?;
    let blk = match tag {
        // Byron (tag 1) and the epoch-boundary block (tag 0): the caller has its
        // own path for these, and only the boundary matters here.
        0 | 1 => {
            d.skip()?;
            DecodedBlock::Fallback
        }
        2 => {
            let b: alonzo::Block<'_> = d.decode()?;
            DecodedBlock::Alonzo(b, Era::Shelley)
        }
        3 => {
            let b: alonzo::Block<'_> = d.decode()?;
            DecodedBlock::Alonzo(b, Era::Allegra)
        }
        4 => {
            let b: alonzo::Block<'_> = d.decode()?;
            DecodedBlock::Alonzo(b, Era::Mary)
        }
        5 => {
            let b: alonzo::Block<'_> = d.decode()?;
            DecodedBlock::Alonzo(b, Era::Alonzo)
        }
        6 => DecodedBlock::Babbage(d.decode()?),
        7 => DecodedBlock::Conway(d.decode()?),
        t => bail!("unknown block era tag {t} — pallas knows 0–7"),
    };
    Ok((blk, d.position()))
}

/// The lean pull straight off the decoded primitives — no `MultiEraTx`, no
/// `txs()` clone, no `Box` per output.
///
/// What this removes, all of it measured (`DECODE_SHAPE.md` §2.2, §2.5):
///
/// - `block.txs()`, which CLONES every body and witness set out of the block —
///   16.4% of decode, and its teardown pays again (`drop_glue<WitnessSet>`,
///   `clone_subtree<Hash, Bytes>`);
/// - `MultiEraTx::outputs()`, which builds a `Vec` and a `Box<Cow>` per output;
/// - `MultiEraTx::consumes()`, a `HashSet` plus a `Vec` per transaction — the
///   body's inputs are already a `NonEmptySet`;
/// - `MultiEraOutput::value().assets()`, a `Vec` per output;
/// - `MultiEraOutput::datum()`, which clones the `KeepRaw` — for an inline datum
///   that is the whole `PlutusData` tree, and this walk wants a LENGTH.
///
/// The era is matched once per block and the loop inside is monomorphic, which
/// is the shape `DECODE_SHAPE.md` §5.3 argues a hand-rolled decoder should have.
/// What it does NOT remove is `MultiEraBlock::decode`'s own materialisation —
/// the `BTreeMap` values, the `Vec<u8>` addresses and names, the `PlutusData`
/// trees, the metadata — which is ~37% and needs the decoder, not the walk.
///
/// **Byron falls back.** A Byron spend is self-contained (`TxIn` embeds the
/// spent output) and its outref is computed rather than read, so it needs
/// `MultiEraInput`'s machinery. Those blocks go to `plan_block` and are counted.
fn plan_block_primitives(
    blk: ShelleyBlock<'_>,
    chunk: u16,
    plan: &mut BlockPlan,
    st: &mut DecodeStats,
    hashes: &mut Vec<Hash<32>>,
) {
    plan.clear();
    let era = blk.era();
    if !st.eras.contains(&era) {
        st.eras.push(era);
    }

    match blk {
        ShelleyBlock::Alonzo(b, _) => {
            let t = Instant::now();
            hash_bodies(&b.transaction_bodies, hashes);
            st.hash_secs += t.elapsed().as_secs_f64();
            for (i, body) in b.transaction_bodies.iter().enumerate() {
                if tx_failed(b.invalid_transactions.as_ref(), i) {
                    st.invalid_txs += 1;
                    continue;
                }
                let hash = hashes[i];
                let s0 = plan.spends.len() as u32;
                for inp in body.inputs.iter() {
                    plan.spends.push((inp.transaction_id, inp.index as u32));
                }
                let s1 = plan.spends.len() as u32;
                let c0 = plan.creates.len() as u32;
                for (idx, o) in body.outputs.iter().enumerate() {
                    let f = legacy_facts(o);
                    note_output(st, plan, chunk, hash, idx as u32, f);
                }
                let c1 = plan.creates.len() as u32;
                plan.spans.push((s0, s1, c0, c1));
                st.txs += 1;
                st.inputs += u64::from(s1 - s0);
                st.outputs += u64::from(c1 - c0);
            }
        }
        ShelleyBlock::Babbage(b) => {
            let t = Instant::now();
            hash_bodies(&b.transaction_bodies, hashes);
            st.hash_secs += t.elapsed().as_secs_f64();
            for (i, body) in b.transaction_bodies.iter().enumerate() {
                if tx_failed(b.invalid_transactions.as_ref(), i) {
                    st.invalid_txs += 1;
                    continue;
                }
                let hash = hashes[i];
                let s0 = plan.spends.len() as u32;
                for inp in body.inputs.iter() {
                    plan.spends.push((inp.transaction_id, inp.index as u32));
                }
                let s1 = plan.spends.len() as u32;
                let c0 = plan.creates.len() as u32;
                for (idx, o) in body.outputs.iter().enumerate() {
                    let f = match &**o {
                        babbage::TransactionOutput::Legacy(x) => legacy_facts(x),
                        babbage::TransactionOutput::PostAlonzo(x) => {
                            // `babbage::Value` is `alonzo::Value`; only Conway has
                            // its own, so this arm reads the same shape the legacy
                            // arm does.
                            let (lovelace, n_assets, asset_bytes) = value_facts_alonzo(&x.value);
                            let (datum_bytes, inline_bytes) =
                                datum_facts(x.datum_option.as_deref());
                            OutFacts {
                                lovelace,
                                n_assets,
                                asset_bytes,
                                datum_bytes,
                                inline_bytes,
                            }
                        }
                    };
                    note_output(st, plan, chunk, hash, idx as u32, f);
                }
                let c1 = plan.creates.len() as u32;
                plan.spans.push((s0, s1, c0, c1));
                st.txs += 1;
                st.inputs += u64::from(s1 - s0);
                st.outputs += u64::from(c1 - c0);
            }
        }
        ShelleyBlock::Conway(b) => {
            let t = Instant::now();
            hash_bodies(&b.transaction_bodies, hashes);
            st.hash_secs += t.elapsed().as_secs_f64();
            for (i, body) in b.transaction_bodies.iter().enumerate() {
                if tx_failed(b.invalid_transactions.as_ref(), i) {
                    st.invalid_txs += 1;
                    continue;
                }
                let hash = hashes[i];
                let s0 = plan.spends.len() as u32;
                for inp in body.inputs.iter() {
                    plan.spends.push((inp.transaction_id, inp.index as u32));
                }
                let s1 = plan.spends.len() as u32;
                let c0 = plan.creates.len() as u32;
                for (idx, o) in body.outputs.iter().enumerate() {
                    let f = match o {
                        conway::TransactionOutput::Legacy(x) => legacy_facts(x),
                        conway::TransactionOutput::PostAlonzo(x) => {
                            let (lovelace, n_assets, asset_bytes) = value_facts_conway(&x.value);
                            let (datum_bytes, inline_bytes) =
                                datum_facts(x.datum_option.as_deref());
                            OutFacts {
                                lovelace,
                                n_assets,
                                asset_bytes,
                                datum_bytes,
                                inline_bytes,
                            }
                        }
                    };
                    note_output(st, plan, chunk, hash, idx as u32, f);
                }
                let c1 = plan.creates.len() as u32;
                plan.spans.push((s0, s1, c0, c1));
                st.txs += 1;
                st.inputs += u64::from(s1 - s0);
                st.outputs += u64::from(c1 - c0);
            }
        } // Byron, its epoch-boundary block, and — since `MultiEraBlock` is
          // `#[non_exhaustive]` — whatever a future pallas adds. All fall back:
          // Byron's spends are self-contained (a `TxIn` embeds the output it
          // spends) and its outrefs are computed rather than read, so it needs
          // `MultiEraInput`'s machinery instead of a body walk. The caller decides
          // this before calling here (`ShelleyBlock` has no such variant), so this
          // arm is a compile-time impossibility rather than a runtime branch.
    }
    st.blocks += 1;
}

// ------------------------------------------------------------- the apply

/// FxHash (the rustc one), hand-rolled in 30 lines so the *hasher question*
/// can be answered with a number before anyone opens a dependency decision.
///
/// Not cryptographic. The keys are outrefs read off the chain, so the
/// HashDoS resistance SipHash buys is not the threat it is for untrusted
/// request keys.
#[derive(Default)]
struct FxHasher {
    hash: u64,
}

const FX_SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

impl std::hash::Hasher for FxHasher {
    fn finish(&self) -> u64 {
        self.hash
    }

    fn write(&mut self, mut bytes: &[u8]) {
        while bytes.len() >= 8 {
            let mut w = [0u8; 8];
            w.copy_from_slice(&bytes[..8]);
            self.write_u64(u64::from_le_bytes(w));
            bytes = &bytes[8..];
        }
        if bytes.len() >= 4 {
            let mut w = [0u8; 4];
            w.copy_from_slice(&bytes[..4]);
            self.write_u32(u32::from_le_bytes(w));
            bytes = &bytes[4..];
        }
        for b in bytes {
            self.write_u8(*b);
        }
    }

    fn write_u8(&mut self, n: u8) {
        self.hash = (self.hash.rotate_left(5) ^ u64::from(n)).wrapping_mul(FX_SEED);
    }

    fn write_u32(&mut self, n: u32) {
        self.write_u64(u64::from(n));
    }

    fn write_u64(&mut self, n: u64) {
        self.hash = (self.hash.rotate_left(5) ^ n).wrapping_mul(FX_SEED);
    }

    fn write_usize(&mut self, n: usize) {
        self.write_u64(n as u64);
    }
}

/// Which hasher the live set uses. A flag rather than a decision, because the
/// answer at 10.9 M entries is not the answer at 70 k.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Hasher {
    Fx,
    Std,
}

/// The live set under construction, hasher chosen at runtime.
///
/// This is the probe's own structure, not a proposal: `LIVE_BOOK.md` specifies a
/// packed arena with a bucket directory, and the measured reason to want one is
/// in `DECODE_SHAPE.md` §2.6's apply figures — the op costs ~800 ns at 10.9 M
/// entries against ~10 ns in a window, which is the memory hierarchy, not the
/// hash function. This exists to price the small half.
#[allow(clippy::large_enum_variant)]
enum Live {
    Fx(HashMap<OutRef, Entry, BuildHasherDefault<FxHasher>>),
    Std(HashMap<OutRef, Entry>),
}

impl Live {
    fn new(h: Hasher) -> Self {
        match h {
            Hasher::Fx => Live::Fx(HashMap::default()),
            Hasher::Std => Live::Std(HashMap::new()),
        }
    }

    fn len(&self) -> usize {
        match self {
            Live::Fx(m) => m.len(),
            Live::Std(m) => m.len(),
        }
    }

    /// `(lovelace held, asset entries, payload bytes)` over the surviving rows.
    fn totals(&self) -> (u128, u64, u64) {
        let mut t = (0u128, 0u64, 0u64);
        let mut add = |e: &Entry| {
            t.0 = t.0.saturating_add(u128::from(e.lovelace));
            t.1 += u64::from(e.n_assets);
            t.2 += u64::from(e.asset_bytes) + u64::from(e.datum_bytes);
        };
        match self {
            Live::Fx(m) => m.values().for_each(&mut add),
            Live::Std(m) => m.values().for_each(&mut add),
        }
        t
    }

    fn apply(&mut self, plan: &BlockPlan, chunk: u16, st: &mut ApplyStats) {
        match self {
            Live::Fx(m) => apply_plan(m, plan, chunk, st),
            Live::Std(m) => apply_plan(m, plan, chunk, st),
        }
    }
}

/// The serial part: resolve every spend against the set, then insert every
/// output. Spends before creates *within a tx*, in tx order — see `BlockPlan`.
fn apply_plan<S: BuildHasher>(
    set: &mut HashMap<OutRef, Entry, S>,
    plan: &BlockPlan,
    chunk: u16,
    st: &mut ApplyStats,
) {
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

/// Read a chunk into a buffer that is reused across chunks.
///
/// `fs::read` allocates fresh each time, and a 5–40 MB allocation goes to
/// `mmap`, so every chunk re-faults its whole extent on first touch. Reusing the
/// buffer keeps the mapping and the pages, and the reads land in memory the
/// process already has.
fn read_chunk_into(buf: &mut Vec<u8>, path: &std::path::Path) -> Result<()> {
    use std::io::Read;

    let mut f = std::fs::File::open(path)?;
    let hint = f.metadata().map(|m| m.len() as usize).unwrap_or(0);
    buf.clear();
    if buf.capacity() < hint {
        buf.reserve(hint - buf.len());
    }
    f.read_to_end(buf)?;
    Ok(())
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
        "  walk        {}",
        if a.cursor {
            "CURSOR — one decode pass: the boundary comes from `position()`"
        } else if a.primitives {
            "PRIMITIVES — no txs() clone, no Box per output, era matched per block"
        } else {
            "MultiEraTx — `block.txs()` then `MultiEraTx` accessors (the control)"
        }
    );
    println!(
        "  set         {} hasher · chunk buffer reused across chunks",
        match a.hasher {
            Hasher::Fx => "fx (FxHash, hand-rolled)",
            Hasher::Std => "std (SipHash-1-3)",
        }
    );
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

    let mut set = Live::new(a.hasher);
    let mut plan = BlockPlan::default();
    let mut dec_acc = DecodeStats::default();
    let mut chain_acc = ChainStats::default();
    let mut app_acc = ApplyStats::default();
    // Reused across chunks: `fs::read` allocates and frees ~25 MB per chunk, and
    // glibc serves that from mmap, so every chunk re-faults 6,400 pages. Keeping
    // the buffer mapped pays that once.
    let mut chunk_buf: Vec<u8> = Vec::new();
    let mut hashes: Vec<Hash<32>> = Vec::new();
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
        read_chunk_into(&mut chunk_buf, &chunk_path(&a.immutable, chunk))
            .with_context(|| format!("reading chunk {chunk}"))?;
        read_secs += t.elapsed().as_secs_f64();
        let bytes = chunk_buf.as_slice();

        let mut chunk_peak_anon = 0u64;
        dec.bytes = bytes.len() as u64;

        if a.cursor {
            let mut pos = 0usize;
            let mut i = 0usize;
            while pos < bytes.len() {
                let t = Instant::now();
                let (blk, len) = decode_block_at(&bytes[pos..])
                    .with_context(|| format!("chunk {chunk}: block at byte {pos}"))?;
                let end = pos + len;
                let era;
                if let Some(b) = blk.shelley() {
                    era = b.era();
                    plan_block_primitives(b, chunk, &mut plan, &mut dec, &mut hashes);
                } else {
                    dec.fallback_blocks += 1;
                    let mb = MultiEraBlock::decode(&bytes[pos..end])
                        .with_context(|| format!("chunk {chunk}: byte {pos}"))?;
                    era = mb.era();
                    plan_block(&mb, chunk, &mut plan, &mut dec, None);
                }
                dec.secs += t.elapsed().as_secs_f64();

                // The per-block tail is written twice (here and below) rather
                // than extracted: it mutates six locals, so a function would
                // need six `&mut`s and a macro would reach into the enclosing
                // scope from the middle of a loop.
                let t = Instant::now();
                set.apply(&plan, chunk, &mut app);
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
                        "    chunk {chunk} block {i} era {era:?} txs {} spends {} creates {}",
                        plan.spans.len(),
                        plan.spends.len(),
                        plan.creates.len()
                    );
                }
                pos = end;
                i += 1;
            }
        } else {
            let t = Instant::now();
            let raw_blocks = split_blocks(bytes)?;
            split_secs += t.elapsed().as_secs_f64();

            for (i, raw) in raw_blocks.iter().enumerate() {
                let t = Instant::now();
                let block = MultiEraBlock::decode(raw)
                    .with_context(|| format!("chunk {chunk}: decoding block {i}"))?;
                if a.primitives {
                    match shelley_block_of(&block) {
                        Some(b) => {
                            plan_block_primitives(b, chunk, &mut plan, &mut dec, &mut hashes)
                        }
                        None => {
                            // Byron and the epoch boundary.
                            dec.fallback_blocks += 1;
                            plan_block(&block, chunk, &mut plan, &mut dec, None);
                        }
                    }
                } else {
                    plan_block(
                        &block,
                        chunk,
                        &mut plan,
                        &mut dec,
                        a.chain_decode.then_some(&mut chain_acc),
                    );
                }
                dec.secs += t.elapsed().as_secs_f64();

                let t = Instant::now();
                set.apply(&plan, chunk, &mut app);
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
        }
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
    // `--chain-decode` times the eager projection into its own accumulator, but
    // the call sits inside the per-block decode timer — so every decode figure
    // below is the LEAN pull, and `chain` is reported separately.
    let dec_secs = dec_acc.secs - chain_acc.secs;
    let mb = dec_acc.bytes as f64 / 1e6;
    let mb_per_s = mb / dec_secs.max(1e-9);
    let store_bytes = chunk_bytes_total(&a.immutable);
    println!();
    println!("decode — parallelisable");
    println!(
        "  decode     {:>8.2} s/chunk · mean chunk {:.1} MB · {mb_per_s:.0} MB/s",
        dec_secs / n,
        mb / n,
    );
    println!(
        "  ↳+split    {:>8.2} s/chunk — decode and boundary-finding together, which is\n\
     \
             the figure to compare across modes: `--cursor` folds the walk into the\
\
     \
             decode, the other modes pay for it separately.",
        (dec_secs + split_secs) / n,
    );
    println!(
        "  txs()      {:>8.4} s/chunk ({:.1}% of decode) — pallas CLONES every tx body\n\
     \
             and witness set out of the block here, so this line is a SECOND\n\
     \
             materialisation of the block's contents, `PlutusData` trees included,\n\
     \
             and it is part of `decode` above",
        dec_acc.txs_secs / n,
        dec_acc.txs_secs / dec_secs.max(1e-9) * 100.0,
    );
    println!(
        "  by chunks  {:.0} × {:.2} s  →  {}",
        chunks_total,
        dec_secs / n,
        human_secs(dec_secs / n * chunks_total),
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
        dec_secs / n * chunks_total
    };
    println!(
        "  split      {}",
        if split_secs <= f64::EPSILON {
            "none — there is no separate frame walk in this mode (see `walk`)".to_string()
        } else {
            format!(
                "{:>8.2} s/chunk · {:.0} MB/s — the CBOR frame walk, timed apart from\n\
     \
             the decode it precedes",
                split_secs / n,
                mb / split_secs.max(1e-9),
            )
        }
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
    if a.primitives {
        println!(
            "  hash       {:>8.4} s/chunk ({:.0}% of decode, {:.3} s in window) — blake2b over\n\
         \
                 every body, every tx hash is needed for an outref key. pallas hashes\n\
         \
                 through `cryptoxide`, a portable implementation.",
            dec_acc.hash_secs / n,
            dec_acc.hash_secs / dec_secs.max(1e-9) * 100.0,
            dec_acc.hash_secs,
        );
    }
    if a.primitives && dec_acc.fallback_blocks > 0 {
        println!(
            "  ⚠ {} block(s) fell back to the MultiEraTx walk — Byron (its spends are\n\
     \
             self-contained, so its outrefs are computed rather than read) or an era\n\
     \
             this walk does not know. A mixed run.",
            dec_acc.fallback_blocks
        );
    }

    if a.chain_decode {
        let lean = dec_secs.max(1e-9);
        println!();
        println!("chain-decode — the shared eager projection, over the SAME bytes");
        println!(
            "  chain      {:>8.2} s/chunk · {:.0} MB/s · {:.0}% of the lean decode",
            chain_acc.secs / n,
            mb / chain_acc.secs.max(1e-9),
            chain_acc.secs / lean * 100.0,
        );
        println!(
            "  on top of  {:.2} s/chunk lean → a walker decodes at {:.0} MB/s, not {mb_per_s:.0}",
            (dec_secs + chain_acc.secs) / n,
            mb / (dec_secs + chain_acc.secs).max(1e-9),
        );
        println!(
            "  it builds  {} outputs, {} asset entries, {:.1} M bech32 chars, {:.1} M priced\n\
     \
             by re-encoding their own CBOR, {} datum clones ({} of inline datum),\n\
     \
             {} witness datums ({}), {} redeemers, {} required signers, {} of aux",
            chain_acc.outputs,
            chain_acc.assets,
            chain_acc.address_chars as f64 / 1e6,
            chain_acc.min_utxo_priced as f64 / 1e6,
            chain_acc.datum_options,
            human_bytes(chain_acc.inline_datum_bytes),
            chain_acc.witness_datums,
            human_bytes(chain_acc.witness_datum_bytes),
            chain_acc.redeemers,
            chain_acc.required_signers,
            human_bytes(chain_acc.aux_bytes),
        );
        println!(
            "  the fold   built {} outputs and {} asset entries over the same txs — the\n\
     \
             two counts must AGREE (same guard, same `txs()`), so a mismatch here\n\
     \
             is a projection bug in one of them, not a rounding difference",
            dec_acc.outputs, dec_acc.shape.asset_entries,
        );
        println!(
            "             checksum {} (so the pass provably did the work)",
            chain_acc.checksum
        );
    }
    println!();
    println!("apply — ONE CORE, and NOT extrapolable from a window");
    println!(
        "  apply      {:>8.4} s/chunk ({:.3} s in window), sampled peak set size {max_entries} entries",
        app_acc.secs / n,
        app_acc.secs,
    );
    println!(
        "             the set has {} entries · {:.1} M ops · {:.0} ns/op",
        set.len(),
        (app_acc.spends + app_acc.inserted) as f64 / 1e6,
        if app_acc.spends + app_acc.inserted > 0 {
            app_acc.secs * 1e9 / (app_acc.spends + app_acc.inserted) as f64
        } else {
            0.0
        },
    );
    println!("             a whole-chain set is orders of magnitude larger, so lookups and");
    println!("             inserts get slower as it grows. Read the apply_s column: flat means");
    println!("             the set is not the bottleneck YET; climbing within the window does.");
    println!(
        "  serial     {:>8.2} s/chunk in a window — a floor, see above",
        (dec_secs + app_acc.secs) / n,
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
    let (live_lovelace, live_assets, live_payload) = set.totals();
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
    acc.txs_secs += one.txs_secs;
    acc.hash_secs += one.hash_secs;
    acc.fallback_blocks += one.fallback_blocks;
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
    acc.shape.asset_entries += one.shape.asset_entries;
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

/// How often `rss_now` is called while folding. The comment here said
/// "1024-ish blocks" while the value said 128 — and 128 cost 4% of a short
/// window in kernel page-table walking alone (`smaps_account` in the profile),
/// i.e. the probe paying to measure itself. The peak is near-monotonic and the
/// chunks are ~1,000 blocks, so one sample per chunk still catches it.
const RSS_SAMPLE_BLOCKS: usize = 1024;

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
