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
use std::collections::HashSet;
use std::collections::VecDeque;
use std::hash::BuildHasher;
use std::hash::BuildHasherDefault;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
// Through the walker's re-export rather than a new `chain-decode` dependency:
// it is the same symbol, and a probe should not be a reason to edit a manifest.
// The alias is what `chain-decode` expects to lose once call sites point there.
use live_book::row::Datum;
use mitos_chain_walk::decode::decode_tx;
use mitos_chain_walk::mithril::CHUNK_SLOTS;
use mitos_meter::{Meter, Op, Run, human};
use pallas_addresses::byron::AddressPayload;
use pallas_codec::minicbor::Decoder;
use pallas_primitives::conway::DatumOption;
use pallas_primitives::{Hash, alonzo, babbage, conway};
use pallas_traverse::{Era, MultiEraBlock, MultiEraOutput, MultiEraValue, OriginalHash};
use tx_index::extract::list_chunks;

use crate::book::Book;
use crate::delta::Delta;

mod book;
mod delta;

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
    /// Read a published book back the way a consumer would.
    Verify(VerifyArgs),
    /// Rebuild the orderings derivable from the row store, without a refold.
    Reindex(ReindexArgs),
    /// Ask a published book a question, through the reader a consumer uses.
    Lookup(LookupArgs),
    /// Record where a published book sits on chain, from the chunk it was built at.
    /// A build writes `base_slot` itself; this is for an artifact built before the
    /// field existed, so `tip()` can answer instead of refusing.
    StampBoundary(StampArgs),
    /// Lay a published book out for upload: uniform 1 MiB parts plus an
    /// attestation. Writes a directory and nothing else — no credentials, no
    /// network, so the layout is inspectable before it is anywhere
    Publish(PublishArgs),
    /// Resolve a chunk's transaction inputs against a book, and classify what it
    /// cannot answer. Answers "can the book find a UTxO that is no longer live".
    ResolveInputs(ResolveArgs),
    /// Follow chunks past a book's boundary with an in-memory delta: the structure
    /// that resolves the inputs the book cannot, and what it costs to hold.
    Follow(FollowArgs),
}

#[derive(Args)]
struct FollowArgs {
    /// The book the follower starts from — its boundary is where the delta begins.
    #[arg(long)]
    book: PathBuf,
    /// The chunk whose boundary the book was built at. **Not in `book.meta`** — see
    /// the report note — so it is passed in and recorded in the run context.
    #[arg(long)]
    base_chunk: u16,
    /// The Mithril immutable directory.
    #[arg(long)]
    immutable: PathBuf,
    /// First chunk to follow. Default: the chunk after the base boundary.
    #[arg(long)]
    from: Option<u16>,
    /// How many chunks to follow.
    #[arg(long, default_value_t = 10)]
    chunks: u16,
    /// The book was folded from a window rather than from genesis. Inputs created
    /// before the window are invisible to it, so they land in `uncreated` and the
    /// rate says nothing about the chain — recorded so the two are never confused.
    #[arg(long)]
    window_base: bool,
    /// The control: the same chunks, the same walk, **no sink**. The difference
    /// between this and a delta run is what the delta costs, on identical bytes —
    /// the same A/B `fold` runs between `--primitives` and `--chain-decode`.
    #[arg(long)]
    bare: bool,
    /// Write the meter's records here as one JSON object per line.
    #[arg(long)]
    jsonl: Option<PathBuf>,
    /// Read this many delta rows back through the accessor a consumer uses, and
    /// check each one resolves. 0 = off; larger than the delta means every row,
    /// which is also how the cost of a linear scan over the delta is measured.
    #[arg(long, default_value_t = 2_000)]
    sample: u64,
}

#[derive(Args)]
struct ResolveArgs {
    /// The book to resolve against.
    #[arg(long)]
    book: PathBuf,
    /// The chunk whose boundary the book was built at. **Not in `book.meta`** — see
    /// the report note — so it is passed in and recorded in the run context.
    #[arg(long)]
    base_chunk: u16,
    /// The Mithril immutable directory.
    #[arg(long)]
    immutable: PathBuf,
    /// First chunk to resolve, inclusive.
    #[arg(long)]
    from: u16,
    /// Last chunk to resolve, inclusive.
    #[arg(long)]
    to: u16,
}

#[derive(Args)]
struct LookupArgs {
    /// The book directory to open.
    #[arg(long)]
    book: PathBuf,
    /// An outref to resolve: `<64 hex>` with an optional `#<index>` (default 0).
    #[arg(long, group = "query")]
    outref: Option<String>,
    /// An address: bech32, or hex of its raw bytes.
    #[arg(long, group = "query")]
    address: Option<String>,
    /// A 28-byte payment credential, hex — what `utxos_by_payment_cred` takes.
    #[arg(long, group = "query")]
    payment_cred: Option<String>,
    /// A 28-byte policy id, hex.
    #[arg(long, group = "query")]
    policy: Option<String>,
    /// Refs per page.
    #[arg(long, default_value_t = 20)]
    limit: usize,
    /// Keep asking for pages until the run is exhausted, counting as it goes.
    #[arg(long)]
    all: bool,
    /// After enumerating, read every ref back and total what the rows hold. This is
    /// the shape a module consumes the book in — enumerate, then read — so exercising
    /// it is the point, and on an address with a known balance it is also the one
    /// check here with an oracle outside this repository.
    #[arg(long)]
    sum: bool,
    /// Sample this many policies and walk each one's holders through all three
    /// orderings, requiring them to agree. 0 = off.
    #[arg(long, default_value_t = 0)]
    cross_check: usize,
    /// Count distinct addresses behind the run. This is `holder_count` as the
    /// module path defines it — per *address*, not per output — measured over the
    /// whole run rather than the trait's capped first page.
    #[arg(long)]
    holders: bool,
    /// Count the policy's own assets: distinct names, and how many entries carry
    /// them. `--sum` totals every asset in each row, which is the holder's whole
    /// balance — this is the number a collection page shows.
    #[arg(long)]
    assets: bool,
}

#[derive(Args)]
struct PublishArgs {
    /// The book directory to publish from.
    #[arg(long)]
    book: PathBuf,
    /// Where to write the part layout. Created if absent.
    #[arg(long)]
    out: PathBuf,
    /// Skip the local open that produces the attestation's `validated` flag.
    ///
    /// ⚠️ The open is what proves each directory partitions its entry array, and a
    /// remote reader cannot run that check itself (2^21 buckets, ~2.1 M round
    /// trips). Skipping it publishes `validated false`, which a reader should
    /// refuse. Only useful for laying out a book this binary is too old to open.
    #[arg(long)]
    skip_validate: bool,
}

#[derive(Args)]
struct StampArgs {
    /// The book directory to stamp, in place.
    #[arg(long)]
    book: PathBuf,
    /// The Mithril immutable directory.
    #[arg(long)]
    immutable: PathBuf,
    /// The chunk whose boundary the book was built at. Its **last block's slot** is
    /// what gets recorded — read from the log, never inferred from the chunk number,
    /// because a chunk's end is not a slot.
    #[arg(long)]
    base_chunk: u16,
}

#[derive(Args)]
struct ReindexArgs {
    /// The book directory to reindex, in place.
    #[arg(long)]
    book: PathBuf,
}

#[derive(Args)]
struct VerifyArgs {
    /// The book directory to read back.
    #[arg(long)]
    book: PathBuf,
    /// Entries to resolve through the index into a row. 0 = every entry.
    #[arg(long, default_value_t = 200_000)]
    sample: u64,
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
    /// Capture the witness datums this walk reveals into a `datum.tbl` + `datum.bin`
    /// pair in this directory — the table `live_book::format::DatumTable` reads, and
    /// the payload `read_output_datums`/`read_datum` cannot answer without.
    ///
    /// Implies `--chain-decode`: the bytes exist only in the eager projection's
    /// `DecodedTx::witness_datums`, so this rides the deep walk and therefore cannot
    /// ride `--cursor`'s one-pass decode (the two flags are refused together).
    #[arg(long, value_name = "DIR", conflicts_with_all = ["cursor", "primitives"])]
    datum_tbl: Option<PathBuf>,
    /// Which hasher the live set uses. `fx` is a hand-rolled FxHash, `std` is
    /// SipHash. Same run, one flag, so the difference is the hasher.
    #[arg(long, value_enum, default_value = "fx")]
    hasher: Hasher,
    /// Build the live book (`LIVE_BOOK.md` phase 1) into this directory: `rows.bin`,
    /// `by_outref.idx` + `dir.idx`, `by_policy.idx` + `policy.dir.idx`,
    /// `by_addr.idx` + `addr.dir.idx`, `policy.tbl`. Needs the primitives walk,
    /// because the book wants addresses, values and datums.
    #[arg(long, value_name = "DIR")]
    book: Option<PathBuf>,
    /// Compact the book's scratch arena once it holds this many MiB of *dead*
    /// rows — rows the index no longer points at — so the `u32` row offset never
    /// wraps. Small values force compactions, which is how that path is tested.
    #[arg(long, default_value_t = 1024)]
    book_compact_mb: u64,
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
        Cmd::Verify(a) => verify(a),
        Cmd::Reindex(a) => reindex(a),
        Cmd::Lookup(a) => lookup(a),
        Cmd::StampBoundary(a) => stamp_boundary(a),
        Cmd::Publish(a) => publish(a),
        Cmd::ResolveInputs(a) => resolve_inputs(a),
        Cmd::Follow(a) => follow(a),
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

/// Witness datums captured for a `--datum-tbl` build, and what the capture measures
/// on the way past.
///
/// The table's key is a commitment — pallas hashes the datum's own CBOR — so two
/// entries can never share a key without sharing bytes, and this is a set keyed by
/// hash rather than a log. What that makes measurable while it fills:
///
/// - how many *instances* were offered for how many *distinct* datums, which is the
///   dedup ratio, and the ratio is what decides whether a store-wide table is GBs or
///   tens of GBs;
/// - how many outputs were created with a datum attached **by hash**, and how many of
///   those the capture could already answer for. The second number is the interesting
///   one: a hit is a *live* output a reader can resolve now, because the preimage was
///   already public — and a miss is an output whose datum **no source can supply**
///   until someone spends it.
#[derive(Clone, Default)]
struct DatumCapture {
    dir: PathBuf,
    by_hash: HashMap<Hash<32>, Vec<u8>>,
    /// Instances offered, and their Σ bytes — before dedup.
    seen: u64,
    seen_bytes: u64,
    /// Outputs created with a datum attached by hash and no inline payload.
    hash_attached: u64,
    /// ...of those, the ones whose hash this capture already held.
    live_hits: u64,
    first_slot: Option<u64>,
    last_slot: u64,
}

/// What the capture left behind, for the report.
struct DatumReport {
    entries: u64,
    payload_bytes: u64,
    seen: u64,
    seen_bytes: u64,
    hash_attached: u64,
    live_hits: u64,
    first_slot: u64,
    last_slot: u64,
    wrote_secs: f64,
}

impl DatumCapture {
    fn new(dir: PathBuf) -> DatumCapture {
        DatumCapture {
            dir,
            ..Default::default()
        }
    }

    /// The walk's own coordinate: the first block it saw and the last it applied,
    /// which is the range a miss is scoped to.
    fn set_slot(&mut self, slot: u64) {
        self.first_slot.get_or_insert(slot);
        self.last_slot = slot;
    }

    /// One valid tx. The witness set is absorbed **first**: a transaction may reveal a
    /// datum and attach that same hash to an output it creates, which is exactly the
    /// reuse the `live_hits` count is looking for.
    fn absorb(&mut self, d: &mitos_chain_walk::decode::DecodedTx) {
        for (hash, bytes) in &d.witness_datums {
            self.seen += 1;
            self.seen_bytes += bytes.len() as u64;
            self.by_hash.entry(*hash).or_insert_with(|| bytes.clone());
        }
        for o in &d.outputs {
            let (Some(hash), None) = (o.datum_hash.as_ref(), o.inline_datum.as_ref()) else {
                continue;
            };
            self.hash_attached += 1;
            if self.by_hash.contains_key(hash) {
                self.live_hits += 1;
            }
        }
    }

    /// Sort, write both files, and hand the report its numbers.
    fn finish(mut self) -> Result<DatumReport> {
        let t = Instant::now();
        let mut v: Vec<([u8; 32], Vec<u8>)> = self
            .by_hash
            .drain()
            .map(|(h, bytes)| {
                let mut k = [0u8; 32];
                k.copy_from_slice(h.as_ref());
                (k, bytes)
            })
            .collect();
        v.sort_by_key(|a| a.0);
        let payload_bytes: u64 = v.iter().map(|(_, b)| b.len() as u64).sum();
        let entries = v.len() as u64;
        let first_slot = self.first_slot.unwrap_or(self.last_slot);
        live_book::format::write_datum_table(&self.dir, (first_slot, self.last_slot), &v)?;
        Ok(DatumReport {
            entries,
            payload_bytes,
            seen: self.seen,
            seen_bytes: self.seen_bytes,
            hash_attached: self.hash_attached,
            live_hits: self.live_hits,
            first_slot,
            last_slot: self.last_slot,
            wrote_secs: t.elapsed().as_secs_f64(),
        })
    }
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
    /// Set when `--datum-tbl` asked for a table; the capture rides this pass, so the
    /// projection's own cost is the whole of what it adds.
    datum: Option<DatumCapture>,
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
///
/// This is also the walk the book is fed from when a block is **not** a
/// `ShelleyBlock`: Byron, whose spends are self-contained, and the epoch
/// boundary. Feeding it here rather than in `plan_block_primitives` is not a
/// preference — those blocks have no primitives representation to walk.
#[allow(clippy::too_many_arguments)]
fn plan_block(
    block: &MultiEraBlock<'_>,
    chunk: u16,
    plan: &mut BlockPlan,
    st: &mut DecodeStats,
    chain: Option<&mut ChainStats>,
    mut sink: Option<&mut Sink<'_, '_>>,
) -> Result<()> {
    plan.clear();
    let era_b = tx_index::format::era_to_u8(block.era());
    if !st.eras.contains(&block.era()) {
        st.eras.push(block.era());
    }

    // The artifact's own position. Here rather than in the primitives walk because
    // this is the path Byron and the epoch boundary take, and `MultiEraBlock`
    // carries a slot for both.
    if let Some(sk) = sink.as_deref_mut() {
        sk.set_slot(block.slot());
    }
    // The capture's coverage is the same fact: the slot of the block being applied.
    // The binding turns mutable here rather than just above the tx loop, because an
    // `Option<&mut _>` parameter cannot be reborrowed and both places need it.
    let mut chain = chain;
    if let Some(c) = chain.as_deref_mut().and_then(|c| c.datum.as_mut()) {
        c.set_slot(block.slot());
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
            if let Some(cap) = c.datum.as_mut() {
                cap.absorb(&d);
            }
            c.absorb(t.elapsed().as_secs_f64(), &d);
        }

        let s0 = plan.spends.len() as u32;
        for inp in tx.consumes() {
            plan.spends.push((*inp.hash(), inp.index() as u32));
            if let Some(sk) = sink.as_deref_mut() {
                sk.spend((*inp.hash(), inp.index() as u32));
            }
        }
        let s1 = plan.spends.len() as u32;

        let c0 = plan.creates.len() as u32;
        for (i, o) in tx.outputs().iter().enumerate() {
            let f = classic_facts(&o.value(), o);
            if let Some(sk) = sink.as_deref_mut() {
                feed_multi_era(sk, (hash, i as u32), era_b, o)?;
            }
            note_output(st, plan, chunk, hash, i as u32, f);
        }
        let c1 = plan.creates.len() as u32;

        plan.spans.push((s0, s1, c0, c1));
        st.txs += 1;
        st.inputs += u64::from(s1 - s0);
        st.outputs += u64::from(c1 - c0);
    }
    st.blocks += 1;
    Ok(())
}

// ------------------------------------------------- the primitives walk

/// Where one walk's spends and creations go.
///
/// One walk, more than one artifact: the same decode feeds the published book (a
/// byte-addressed arena of rows on disk, and three orderings over it) and the
/// delta (the same rows in memory, for a follower whose questions are about
/// transactions the book's boundary is behind). Both are written by `row`'s
/// encoder, so a delta row and a book row are the same bytes — which is what lets
/// one consumer read across them.
///
/// Byron and the epoch-boundary blocks reach this through `feed_multi_era`, the
/// expensive route; the primitives walk reaches it through `feed_alonzo` and
/// `feed_conway`, which is where every Shelley+ output goes.
enum Sink<'a, 'b> {
    Book(&'a mut Book),
    Delta(&'a mut Delta<'b>),
}

impl Sink<'_, '_> {
    /// Where the walk has got to.
    ///
    /// The book records it; the delta does not, and the asymmetry is the design
    /// rather than an omission — a delta is defined *relative to* a boundary, so it
    /// has no position of its own to report (`DELTA_CHAIN.md` §7).
    fn set_slot(&mut self, slot: u64) {
        if let Sink::Book(b) = self {
            b.set_base_slot(slot);
        }
    }

    fn spend(&mut self, outref: OutRef) {
        match self {
            Sink::Book(b) => {
                b.spend(outref);
            }
            Sink::Delta(d) => {
                d.spend(outref);
            }
        }
    }

    fn create_output<'e>(
        &mut self,
        outref: OutRef,
        era: u8,
        lovelace: u64,
        address: &[u8],
        datum: Datum<'_>,
        assets: impl Iterator<Item = (&'e [u8], &'e [u8], u64)>,
    ) -> Result<()> {
        match self {
            Sink::Book(b) => b.create_output(outref, era, lovelace, address, datum, assets),
            Sink::Delta(d) => d.create_output(outref, era, lovelace, address, datum, assets),
        }
    }
}

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

    /// The block's slot, which every era's header carries at the same path.
    ///
    /// On the block rather than in a `BlockPlan`, because it is the one thing an
    /// artifact needs that no transaction in it carries: where the artifact *is*.
    fn slot(self) -> u64 {
        match self {
            ShelleyBlock::Alonzo(b, _) => b.header.header_body.slot,
            ShelleyBlock::Babbage(b) => b.header.header_body.slot,
            ShelleyBlock::Conway(b) => b.header.header_body.slot,
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

/// A post-Alonzo output's datum, as the book wants it. The bytes borrow from
/// the option, so the returned `Datum` is tied to the reference, not to the
/// `DatumOption`'s own lifetime.
fn datum_of<'a, 'b>(d: Option<&'a DatumOption<'b>>) -> Datum<'a> {
    match d {
        Some(DatumOption::Hash(h)) => Datum::Hash(h.as_ref()),
        Some(DatumOption::Data(w)) => Datum::Inline(w.0.raw_cbor()),
        None => Datum::None,
    }
}

/// One output into the book, from an Alonzo/Babbage value.
fn feed_alonzo(
    b: &mut Sink<'_, '_>,
    oref: (Hash<32>, u32),
    era: u8,
    address: &[u8],
    value: &alonzo::Value,
    datum: Datum<'_>,
) -> Result<()> {
    match value {
        alonzo::Value::Coin(c) => {
            b.create_output(oref, era, *c, address, datum, std::iter::empty())
        }
        alonzo::Value::Multiasset(c, m) => b.create_output(
            oref,
            era,
            *c,
            address,
            datum,
            m.iter()
                .flat_map(|(p, a)| a.iter().map(move |(n, q)| (p.as_slice(), n.as_slice(), *q))),
        ),
    }
}

/// One output into the book, from a Conway value. The only difference from the
/// Alonzo shape is the quantity type, which is why they are two functions.
fn feed_conway(
    b: &mut Sink<'_, '_>,
    oref: (Hash<32>, u32),
    era: u8,
    address: &[u8],
    value: &conway::Value,
    datum: Datum<'_>,
) -> Result<()> {
    match value {
        conway::Value::Coin(c) => {
            b.create_output(oref, era, *c, address, datum, std::iter::empty())
        }
        conway::Value::Multiasset(c, m) => b.create_output(
            oref,
            era,
            *c,
            address,
            datum,
            m.iter().flat_map(|(p, a)| {
                a.iter()
                    .map(move |(n, q)| (p.as_slice(), n.as_slice(), u64::from(q)))
            }),
        ),
    }
}

/// CRC-32/ISO-HDLC — the checksum a Byron address carries over its own payload.
///
/// `pallas-addresses` keeps `crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC)` private,
/// so this is that definition written out: reflected, polynomial `0xEDB88320`,
/// initial and final XOR `0xFFFF_FFFF`. Twelve lines against a dependency the
/// probe does not otherwise need. `crc_matches_its_check_vector` pins it.
fn crc32_iso_hdlc(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in bytes {
        crc ^= u32::from(b);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// A Byron address, checked against its own encoding before the book stores it.
///
/// pallas models a Byron address as an opaque tag-24 payload plus a CRC
/// (`byron::Address { payload: TagWrap<ByteVec, 24>, crc }`) and `TagWrap`'s
/// decoder reads a tag *without checking its number*, so nothing along the path
/// validates that a payload is an address at all.
///
/// That is not academic, because a Byron address's size is whatever the chain
/// says. Its attributes map is `{ * any => any}` — unconstrained — and mainnet
/// really does carry multi-kilobyte addresses: over chunk 81, 4 of 16,013
/// created outputs hold payloads of 502, 682, 828 and 7,678 B against a plain
/// address's ~43 B, with attribute 1 carrying text (one is a hex-encoded JPEG).
/// Early Byron had no metadata field, so the address was the data carrier.
///
/// So this is not a hunt for a mis-decode — it is the check that a row's address
/// bytes are what they claim to be, which is the one thing the three
/// whole-artifact identities structurally cannot do: they compare two
/// implementations of the same live set over the same decode, so a
/// wrong-but-consistent decode balances on all three.
///
/// The CRC is what makes it decisive rather than indicative. `AddrAttrProperty`
/// bounds the attribute *keys* to 0, 1 and 2, but `DerivationPath(ByteVec)` and
/// `NetworkTag(ByteVec)` are unbounded byte strings, so a garbage byte string
/// can parse as a well-formed `AddressPayload` and consume exactly its length.
/// The CRC is four bytes read from the *same* stream and cannot be satisfied by
/// accident: a decode that took a length from the wrong offset fails it with
/// probability 1 − 2⁻³².
fn validate_byron_address(a: &pallas_primitives::byron::Address) -> Result<()> {
    let payload: &[u8] = &a.payload.0;
    let mut d = Decoder::new(payload);
    let parsed: AddressPayload = d.decode().with_context(|| {
        format!(
            "a {}-byte Byron address payload does not parse as an AddressPayload",
            payload.len()
        )
    })?;
    if d.position() != payload.len() {
        bail!(
            "a {}-byte Byron address payload parses as an AddressPayload only for its first \
             {} ({} bytes left over, addrtype {:?})",
            payload.len(),
            d.position(),
            payload.len() - d.position(),
            parsed.addrtype,
        );
    }
    let crc = crc32_iso_hdlc(payload);
    if crc != a.crc {
        bail!(
            "a {}-byte Byron address payload fails its own CRC (payload says {crc:#010x}, \
             the address record says {:#010x}, addrtype {:?}, {} attribute(s)) — the decode \
             read these bytes from the wrong offset",
            payload.len(),
            a.crc,
            parsed.addrtype,
            parsed.attributes.len(),
        );
    }
    Ok(())
}

/// One output into the book, from a `MultiEraOutput` — the fallback walk's route.
///
/// Byron and the epoch-boundary blocks do not decode into a `ShelleyBlock`, so
/// they cannot ride `plan_block_primitives` and the book has to be fed through
/// the generic accessors instead. That is the expensive route — an `Address`
/// plus a re-serialisation for the bytes, a `Vec` per output for the assets —
/// and it is paid over 208 of 9,181 chunks, which is **4.49 M blocks and 32% of
/// the store's**. Worth knowing before optimising it: it is a third of the
/// blocks and a small fraction of the bytes.
///
/// The value goes through `into_alonzo` rather than being re-derived, so the
/// book receives exactly the arithmetic `feed_alonzo` already applies — one
/// conversion, not two implementations. For Byron that conversion is
/// `Coin(*u64)`: no clone and no allocation, which is the case that matters at
/// this block count.
fn feed_multi_era(
    b: &mut Sink<'_, '_>,
    oref: (Hash<32>, u32),
    era: u8,
    o: &MultiEraOutput<'_>,
) -> Result<()> {
    // Checked before the row is written, not after: pallas hands a Byron address
    // over as an opaque payload, so this is the only place its shape is tested.
    if let MultiEraOutput::Byron(b) = o {
        validate_byron_address(&b.address)?;
    }
    let addr = o
        .address()
        .map_err(|e| anyhow::anyhow!("address of {}#{}: {e}", oref.0, oref.1))?;
    let addr_bytes = addr.to_vec();
    let value = o.value().into_alonzo();
    // Bound to a local, not matched in place: `datum()` returns an *owned*
    // option, so a binding made in the scrutinee dies at the end of the match
    // and cannot be lent to `create_output`.
    let datum_opt = o.datum();
    let datum = match &datum_opt {
        Some(DatumOption::Hash(h)) => Datum::Hash(h.as_ref()),
        Some(DatumOption::Data(w)) => Datum::Inline(w.0.raw_cbor()),
        None => Datum::None,
    };
    feed_alonzo(b, oref, era, &addr_bytes, &value, datum).with_context(|| {
        let head = &addr_bytes[..addr_bytes.len().min(16)];
        format!(
            "output {}#{} — era {era}, address {} B, typeid {}, head {head:02x?}",
            oref.0,
            oref.1,
            addr_bytes.len(),
            addr.typeid(),
        )
    })
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
    sink: Option<&mut Sink<'_, '_>>,
) -> Result<()> {
    plan.clear();
    let era = blk.era();
    let era_b = tx_index::format::era_to_u8(era);
    if !st.eras.contains(&era) {
        st.eras.push(era);
    }
    let mut sink = sink;

    // The artifact's own position, recorded as the walk passes each block — cheap
    // enough per block rather than once at the end, which would need the last block
    // decoded a second time.
    if let Some(sk) = sink.as_deref_mut() {
        sk.set_slot(blk.slot());
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
                    if let Some(bk) = sink.as_deref_mut() {
                        bk.spend((inp.transaction_id, inp.index as u32));
                    }
                }
                let s1 = plan.spends.len() as u32;
                let c0 = plan.creates.len() as u32;
                for (idx, o) in body.outputs.iter().enumerate() {
                    let f = legacy_facts(o);
                    if let Some(bk) = sink.as_deref_mut() {
                        let datum = match &o.datum_hash {
                            Some(h) => Datum::Hash(h.as_ref()),
                            None => Datum::None,
                        };
                        feed_alonzo(
                            bk,
                            (hash, idx as u32),
                            era_b,
                            o.address.as_slice(),
                            &o.amount,
                            datum,
                        )?;
                    }
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
                    if let Some(bk) = sink.as_deref_mut() {
                        bk.spend((inp.transaction_id, inp.index as u32));
                    }
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
                    if let Some(bk) = sink.as_deref_mut() {
                        match &**o {
                            babbage::TransactionOutput::Legacy(x) => {
                                let datum = match &x.datum_hash {
                                    Some(h) => Datum::Hash(h.as_ref()),
                                    None => Datum::None,
                                };
                                feed_alonzo(
                                    bk,
                                    (hash, idx as u32),
                                    era_b,
                                    x.address.as_slice(),
                                    &x.amount,
                                    datum,
                                )?;
                            }
                            babbage::TransactionOutput::PostAlonzo(x) => feed_alonzo(
                                bk,
                                (hash, idx as u32),
                                era_b,
                                x.address.as_slice(),
                                &x.value,
                                datum_of(x.datum_option.as_deref()),
                            )?,
                        }
                    }
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
                    if let Some(bk) = sink.as_deref_mut() {
                        bk.spend((inp.transaction_id, inp.index as u32));
                    }
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
                    if let Some(bk) = sink.as_deref_mut() {
                        match o {
                            conway::TransactionOutput::Legacy(x) => {
                                let datum = match &x.datum_hash {
                                    Some(h) => Datum::Hash(h.as_ref()),
                                    None => Datum::None,
                                };
                                feed_alonzo(
                                    bk,
                                    (hash, idx as u32),
                                    era_b,
                                    x.address.as_slice(),
                                    &x.amount,
                                    datum,
                                )?;
                            }
                            conway::TransactionOutput::PostAlonzo(x) => feed_conway(
                                bk,
                                (hash, idx as u32),
                                era_b,
                                x.address.as_slice(),
                                &x.value,
                                datum_of(x.datum_option.as_deref()),
                            )?,
                        }
                    }
                    note_output(st, plan, chunk, hash, idx as u32, f);
                }
                let c1 = plan.creates.len() as u32;
                plan.spans.push((s0, s1, c0, c1));
                st.txs += 1;
                st.inputs += u64::from(s1 - s0);
                st.outputs += u64::from(c1 - c0);
            }
        }
    }
    st.blocks += 1;
    Ok(())
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

    /// Accounts for what the live set itself holds, so a book's index can be
    /// compared against it rather than against peak RSS (which includes the
    /// chunk buffer, the plan, and the allocator's own slack).
    fn accounted_bytes(&self) -> usize {
        let per_slot = std::mem::size_of::<(OutRef, Entry)>() + 1;
        match self {
            Live::Fx(m) => m.capacity() * per_slot,
            Live::Std(m) => m.capacity() * per_slot,
        }
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
    // `--datum-tbl` turns the deep pass on rather than sitting beside it: the witness
    // datum bytes exist nowhere else in a decode, and a table silently built from a
    // lean walk would be empty and would look like "no datums on chain".
    if let Some(dir) = &a.datum_tbl {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating datum table dir {}", dir.display()))?;
        chain_acc.datum = Some(DatumCapture::new(dir.clone()));
    }
    let deep = a.chain_decode || a.datum_tbl.is_some();
    let mut app_acc = ApplyStats::default();
    // Kept for the report: `finish` consumes the builder, and the report reads the
    // boundary back out of the artifact rather than out of a variable.
    let book_dir = a.book.clone();
    let mut book = match &a.book {
        Some(dir) => {
            if !a.cursor && !a.primitives {
                bail!(
                    "--book needs --cursor or --primitives (it wants addresses, values and datums)"
                );
            }
            Some(Book::create(dir, a.book_compact_mb * 1024 * 1024)?)
        }
        None => None,
    };
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

        // One walk, one destination here — the probe's own `follow` is what feeds
        // a delta. Rebuilt per chunk so the borrow of `book` ends with the
        // iteration and the builder can be finished after the loop.
        let mut sink = book.as_mut().map(Sink::Book);

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
                    plan_block_primitives(
                        b,
                        chunk,
                        &mut plan,
                        &mut dec,
                        &mut hashes,
                        sink.as_mut(),
                    )?;
                } else {
                    dec.fallback_blocks += 1;
                    let mb = MultiEraBlock::decode(&bytes[pos..end])
                        .with_context(|| format!("chunk {chunk}: byte {pos}"))?;
                    era = mb.era();
                    plan_block(&mb, chunk, &mut plan, &mut dec, None, sink.as_mut())?;
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
                        Some(b) => plan_block_primitives(
                            b,
                            chunk,
                            &mut plan,
                            &mut dec,
                            &mut hashes,
                            sink.as_mut(),
                        )?,
                        None => {
                            // Byron and the epoch boundary.
                            dec.fallback_blocks += 1;
                            plan_block(&block, chunk, &mut plan, &mut dec, None, sink.as_mut())?;
                        }
                    }
                } else {
                    plan_block(
                        &block,
                        chunk,
                        &mut plan,
                        &mut dec,
                        deep.then_some(&mut chain_acc),
                        sink.as_mut(),
                    )?;
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

    if deep {
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
    // The capture's own report, written rather than reported: this is the artifact.
    if let Some(cap) = chain_acc.datum.take() {
        let d = cap.finish()?;
        let tbl = std::fs::metadata(
            a.datum_tbl
                .as_ref()
                .expect("set with the capture")
                .join(live_book::format::DATUM_FILE),
        )
        .map(|m| m.len())?;
        println!();
        println!("datum table — the witness datums this walk revealed (`datum.tbl`)");
        println!(
            "  coverage   slots {}..={} · a miss means \"not revealed in this range\"",
            d.first_slot, d.last_slot
        );
        println!(
            "  entries    {} distinct hashes · {} payload · offered {} times in {} → {:.0}% dedup",
            d.entries,
            human_bytes(d.payload_bytes),
            d.seen,
            human_bytes(d.seen_bytes),
            (1.0 - d.payload_bytes as f64 / d.seen_bytes.max(1) as f64) * 100.0,
        );
        println!(
            "  attached   {} hash-attached outputs created · {} answerable at creation ({:.2}%) —\n\
             the rest had no preimage on chain yet, and a reveal later in the range lands here",
            d.hash_attached,
            d.live_hits,
            d.live_hits as f64 / d.hash_attached.max(1) as f64 * 100.0,
        );
        println!(
            "  wrote      datum.tbl {} + datum.bin {} in {:.1} s",
            human_bytes(tbl),
            human_bytes(d.payload_bytes),
            d.wrote_secs,
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

    if let Some(b) = book.take() {
        let cost = b.finish()?;
        println!();
        println!("book — phase 1: rows.bin + by_outref.idx + dir.idx + policy.tbl");
        println!(
            "  rows       {} live · {} created · {} spends resolved · {} missed",
            cost.rows_live, cost.created, cost.spent_hits, cost.spent_misses
        );
        println!(
            "  memory     index {} · rows {} on disk · {} rehashes · {} compactions",
            human_bytes(cost.index_bytes),
            human_bytes(cost.rows_bytes),
            cost.rehashes,
            cost.compactions,
        );
        println!(
            "  artifact   {} × 12 B index entries · {} distinct prefixes · {:.2} row \
             confirms per lookup",
            cost.rows_live,
            cost.distinct_prefixes,
            cost.rows_live as f64 / cost.distinct_prefixes.max(1) as f64,
        );
        println!(
            "  addresses  longest {} B · {} over 255 B ({:.2}% of rows) — `B2` gave \
             `addr_len` one byte",
            cost.max_addr_len,
            cost.addrs_over_255,
            share(cost.addrs_over_255, cost.rows_live),
        );
        // The by_addr distribution, per kind rather than per row: it is the only
        // evidence that the dispatch is right for the shapes mainnet actually
        // holds. A `by_addr` keyed on `address[1..29]` alone would be wrong for
        // exactly one of these four numbers.
        println!(
            "  by_addr    {} entries — {} payment · {} stake · {} bootstrap · {} unkeyed",
            cost.by_addr_entries,
            cost.addr_payment,
            cost.addr_stake,
            cost.addr_bootstrap,
            cost.addr_unkeyed,
        );
        println!(
            "  against    the HashMap live set accounts {} for {} entries ({:.0} B/entry)",
            human_bytes(set.accounted_bytes() as u64),
            entries,
            set.accounted_bytes() as f64 / entries.max(1) as f64,
        );

        // Three identities, each comparing two independent implementations of the
        // same live set. Any of them failing means one of the two is wrong.
        let fold_live = dec_acc.outputs.saturating_sub(app_acc.hit);
        if cost.rows_live != fold_live {
            bail!(
                "book holds {} live rows, the fold's set holds {} (created {} − resolved {})",
                cost.rows_live,
                fold_live,
                dec_acc.outputs,
                app_acc.hit
            );
        }
        if cost.live_lovelace != live_lovelace {
            bail!(
                "book holds {} ₳, the fold's set holds {} ₳",
                cost.live_lovelace / 1_000_000,
                live_lovelace / 1_000_000
            );
        }
        if cost.live_assets != live_assets {
            bail!(
                "book holds {} asset entries, the fold's set holds {}",
                cost.live_assets,
                live_assets
            );
        }
        println!(
            "  identities rows = created − resolved · {} ₳ · {} asset entries — all three agree",
            cost.live_lovelace / 1_000_000,
            cost.live_assets
        );
        // The one fact no transaction in the book carries, and the one a data plane
        // needs before it can answer `tip`. Read back from the artifact rather than
        // from the walk's own variable, so this line reports what was written.
        if let Some(dir) = &book_dir {
            match live_book::Meta::open(dir)?.base_slot {
                Some(slot) => {
                    println!("  position   slot {slot} — the boundary `tip()` answers from")
                }
                None => println!(
                    "  position   NOT RECORDED — `tip()` will refuse; the walk saw no block"
                ),
            }
        }
    }
    Ok(())
}

/// Every complete chunk, no set, counters only. `created − spent` is the live
/// set's entry count: every output a valid tx creates, minus every output a
/// valid tx spends, with no ordering requirement anywhere in the pass — which
/// is what makes it a thread pool rather than a fold.
/// Read a published book back through its own indexes and report what breaks.
///
/// Deliberately a subcommand rather than a step inside `fold`: it reads an
/// artifact that already exists, so a whole-store book can be checked in seconds
/// instead of after another fifty-minute fold.
fn verify(a: VerifyArgs) -> Result<()> {
    let rep = book::verify(&a.book, a.sample)?;
    println!("book verify — {}", a.book.display());
    // One line per ordering, from one check. The three differ in what a row has to
    // corroborate and in nothing else, so printing them differently would be the
    // reader inventing a distinction the verifier does not make.
    for (name, o) in [
        ("by_outref", &rep.outref),
        ("by_policy", &rep.policy),
        ("by_addr", &rep.addr),
    ] {
        println!("  {name:10} {} entries", o.entries);
        println!(
            "  {:10} {} out of canonical order · {} directory violations · {} entries whose \
             row does not corroborate the prefix",
            "", o.unsorted, o.dir_violations, o.mismatches
        );
        println!(
            "  {:10} {} sampled · {:.0} ns · {} pointing outside rows.bin · {} unreadable",
            "",
            o.sampled,
            o.lookup_nanos as f64 / o.sampled.max(1) as f64,
            o.out_of_range,
            o.ill_formed,
        );
    }
    if rep.failures() > 0 {
        bail!(
            "the artifact does not verify: {} failed checks",
            rep.failures()
        );
    }
    println!("  all checks pass");
    Ok(())
}

/// Rebuild the orderings that do not need the chain, and say which those are.
///
/// Deliberately a subcommand, not a step in `fold`: the point is that a view can be
/// derived from an artifact that already exists, so this must not require the walk
/// that produced it.
/// Record where a published book sits on chain, from the chunk it was built at.
///
/// A build writes `base_slot` itself, from the walk. This exists for the artifacts
/// built before the field did — an artifact that is otherwise complete and whose
/// only gap is that it cannot say where it is, which is the one thing a data plane
/// needs before it can answer `tip()`.
///
/// The slot is the **last block of `--base-chunk`**, read out of the log. It is not
/// derived from the chunk number: a chunk ends where its last block says, and
/// `chunk * 21_600` is a claim the log would have to contradict.
/// Lay a book out as uniform parts plus an attestation, ready to upload.
///
/// **Opens the book first**, and that is the point rather than a precaution: the
/// open is what runs `Directory::validate` over every bucket, which a remote
/// reader cannot do. Its success is the only thing the attestation's `validated`
/// flag is allowed to mean.
fn publish(a: PublishArgs) -> Result<()> {
    let validated = if a.skip_validate {
        println!("⚠️  --skip-validate: publishing `validated false`, which a reader should refuse");
        false
    } else {
        let base = live_book::Base::open(&a.book)
            .with_context(|| format!("opening {} to validate it", a.book.display()))?;
        println!(
            "validated   {} live rows · {} · directories partition their arrays",
            base.rows(),
            base.meta().fingerprint(),
        );
        true
    };

    let att = live_book::publish::publish(&a.book, &a.out, validated)?;
    let total: u64 = att.files.iter().map(|f| f.len).sum();
    let parts: u64 = att.files.iter().map(|f| f.parts).sum();

    println!("publish     {} → {}", a.book.display(), a.out.display());
    println!(
        "  layout    {} files · {parts} parts of {} · {}",
        att.files.len(),
        human_bytes(att.part_size),
        human_bytes(total),
    );
    for f in &att.files {
        println!(
            "    {:<16} {:>10}  {:>5} part(s)",
            f.name,
            human_bytes(f.len),
            f.parts
        );
    }
    println!("  fingerprint {}", att.fingerprint);
    println!(
        "\nupload the whole directory, preserving paths, with:\n  \
         Content-Type: application/octet-stream\n  \
         Cache-Control: public, max-age=31536000, immutable\n\
         …then flip the epoch pointer LAST, conditionally, so no reader ever sees\n\
         a pointer naming parts that are not there yet."
    );
    Ok(())
}

fn stamp_boundary(a: StampArgs) -> Result<()> {
    let path = chunk_path(&a.immutable, a.base_chunk);
    let bytes = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
    let blocks = split_blocks(&bytes)?;
    let last = blocks
        .last()
        .with_context(|| format!("chunk {} holds no blocks", a.base_chunk))?;
    let slot = MultiEraBlock::decode(last)
        .with_context(|| format!("chunk {}: decoding its last block", a.base_chunk))?
        .slot();

    let before = live_book::Meta::open(&a.book)?;
    println!("book stamp-boundary — {}", a.book.display());
    println!(
        "  chunk      {} · {} B · {} block(s) · last at slot {slot}",
        a.base_chunk,
        bytes.len(),
        blocks.len(),
    );
    match before.base_slot {
        Some(old) if old == slot => {
            println!("  position   already slot {old} — nothing to do");
            return Ok(());
        }
        Some(old) => println!("  position   was slot {old} — correcting to {slot}"),
        None => println!("  position   was unrecorded"),
    }
    live_book::Meta::stamp_base_slot(&a.book, slot)?;

    // Read back through the reader a consumer uses, so this reports the artifact and
    // not the write.
    let after = live_book::Meta::open(&a.book)?;
    if after.base_slot != Some(slot) {
        bail!("wrote base_slot {slot} but read back {:?}", after.base_slot);
    }
    println!("  stamped    base_slot {slot} — `tip()` now answers `ChainPoint::Slot({slot})`");
    Ok(())
}

fn reindex(a: ReindexArgs) -> Result<()> {
    let t = Instant::now();
    let rep = book::reindex(&a.book)?;
    println!(
        "book reindex — {} in {:.1} s",
        a.book.display(),
        t.elapsed().as_secs_f64()
    );
    println!(
        "  by_policy  {} entries → by_policy.idx + policy.dir.idx",
        rep.policy_entries
    );
    println!(
        "  by_addr    {} entries → by_addr.idx + addr.dir.idx — {} payment · {} stake · \
         {} bootstrap",
        rep.addr_entries, rep.addr_payment, rep.addr_stake, rep.addr_bootstrap
    );
    println!(
        "  dir.idx    rebuilt over {} prefixes from by_outref.idx",
        rep.outref_entries
    );
    println!("  read only  rows.bin, by_outref.idx, policy.tbl — never written");
    println!(
        "  not here   anything whose key is in neither the rows nor the indexes:\n\
         \x20            the mint ledger, witness datums, aux data. Those need the walk."
    );
    Ok(())
}

/// What a caller asked, parsed once so a paged loop can re-ask it. The three
/// enumerations differ only in this, which is the point of the reader taking the
/// confirm and not the caller.
enum Query {
    Outref([u8; 32], u32),
    Address(Vec<u8>),
    Cred([u8; 28]),
    Policy([u8; 28]),
}

impl Query {
    fn name(&self) -> &'static str {
        match self {
            Query::Outref(..) => "by_outref",
            Query::Address(_) => "by_address",
            Query::Cred(_) => "by_payment_cred",
            Query::Policy(_) => "by_policy",
        }
    }

    fn ask(
        &self,
        book: &live_book::Base,
        after: Option<live_book::Cursor>,
        limit: usize,
    ) -> Result<live_book::Page> {
        Ok(match self {
            Query::Outref(..) => bail!("--outref is a point lookup, not an enumeration"),
            Query::Address(a) => book.by_address(a, after, limit)?,
            Query::Cred(c) => book.by_payment_cred(c, after, limit)?,
            Query::Policy(p) => book.by_policy(p, after, limit)?,
        })
    }
}

/// Ask a published book a question.
///
/// This exists because `verify` proves the artifact is *correct* and nothing
/// proved it was *usable*: it is the first caller that goes through `Base`, the
/// reader a consumer gets, rather than through the audit path. Both matter — they
/// check different things about the same bytes.
fn lookup(a: LookupArgs) -> Result<()> {
    let t = Instant::now();
    let book = live_book::Base::open(&a.book)?;
    let opened = t.elapsed();
    println!("book lookup — {}", a.book.display());
    println!(
        "  opened     {} rows in {:.3} s · {} policies · layout_version {}",
        book.rows(),
        opened.as_secs_f64(),
        book.policies().len(),
        book.meta().layout_version,
    );

    if a.cross_check > 0 {
        return cross_check(&book, a.cross_check, a.limit);
    }

    let query = if let Some(s) = &a.outref {
        let (hash, index) = parse_outref(s)?;
        Query::Outref(hash, index)
    } else if let Some(s) = &a.address {
        Query::Address(parse_address(s)?)
    } else if let Some(s) = &a.payment_cred {
        Query::Cred(parse_28(s, "--payment-cred")?)
    } else if let Some(s) = &a.policy {
        Query::Policy(parse_28(s, "--policy")?)
    } else {
        bail!("give one of --outref, --address, --payment-cred or --policy");
    };

    // A point lookup is one question with one answer, and the reader answers it
    // with a single row read — the number to compare against a `redb` point read.
    if let Query::Outref(hash, index) = &query {
        let t = Instant::now();
        let row = book.utxo(hash, *index)?;
        let ns = t.elapsed().as_nanos();
        match row {
            Some(r) => {
                println!(
                    "  outref     {}#{index} is live — {} ₳ · {} B address · era {} · {} assets · \
                     {ns} ns",
                    hex_of(hash),
                    r.lovelace(),
                    r.address().len(),
                    r.era(),
                    r.n_assets(),
                );
                println!("  address    {}", hex_of(r.address()));
            }
            None => println!(
                "  outref     {}#{index} is not in the live set · {ns} ns",
                hex_of(hash)
            ),
        }
        return Ok(());
    }

    // The paged shape: a caller keeps asking until `next` is `None`. Not until the
    // page is short or empty — a page can confirm nothing and still have more, and
    // that is the mistake the loop is here to make visible.
    let t = Instant::now();
    let mut pages = 0u64;
    let mut refs = 0u64;
    let mut first = Vec::new();
    let mut after = None;
    let mut exhausted = false;
    loop {
        let page = query.ask(&book, after, a.limit)?;
        pages += 1;
        refs += page.refs.len() as u64;
        if first.is_empty() {
            first = page
                .refs
                .iter()
                .take(5)
                .map(|(h, i)| format!("{}#{i}", hex_of(h)))
                .collect();
        }
        match page.next {
            Some(c) if a.all => after = Some(c),
            Some(_) => break,
            None => {
                exhausted = true;
                break;
            }
        }
    }
    let ns = t.elapsed().as_nanos();

    println!(
        "  {:<14} {refs} refs in {pages} page(s) of {} · {} ns/ref",
        query.name(),
        a.limit,
        if refs > 0 { ns / refs as u128 } else { 0 },
    );
    if first.is_empty() {
        println!("  first      —");
    } else {
        println!("  first      {}", first.join("\n             "));
    }
    println!(
        "  exhausted  {}",
        if exhausted {
            "yes".to_string()
        } else {
            "no — pass --all to keep paging".to_string()
        }
    );

    if a.sum {
        let t = Instant::now();
        let mut lovelace: u128 = 0;
        let (mut rows, mut assets) = (0u64, 0u64);
        let mut absent = 0u64;
        let mut after = None;
        loop {
            let p = query.ask(&book, after, a.limit)?;
            for (hash, index) in &p.refs {
                match book.utxo(hash, *index)? {
                    Some(row) => {
                        rows += 1;
                        lovelace += u128::from(row.lovelace());
                        assets += row.n_assets() as u64;
                    }
                    None => absent += 1,
                }
            }
            match p.next {
                Some(c) => after = Some(c),
                None => break,
            }
        }
        println!(
            "  sum        {rows} rows read back in {:.1} s · {} ₳ · {assets} asset entries \
             · {absent} refs the row store could not answer",
            t.elapsed().as_secs_f64(),
            lovelace / 1_000_000,
        );
    }

    // The most expensive query shape, and the one a holder-distribution view is:
    // distinct *addresses*, which means every row has to come back. Measured
    // separately from `--sum` because it is a different question, and over the
    // whole run rather than the trait's first page — `holder_count` on a policy
    // past the walk cap answers a lower bound, and the gap is the point.
    if a.holders {
        let t = Instant::now();
        let mut addresses: HashSet<&[u8]> = HashSet::new();
        let mut rows = 0u64;
        let mut after = None;
        loop {
            let p = query.ask(&book, after, a.limit)?;
            for (hash, index) in &p.refs {
                if let Some(row) = book.utxo(hash, *index)? {
                    rows += 1;
                    addresses.insert(row.address());
                }
            }
            match p.next {
                Some(c) => after = Some(c),
                None => break,
            }
        }
        let bytes: usize = addresses.iter().map(|a| a.len()).sum();
        println!(
            "  holders    {} distinct addresses behind {rows} rows in {:.2} s · {:.1} B mean \
             address",
            addresses.len(),
            t.elapsed().as_secs_f64(),
            bytes as f64 / addresses.len().max(1) as f64,
        );
    }

    // The collection's own size, as distinct asset names. `--sum`'s asset count
    // is every entry of every row the query returned, so for a policy query it
    // counts the *holders'* balances — a row holding twenty other tokens adds
    // twenty. This is the number a page shows above the grid, and it is what a
    // per-collection projection has to carry one row per.
    if a.assets {
        let Query::Policy(policy) = &query else {
            bail!("--assets counts the assets of a policy; give --policy");
        };
        let t = Instant::now();
        let mut names: HashSet<&[u8]> = HashSet::new();
        let mut entries = 0u64;
        let mut rows = 0u64;
        let mut after = None;
        loop {
            let p = query.ask(&book, after, a.limit)?;
            for (hash, index) in &p.refs {
                let Some(row) = book.utxo(hash, *index)? else {
                    continue;
                };
                rows += 1;
                for asset in row.assets() {
                    if book.policies().get(asset.policy)? == Some(*policy) {
                        entries += 1;
                        names.insert(asset.name);
                    }
                }
            }
            match p.next {
                Some(c) => after = Some(c),
                None => break,
            }
        }
        println!(
            "  assets     {} distinct names in {entries} entries over {rows} rows in {:.2} s",
            names.len(),
            t.elapsed().as_secs_f64(),
        );
    }
    Ok(())
}

/// Cross-check the three orderings against each other, on the whole store.
///
/// The point is that `verify` audits *entries* and this walks *queries*: `verify`
/// reads every entry of an array and confirms the row it points at, so it never
/// exercises `Table::run` — the binary search inside a directory bucket, the thing
/// a lookup actually does, and the one piece of the reader whose behaviour could
/// differ on a run of ten thousand entries from a run of four.
///
/// Driven from `policy.tbl`, because that is the only key the reader hands back a
/// list of and it needs no new API to enumerate. For each sampled policy:
///
/// 1. `by_policy` returns holders — their refs are the arms of the check.
/// 2. `utxo(ref)` must find each one, and the row must resolve the id back to the
///    policy asked for (so the confirm is not merely returning candidates).
/// 3. `by_address(row.address())` must contain that same ref — two orderings, two
///    searches, one row, which is "the views cannot disagree" checked at the
///    reader rather than at the entry.
///
/// It also times step 2, over distinct keys spread across the store — the one
/// figure a *serving* process cares about that nothing else here produces. It is
/// timed over many keys rather than by repeating one, because a repeated lookup of
/// the same row is an L1 hit and a compiler is free to hoist it entirely.
fn cross_check(book: &live_book::Base, sample: usize, limit: usize) -> Result<()> {
    let n_policies = book.policies().len();
    if n_policies == 0 {
        bail!("the artifact holds no policies to drive a cross-check from");
    }
    let stride = (n_policies / sample.max(1)).max(1);
    let limit = limit.max(1_000);
    let mut policies = 0u64;
    let mut refs = 0u64;
    let mut addr_checked = 0u64;
    let mut missing_from_outref = 0u64;
    let mut missing_from_addr = 0u64;
    let mut policy_not_held = 0u64;
    let mut utxo_nanos = 0u128;
    let t = Instant::now();
    let mut id = 0usize;
    while id < n_policies {
        let policy = book
            .policies()
            .get(id as u32)
            .expect("read")
            .expect("in range");
        policies += 1;
        let mut after = None;
        let mut checked_here = 0u64;
        loop {
            let page = book.by_policy(&policy, after, limit)?;
            for (hash, index) in &page.refs {
                refs += 1;
                checked_here += 1;
                let t = Instant::now();
                let row = book.utxo(hash, *index)?;
                utxo_nanos += t.elapsed().as_nanos();
                let Some(row) = row else {
                    missing_from_outref += 1;
                    continue;
                };
                if !row
                    .assets()
                    .any(|a| book.policies().get(a.policy).ok().flatten() == Some(policy))
                {
                    policy_not_held += 1;
                }
                // A busy address can hold thousands of outputs, so the address
                // check is sampled within the run rather than exhaustive — the
                // property under test is "the search finds the row", not "the
                // page is complete", which `verify` already covers.
                if checked_here.is_multiple_of(64) {
                    addr_checked += 1;
                    let mut a_after = None;
                    let mut found = false;
                    loop {
                        let page = book.by_address(row.address(), a_after, limit)?;
                        found |= page.refs.contains(&(*hash, *index));
                        match page.next {
                            Some(c) if !found => a_after = Some(c),
                            _ => break,
                        }
                    }
                    if !found {
                        missing_from_addr += 1;
                    }
                }
            }
            match page.next {
                Some(c) if checked_here < 5_000 => after = Some(c),
                _ => break,
            }
        }
        id += stride;
    }
    let secs = t.elapsed().as_secs_f64();
    println!(
        "  cross      {policies} policies · {refs} refs in {secs:.1} s ({:.1}/s)",
        refs as f64 / secs.max(f64::MIN_POSITIVE),
    );
    println!(
        "  utxo       {} ns per point lookup over {refs} distinct refs",
        utxo_nanos / refs.max(1) as u128
    );
    println!("             {addr_checked} refs also resolved through `by_addr`");
    println!("             {missing_from_outref} not found by `utxo`");
    println!("             {missing_from_addr} not found by `by_address`");
    println!("             {policy_not_held} rows that do not resolve the policy back");
    let failures = missing_from_outref + missing_from_addr + policy_not_held;
    if failures > 0 {
        bail!("the three orderings disagree about {failures} refs");
    }
    println!("  the three orderings agree on every ref sampled");
    Ok(())
}

/// Resolve one chunk's transaction inputs against a published book, and classify
/// every input the book cannot answer.
///
/// This is the measurement behind "can the book find a UTxO that is no longer
/// part of the current set" — the question a transaction landing at tip asks,
/// because the outputs it spends were live one block ago and are not live now.
///
/// Three outcomes, and they are a **partition**, not a sample:
///
/// | outcome | where the answer is |
/// |---|---|
/// | **hit** | in the book — the output existed at the book's boundary and had not been spent before this chunk |
/// | **same-chunk** | in this chunk's own blocks, so a follower processing the chunk has it in hand |
/// | **never-created** | no transaction in the log ever created it: Byron's genesis distribution and redeem-style inputs, the `misresolved` class the fold reports |
///
/// `hits + same_chunk + gap + never_created == inputs` is asserted rather than
/// hoped for, which is what makes this a measurement instead of an impression. An
/// output has to exist before it is spent, so a miss was created in an earlier
/// chunk, this chunk, or never — and "an earlier chunk" splits into **before the
/// book's boundary** (which is a hit: the row is still in the base) or **after it**,
/// which is the gap.
///
/// ⚠️ **What the last bucket means depends on the base.** For a book folded from
/// genesis, `never_created` is `misresolved` — Byron's genesis distribution and
/// redeem-style inputs that no transaction in the log ever created. For a book
/// built from a *window*, it also swallows anything created before the window
/// began, so a run against a window base reports a large bucket that says nothing
/// about the chain. The run context records `base_chunk` so the two are never
/// confused.
///
/// # What the three non-hit buckets are, in the design's terms
///
/// This is the point of the classification, and it is why the buckets are named
/// the way they are:
///
/// | bucket | where the answer is, for a follower at this chunk |
/// |---|---|
/// | **hit** | the book's base — one lookup, no I/O |
/// | **same-chunk** | the block being processed, already in hand |
/// | **gap** | **the delta chain's working set** — created after the base and before this chunk |
/// | **never-created** | nowhere: the log cannot answer it either |
///
/// So resolving a range at increasing lag gives the **staleness curve**, and the
/// gap bucket at each lag is a direct measurement of what a delta chain would have
/// to hold to keep the resolution rate at 100%.
fn resolve_inputs(a: ResolveArgs) -> Result<()> {
    let t = Instant::now();
    let base = live_book::Base::open(&a.book)?;
    let opened = t.elapsed();
    let mut meter = Meter::new();
    let run = Run::new("resolve-inputs")
        .target(a.book.display().to_string())
        .key("rows", base.rows())
        .key("base_chunk", a.base_chunk)
        .key("from", a.from)
        .key("to", a.to);

    println!("book resolve-inputs — {}", a.book.display());
    println!(
        "  base       {} rows · mapped in {:.3} s · boundary chunk {} (passed in, not recorded \
         in `book.meta`)",
        base.rows(),
        opened.as_secs_f64(),
        a.base_chunk,
    );
    println!(
        "  {:<8} {:>6} {:>10} {:>10} {:>11} {:>11} {:>11} {:>9}",
        "chunk", "lag", "inputs", "from book", "same-chunk", "gap", "uncreated", "ns/input"
    );

    // The gap: everything created after the book's boundary and before the chunk
    // being resolved. A delta chain would hold exactly this set, so counting it
    // separately is what makes the lag curve interpretable — without it those
    // resolutions would be indistinguishable from ones the log cannot answer at
    // all.
    //
    // It is rolled forward with the walk rather than computed once for the range:
    // the gap for chunk N is `base+1..N`, so a single set built for the range's
    // first chunk would leave every later chunk's recent creations in `uncreated`,
    // where they look like inputs the log cannot answer. (Measured the hard way:
    // 2,444 of chunk 1002's inputs were misclassified as never created before
    // this was rolled.)
    let mut gap: HashSet<[u8; 36]> = HashSet::new();
    let mut gap_meter = Meter::new();
    let gap_from = u32::from(a.base_chunk) + 1;
    for chunk in gap_from..u32::from(a.from) {
        collect_creations(
            &a.immutable,
            chunk as u16,
            &mut gap,
            &mut gap_meter,
            &mut BlockPlan::default(),
            &mut DecodeStats::default(),
            &mut Vec::new(),
        )?;
    }
    if !gap.is_empty() {
        println!(
            "  gap pass   chunks {gap_from}..{} · {} creations collected to classify the miss \
             bucket (this is the set a delta chain would hold)",
            a.from,
            gap.len()
        );
    }

    let (mut all_inputs, mut all_hits, mut all_same, mut all_gap, mut all_uncreated) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    for chunk in a.from..=a.to {
        let path = chunk_path(&a.immutable, chunk);
        let bytes = meter
            .observe(Op::ChunkRead, || std::fs::read(&path))
            .with_context(|| format!("reading {}", path.display()))?;

        // One pass, because an output is always created before it is spent — even
        // inside a block. So creations seen so far are exactly the creations that
        // could have been spent so far, and no second pass is needed to attribute a
        // miss to this chunk.
        let mut created: HashSet<[u8; 36]> = HashSet::new();
        let (mut inputs, mut hits, mut same, mut gap_hits, mut uncreated) =
            (0u64, 0u64, 0u64, 0u64, 0u64);
        let mut at = 0usize;
        let mut skipped = 0u64;
        let mut plan = BlockPlan::default();
        let mut stats = DecodeStats::default();
        let mut hashes: Vec<Hash<32>> = Vec::new();
        let t = Instant::now();
        while at < bytes.len() {
            let (decoded, len) =
                meter.observe(Op::BlockDecode, || decode_block_at(&bytes[at..]))?;
            if len == 0 {
                bail!("chunk {chunk}: a block decoded to zero bytes at offset {at}");
            }
            at += len;
            let Some(blk) = decoded.shelley() else {
                // Byron and the epoch boundary. They are handled by the fold's own
                // fallback path; here they are counted so a run over a Byron-era
                // range cannot look complete when it is not.
                skipped += 1;
                continue;
            };
            meter.observe(Op::BlockApply, || {
                plan_block_primitives(blk, chunk, &mut plan, &mut stats, &mut hashes, None)
            })?;
            for (oref, _) in &plan.creates {
                created.insert(outref_key(oref));
            }
            for (hash, index) in &plan.spends {
                inputs += 1;
                let key = outref_key(&(*hash, *index));
                let found = meter.observe(Op::BookUtxo, || {
                    base.utxo(hash.as_ref().try_into().expect("32"), *index)
                })?;
                match found {
                    Some(_) => hits += 1,
                    None if created.contains(&key) => same += 1,
                    None if gap.contains(&key) => gap_hits += 1,
                    None => uncreated += 1,
                }
            }
        }
        let nanos = t.elapsed().as_nanos();
        if skipped > 0 {
            println!("  (chunk {chunk}: {skipped} blocks fell back and were not counted)");
        }
        println!(
            "  {:<8} {:>6} {:>10} {:>9.2}% {:>10.2}% {:>10.2}% {:>11} {:>9}",
            chunk,
            i32::from(chunk) - i32::from(a.base_chunk),
            inputs,
            100.0 * hits as f64 / inputs.max(1) as f64,
            100.0 * same as f64 / inputs.max(1) as f64,
            100.0 * gap_hits as f64 / inputs.max(1) as f64,
            uncreated,
            nanos / u128::from(inputs.max(1)),
        );
        all_inputs += inputs;
        all_hits += hits;
        all_same += same;
        all_gap += gap_hits;
        all_uncreated += uncreated;

        // This chunk's creations become gap for the chunk after it: created after
        // the boundary, and no longer "this chunk" once we move on.
        gap.extend(created);
    }

    if all_hits + all_same + all_gap + all_uncreated != all_inputs {
        bail!(
            "the classification does not partition: {} hits + {} same-chunk + {} gap + {} \
             uncreated != {} inputs",
            all_hits,
            all_same,
            all_gap,
            all_uncreated,
            all_inputs
        );
    }
    let pct = |n: u64| 100.0 * n as f64 / all_inputs.max(1) as f64;
    println!();
    println!(
        "  total      {all_inputs} inputs in chunks {}..{}",
        a.from, a.to
    );
    println!(
        "             {:.2}% from the book's base · {:.2}% from their own chunk · {:.2}% from the \
         gap ({}) · {} never created in the log",
        pct(all_hits),
        pct(all_same),
        pct(all_gap),
        all_gap,
        all_uncreated
    );
    if all_hits + all_same + all_gap == all_inputs {
        println!(
            "             base + chunk + gap is the whole answer: every input resolves without an \
             archive"
        );
    }
    println!();
    print!("{}", meter.table(&run));
    let o = meter.get(Op::BookUtxo);
    println!(
        "  book_utxo  {} mean · {} p99 over {} lookups ({:.1}% hit rate)",
        human(o.mean_nanos()),
        human(o.p99()),
        o.count,
        pct(all_hits),
    );
    Ok(())
}

/// Follow chunks past a published book's boundary, holding everything created
/// since it in memory.
///
/// This is the other half of `resolve-inputs`. That command *classifies* a
/// chunk's inputs into four buckets and shows that `base + same-chunk + gap` is
/// the whole answer; this one **builds the gap and prices it**, so "can mitos
/// answer a module about a transaction that just landed" stops being a design
/// question and becomes a number.
///
/// The delta is one structure with three parts, and each is there because a
/// different question needs it (see `delta.rs`):
///
/// - the **rows** created since the boundary — an input is answered with the
///   output it spent, not with a yes;
/// - a **spent bit per creation** — a follower's live set is `base ∪ delta −
///   spent`, and a module asking for an address must not be handed a spent row;
/// - a **spent bit per base row** — the book is a snapshot, so it does not know
///   what the followed chunks spent, and that bitmap is **1.36 MB** over mainnet.
///
/// # What the numbers mean, and what they do not
///
/// `base`/`delta`/`uncreated` are a partition of the inputs by construction —
/// every spend is handed to the delta once and returns exactly one tier — so the
/// useful checks are the cross-checks against the walk: the delta must have seen
/// every spend the planner counted, and hold every output it counted.
///
/// A base folded from a **window** rather than from genesis makes `uncreated`
/// meaningless (everything created before the window is invisible to it), which
/// is why `--window-base` records that in the run context. The delta's own size
/// and cost are exact either way — they do not depend on the base's completeness
/// — and that is what this command is for.
fn follow(a: FollowArgs) -> Result<()> {
    let base = live_book::Base::open(&a.book)?;
    let mut delta = Delta::new(&base)?;
    let from = a.from.unwrap_or_else(|| a.base_chunk.saturating_add(1));
    let to = from.saturating_add(a.chunks.saturating_sub(1));
    let mut meter = Meter::new();
    let run = Run::new("follow")
        .target(a.book.display().to_string())
        .key("rows", base.rows())
        // Which base these figures are about. `--base-chunk` is the operator's
        // claim; this is the artifact's own account of itself, so two runs can be
        // compared without trusting that the same flag meant the same thing.
        .key("base", delta.base_fingerprint().to_string())
        .key("base_slot", base.meta().base_slot.unwrap_or_default())
        .key("base_chunk", a.base_chunk)
        .key("window_base", a.window_base)
        .key("sink", !a.bare)
        .key("from", from)
        .key("to", to);

    println!("delta follow — {} + chunks {from}..={to}", a.book.display());
    if a.bare {
        println!(
            "  control    NO SINK — the same chunks and the same walk, so the difference from a \
             delta run is what the delta costs"
        );
    }
    println!(
        "  base       {} live rows · {} policies · boundary chunk {}",
        base.rows(),
        base.policies().len(),
        a.base_chunk,
    );
    match base.meta().base_slot {
        Some(slot) => println!("             {} @ slot {slot}", delta.base_fingerprint()),
        // Not fatal: `follow` measures what a delta costs, and that does not
        // depend on the base knowing where it sits. But every figure below is
        // about a gap whose start is `--base-chunk` and nothing else, so say so.
        None => println!(
            "             {} — ⚠️ no `base_slot`, so `--base-chunk {}` is unchecked \
             (`fold-probe stamp-boundary` fills it in)",
            delta.base_fingerprint(),
            a.base_chunk
        ),
    }
    if a.window_base {
        println!(
            "             FOLDED FROM A WINDOW: anything created before it is invisible, so \
             `uncreated` is not the chain's number"
        );
    }
    if !base.policies().is_empty() {
        // Not the delta's own bytes: a production delta is written by the process
        // that wrote the base, which holds this map already. A *reader* has to
        // rebuild it, and that is worth seeing before it is designed away.
        println!(
            "             + {} to rebuild policy → id, which the artifact publishes only the \
             other way round",
            human_bytes(delta.policy_map_bytes() as u64),
        );
    }

    let mut chunk_buf: Vec<u8> = Vec::new();
    let mut hashes: Vec<Hash<32>> = Vec::new();
    let mut plan = BlockPlan::default();
    let mut stats = DecodeStats::default();
    let started = Instant::now();

    // The smallest non-zero step the clock can report — which is what bounds how
    // much a per-observation figure can be trusted, and is not the same question as
    // what one observation costs.
    //
    // ⚠ Both halves are load-bearing, and each was found the hard way. Written
    // plainly, `Instant::now().elapsed()` folds to `Duration::ZERO` — it has no
    // modelled side effects, so the measurement silently becomes a constant. And
    // read through `black_box`, the **minimum is still 0** on this box: the ARM
    // `arch_sys_counter` ticks at tens of nanoseconds, so two consecutive reads
    // inside one tick are equal. Only the smallest non-zero step is real.
    let clock_tick_nanos = {
        let mut tick = u64::MAX;
        let mut prev = std::hint::black_box(Instant::now());
        for _ in 0..1_000_000 {
            let now = std::hint::black_box(Instant::now());
            let d = now.saturating_duration_since(prev).as_nanos() as u64;
            if d > 0 {
                tick = tick.min(d);
            }
            prev = now;
        }
        tick
    };

    println!();
    println!(
        "  {:<7} {:>9} {:>9} {:>9} {:>8} {:>8} {:>8} {:>10} {:>10}",
        "chunk", "creates", "delta", "delta MB", "base", "delta", "uncred", "ns/input", "ns/create"
    );

    let (mut all_inputs, mut all_base, mut all_delta, mut all_uncreated) = (0u64, 0u64, 0u64, 0u64);
    let mut bytes_read = 0u64;
    for chunk in from..=to {
        let path = chunk_path(&a.immutable, chunk);
        meter
            .observe(Op::ChunkRead, || read_chunk_into(&mut chunk_buf, &path))
            .with_context(|| format!("reading {}", path.display()))?;
        bytes_read += chunk_buf.len() as u64;
        let bytes = chunk_buf.as_slice();

        let before = delta.stat();
        let t = Instant::now();
        {
            // The delta is the sink for this chunk's walk — unless this run is the
            // control, in which case the walk is identical and writes nowhere.
            let mut sink = if a.bare {
                None
            } else {
                Some(Sink::Delta(&mut delta))
            };
            let mut at = 0usize;
            while at < bytes.len() {
                let (decoded, len) =
                    meter.observe(Op::BlockDecode, || decode_block_at(&bytes[at..]))?;
                if len == 0 {
                    bail!("chunk {chunk}: a block decoded to zero bytes at offset {at}");
                }
                at += len;
                let Some(blk) = decoded.shelley() else {
                    // Byron and the epoch boundary have no primitives to walk, so
                    // there is nothing to feed the delta from. Counted rather than
                    // dropped quietly: those blocks' outputs are *not* in the delta.
                    stats.fallback_blocks += 1;
                    continue;
                };
                meter.observe(Op::BlockApply, || {
                    plan_block_primitives(
                        blk,
                        chunk,
                        &mut plan,
                        &mut stats,
                        &mut hashes,
                        sink.as_mut(),
                    )
                })?;
            }
        }
        let secs = t.elapsed().as_secs_f64();
        let after = delta.stat();
        let acc = delta.accounted();

        let creates = after.created - before.created;
        let (b, d, u) = (
            after.spend_base - before.spend_base,
            after.spend_delta - before.spend_delta,
            after.spend_uncreated - before.spend_uncreated,
        );
        let inputs = b + d + u;
        let probes = after.lookup_nanos - before.lookup_nanos;
        let creating = after.create_nanos - before.create_nanos;
        println!(
            "  {:<7} {:>9} {:>9} {:>9.1} {:>7.1}% {:>7.1}% {:>8} {:>10} {:>10}",
            chunk,
            creates,
            delta.stat().created,
            acc.total() as f64 / (1024.0 * 1024.0),
            100.0 * b as f64 / inputs.max(1) as f64,
            100.0 * d as f64 / inputs.max(1) as f64,
            u,
            probes / inputs.max(1),
            creating / creates.max(1),
        );
        let _ = secs;
        all_inputs += inputs;
        all_base += b;
        all_delta += d;
        all_uncreated += u;
    }

    // Not a partition check — that holds by construction. These are the two ways
    // the sink could silently stop receiving what the walk counted, so they are
    // checks on the delta rather than on the walk.
    if !a.bare {
        if all_inputs != stats.inputs {
            bail!(
                "the delta classified {all_inputs} spends; the walk counted {}",
                stats.inputs
            );
        }
        if delta.stat().created != stats.outputs {
            bail!(
                "the delta holds {} rows; the walk counted {} outputs",
                delta.stat().created,
                stats.outputs
            );
        }
    }

    let st = delta.stat();
    let acc = delta.accounted();
    let secs = started.elapsed().as_secs_f64();
    let pct = |n: u64| 100.0 * n as f64 / all_inputs.max(1) as f64;
    println!();
    println!(
        "  total      {all_inputs} inputs in chunks {from}..={to} · {:.1} s · {:.1} MiB/s",
        secs,
        bytes_read as f64 / (1024.0 * 1024.0) / secs.max(f64::MIN_POSITIVE),
    );
    println!(
        "             {:.2}% from the base · {:.2}% from the delta · {} never created in the log",
        pct(all_base),
        pct(all_delta),
        all_uncreated,
    );
    println!(
        "             {} blocks · {} txs · {} outputs · {} blocks skipped (no primitives)",
        stats.blocks, stats.txs, stats.outputs, stats.fallback_blocks
    );
    println!();
    println!(
        "  delta      {} creations · {} still live · {} spent",
        st.created,
        delta.live(),
        st.delta_rows_spent
    );
    println!(
        "             rows {} · probes {} · spent bits {} · policy tail {} = {}",
        human_bytes(acc.arena as u64),
        human_bytes(acc.probes as u64),
        human_bytes(acc.spent_bits as u64),
        human_bytes(acc.policy_tail as u64),
        human_bytes(acc.total() as u64),
    );
    println!(
        "             {} B per creation over {} · {} base rows spent ({} per chunk) · base bitmap {}",
        acc.total() / st.created.max(1) as usize,
        st.created,
        st.base_rows_spent,
        st.base_rows_spent / u64::from(to - from + 1),
        human_bytes((base.rows() as usize).div_ceil(8) as u64),
    );
    println!(
        "             {} policies the base never saw; their ids continue from its {}",
        st.policies_new,
        base.policies().len(),
    );
    println!();
    println!(
        "  probe      {} ns mean per input, over {} delta probes",
        st.lookup_nanos / all_inputs.max(1),
        all_inputs,
    );
    println!(
        "  create     {} ns mean per creation, over {} creations",
        st.create_nanos / st.created.max(1),
        st.created,
    );
    println!(
        "  clock      {clock_tick_nanos} ns is the smallest step this box's `arch_sys_counter` \
         reports, so every per-op figure above carries up to that much quantisation; the timer's \
         own cost is below it"
    );
    println!();
    print!("{}", meter.table(&run));
    if let Some(path) = &a.jsonl {
        std::fs::write(path, meter.jsonl(&run))
            .with_context(|| format!("writing {}", path.display()))?;
        println!("  jsonl      {}", path.display());
    }
    if a.sample > 0 && !a.bare {
        sample_delta(&base, &delta, a.sample)?;
    }
    Ok(())
}

/// Read a sample of the delta back the way a consumer would.
///
/// The point of the delta being *the same encoding as the book* is that one
/// consumer reads across both, so this walks a sample of the creations and asks
/// the delta for each one by outref — the accessor a module uses — and then
/// resolves every asset id in the row through the base's `policy.tbl` plus the
/// delta's own tail. An id that resolves through neither is a row no consumer
/// could read, which is the failure the shared encoding is supposed to make
/// impossible.
fn sample_delta(base: &live_book::Base, delta: &Delta<'_>, sample: u64) -> Result<()> {
    let base_len = base.policies().len();
    let n = delta.stat().created;
    if n == 0 {
        println!("  sample     the delta is empty");
        return Ok(());
    }
    let step = (n / sample.max(1)).max(1);
    let (mut read, mut live, mut bad_ref, mut bad_policy, mut assets) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    let started = Instant::now();
    let mut ordinal = 0u64;
    while ordinal < n {
        let Some(row) = delta.row(ordinal as u32).ok() else {
            bad_ref += 1;
            ordinal += step;
            continue;
        };
        read += 1;
        if delta.is_live(ordinal as u32) {
            live += 1;
        }
        let (tx, index) = row.outref();
        match delta.utxo((Hash::from(tx), index)) {
            Some(Ok(again)) if again.outref() == (tx, index) => {}
            _ => bad_ref += 1,
        }
        for a in row.assets() {
            assets += 1;
            let id = a.policy as usize;
            let readable = if id < base_len {
                base.policies().get(a.policy).is_ok_and(|p| p.is_some())
            } else {
                id - base_len < delta.policy_tail().len()
            };
            if !readable {
                bad_policy += 1;
            }
        }
        ordinal += step;
    }
    let nanos = started.elapsed().as_nanos();
    println!(
        "  sample     {read} rows read back from {n} creations · {live} still live · {assets} asset \
         records"
    );
    println!(
        "             {bad_ref} refs that did not resolve · {bad_policy} asset ids unreadable"
    );
    // A delta *without* its own orderings can only answer `by_address` by reading
    // every row, so this is the floor for the simplest enumeration a module could
    // be given — and the number that argues for carrying the permutation instead.
    println!(
        "             {} ns per row · {} for a full scan of {n} rows ({} row per lookup)",
        nanos / read.max(1) as u128,
        human((nanos / read.max(1) as u128 * n as u128) as u64),
        n / read.max(1),
    );
    if bad_ref + bad_policy > 0 {
        bail!("the delta does not read back: {bad_ref} refs and {bad_policy} asset ids");
    }
    Ok(())
}

/// A chunk's created outputs, as membership keys.
///
/// Walks a chunk and keeps only the creations — the same decode as resolution, but
/// with the book untouched. Used for the gap pass, which has to know what was
/// created after the base boundary without resolving anything against it.
fn collect_creations(
    immutable: &Path,
    chunk: u16,
    out: &mut HashSet<[u8; 36]>,
    meter: &mut Meter,
    plan: &mut BlockPlan,
    stats: &mut DecodeStats,
    hashes: &mut Vec<Hash<32>>,
) -> Result<()> {
    let path = chunk_path(immutable, chunk);
    let bytes = meter
        .observe(Op::ChunkRead, || std::fs::read(&path))
        .with_context(|| format!("reading {} (gap pass)", path.display()))?;
    let mut at = 0usize;
    while at < bytes.len() {
        let (decoded, len) = meter.observe(Op::BlockDecode, || decode_block_at(&bytes[at..]))?;
        if len == 0 {
            bail!("chunk {chunk}: a block decoded to zero bytes at offset {at} (gap pass)");
        }
        at += len;
        let Some(blk) = decoded.shelley() else {
            continue;
        };
        meter.observe(Op::BlockApply, || {
            plan_block_primitives(blk, chunk, plan, stats, hashes, None)
        })?;
        for (oref, _) in &plan.creates {
            out.insert(outref_key(oref));
        }
    }
    Ok(())
}

/// The 36-byte membership key for an outref: the hash then the index, packed.
fn outref_key(oref: &OutRef) -> [u8; 36] {
    let mut key = [0u8; 36];
    key[..32].copy_from_slice(oref.0.as_ref());
    key[32..].copy_from_slice(&oref.1.to_le_bytes());
    key
}

fn hex_of(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit(u32::from(b >> 4), 16).expect("nibble"));
        s.push(char::from_digit(u32::from(b & 0xF), 16).expect("nibble"));
    }
    s
}

/// Hex to bytes.
///
/// Hand-rolled, like `crc32_iso_hdlc`: twenty lines against a dependency this
/// probe does not otherwise need, and `hex_of` above is already here so the
/// round trip is testable in the same place.
fn parse_hex(s: &str, what: &str) -> Result<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        bail!("{what} `{s}` has an odd number of hex digits");
    }
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..b.len()).step_by(2) {
        let hi = (b[i] as char)
            .to_digit(16)
            .with_context(|| format!("{what} `{s}` is not hex"))?;
        let lo = (b[i + 1] as char)
            .to_digit(16)
            .with_context(|| format!("{what} `{s}` is not hex"))?;
        out.push(((hi << 4) | lo) as u8);
    }
    Ok(out)
}

fn parse_28(s: &str, what: &str) -> Result<[u8; 28]> {
    let bytes = parse_hex(s, what)?;
    bytes.as_slice().try_into().map_err(|_| {
        anyhow::anyhow!(
            "{what} must be 28 bytes (56 hex digits), got {}",
            bytes.len()
        )
    })
}

/// `<64 hex>`, optionally `#<index>` — the shape a tx id and an outref are
/// usually quoted in.
fn parse_outref(s: &str) -> Result<([u8; 32], u32)> {
    let (hash, index) = match s.split_once('#') {
        Some((h, i)) => (
            h,
            i.parse::<u32>()
                .with_context(|| format!("output index `{i}` is not a number"))?,
        ),
        None => (s, 0),
    };
    let bytes = parse_hex(hash, "--outref")?;
    let h: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
        anyhow::anyhow!(
            "--outref must be 32 bytes (64 hex digits), got {}",
            bytes.len()
        )
    })?;
    Ok((h, index))
}

/// An address as a human has it: bech32, or the raw bytes in hex.
///
/// bech32 first because that is the form the question arrives in — the wasm
/// surface takes a string — and hex second so an address whose encoding is in
/// doubt can be asked about exactly.
fn parse_address(s: &str) -> Result<Vec<u8>> {
    match pallas_addresses::Address::from_bech32(s) {
        Ok(a) => Ok(a.to_vec()),
        Err(e) => match parse_hex(s, "--address") {
            Ok(bytes) => Ok(bytes),
            Err(_) => bail!("--address `{s}` is neither bech32 ({e:?}) nor hex"),
        },
    }
}

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
    use crate::delta::Tier;

    /// The delta is the gap, and the gap is exactly what the book cannot answer.
    ///
    /// Four claims, and each is a way the structure could be wrong rather than an
    /// echo of the implementation: an output created after the boundary resolves
    /// from the delta; a spend does **not** remove the row, because a consumer
    /// analysing a landed transaction is asking about that transaction's inputs;
    /// the spend is nevertheless visible as liveness, which is what a module
    /// enumerating an address needs; and a spend of a *base* row is recorded
    /// against the base's own bitmap, once.
    #[test]
    fn a_delta_answers_past_the_boundary_and_records_what_it_spends() {
        let dir = std::env::temp_dir().join(format!("fold-probe-delta-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        // A base of three live rows. Two assets on one policy so the delta's
        // interning has something the base already knows, and a real base address
        // (57 bytes, header 0x01) so the rows are keyed the way the artifact keys
        // them.
        let pol_a = [0xAAu8; 28];
        let pol_b = [0xBBu8; 28];
        let address = |i: u32| -> Vec<u8> {
            let mut a = vec![0x01u8];
            a.extend_from_slice(&[i as u8; 28]);
            a.extend_from_slice(&[0xCD; 28]);
            a
        };
        let mut b = Book::create(&dir, 1 << 20).expect("create");
        for i in 0..3u32 {
            let v: &[u8] = b"one";
            let pol: &[u8] = &pol_a;
            b.create_output(
                (Hash::<32>::from([i as u8; 32]), 0),
                1,
                1_000,
                &address(i),
                Datum::None,
                [(pol, v, 1u64)].into_iter(),
            )
            .expect("create_output");
        }
        b.finish().expect("finish");

        let base = live_book::Base::open(&dir).expect("open");
        assert_eq!(base.rows(), 3);
        let mut delta = Delta::new(&base).expect("delta");

        // A base row: answered, and marked spent once rather than per reference.
        let base_ref = (Hash::<32>::from([1u8; 32]), 0);
        assert_eq!(delta.spend(base_ref), Tier::Base);
        assert_eq!(delta.spend(base_ref), Tier::Base);
        assert_eq!(
            delta.stat().base_rows_spent,
            1,
            "a row spent twice in one chunk is one spent row"
        );

        // Never created: Byron's genesis distribution and redeem-style inputs.
        assert_eq!(
            delta.spend((Hash::<32>::from([0xFFu8; 32]), 0)),
            Tier::Uncreated
        );

        // Created after the boundary: only the delta can answer this one, and a
        // second creation on a policy the base never saw must take an id that
        // continues from the base's own table.
        let new_ref = (Hash::<32>::from([9u8; 32]), 0);
        let name: &[u8] = b"nine";
        let pol: &[u8] = &pol_b;
        delta
            .create_output(
                new_ref,
                1,
                7,
                &address(9),
                Datum::None,
                [(pol, name, 5u64)].into_iter(),
            )
            .expect("create_output");
        assert_eq!(delta.spend(new_ref), Tier::Delta);
        assert_eq!(delta.stat().created, 1);
        assert!(
            !delta.is_live(0),
            "the delta spent the only creation it holds"
        );
        assert_eq!(delta.live(), 0);

        // Spending does not unmake the row: the transaction being analysed names
        // this output as an input, and it has to be readable.
        let row = delta
            .utxo(new_ref)
            .expect("a spent row is still a row")
            .expect("parses");
        assert_eq!(row.outref(), ([9u8; 32], 0));
        assert_eq!(row.lovelace(), 7);
        assert_eq!(row.address(), address(9).as_slice());
        let asset = row.assets().next().expect("one asset");
        assert_eq!(asset.name, name);
        assert_eq!(asset.quantity, 5);
        assert_eq!(
            asset.policy as usize,
            base.policies().len(),
            "a policy the base never saw continues from the base's own count"
        );
        assert_eq!(delta.policy_tail(), &[pol_b]);
        assert_eq!(
            delta.policy_tail().len(),
            delta.stat().policies_new as usize
        );
    }

    /// The writer and the reader have to agree — and the reader has to notice when
    /// the bytes have been meddled with. Both halves in one test, because the
    /// second is only meaningful once the first passes: a verifier that accepts
    /// everything would sail through the corrupt case too.
    #[test]
    fn a_book_round_trips_and_a_corrupted_entry_is_caught() {
        let dir = std::env::temp_dir().join(format!("fold-probe-book-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let mut b = Book::create(&dir, 1 << 20).expect("create");
        // Two assets on the same policy in one row, plus a second policy, so the
        // (row, policy) dedup and the ordering are both exercised rather than
        // assumed. The address is a real base address (57 bytes, header 0x01) whose
        // payment credential varies per row — otherwise `by_addr`'s confirm would
        // have nothing to reject, and a row whose address is three bytes long
        // would make every row unkeyed.
        let pol_a = [0xAAu8; 28];
        let pol_b = [0xBBu8; 28];
        let address = |i: u32| -> Vec<u8> {
            let mut a = vec![0x01u8];
            a.extend_from_slice(&[i as u8; 28]);
            a.extend_from_slice(&[0xCD; 28]);
            a
        };
        for i in 0..64u32 {
            let h = Hash::<32>::from([i as u8; 32]);
            let v1: &[u8] = b"one";
            let v2: &[u8] = b"two";
            let assets: Vec<(&[u8], &[u8], u64)> = if i % 2 == 0 {
                vec![(&pol_a, v1, 1), (&pol_a, v2, 2), (&pol_b, v1, 3)]
            } else {
                vec![(&pol_b, v2, 4)]
            };
            b.create_output(
                (h, i % 3),
                1,
                1_000 * u64::from(i),
                &address(i),
                Datum::None,
                assets.into_iter(),
            )
            .expect("create_output");
        }
        let cost = b.finish().expect("finish");
        assert_eq!(cost.rows_live, 64);
        assert_eq!(
            cost.by_addr_entries, 64,
            "one entry per row, and no row may be unkeyed"
        );
        assert_eq!(
            (
                cost.addr_payment,
                cost.addr_stake,
                cost.addr_bootstrap,
                cost.addr_unkeyed
            ),
            (64, 0, 0, 0),
            "every row here is a type-0 base address"
        );
        // 32 rows carry both policies, 32 carry one: 96 (row, policy) pairs, and
        // *not* 160 — the two assets on `pol_a` in the same row are one entry.
        assert_eq!(
            cost.by_policy_entries, 96,
            "one entry per (row, policy), not per asset name"
        );

        let ok = book::verify(&dir, 0).expect("verify");
        assert_eq!(ok.failures(), 0, "a freshly built book must verify: {ok:?}");
        assert_eq!(ok.outref.sampled, 64);
        assert_eq!(ok.policy.sampled, 96);
        assert_eq!(ok.addr.sampled, 64);
        assert_eq!(
            ok.addr.entries, ok.outref.entries,
            "`by_outref` and `by_addr` must be permutations of the same row set"
        );
        assert_eq!(
            ok.addr.entries, 64,
            "`by_addr` must hold one entry per live row — `book.meta` and the file agreeing \
             on a smaller number would be the index quietly omitting rows"
        );

        // Phase 2a: point a `by_addr` entry at a row whose credential is different
        // — the analogous targeted test, and the only check that can see it, since
        // the prefix is untouched and the directory is still a partition.
        let addr_path = dir.join("by_addr.idx");
        let mut abytes = std::fs::read(&addr_path).expect("read by_addr.idx");
        let original = abytes[12 + 8..12 + 12].to_vec();
        // Row 1 begins at row 0's length: 56 header + 57 address + 3 assets × 16.
        assert_eq!(
            original,
            0u32.to_le_bytes(),
            "entry 0 is the row-0 credential"
        );
        abytes[12 + 8..12 + 12].copy_from_slice(&161u32.to_le_bytes());
        std::fs::write(&addr_path, &abytes).expect("write by_addr.idx");
        let bad_addr = book::verify(&dir, 0).expect("verify after credential swap");
        assert!(
            bad_addr.addr.mismatches > 0,
            "an entry whose row carries a different credential must be caught: {bad_addr:?}"
        );
        abytes[12 + 8..12 + 12].copy_from_slice(&original);
        std::fs::write(&addr_path, &abytes).expect("restore by_addr.idx");

        // Phase 2: point a `by_policy` entry at a row that does not hold that
        // policy. This is the targeted test of the confirm — the prefix is
        // untouched, so canonical order and the directory are both still valid and
        // only the row-corroboration check can see it. Entry 0 is a `pol_a` entry
        // (prefix sorted ascending, `0xAA…` < `0xBB…`); row 1 is an odd row, and
        // odd rows hold only `pol_b`. Row 1's offset is row 0's length:
        // 56 header + 3 address + 3 assets × (4 id + 1 name-len + 3 name + 8 qty).
        let pol_path = dir.join("by_policy.idx");
        let mut pbytes = std::fs::read(&pol_path).expect("read by_policy.idx");
        let original_row = pbytes[12 + 8..12 + 12].to_vec();
        pbytes[12 + 8..12 + 12].copy_from_slice(&161u32.to_le_bytes());
        std::fs::write(&pol_path, &pbytes).expect("write by_policy.idx");
        let bad_pol = book::verify(&dir, 0).expect("verify after row swap");
        assert!(
            bad_pol.policy.mismatches > 0,
            "an entry naming a policy its row does not hold must be caught: {bad_pol:?}"
        );
        pbytes[12 + 8..12 + 12].copy_from_slice(&original_row);
        std::fs::write(&pol_path, &pbytes).expect("restore by_policy.idx");

        // Phase 3: the outref prefix, as before.
        let path = dir.join("by_outref.idx");
        let mut bytes = std::fs::read(&path).expect("read by_outref.idx");
        bytes[12] ^= 0xFF;
        std::fs::write(&path, &bytes).expect("write by_outref.idx");

        let bad = book::verify(&dir, 0).expect("verify after corruption");
        assert!(
            bad.failures() > 0,
            "a corrupted prefix must be caught, not reported as {bad:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `reindex` must be **equivalent to the builder** on a fresh artifact, and it
    /// must **repair a damaged one**. Both halves matter: the first is what lets a
    /// new ordering skip the chain walk, the second is what lets an existing
    /// artifact be fixed in place. Without the first, the reindex path would be a
    /// second implementation of the ordering, which is the thing it exists to avoid.
    #[test]
    fn reindex_reproduces_the_builder_and_repairs_a_directory() {
        let dir = std::env::temp_dir().join(format!("fold-probe-reindex-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let mut b = Book::create(&dir, 1 << 20).expect("create");
        let pol = [0xCDu8; 28];
        for i in 0..32u32 {
            let h = Hash::<32>::from([i as u8; 32]);
            let name: &[u8] = b"n";
            let assets: Vec<(&[u8], &[u8], u64)> = vec![(&pol, name, 1)];
            // A 29-byte enterprise address, so the shortest Shelley shape is also
            // exercised rather than only the 57-byte one.
            let mut addr = vec![0x61u8];
            addr.extend_from_slice(&[i as u8; 28]);
            b.create_output((h, i), 1, 5, &addr, Datum::None, assets.into_iter())
                .expect("create_output");
        }
        b.finish().expect("finish");

        let built_policy = std::fs::read(dir.join("by_policy.idx")).expect("read built");
        let built_addr = std::fs::read(dir.join("by_addr.idx")).expect("read built addr");
        let built_dir = std::fs::read(dir.join("dir.idx")).expect("read built dir");

        let rep = book::reindex(&dir).expect("reindex");
        assert_eq!(
            rep.policy_entries, 32,
            "one entry per row, all on one policy"
        );
        assert_eq!(
            rep.addr_entries, 32,
            "one entry per row, one credential each"
        );
        assert_eq!(
            std::fs::read(dir.join("by_policy.idx")).expect("read"),
            built_policy,
            "reindex must reproduce the builder byte for byte"
        );
        assert_eq!(
            std::fs::read(dir.join("by_addr.idx")).expect("read"),
            built_addr,
            "and reproduce `by_addr`"
        );
        assert_eq!(
            std::fs::read(dir.join("dir.idx")).expect("read"),
            built_dir,
            "and reproduce the outref directory"
        );

        // Damage the directory the way the old fill did — a bucket pointing past the
        // one that follows it — then require the repair to fix it. The addr directory
        // too, so the repair is exercised on an ordering that is not the first one.
        let mut d = std::fs::read(dir.join("dir.idx")).expect("read dir");
        d[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        std::fs::write(dir.join("dir.idx"), &d).expect("write dir");
        let mut ad = std::fs::read(dir.join("addr.dir.idx")).expect("read addr dir");
        ad[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        std::fs::write(dir.join("addr.dir.idx"), &ad).expect("write addr dir");
        assert!(
            book::verify(&dir, 0).expect("verify").failures() > 0,
            "a bucket pointing past its successor must fail the partition check"
        );

        // Strip the counts a repair records, so the repair has to put them back:
        // `verify` must not require the line (the file's own header is the count),
        // and `reindex` must not leave `book.meta` describing an artifact that no
        // longer exists.
        let meta_path = dir.join("book.meta");
        let meta = std::fs::read_to_string(&meta_path).expect("read meta");
        let stripped: String = meta
            .lines()
            .filter(|l| !l.starts_with("by_addr_") && !l.starts_with("addr_"))
            .map(|l| format!("{l}\n"))
            .collect();
        std::fs::write(&meta_path, &stripped).expect("write meta");

        book::reindex(&dir).expect("reindex again");
        let after = book::verify(&dir, 0).expect("verify after repair");
        assert_eq!(
            after.failures(),
            0,
            "the repair must satisfy the verifier: {after:?}"
        );
        let meta = std::fs::read_to_string(&meta_path).expect("read meta");
        assert!(
            meta.contains("by_addr_entries 32") && meta.contains("addr_payment 32"),
            "reindex must record what it wrote: {meta}"
        );
        assert_eq!(
            meta.matches("by_addr_entries").count(),
            1,
            "and running it again must not accumulate the line: {meta}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The standard check value for CRC-32/ISO-HDLC. Without this the check that
    /// catches a mis-decoded Byron address could itself be wrong — and a CRC test
    /// that is wrong in the safe direction fails nothing, which is the failure
    /// mode this whole line of investigation has been about.
    #[test]
    fn crc_matches_its_check_vector() {
        assert_eq!(crc32_iso_hdlc(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32_iso_hdlc(b""), 0);
        assert_eq!(crc32_iso_hdlc(b"a"), 0xE8B7_BE43);
        assert_eq!(crc32_iso_hdlc(b"abc"), 0x3524_41C2);
    }

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
