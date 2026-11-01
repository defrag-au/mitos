//! The native walker for the fold plane: a Mithril chunk window, decoded once, handed to
//! a [`mitos_fold::Registry`].
//!
//! `docs/design/FOLD_PLANE.md` is the spec. This crate is the second half of the seam the
//! contract crate describes:
//!
//! - [`mitos-fold`](mitos_fold) owns the contract — the aggregate trait, the registry, the
//!   artifact envelope. It is dependency-free and does no I/O.
//! - this crate owns the walk — chunk selection, block splitting, the pallas decode, and
//!   the [`DecodedBlock`] every aggregate observes. It is native and depends on the
//!   decoder, so it is deliberately a sibling rather than a feature of the contract.
//!
//! The walk is one pass. [`chain_decode::decode_block`] builds the owned block view once,
//! then each registered aggregate observes it; nothing re-reads a chunk per aggregate.
//!
//! # What this crate does not do
//!
//! No artifact writing, no publishing, no reader, no tip follow. [`walk`] returns the
//! finished artifacts (or whatever `A` the registry produces) and the [`Stamp`] they were
//! produced at. The writer and the publisher consume those.
//!
//! # The chunk window
//!
//! Mithril keeps appending to the highest-numbered immutable file until the next one
//! seals, so [`chunk_numbers`] excludes the newest file. A window passed to [`walk`] must
//! be non-empty and strictly ascending: a fold is a chain-order operation, and a silently
//! sorted or deduplicated window would hide a caller's mistake.

use std::fmt;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use chain_decode::DecodedBlock;
use mitos_fold::{Fingerprint, Finished, Registry, Stamp};
use pallas_codec::minicbor::Decoder;
use pallas_traverse::MultiEraBlock;

/// Everything the walker needs that is not the registry.
#[derive(Debug, Clone, Copy)]
pub struct WalkSpec<'a> {
    /// The Mithril immutable directory (holds `NNNNN.chunk`).
    pub immutable: &'a Path,
    /// The chunks to walk, in ascending order. See [`chunk_numbers`] for the rule that
    /// excludes the newest (still-growing) file.
    pub chunks: &'a [u32],
    /// The identity of the source, stamped into every artifact.
    ///
    /// Supplied rather than derived: a fingerprint that silently changes when a chunk is
    /// re-read would make two artifacts look unrelated, and one that silently stays the
    /// same after a different window would make them look paired when they are not.
    pub source: Fingerprint,
}

/// What one walk produced.
#[derive(Debug)]
pub struct Walked<A> {
    /// Blocks decoded and observed.
    pub blocks: u64,
    /// Transactions handed to the aggregates.
    pub txs: u64,
    /// The base the artifacts describe.
    pub stamp: Stamp,
    /// One artifact per registered aggregate, in registration order.
    pub artifacts: Vec<Finished<A>>,
}

/// The complete chunk numbers under `immutable`, newest excluded.
///
/// Mithril appends to the highest-numbered file until it seals, so the newest file is not
/// part of the settled log. This mirrors the rule every other consumer in the workspace
/// applies; a walk that included it would build an artifact from a moving target.
pub fn chunk_numbers(immutable: &Path) -> Result<Vec<u32>> {
    let names = std::fs::read_dir(immutable)
        .with_context(|| format!("reading {}", immutable.display()))?
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok());
    chunk_numbers_from(names)
}

fn chunk_numbers_from(names: impl IntoIterator<Item = String>) -> Result<Vec<u32>> {
    let mut numbers: Vec<u32> = names
        .into_iter()
        .filter_map(|name| name.strip_suffix(".chunk")?.parse().ok())
        .collect();
    numbers.sort_unstable();
    if numbers.len() < 2 {
        bail!("need at least 2 chunk files — the newest is still growing and is excluded");
    }
    numbers.pop();
    Ok(numbers)
}

/// The path of one chunk under `immutable`.
///
/// Generic over the number's width because the workspace has two: this crate walks `u32`
/// chunk numbers, while `live-book` and `tx-index` address a chunk as `u16` (it is a `u16`
/// coordinate in the book's row plan). Both widen losslessly, and a caller should not have
/// to convert at every site to use the one implementation of the file name.
pub fn chunk_path(immutable: &Path, chunk: impl Into<u32>) -> PathBuf {
    let chunk = chunk.into();
    immutable.join(format!("{chunk:05}.chunk"))
}

/// A leaner consumer of the same block.
///
/// The book is the first sink: it traverses the decoded `MultiEraBlock` itself and must
/// not pay for the owned [`DecodedBlock`] every aggregate shares. A sink observes the
/// block before the aggregates and must not retain the borrow — the chunk buffer is
/// reused for the next chunk.
pub trait BlockSink {
    /// Observe one block. `chunk` is the Mithril chunk it came from, for a consumer that
    /// records the walk's own coordinate.
    fn observe(&mut self, block: &MultiEraBlock<'_>, chunk: u32) -> Result<()>;
}

struct NoopSink;

impl BlockSink for NoopSink {
    fn observe(&mut self, _block: &MultiEraBlock<'_>, _chunk: u32) -> Result<()> {
        Ok(())
    }
}

/// Walk `spec.chunks`, observe every block with `registry`, and finish the round.
///
/// The registry is emptied by [`Registry::finish`](mitos_fold::Registry::finish) whether
/// the walk succeeds or fails, so a caller that retries starts a fresh registry.
pub fn walk<A>(spec: WalkSpec<'_>, registry: &mut Registry<DecodedBlock, A>) -> Result<Walked<A>> {
    walk_with_sink(spec, &mut NoopSink, registry)
}

/// Walk with a lean [`BlockSink`] beside the aggregate registry.
///
/// The block is decoded once. The sink sees the `MultiEraBlock` first — the book's lean
/// path — and then [`chain_decode::decode_block`] builds the owned view every aggregate
/// observes. A sink that fails stops the walk before the aggregates see that block.
pub fn walk_with_sink<A, S>(
    spec: WalkSpec<'_>,
    sink: &mut S,
    registry: &mut Registry<DecodedBlock, A>,
) -> Result<Walked<A>>
where
    S: BlockSink,
{
    validate_window(spec.chunks)?;

    let mut buffer = Vec::new();
    let mut blocks = 0u64;
    let mut txs = 0u64;
    let mut base: Option<(u64, [u8; 32])> = None;

    for &chunk in spec.chunks {
        let path = chunk_path(spec.immutable, chunk);
        read_chunk_into(&mut buffer, &path)
            .with_context(|| format!("reading chunk {chunk} from {}", path.display()))?;
        let raw_blocks =
            split_blocks(&buffer).with_context(|| format!("splitting chunk {chunk}"))?;

        for (index, raw) in raw_blocks.iter().enumerate() {
            let block = MultiEraBlock::decode(raw)
                .with_context(|| format!("decoding chunk {chunk} block {index}"))?;
            sink.observe(&block, chunk)
                .with_context(|| format!("chunk {chunk} block {index}"))?;
            let decoded = chain_decode::decode_block(&block);
            // The last block decoded is the base, whether or not its slot is the highest:
            // chunk order is chain order, and a fold's boundary is where the walk stopped.
            base = Some((decoded.slot, *decoded.hash));
            txs += decoded.txs.len() as u64;
            blocks += 1;
            registry
                .observe(&decoded)
                .with_context(|| format!("chunk {chunk} block {index}"))?;
        }
    }

    let Some((base_slot, base_hash)) = base else {
        bail!("the chunk window contained no blocks");
    };
    let stamp = Stamp::new(base_slot, Some(base_hash), spec.source);
    let artifacts = registry.finish(stamp)?;

    Ok(Walked {
        blocks,
        txs,
        stamp,
        artifacts,
    })
}

/// Split a chunk file into its top-level CBOR items — the block bytes.
///
/// The `.secondary` sidecar is deliberately not used: a walk that trusts a sidecar is
/// measuring a different decode path than the one that records offsets, and this walker is
/// the one that has to agree with the rest of the workspace.
pub fn split_blocks(bytes: &[u8]) -> Result<Vec<&[u8]>> {
    let mut blocks = Vec::new();
    let mut position = 0usize;
    while position < bytes.len() {
        let mut decoder = Decoder::new(&bytes[position..]);
        decoder
            .skip()
            .with_context(|| format!("splitting block at byte {position}"))?;
        let length = decoder.position();
        if length == 0 {
            bail!("zero-length CBOR item at byte {position}");
        }
        blocks.push(&bytes[position..position + length]);
        position += length;
    }
    Ok(blocks)
}

/// Read a chunk into a buffer that is reused across chunks.
///
/// `fs::read` allocates fresh each time, and a 5–40 MB allocation goes to `mmap`, so every
/// chunk re-faults its whole extent on first touch. Reusing the buffer keeps the mapping and
/// the pages, and the reads land in memory the process already has.
pub fn read_chunk_into(buffer: &mut Vec<u8>, path: &Path) -> Result<()> {
    let mut file = std::fs::File::open(path)?;
    let length = file.metadata().map(|meta| meta.len() as usize).unwrap_or(0);
    buffer.clear();
    if buffer.capacity() < length {
        buffer.reserve(length - buffer.len());
    }
    file.read_to_end(buffer)?;
    Ok(())
}

/// Where a window begins when the caller names no start chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowStrategy {
    /// From the oldest complete chunk forward. A **build**: a window that began anywhere
    /// else describes an artifact it did not build.
    FromHead,
    /// The newest `chunks`, ending at the last complete one. A **measurement**: the point is
    /// what a walk costs now, not what it cost at genesis.
    FromTail,
}

/// The chunks a command walks.
///
/// One function with two strategies rather than two functions with one rule each. *Where a
/// default window starts* is a decision the caller makes; everything else is the same either
/// way — and a named start is a chunk the store must actually have.
///
/// **`chunks == 0` is "to the end", not "from the beginning".** With a start the two
/// readings differ by hours, and a resume that silently re-walked the store would look like
/// it was working.
///
/// **A named `start` that is not a chunk is refused, never snapped forward.** Snapping lets a
/// typo describe a window the caller did not ask for, and for a build that is the difference
/// between an artifact stamped with its real base and one that silently claims another. Chunk
/// numbers are dense in practice, so the strict rule costs a caller nothing.
///
/// Generic over the chunk number so a caller keeps its own width: `u32` for
/// [`chunk_numbers`], `u16` for the book's row coordinate.
pub fn chunk_window<T>(
    all: &[T],
    start: Option<T>,
    chunks: usize,
    strategy: WindowStrategy,
) -> Result<Vec<T>>
where
    T: Copy + Ord + fmt::Display,
{
    let (Some(&first), Some(&last)) = (all.first(), all.last()) else {
        bail!("no complete chunks under the immutable directory");
    };

    let begin = match start {
        Some(start) => all
            .iter()
            .position(|chunk| *chunk == start)
            .ok_or_else(|| {
                anyhow!("start chunk {start} is not in the complete set ({first}..={last})")
            })?,
        // An open-ended tail window is the whole store: "the newest, all of them" has one
        // reading, and it is not "the empty set".
        None if strategy == WindowStrategy::FromHead || chunks == 0 => 0,
        None => all.len().saturating_sub(chunks),
    };
    let take = if chunks == 0 {
        all.len() - begin
    } else {
        chunks
    };
    let end = (begin + take).min(all.len());

    if begin == end {
        bail!("the window is empty: the store holds {first}..={last}");
    }
    Ok(all[begin..end].to_vec())
}

fn validate_window(chunks: &[u32]) -> Result<()> {
    if chunks.is_empty() {
        bail!("the chunk window is empty");
    }
    if chunks.windows(2).any(|pair| pair[0] >= pair[1]) {
        bail!("the chunk window must be strictly ascending");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../mitos-platform/tests/fixtures/186000000.block.cbor")
    }

    fn scratch(name: &str) -> PathBuf {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../.tmp")
            .join(format!("fold-walk-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        dir
    }

    fn stage_fixture(name: &str) -> (PathBuf, Vec<u32>) {
        let dir = scratch(name);
        std::fs::copy(fixture(), dir.join("00000.chunk")).expect("stage the fixture chunk");
        std::fs::write(dir.join("00001.chunk"), []).expect("stage the growing newest chunk");
        let chunks = chunk_numbers(&dir).expect("list chunks");
        assert_eq!(chunks, vec![0]);
        (dir, chunks)
    }

    struct Count {
        schema: mitos_fold::SchemaId,
        blocks: u64,
        txs: u64,
    }

    impl mitos_fold::Aggregate<DecodedBlock> for Count {
        type Artifact = (u64, u64);

        fn schema(&self) -> mitos_fold::SchemaId {
            self.schema
        }

        fn observe(&mut self, block: &DecodedBlock) -> mitos_fold::AggregateResult<()> {
            self.blocks += 1;
            self.txs += block.txs.len() as u64;
            Ok(())
        }

        fn finish(self: Box<Self>, _stamp: Stamp) -> mitos_fold::AggregateResult<(u64, u64)> {
            Ok((self.blocks, self.txs))
        }
    }

    fn registry() -> Registry<DecodedBlock, (u64, u64)> {
        let mut registry = Registry::new();
        registry
            .register(
                "count",
                Count {
                    schema: mitos_fold::SchemaId::new("block-count", 1),
                    blocks: 0,
                    txs: 0,
                },
            )
            .expect("register");
        registry
    }

    #[test]
    fn the_newest_chunk_is_excluded() {
        let numbers = chunk_numbers_from([
            "00002.chunk".to_owned(),
            "00000.chunk".to_owned(),
            "00001.chunk".to_owned(),
            "00002.secondary".to_owned(),
        ])
        .unwrap();
        assert_eq!(numbers, vec![0, 1]);
    }

    #[test]
    fn a_single_chunk_is_not_a_window() {
        let error = chunk_numbers_from(["00000.chunk".to_owned()]).unwrap_err();
        assert!(error.to_string().contains("at least 2"));
    }

    #[test]
    fn a_chunk_window_must_be_ascending() {
        assert!(validate_window(&[]).is_err());
        assert!(validate_window(&[1, 1]).is_err());
        assert!(validate_window(&[2, 1]).is_err());
        assert!(validate_window(&[1, 2, 3]).is_ok());
    }

    #[test]
    fn split_blocks_returns_one_slice_for_one_block() {
        let bytes = std::fs::read(fixture()).expect("read fixture");
        let blocks = split_blocks(&bytes).expect("split");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0], bytes.as_slice());
    }

    #[test]
    fn split_blocks_returns_one_slice_per_top_level_item() {
        let one = std::fs::read(fixture()).expect("read fixture");
        let mut two = one.clone();
        two.extend_from_slice(&one);

        let blocks = split_blocks(&two).expect("split");
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0], one.as_slice());
        assert_eq!(blocks[1], one.as_slice());
    }

    #[test]
    fn split_blocks_splits_two_one_byte_items() {
        // 0x01 (1) and 0x02 (2): the smallest items CBOR has.
        let blocks = split_blocks(&[0x01, 0x02]).expect("split");
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0], &[0x01]);
        assert_eq!(blocks[1], &[0x02]);
    }

    #[test]
    fn split_blocks_takes_a_multi_byte_item_whole() {
        // 0x42 0xAA 0xBB is a two-byte byte-string: one item, not three.
        let blocks = split_blocks(&[0x42, 0xAA, 0xBB]).expect("split");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].len(), 3);
    }

    // ---- the window ----

    fn store() -> Vec<u16> {
        (0..20).collect()
    }

    #[test]
    fn a_tail_window_takes_the_newest_chunks() {
        let window = chunk_window(&store(), None, 5, WindowStrategy::FromTail).expect("window");
        assert_eq!(window, vec![15, 16, 17, 18, 19]);
    }

    #[test]
    fn a_head_window_takes_from_the_oldest_chunk() {
        let window = chunk_window(&store(), None, 5, WindowStrategy::FromHead).expect("window");
        assert_eq!(window, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn a_window_from_a_start_chunk_is_contiguous() {
        let all: Vec<u16> = vec![10, 11, 12, 13, 14];
        let window = chunk_window(&all, Some(12), 2, WindowStrategy::FromHead).expect("window");
        assert_eq!(window, vec![12, 13]);
    }

    #[test]
    fn a_start_that_is_not_a_chunk_is_refused() {
        // Below the store, and — the case the strict rule exists for — a number between two
        // chunks, where snapping forward would walk a window nobody asked for.
        assert!(chunk_window(&[100, 101], Some(9), 1, WindowStrategy::FromHead).is_err());
        assert!(chunk_window(&[100, 102], Some(101), 1, WindowStrategy::FromHead).is_err());
        assert_eq!(
            chunk_window(&[100, 102], Some(102), 1, WindowStrategy::FromHead).expect("window"),
            vec![102]
        );
    }

    /// `--chunks 0` with a `--start` is "from there to the end" — the resume path. Read the
    /// other way it would re-walk the store, which is hours of work that looks like progress.
    #[test]
    fn an_open_ended_window_starts_where_it_is_told() {
        let all: Vec<u16> = vec![10, 11, 12, 13, 14, 15];
        assert_eq!(
            chunk_window(&all, Some(13), 0, WindowStrategy::FromHead).expect("to the end"),
            vec![13, 14, 15]
        );
        // "The newest, all of them" is the whole store, not the empty set — and the strategy
        // cannot change that, because the two readings coincide.
        assert_eq!(
            chunk_window(&all, None, 0, WindowStrategy::FromTail).expect("the store"),
            all
        );
        assert_eq!(
            chunk_window(&all, None, 0, WindowStrategy::FromHead).expect("the store"),
            all
        );
    }

    #[test]
    fn a_window_wider_than_the_store_is_the_store() {
        let all: Vec<u32> = vec![7, 8, 9];
        assert_eq!(
            chunk_window(&all, None, 99, WindowStrategy::FromTail).expect("window"),
            all
        );
    }

    #[test]
    fn an_empty_store_has_no_window() {
        let all: Vec<u32> = Vec::new();
        assert!(chunk_window(&all, None, 1, WindowStrategy::FromHead).is_err());
    }

    #[test]
    fn a_walk_decodes_the_fixture_and_finishes_the_registry() {
        let (dir, chunks) = stage_fixture("fixture");
        let source = Fingerprint::from_u128(0xfeed);
        let mut registry = registry();

        let walked = walk(
            WalkSpec {
                immutable: &dir,
                chunks: &chunks,
                source,
            },
            &mut registry,
        )
        .expect("walk");

        assert_eq!(walked.blocks, 1);
        assert!(walked.txs > 0);
        assert_eq!(walked.artifacts.len(), 1);
        assert_eq!(walked.artifacts[0].artifact.0, 1);
        assert_eq!(walked.artifacts[0].artifact.1, walked.txs);
        assert_eq!(walked.stamp.source, source);

        let fixture_bytes = std::fs::read(fixture()).expect("read fixture");
        let block = MultiEraBlock::decode(&fixture_bytes).expect("decode");
        assert_eq!(walked.stamp.base_slot, block.slot());
        assert_eq!(walked.stamp.base_hash, Some(*block.hash()));

        std::fs::remove_dir_all(dir).expect("clean scratch dir");
    }

    #[test]
    fn a_sink_sees_every_block_in_the_same_walk() {
        struct CountBlocks {
            blocks: u64,
        }

        impl BlockSink for CountBlocks {
            fn observe(&mut self, _block: &MultiEraBlock<'_>, _chunk: u32) -> Result<()> {
                self.blocks += 1;
                Ok(())
            }
        }

        let (dir, chunks) = stage_fixture("sink");
        let source = Fingerprint::from_u128(0xbeef);
        let mut sink = CountBlocks { blocks: 0 };
        let mut registry = registry();

        let walked = walk_with_sink(
            WalkSpec {
                immutable: &dir,
                chunks: &chunks,
                source,
            },
            &mut sink,
            &mut registry,
        )
        .expect("walk");

        assert_eq!(walked.blocks, 1);
        assert_eq!(sink.blocks, walked.blocks);
        assert_eq!(walked.artifacts[0].artifact.0, 1);

        std::fs::remove_dir_all(dir).expect("clean scratch dir");
    }

    #[test]
    fn a_sink_error_stops_the_walk_before_the_aggregates() {
        struct Refuse;

        impl BlockSink for Refuse {
            fn observe(&mut self, _block: &MultiEraBlock<'_>, _chunk: u32) -> Result<()> {
                anyhow::bail!("refused")
            }
        }

        let (dir, chunks) = stage_fixture("refuse");
        let source = Fingerprint::from_u128(0xcafe);
        let mut registry = registry();

        let error = walk_with_sink(
            WalkSpec {
                immutable: &dir,
                chunks: &chunks,
                source,
            },
            &mut Refuse,
            &mut registry,
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("refused"),
            "the sink's reason must survive the context: {error:#}"
        );

        std::fs::remove_dir_all(dir).expect("clean scratch dir");
    }
}
