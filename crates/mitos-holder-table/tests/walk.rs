//! One walk, two views: a lean sink sees each block, and the holder table observes the
//! owned [`DecodedBlock`](chain_decode::DecodedBlock) from the same decode.
//!
//! This is the structural pilot for the fold plane. The full book sink lives in
//! `fold-probe`; the counting sink here stands in for it, so the test proves the seam
//! without moving the builder.

use std::path::PathBuf;

use anyhow::Result;
use chain_decode::DecodedBlock;
use mitos_fold::{Fingerprint, Registry};
use mitos_fold_walk::{BlockSink, WalkSpec, chunk_numbers, walk_with_sink};
use mitos_holder_table::{HolderTable, HolderTableAggregate};
use pallas_traverse::MultiEraBlock;

struct CountBlocks {
    blocks: u64,
}

impl BlockSink for CountBlocks {
    fn observe(&mut self, _block: &MultiEraBlock<'_>, _chunk: u32) -> Result<()> {
        self.blocks += 1;
        Ok(())
    }
}

#[test]
fn one_walk_feeds_a_sink_and_the_holder_table() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../mitos-platform/tests/fixtures/186000000.block.cbor");
    let Ok(bytes) = std::fs::read(&fixture) else {
        eprintln!("skipping: fixture not present at {}", fixture.display());
        return;
    };
    let block = MultiEraBlock::decode(&bytes).expect("decode the fixture");
    let decoded = chain_decode::decode_block(&block);

    let Some(policy) = decoded
        .txs
        .iter()
        .flat_map(|tx| &tx.outputs)
        .flat_map(|output| &output.assets)
        .find(|asset| asset.policy.len() == 28)
        .and_then(|asset| asset.policy.as_slice().try_into().ok())
    else {
        eprintln!("skipping: fixture has no asset policy");
        return;
    };

    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.tmp")
        .join(format!("holder-table-walk-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    std::fs::copy(&fixture, dir.join("00000.chunk")).expect("stage the fixture chunk");
    std::fs::write(dir.join("00001.chunk"), b"").expect("stage the growing newest chunk");

    let chunks = chunk_numbers(&dir).expect("list chunks");
    assert_eq!(chunks, vec![0]);

    let mut sink = CountBlocks { blocks: 0 };
    let mut registry: Registry<DecodedBlock, HolderTable> = Registry::new();
    registry
        .register(
            "holder-table",
            HolderTableAggregate::new(policy, Vec::new(), Vec::new()),
        )
        .expect("register");

    let walked = walk_with_sink(
        WalkSpec {
            immutable: &dir,
            chunks: &chunks,
            source: Fingerprint::from_u128(0xabc),
        },
        &mut sink,
        &mut registry,
    )
    .expect("walk");

    assert_eq!(walked.blocks, 1);
    assert_eq!(sink.blocks, walked.blocks);
    assert_eq!(walked.artifacts.len(), 1);

    let table = &walked.artifacts[0].artifact;
    assert_eq!(table.policy, policy);
    assert!(!table.holders.is_empty(), "the policy has holders");
    assert!(table.total_quantity() > 0);

    println!(
        "holder table: {} holders, {} total quantity",
        table.distinct_holders(),
        table.total_quantity()
    );

    std::fs::remove_dir_all(dir).expect("clean scratch dir");
}

/// A real chunk, when the local store is named. The fixture proves the shape; this proves
/// the walker over a whole chunk's worth of blocks and transactions.
///
/// Run with:
/// `MITOS_CHUNK_DIR=/Users/damo/data/mithril/immutable cargo test -p mitos-holder-table --test walk -- --nocapture`
#[test]
fn walks_one_real_chunk_when_the_store_is_present() {
    let Ok(immutable) = std::env::var("MITOS_CHUNK_DIR") else {
        return;
    };
    let chunks = chunk_numbers(std::path::Path::new(&immutable)).expect("list chunks");
    let chunk = *chunks.get(1848).unwrap_or(&chunks[0]);

    let mut sink = CountBlocks { blocks: 0 };
    // The walker test is the block/tx count; a dummy policy keeps the table empty and
    // makes the run independent of which assets this chunk happens to hold.
    let mut registry: Registry<DecodedBlock, HolderTable> = Registry::new();
    registry
        .register(
            "holder-table",
            HolderTableAggregate::new([0u8; 28], Vec::new(), Vec::new()),
        )
        .expect("register");

    let walked = walk_with_sink(
        WalkSpec {
            immutable: std::path::Path::new(&immutable),
            chunks: &[chunk],
            source: Fingerprint::from_u128(0xdef),
        },
        &mut sink,
        &mut registry,
    )
    .expect("walk");

    assert!(walked.blocks > 0, "chunk {chunk} has blocks");
    assert!(walked.txs > 0, "chunk {chunk} has transactions");
    assert_eq!(sink.blocks, walked.blocks);
    assert!(walked.stamp.base_slot > 0);

    println!(
        "chunk {chunk}: {} blocks, {} txs, base slot {}",
        walked.blocks, walked.txs, walked.stamp.base_slot
    );
}
