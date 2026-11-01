//! The sidecar parser and the reader, against real snapshot bytes.
//!
//! A default `cargo test` skips these: they need ~20 MB of a real unpacked
//! snapshot, which is not in the repository. They are `#[ignore]`d rather than
//! silently returning, so a default run reports them as ignored instead of
//! counting an assertion that never ran as a pass.
//!
//! ```sh
//! # the two sidecars and the chunk, straight off the mirror, into the fixture dir
//! nix shell nixpkgs#rclone -c rclone copy \
//!   --filter "+ 00000.*" --filter "+ 09161.*" --filter "- *" \
//!   r2:mithril-snapshots/mainnet .tmp/mithril-probe/
//!
//! MITHRIL_SOURCE_FIXTURES=.tmp/mithril-probe \
//!   direnv exec . cargo test -p mithril-source -- --ignored
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use mithril_source::{
    Chunk, Entry, HttpMirror, LocalDir, RangeSource, SECONDARY_ENTRY_LEN, Span, parse_primary,
    read_entries,
};
use pallas_crypto::hash::Hasher;
use pallas_traverse::{Era, MultiEraBlock};

fn source() -> Arc<dyn RangeSource> {
    let raw = std::env::var("MITHRIL_SOURCE_FIXTURES").expect(
        "set MITHRIL_SOURCE_FIXTURES to a directory holding NNNNN.chunk/.primary/.secondary",
    );
    let dir = PathBuf::from(raw);
    // Cargo runs a test with the PACKAGE directory as its working directory, so a
    // path written as if from the repository root has to be resolved from there —
    // otherwise the documented `.tmp/mithril-probe` silently means
    // `crates/mithril-source/.tmp/mithril-probe`.
    let dir = if dir.is_absolute() {
        dir
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(dir)
    };
    assert!(
        dir.is_dir(),
        "MITHRIL_SOURCE_FIXTURES points at {}, which is not a directory",
        dir.display()
    );
    Arc::new(LocalDir::new(dir))
}

fn mirror_source(mirror: &Arc<HttpMirror>) -> Arc<dyn RangeSource> {
    let owned: Arc<HttpMirror> = Arc::clone(mirror);
    owned
}

/// Byron: the header hash field does not apply, so only the tiling, the CRC32 and
/// the decode are asserted.
#[test]
#[ignore = "needs MITHRIL_SOURCE_FIXTURES; ~20 MB of a real unpacked snapshot"]
fn chunk_00000_verifies() {
    verify(0, HeaderCheck::NotApplicable);
}

/// Shelley/Conway: every block's header hashes to the hash the sidecar recorded.
#[test]
#[ignore = "needs MITHRIL_SOURCE_FIXTURES; ~20 MB of a real unpacked snapshot"]
fn chunk_09161_verifies() {
    verify(9161, HeaderCheck::Applies);
}

enum HeaderCheck {
    Applies,
    NotApplicable,
}

fn verify(number: u16, header: HeaderCheck) {
    let base = format!("{number:05}");
    let source = source();

    let primary_bytes = source.read_all(&format!("{base}.primary")).unwrap();
    let primary = parse_primary(&primary_bytes).unwrap();
    let secondary = source.read_all(&format!("{base}.secondary")).unwrap();
    let entries = read_entries(&primary, &secondary).unwrap();

    // The invariant that ties the two sidecars together, and the one a wrong
    // occupancy rule would break.
    assert!(secondary.len().is_multiple_of(SECONDARY_ENTRY_LEN));
    assert_eq!(
        primary.occupied_count(),
        secondary.len() / SECONDARY_ENTRY_LEN,
        "occupied slots must equal secondary entries"
    );
    assert_eq!(entries.len(), primary.occupied_count());
    assert_eq!(
        entries[0].block_offset, 0,
        "the first block starts at byte 0"
    );
    assert!(
        entries
            .windows(2)
            .all(|w| w[1].block_offset > w[0].block_offset),
        "block offsets must strictly increase"
    );

    let chunk = Chunk::open(Arc::clone(&source), number).unwrap();
    assert_eq!(chunk.block_count(), entries.len());
    assert_eq!(
        (0..chunk.block_count())
            .map(|i| chunk.block_len(i).unwrap())
            .sum::<u64>(),
        chunk.len(),
        "the blocks must tile the file exactly"
    );

    let mut blocks = 0u64;
    for (i, block) in chunk.blocks().enumerate() {
        let block = block.unwrap();
        let entry = entries[i];
        blocks += 1;

        assert_eq!(
            crc32fast::hash(&block.bytes),
            entry.checksum,
            "block {i} at offset {} has the wrong CRC32",
            entry.block_offset
        );

        let decoded = MultiEraBlock::decode(&block.bytes).unwrap_or_else(|e| {
            panic!(
                "block {i} at offset {} does not decode: {e:?}",
                entry.block_offset
            )
        });

        match header {
            HeaderCheck::Applies => {
                assert_ne!(decoded.era(), Era::Byron);
                let span = entry.header_span().unwrap();
                let got = Hasher::<256>::hash(&block.bytes[span]);
                assert_eq!(
                    got.as_ref(),
                    entry.header_hash,
                    "block {i} at offset {}: header hash",
                    entry.block_offset
                );
            }
            HeaderCheck::NotApplicable => {
                assert_eq!(decoded.era(), Era::Byron);
            }
        }
    }
    assert_eq!(
        blocks as usize,
        chunk.block_count(),
        "the walk must yield every block"
    );
}

/// The batched reader against the live mirror, which is the only way to prove the
/// `multipart/byteranges` parse against the bytes Cloudflare actually sends — and
/// the only way to prove the 299-range cap is where Cloudflare's own limit is.
///
/// `#[ignore]`d *and* gated on a URL, so neither a default `cargo test` nor an
/// offline machine can pretend to have run it:
///
/// ```sh
/// MITHRIL_SOURCE_MIRROR=https://mithril.defrag.cc/mainnet \
///   direnv exec . cargo test -p mithril-source -- --ignored batched
/// ```
#[test]
#[ignore = "needs MITHRIL_SOURCE_MIRROR; makes real HTTP requests"]
fn batched_spans_over_the_mirror_are_the_bytes_the_node_recorded() {
    let base = std::env::var("MITHRIL_SOURCE_MIRROR").expect(
        "set MITHRIL_SOURCE_MIRROR to a mirror base URL, e.g. https://mithril.defrag.cc/mainnet",
    );
    let mirror = Arc::new(HttpMirror::new(base).unwrap());
    let chunk = Chunk::open(mirror_source(&mirror), 9161).unwrap();
    let entries = chunk.entries().to_vec();

    // Every 20th block: scattered, so nothing merges and the whole set is one
    // request. Contiguous spans would collapse into a single range and prove
    // nothing about batching.
    let scattered = |stride: usize, take: usize| -> Vec<Span> {
        (0..entries.len())
            .step_by(stride)
            .take(take)
            .map(|i| Span::new(entries[i].block_offset, chunk.block_len(i).unwrap()))
            .collect()
    };

    let spans = scattered(20, usize::MAX);
    let before = mirror.requests();
    let got = chunk.read_spans(&spans).unwrap();
    assert_eq!(
        mirror.requests() - before,
        1,
        "{} scattered spans must be one request",
        spans.len()
    );
    assert_crcs(
        &entries,
        &spans,
        &got,
        &(0..entries.len()).step_by(20).collect::<Vec<_>>(),
    );

    // Past 299 the cap has to split, or Cloudflare ignores `Range` and hands back
    // the whole 5 MB chunk as a 200 — which the content-type check would catch, but
    // only after the transfer.
    let spans = scattered(2, 330);
    assert!(
        spans.len() > 300,
        "this case is about crossing the range limit"
    );
    let before = mirror.requests();
    let got = chunk.read_spans(&spans).unwrap();
    let requests = mirror.requests() - before;
    assert!(
        (2..=3).contains(&requests),
        "{} spans should split into 2 batches of <=299, took {requests} request(s)",
        spans.len()
    );
    assert_crcs(
        &entries,
        &spans,
        &got,
        &(0..entries.len()).step_by(2).take(330).collect::<Vec<_>>(),
    );
}

fn assert_crcs(entries: &[Entry], spans: &[Span], got: &[Vec<u8>], chosen: &[usize]) {
    assert_eq!(got.len(), spans.len());
    for (n, &i) in chosen.iter().enumerate() {
        assert_eq!(
            crc32fast::hash(&got[n]),
            entries[i].checksum,
            "span {n} (block {i}, slot {}, offset {}) is not the bytes the node recorded",
            entries[i].slot,
            entries[i].block_offset
        );
    }
}
