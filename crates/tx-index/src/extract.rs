//! Chunk → segment: the ONE decode+hash pass per completed chunk.
//!
//! The chunk file is read whole and split into blocks by walking top-level
//! CBOR items — not via the `.secondary` sidecar — so every offset recorded
//! here is a fact about the bytes this code actually saw. Bodies are found
//! by decoding each block with pallas and taking the `KeepRaw` slice of each
//! transaction body; those slices borrow from the chunk buffer, so the body's
//! offset is pointer arithmetic, range-checked before it is trusted.
//!
//! Hashing is blake2b-256 over the body bytes — the same computation
//! `MultiEraTx::hash()` does — but done here on the borrowed slice without
//! cloning the witness set alongside, which is what `block.txs()` would cost.

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use pallas_codec::minicbor::Decoder;
use pallas_crypto::hash::Hasher;
use pallas_traverse::{Era, MultiEraBlock};

use crate::format::{
    Entry, Location, SegmentHeader, chunk_number, chunk_u16, era_to_u8, prefix_of,
};
use crate::segment::write_segment;

/// Sorted chunk numbers under `immutable`, NEWEST EXCLUDED — the newest
/// immutable file is still being appended to until the next one seals, and
/// Mithril re-ships it whole on every refresh. Same rule as chain-sieve.
pub fn list_chunks(immutable: &Path) -> Result<Vec<u16>> {
    let mut nums: Vec<u64> = std::fs::read_dir(immutable)
        .with_context(|| format!("reading {}", immutable.display()))?
        .filter_map(|e| {
            let name = e.ok()?.file_name().into_string().ok()?;
            chunk_number(&name, ".chunk")
        })
        .collect();
    nums.sort_unstable();
    if nums.len() < 2 {
        bail!("need at least 2 chunk files (the newest is excluded as still-growing)");
    }
    nums.pop();
    nums.into_iter().map(chunk_u16).collect()
}

pub fn chunk_path(immutable: &Path, chunk: u16) -> PathBuf {
    immutable.join(format!("{chunk:05}.chunk"))
}

pub struct Extracted {
    pub header: SegmentHeader,
    pub entries: Vec<Entry>,
    pub blocks: u64,
    pub bytes: u64,
    pub wall_secs: f64,
}

/// Decode every block in `NNNNN.chunk`, hash every tx body, return the
/// sorted entries. Refuses a chunk whose blocks disagree on era.
pub fn extract_chunk(immutable: &Path, chunk: u16) -> Result<Extracted> {
    let started = Instant::now();
    let path = chunk_path(immutable, chunk);
    let bytes = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
    let base_ptr = bytes.as_ptr() as usize;

    let mut entries = Vec::new();
    let mut era: Option<Era> = None;
    let mut blocks = 0u64;
    let mut pos = 0usize;

    while pos < bytes.len() {
        let mut d = Decoder::new(&bytes[pos..]);
        d.skip()
            .map_err(|e| anyhow!("chunk {chunk}: splitting block at byte {pos}: {e}"))?;
        let len = d.position();
        if len == 0 {
            bail!("chunk {chunk}: zero-length CBOR item at byte {pos}");
        }
        let raw = &bytes[pos..pos + len];
        let block = MultiEraBlock::decode(raw)
            .map_err(|e| anyhow!("chunk {chunk}: decoding block at byte {pos}: {e:?}"))?;
        blocks += 1;

        let this_era = block.era();
        match era {
            None => era = Some(this_era),
            Some(e) if e != this_era => {
                bail!("chunk {chunk} mixes eras {e} and {this_era} at byte {pos}")
            }
            Some(_) => {}
        }

        for body in bodies(&block) {
            let start = body.as_ptr() as usize;
            let end = start + body.len();
            if start < base_ptr || end > base_ptr + bytes.len() {
                bail!("chunk {chunk}: tx body at block byte {pos} is not borrowed from the chunk buffer");
            }
            let offset = start - base_ptr;
            let hash = Hasher::<256>::hash(body);
            entries.push(Entry {
                prefix: prefix_of(&hash),
                loc: Location {
                    chunk,
                    offset: u32::try_from(offset)
                        .with_context(|| format!("chunk {chunk}: body offset {offset} exceeds u32"))?,
                    len: u16::try_from(body.len()).with_context(|| {
                        format!("chunk {chunk}: body of {} bytes exceeds u16", body.len())
                    })?,
                },
            });
        }
        pos += len;
    }

    let Some(era) = era else {
        bail!("chunk {chunk}: no blocks");
    };
    entries.sort_unstable_by_key(|e| e.prefix);
    let count = u32::try_from(entries.len())
        .with_context(|| format!("chunk {chunk}: {} entries exceed u32", entries.len()))?;

    Ok(Extracted {
        header: SegmentHeader {
            era: era_to_u8(era),
            chunk,
            count,
        },
        entries,
        blocks,
        bytes: bytes.len() as u64,
        wall_secs: started.elapsed().as_secs_f64(),
    })
}

/// Extract a chunk and write its segment. The unit of incremental work.
pub fn extract_to_segment(immutable: &Path, index_dir: &Path, chunk: u16) -> Result<Extracted> {
    let ex = extract_chunk(immutable, chunk)?;
    write_segment(index_dir, ex.header, &ex.entries)?;
    Ok(ex)
}

/// The raw body slice of every transaction in the block, in block order.
/// Byron's hashed item is the inner `Tx` of each `[tx, witnesses]` payload;
/// Shelley onward it is each element of the body array.
fn bodies<'a>(block: &'a MultiEraBlock<'_>) -> Vec<&'a [u8]> {
    match block {
        MultiEraBlock::EpochBoundary(_) => Vec::new(),
        MultiEraBlock::Byron(b) => b
            .body
            .tx_payload
            .iter()
            .map(|p| p.transaction.raw_cbor())
            .collect(),
        MultiEraBlock::AlonzoCompatible(b, _) => b
            .transaction_bodies
            .iter()
            .map(|k| k.raw_cbor())
            .collect(),
        MultiEraBlock::Babbage(b) => b
            .transaction_bodies
            .iter()
            .map(|k| k.raw_cbor())
            .collect(),
        MultiEraBlock::Conway(b) => b
            .transaction_bodies
            .iter()
            .map(|k| k.raw_cbor())
            .collect(),
    }
}
