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
//!
//! ## Auxiliary data
//!
//! A block stores metadata out-of-line: `auxiliary_data_set` is a map from
//! TRANSACTION INDEX to aux data, not from tx hash. So the pairing is
//! positional — body `i` owns `auxiliary_data_set[i]` — and it is only sound
//! because both come from the same decoded block in the same pass. Most
//! transactions have no entry, which is why the span is optional rather than
//! a sentinel offset.
//!
//! Byron has no auxiliary data at all; its txs always index as `None`.

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use pallas_codec::minicbor::Decoder;
use pallas_crypto::hash::Hasher;
use pallas_traverse::{Era, MultiEraBlock};

use crate::format::{
    AuxSpan, Entry, Location, SegmentHeader, chunk_number, chunk_u16, era_to_u8, prefix_of,
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

        for tx in txs(&block)? {
            let body_span = span_of(tx.body, base_ptr, bytes.len(), chunk, pos, "tx body")?;
            let aux_span = tx
                .aux
                .map(|a| span_of(a, base_ptr, bytes.len(), chunk, pos, "auxiliary data"))
                .transpose()?;
            let hash = Hasher::<256>::hash(tx.body);
            entries.push(Entry {
                prefix: prefix_of(&hash),
                loc: Location {
                    chunk,
                    offset: body_span.0,
                    len: body_span.1,
                },
                aux: aux_span.map(|(offset, len)| AuxSpan { offset, len }),
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

/// Turn a slice borrowed from the chunk buffer into a `(offset, len)` pair,
/// range-checked against the buffer it must have come from.
///
/// The check is not ceremony: an offset recorded for a slice that is NOT
/// inside this buffer would be a plausible-looking number pointing at
/// unrelated bytes, and the reader's hash verification would only catch it
/// for bodies — aux data is not hash-addressed, so a bad span there would be
/// silently served as someone else's metadata.
fn span_of(
    slice: &[u8],
    base_ptr: usize,
    buf_len: usize,
    chunk: u16,
    block_pos: usize,
    what: &str,
) -> Result<(u32, u16)> {
    let start = slice.as_ptr() as usize;
    let end = start + slice.len();
    if start < base_ptr || end > base_ptr + buf_len {
        bail!(
            "chunk {chunk}: {what} at block byte {block_pos} is not borrowed from the chunk buffer"
        );
    }
    let offset = start - base_ptr;
    Ok((
        u32::try_from(offset)
            .with_context(|| format!("chunk {chunk}: {what} offset {offset} exceeds u32"))?,
        u16::try_from(slice.len()).with_context(|| {
            format!("chunk {chunk}: {what} of {} bytes exceeds u16", slice.len())
        })?,
    ))
}

/// One transaction's raw slices, both borrowed from the chunk buffer.
struct TxSlices<'a> {
    body: &'a [u8],
    /// `None` for the majority of transactions, which carry no metadata.
    aux: Option<&'a [u8]>,
}

/// Every transaction in the block, in block order.
///
/// Byron's hashed item is the inner `Tx` of each `[tx, witnesses]` payload;
/// Shelley onward it is each element of the body array. Auxiliary data is
/// looked up positionally in `auxiliary_data_set`, which is keyed by
/// transaction index.
fn txs<'a>(block: &'a MultiEraBlock<'_>) -> Result<Vec<TxSlices<'a>>> {
    /// Shelley-onward eras all encode the block identically for our purposes:
    /// a body array plus an index-keyed aux map.
    macro_rules! bodies_with_aux {
        ($b:expr) => {
            $b.transaction_bodies
                .iter()
                .enumerate()
                .map(|(i, k)| {
                    let idx = u32::try_from(i).expect("tx index fits u32");
                    TxSlices {
                        body: k.raw_cbor(),
                        aux: $b.auxiliary_data_set.get(&idx).map(|a| a.raw_cbor()),
                    }
                })
                .collect()
        };
    }

    Ok(match block {
        MultiEraBlock::EpochBoundary(_) => Vec::new(),
        // Byron predates transaction metadata entirely.
        MultiEraBlock::Byron(b) => b
            .body
            .tx_payload
            .iter()
            .map(|p| TxSlices {
                body: p.transaction.raw_cbor(),
                aux: None,
            })
            .collect(),
        MultiEraBlock::AlonzoCompatible(b, _) => bodies_with_aux!(b),
        MultiEraBlock::Babbage(b) => bodies_with_aux!(b),
        MultiEraBlock::Conway(b) => bodies_with_aux!(b),
        // `MultiEraBlock` is `#[non_exhaustive]` upstream. A block shape this
        // build cannot name must fail the chunk, not silently index nothing.
        other => bail!(
            "block era {} is newer than this build of tx-index",
            other.era()
        ),
    })
}
