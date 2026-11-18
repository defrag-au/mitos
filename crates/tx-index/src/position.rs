//! tx hash → its position inside its block.
//!
//! # Why
//!
//! Chain order is `(slot, position in block)`. The index answers *where the
//! body is* — a chunk and a byte offset — which orders two transactions of
//! the same block correctly but is not the position itself, and a consumer
//! that merges sources needs the real number: a market event and a holder
//! event of the same block must agree on what "tx 3" means.
//!
//! The policy archives written before their `tx_index` column existed carry
//! a zero for every row (see `policy-archive`'s reader), so two transfers of
//! the same asset in the same block came back in file order — and a
//! holder-behaviour replay saw the asset leave a wallet before it arrived.
//! This is the repair: the position, recovered from the block that holds it.
//!
//! # How
//!
//! The located body's offset picks its block out of the chunk's `.secondary`
//! sidecar (dense 56-byte big-endian entries, one per block, starting with
//! the block's byte offset — see `mithril-source`'s `sidecar.rs`), so only
//! that block is read and decoded, never the whole chunk. The transaction is
//! then found by HASH among the block's bodies, not by offset arithmetic, so
//! a wrong block can only fail to answer; it cannot answer wrongly.

use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::FileExt;

use anyhow::{Context, Result, anyhow, bail};
use pallas_traverse::MultiEraBlock;

use crate::extract::{chunk_path, txs};
use crate::reader::Index;

/// One `.secondary` entry; its first 8 bytes are the block's offset.
const SECONDARY_ENTRY_LEN: usize = 56;

/// Positions over one [`Index`], with the sidecars and blocks already read
/// kept for the next lookup — the transactions a policy cares about cluster,
/// so the same block is asked for again and again.
pub struct Positions<'a> {
    index: &'a Index,
    /// chunk → its block offsets, ascending, plus the chunk's length.
    sidecars: HashMap<u16, (Vec<u64>, u64)>,
    /// `(chunk, block offset)` → the block's tx hashes in block order.
    blocks: HashMap<(u16, u64), Vec<[u8; 32]>>,
}

impl<'a> Positions<'a> {
    pub fn new(index: &'a Index) -> Self {
        Self {
            index,
            sidecars: HashMap::new(),
            blocks: HashMap::new(),
        }
    }

    /// The position of `hash` inside its block, or `None` when no completed
    /// chunk the index covers holds it (the newest chunk is never indexed).
    pub fn position(&mut self, hash: &[u8; 32]) -> Result<Option<u32>> {
        let Some(body) = self.index.tx(hash)? else {
            return Ok(None);
        };
        let chunk = body.loc.chunk;
        let offset = u64::from(body.loc.offset);
        if !self.sidecars.contains_key(&chunk) {
            let sidecar = self.read_sidecar(chunk)?;
            self.sidecars.insert(chunk, sidecar);
        }
        let (starts, chunk_len) = &self.sidecars[&chunk];
        let (start, end) = block_span(starts, *chunk_len, offset)
            .ok_or_else(|| anyhow!("chunk {chunk}: no block in the sidecar holds byte {offset}"))?;
        if !self.blocks.contains_key(&(chunk, start)) {
            let hashes = self.read_block(chunk, start, end)?;
            self.blocks.insert((chunk, start), hashes);
        }
        let hashes = &self.blocks[&(chunk, start)];
        let Some(position) = hashes.iter().position(|candidate| candidate == hash) else {
            bail!(
                "chunk {chunk}: the block at byte {start} does not contain the transaction \
                 the index located at byte {offset}"
            );
        };
        Ok(Some(
            u32::try_from(position).context("tx position exceeds u32")?,
        ))
    }

    fn read_sidecar(&self, chunk: u16) -> Result<(Vec<u64>, u64)> {
        let chunk_file = chunk_path(self.index.immutable(), chunk);
        let chunk_len = std::fs::metadata(&chunk_file)
            .with_context(|| format!("reading the length of {}", chunk_file.display()))?
            .len();
        let path = chunk_file.with_extension("secondary");
        let bytes = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        Ok((block_starts(&bytes)?, chunk_len))
    }

    fn read_block(&self, chunk: u16, start: u64, end: u64) -> Result<Vec<[u8; 32]>> {
        let path = chunk_path(self.index.immutable(), chunk);
        let file = File::open(&path).with_context(|| format!("opening {}", path.display()))?;
        let len = usize::try_from(end - start).context("block length exceeds usize")?;
        let mut buf = vec![0u8; len];
        file.read_exact_at(&mut buf, start)
            .with_context(|| format!("reading {len} bytes at {start} of {}", path.display()))?;
        let block = MultiEraBlock::decode(&buf)
            .map_err(|e| anyhow!("chunk {chunk}: decoding the block at byte {start}: {e:?}"))?;
        Ok(txs(&block)?
            .iter()
            .map(|tx| crate::tx_hash(tx.body))
            .collect())
    }
}

/// The block offsets a `.secondary` sidecar lists, in file order.
fn block_starts(secondary: &[u8]) -> Result<Vec<u64>> {
    let (entries, rest) = secondary.as_chunks::<SECONDARY_ENTRY_LEN>();
    if !rest.is_empty() {
        bail!(
            "a .secondary of {} bytes is not whole {SECONDARY_ENTRY_LEN}-byte entries",
            secondary.len()
        );
    }
    let starts: Vec<u64> = entries
        .iter()
        .map(|entry| {
            let mut offset = [0u8; 8];
            offset.copy_from_slice(&entry[..8]);
            u64::from_be_bytes(offset)
        })
        .collect();
    if starts.windows(2).any(|pair| pair[0] >= pair[1]) {
        bail!("a .secondary whose block offsets do not ascend");
    }
    Ok(starts)
}

/// The `[start, end)` byte span of the block holding `offset`: the last block
/// starting at or before it, ending where the next begins or the chunk does.
fn block_span(starts: &[u64], chunk_len: u64, offset: u64) -> Option<(u64, u64)> {
    let after = starts.partition_point(|start| *start <= offset);
    let index = after.checked_sub(1)?;
    let end = starts.get(index + 1).copied().unwrap_or(chunk_len);
    (offset < end).then_some((starts[index], end))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(offset: u64) -> Vec<u8> {
        let mut out = offset.to_be_bytes().to_vec();
        out.resize(SECONDARY_ENTRY_LEN, 0xaa);
        out
    }

    #[test]
    fn a_sidecar_lists_each_block_offset_in_order() {
        let bytes: Vec<u8> = [0, 900, 4_000].into_iter().flat_map(entry).collect();
        assert_eq!(block_starts(&bytes).unwrap(), vec![0, 900, 4_000]);
    }

    #[test]
    fn a_torn_or_unordered_sidecar_is_refused() {
        let mut torn = entry(0);
        torn.pop();
        assert!(block_starts(&torn).is_err());
        let unordered: Vec<u8> = [900, 0].into_iter().flat_map(entry).collect();
        assert!(block_starts(&unordered).is_err());
    }

    #[test]
    fn an_offset_belongs_to_the_last_block_starting_at_or_before_it() {
        let starts = [0, 900, 4_000];
        assert_eq!(block_span(&starts, 5_000, 0), Some((0, 900)));
        assert_eq!(block_span(&starts, 5_000, 899), Some((0, 900)));
        assert_eq!(block_span(&starts, 5_000, 900), Some((900, 4_000)));
        assert_eq!(block_span(&starts, 5_000, 4_999), Some((4_000, 5_000)));
    }

    #[test]
    fn an_offset_outside_every_block_has_no_span() {
        assert_eq!(block_span(&[100, 900], 5_000, 50), None);
        assert_eq!(block_span(&[0, 900], 5_000, 5_000), None);
        assert_eq!(block_span(&[], 5_000, 0), None);
    }
}
