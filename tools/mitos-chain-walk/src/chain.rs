//! Immutable-DB access + slot arithmetic.
//!
//! [`open_blocks`] hands back the block iterator every walker drives: from
//! genesis (fast-skip below the floor is the walker's loop — decoding is the
//! cost, so a caller that knows a `(slot, hash)` point should seek instead) or
//! seeked to a point via `read_blocks_from_point`. The trade-off is documented
//! on `--from-point` in both walkers: a seek starts any resolution buffer cold.

use std::path::Path;

use anyhow::{Context, Result, bail};
use pallas_hardano::storage::immutable::{self, FallibleBlock, Point};

/// Open `<immutable_dir>` as a stream of raw block CBOR: from genesis
/// (`None`), or seeked to `(slot, block_hash)`.
///
/// **An EMPTY `block_hash` is a slot-only FUZZY seek** — pallas-hardano
/// binary-searches the chunk list and yields the first block at
/// `slot >= <slot>`. That is the cheap way to start a walk at a known floor
/// when you do not have the block hash: it skips whole chunk files instead of
/// decoding everything below the floor (which is over an hour of CPU on
/// mainnet). A 32-byte hash seeks exactly and errors if that block is absent.
pub fn open_blocks<'a>(
    immutable_dir: &'a Path,
    from_point: Option<(u64, Vec<u8>)>,
) -> Result<Box<dyn Iterator<Item = FallibleBlock> + 'a>> {
    if !immutable_dir.is_dir() {
        bail!(
            "immutable DB not found at {} — run `bootstrap` first",
            immutable_dir.display()
        );
    }
    match from_point {
        Some((slot, hash)) => {
            let it = immutable::read_blocks_from_point(immutable_dir, Point::Specific(slot, hash))
                .map_err(|e| anyhow::anyhow!("seeking immutable DB to point: {e:?}"))?;
            Ok(Box::new(it))
        }
        None => Ok(Box::new(immutable::read_blocks(immutable_dir).map_err(
            |e| anyhow::anyhow!("opening immutable DB at {}: {e:?}", immutable_dir.display()),
        )?)),
    }
}

/// Parse `<slot>:<block_hash_hex>` (32-byte hash) — the shape of `--from-point`.
pub fn parse_point(s: &str) -> Result<(u64, Vec<u8>)> {
    let (slot, hash) = s
        .split_once(':')
        .context("point must be `<slot>:<block_hash_hex>`")?;
    let slot: u64 = slot.trim().parse().context("point slot")?;
    let hash = hex::decode(hash.trim()).context("point hash hex")?;
    if hash.len() != 32 {
        bail!("point hash must be 32 bytes, got {}", hash.len());
    }
    Ok((slot, hash))
}

/// Mainnet slot → unix seconds. Shelley (slot ≥ 4_492_800) is 1s/slot from
/// 1_596_059_091; Byron before that is 20s/slot (only a floor sanity fallback —
/// nothing either walker cares about predates Shelley).
pub fn slot_to_unix(slot: u64) -> u64 {
    const SHELLEY_START_SLOT: u64 = 4_492_800;
    const SHELLEY_START_UNIX: u64 = 1_596_059_091;
    const BYRON_START_UNIX: u64 = 1_506_203_091;
    if slot >= SHELLEY_START_SLOT {
        SHELLEY_START_UNIX + (slot - SHELLEY_START_SLOT)
    } else {
        BYRON_START_UNIX + slot * 20
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_point_shape() {
        let (slot, hash) = parse_point(&format!("123:{}", hex::encode([9u8; 32]))).unwrap();
        assert_eq!(slot, 123);
        assert_eq!(hash, vec![9u8; 32]);
        assert!(parse_point("123").is_err());
        assert!(parse_point("123:abcd").is_err());
    }

    #[test]
    fn slot_time_boundaries() {
        assert_eq!(slot_to_unix(4_492_800), 1_596_059_091);
        assert_eq!(slot_to_unix(4_492_801), 1_596_059_092);
        assert_eq!(slot_to_unix(0), 1_506_203_091);
    }
}
