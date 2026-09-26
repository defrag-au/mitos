//! The node's own slot → byte-offset index: `.primary` and `.secondary`.
//!
//! # Why these are the reader's foundation and not a derived artifact
//!
//! `cardano-node` writes both beside every chunk, and Mithril ships them inside
//! the snapshot, so they are **already on the mirror** — measured 2026-09-27 at
//! `mainnet/00000.primary` (86,409 B) and `mainnet/09161.secondary` (63,840 B).
//! Nothing has to be generated to make a block addressable, which is what let the
//! plan's "re-host the sidecars or build our own index" question be answered by
//! opening the bucket rather than by writing a build step.
//!
//! They are also the only way to know a block's byte boundaries without holding
//! the whole chunk: the chunk is bare concatenated CBOR with no separators.
//!
//! # The layouts
//!
//! Fixed-width and big-endian, per the consensus report §8.2.2 — the same bytes
//! `pallas-hardano` reads. Both are stated here and then **checked against the
//! data**, not assumed: [`read_entries`] refuses a primary/secondary pair that
//! disagree, and the CLI's `check` verifies every block against the checks it
//! recovers from the resulting table.
//!
//! - **`.primary`** — a one-byte version, then one big-endian `u32` per slot, plus a
//!   trailing sentinel. A slot is *occupied* iff the next value is greater than
//!   its own, because an empty slot repeats the previous offset; the value at slot
//!   `i` is the byte offset of `i`'s entry inside `.secondary`. Testing slot `i`
//!   needs values `i` and `i + 1`, so the file is `1 + 4 × (slots + 1)` and the
//!   slot count is **derived from the length** rather than hardcoded — preview and
//!   mainnet have different windows and neither is typed here. (Measured: the
//!   mainnet file is 86,409 bytes, so 21,602 offsets and 21,601 slots. The common
//!   "a chunk is `10 × k` = 21,600" phrasing is then off by one at the boundary —
//!   no behaviour depends on which is right, and [`read_entries`] is where it would
//!   surface.)
//!
//!   Measured on `00000.primary` as `01 00000000 00000038 00000070 000000a8
//!   000000e0` — version 1, then `0, 56, 112, 168, 224`, i.e. exactly
//!   `ordinal × 56`. That is the observation the whole reader rests on: the
//!   secondaries are dense and in block order.
//!
//! - **`.secondary`** — a dense array of 56-byte big-endian entries, one per
//!   occupied slot, in slot order.

use anyhow::{Context, Result, bail};

/// The version byte both mainnet primaries seen so far start with. Read, recorded,
/// and not required — a future version is reported rather than refused, since only
/// [`read_entries`]'s count invariant actually depends on the layout.
pub const PRIMARY_VERSION: u8 = 1;

/// One `.secondary` entry: `block_offset: u64`, `header_offset: u16`,
/// `header_size: u16`, `checksum: u32`, `header_hash: [u8; 32]`,
/// `block_or_ebb: [u8; 8]`.
pub const SECONDARY_ENTRY_LEN: usize = 56;

const PRIMARY_HEADER: usize = 1;
const OFFSET_LEN: usize = 4;

/// A parsed `.primary`: the occupancy test and the `.secondary` offsets.
///
/// The slot count comes from the file's length, never from a constant. A mainnet
/// chunk is `10 × k` = 21,600 slots, but that is a property of the network and the
/// file already knows it.
#[derive(Clone, Debug)]
pub struct Primary {
    version: u8,
    offsets: Vec<u32>,
}

impl Primary {
    pub fn version(&self) -> u8 {
        self.version
    }

    /// One per slot in the chunk.
    pub fn slots(&self) -> usize {
        self.offsets.len() - 1
    }

    /// A slot holds a block iff its offset is *below* its successor's. An empty
    /// slot inherits the previous slot's offset, so it does not advance.
    pub fn is_occupied(&self, slot: usize) -> bool {
        self.offsets[slot + 1] > self.offsets[slot]
    }

    /// The `.secondary` byte offset for `slot`. Meaningful for an occupied slot.
    pub fn secondary_offset(&self, slot: usize) -> u32 {
        self.offsets[slot]
    }

    /// Every occupied slot and the `.secondary` offset of its entry, in slot order.
    pub fn occupied(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        (0..self.slots()).filter(|s| self.is_occupied(*s)).map(|s| {
            (
                u32::try_from(s).expect("a slot count that fits usize fits u32"),
                self.offsets[s],
            )
        })
    }

    pub fn occupied_count(&self) -> usize {
        self.occupied().count()
    }
}

/// Read a `.primary` file's bytes.
pub fn parse_primary(bytes: &[u8]) -> Result<Primary> {
    if bytes.is_empty() {
        bail!("primary index is empty");
    }
    let version = bytes[0];
    let body = &bytes[PRIMARY_HEADER..];
    if !body.len().is_multiple_of(OFFSET_LEN) {
        bail!(
            "primary holds {} body bytes, not a whole number of {OFFSET_LEN}-byte offsets",
            body.len()
        );
    }
    let offsets: Vec<u32> = body
        .as_chunks::<OFFSET_LEN>()
        .0
        .iter()
        .map(|c| u32::from_be_bytes(*c))
        .collect();
    if offsets.len() < 2 {
        bail!(
            "primary holds {} offsets — at least 2 are needed to test occupancy",
            offsets.len()
        );
    }
    Ok(Primary { version, offsets })
}

/// One block's place in a chunk, as the node recorded it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The block's slot, within the chunk. Add the chunk's slot base for an
    /// absolute slot.
    pub slot: u32,
    /// Byte offset of the block's first byte within the `.chunk`.
    pub block_offset: u64,
    /// Where the block header starts, relative to [`block_offset`](Self::block_offset).
    pub header_offset: u16,
    /// The header's length, from which `block_offset + header_offset + header_size`
    /// is the end of the header and so a cheap way to read it without the block.
    pub header_size: u16,
    /// The CRC32 `cardano-node` recorded for the whole block, from
    /// `block_offset` to the next entry's (or to EOF).
    pub checksum: u32,
    /// Blake2b-256 of the block header — the block hash **from Shelley on**.
    ///
    /// Byron hashes a block differently, so the field does not hold a header hash
    /// there and a comparison must be era-gated. Measured 2026-09-27: chunk `09161`
    /// matches for all 1,140 blocks, chunk `00000` for none of its 21,587 — while
    /// [`checksum`](Self::checksum) and a `MultiEraBlock` decode succeed for every
    /// block in both.
    pub header_hash: [u8; 32],
    /// The node's block-or-EBB marker, kept raw.
    ///
    /// ⚠️ **Era-dependent, and nothing here reads it.** Measured 2026-09-27:
    ///
    /// - **Shelley onward it is the ABSOLUTE slot.** `09161`'s first entry is
    ///   `197,877,666`, and the block's own CBOR header carries `1a 0bcb5fa2` — the
    ///   same number — beside its block number `1a 00d4bec0`.
    /// - **Byron it is not.** `00000`'s first three entries are `0, 0, 1`, which no
    ///   absolute slot can be.
    ///
    /// So treat it as the node's private field. [`slot`](Self::slot), from the
    /// primary, is the one to build on.
    pub block_or_ebb: [u8; 8],
}

impl Entry {
    fn parse(slot: u32, raw: &[u8]) -> Entry {
        let u64_at = |i: usize| u64::from_be_bytes(raw[i..i + 8].try_into().expect("8 bytes"));
        let u16_at = |i: usize| u16::from_be_bytes(raw[i..i + 2].try_into().expect("2 bytes"));
        let u32_at = |i: usize| u32::from_be_bytes(raw[i..i + 4].try_into().expect("4 bytes"));
        let mut header_hash = [0u8; 32];
        header_hash.copy_from_slice(&raw[16..48]);
        let mut block_or_ebb = [0u8; 8];
        block_or_ebb.copy_from_slice(&raw[48..56]);
        Entry {
            slot,
            block_offset: u64_at(0),
            header_offset: u16_at(8),
            header_size: u16_at(10),
            checksum: u32_at(12),
            header_hash,
            block_or_ebb,
        }
    }

    /// The header's span inside the block, if the block is long enough to hold it.
    pub fn header_span(&self) -> Option<std::ops::Range<usize>> {
        let start = usize::from(self.header_offset);
        let end = start.checked_add(usize::from(self.header_size))?;
        Some(start..end)
    }
}

/// Walk the occupied slots of `primary` and read each one's `.secondary` entry.
///
/// # The invariant, and why it is a refusal rather than a warning
///
/// The number of occupied slots must equal `secondary.len() / 56`. That is what
/// ties the two files together: the primary decides *which* offsets exist and the
/// secondary holds exactly those entries. If it does not hold, an assumption in
/// this module is wrong about this chunk, and every offset derived from it would
/// be plausible-looking nonsense — so it fails here rather than handing a reader
/// bytes that are not a block.
pub fn read_entries(primary: &Primary, secondary: &[u8]) -> Result<Vec<Entry>> {
    if !secondary.len().is_multiple_of(SECONDARY_ENTRY_LEN) {
        bail!(
            "secondary holds {} bytes, not a whole number of {SECONDARY_ENTRY_LEN}-byte entries",
            secondary.len()
        );
    }
    let expected = secondary.len() / SECONDARY_ENTRY_LEN;
    let occupied = primary.occupied_count();
    if occupied != expected {
        bail!(
            "primary marks {occupied} occupied slots but the secondary holds {expected} entries — \
             the two sidecars disagree"
        );
    }

    let mut out = Vec::with_capacity(expected);
    for (slot, offset) in primary.occupied() {
        let start = usize::try_from(offset).context("secondary offset exceeds usize")?;
        let raw = secondary
            .get(start..start + SECONDARY_ENTRY_LEN)
            .with_context(|| {
                format!(
                    "secondary entry for slot {slot} runs past the {}-byte file",
                    secondary.len()
                )
            })?;
        out.push(Entry::parse(slot, raw));
    }

    for w in out.windows(2) {
        if w[1].block_offset <= w[0].block_offset {
            bail!(
                "block offsets are not increasing: slot {} at {} then slot {} at {}",
                w[0].slot,
                w[0].block_offset,
                w[1].slot,
                w[1].block_offset
            );
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `1 + 4 × (slots + 2)` bytes: version, one offset per slot, and two sentinels.
    fn primary_bytes(offsets: &[u32]) -> Vec<u8> {
        let mut out = vec![PRIMARY_VERSION];
        for v in offsets {
            out.extend_from_slice(&v.to_be_bytes());
        }
        out
    }

    fn entry_bytes(
        block_offset: u64,
        header_offset: u16,
        header_size: u16,
        checksum: u32,
    ) -> Vec<u8> {
        let mut out = Vec::with_capacity(SECONDARY_ENTRY_LEN);
        out.extend_from_slice(&block_offset.to_be_bytes());
        out.extend_from_slice(&header_offset.to_be_bytes());
        out.extend_from_slice(&header_size.to_be_bytes());
        out.extend_from_slice(&checksum.to_be_bytes());
        out.extend_from_slice(&[0xab; 32]);
        out.extend_from_slice(&[0; 8]);
        out
    }

    #[test]
    fn a_slot_is_occupied_when_its_offset_advances() {
        // Offsets 0, 56, 112, 168, 224: every one of the 4 slots advanced, so all 4
        // are occupied. This is the measured shape of a dense prefix of `00000`.
        let p = parse_primary(&primary_bytes(&[0, 56, 112, 168, 224])).unwrap();
        assert_eq!(p.version(), PRIMARY_VERSION);
        assert_eq!(p.slots(), 4);
        assert_eq!(p.occupied_count(), 4);
        assert_eq!(
            p.occupied().collect::<Vec<_>>(),
            vec![(0, 0), (1, 56), (2, 112), (3, 168)]
        );
    }

    #[test]
    fn a_repeated_offset_marks_an_empty_slot() {
        // Slot 1 repeats slot 0's successor, so nothing landed there.
        let p = parse_primary(&primary_bytes(&[0, 56, 56, 112])).unwrap();
        assert_eq!(p.slots(), 3);
        assert!(!p.is_occupied(1));
        assert_eq!(p.occupied().collect::<Vec<_>>(), vec![(0, 0), (2, 56)]);
    }

    #[test]
    fn the_last_offset_is_a_sentinel_not_a_slot() {
        // The file must carry one value beyond the last slot so that the last
        // slot's occupancy can be tested at all.
        let p = parse_primary(&primary_bytes(&[0, 56, 56])).unwrap();
        assert_eq!(p.slots(), 2);
        assert!(p.is_occupied(0));
        assert!(!p.is_occupied(1));
    }

    #[test]
    fn entries_are_read_at_the_offsets_the_primary_names() {
        // Slot 0 at byte 0, slot 3 at byte 56 — the second entry is therefore read
        // from 56, which is only true if the primary's value was used, not the
        // slot's ordinal.
        let p = parse_primary(&primary_bytes(&[0, 56, 56, 56, 112])).unwrap();
        assert_eq!(p.occupied().collect::<Vec<_>>(), vec![(0, 0), (3, 56)]);

        let mut secondary = entry_bytes(0, 3, 80, 0xdead_beef);
        secondary.extend_from_slice(&entry_bytes(1_234, 3, 859, 0x0000_0001));

        let entries = read_entries(&p, &secondary).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].slot, 0);
        assert_eq!(entries[0].block_offset, 0);
        assert_eq!(entries[0].header_offset, 3);
        assert_eq!(entries[0].header_size, 80);
        assert_eq!(entries[0].checksum, 0xdead_beef);
        assert_eq!(entries[0].header_hash, [0xab; 32]);
        assert_eq!(entries[0].header_span(), Some(3..83));
        assert_eq!(entries[1].slot, 3);
        assert_eq!(entries[1].block_offset, 1_234);
    }

    #[test]
    fn a_secondary_that_disagrees_with_the_primary_is_refused() {
        let p = parse_primary(&primary_bytes(&[0, 56, 112])).unwrap();
        assert_eq!(p.occupied_count(), 2);

        // One entry where the primary names two.
        let short = entry_bytes(0, 3, 80, 0);
        let err = read_entries(&p, &short).unwrap_err().to_string();
        assert!(
            err.contains("2 occupied slots but the secondary holds 1"),
            "{err}"
        );

        // One and a half entries.
        let mut ragged = entry_bytes(0, 3, 80, 0);
        ragged.extend_from_slice(&entry_bytes(1, 3, 80, 0));
        ragged.truncate(SECONDARY_ENTRY_LEN + 9);
        let err = read_entries(&p, &ragged).unwrap_err().to_string();
        assert!(err.contains("not a whole number"), "{err}");
    }

    #[test]
    fn offsets_that_do_not_advance_are_refused() {
        let p = parse_primary(&primary_bytes(&[0, 56, 56, 112])).unwrap();
        let mut secondary = entry_bytes(900, 3, 80, 0);
        secondary.extend_from_slice(&entry_bytes(900, 3, 80, 0));
        let err = read_entries(&p, &secondary).unwrap_err().to_string();
        assert!(err.contains("not increasing"), "{err}");
    }

    #[test]
    fn an_offset_past_the_end_of_the_secondary_is_refused() {
        // One occupied slot, and its entry is said to start past the file.
        let p = parse_primary(&primary_bytes(&[112, 224])).unwrap();
        let err = read_entries(&p, &entry_bytes(0, 3, 80, 0))
            .unwrap_err()
            .to_string();
        assert!(err.contains("runs past"), "{err}");
    }

    #[test]
    fn a_primary_too_short_to_test_occupancy_is_refused() {
        assert!(
            parse_primary(&[])
                .unwrap_err()
                .to_string()
                .contains("empty")
        );
        assert!(
            parse_primary(&[1])
                .unwrap_err()
                .to_string()
                .contains("at least 2")
        );
        let err = parse_primary(&[1, 0, 0]).unwrap_err().to_string();
        assert!(err.contains("not a whole number"), "{err}");
    }
}
