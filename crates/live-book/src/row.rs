//! The row: `B2`'s variable-length record, and the only place its byte offsets
//! are written down.
//!
//! ```text
//! offset  size  field
//!      0    36  outref        — tx hash (32) ‖ output index (u32 LE)
//!     36     8  lovelace
//!     44     4  datum_ref
//!     48     1  datum_kind    — 0 none · 1 inline · 2 hash
//!     49     1  addr_len      — u16, not u8 (see `format::LAYOUT_VERSION`)
//!     51     2  n_assets
//!     53     1  era
//!     54     2  datum_len     — phase 1 keeps the datum bytes in the row
//!   ----    56  fixed header
//!     56  addr_len   address bytes, raw
//!         ...         n_assets × { u32 policy_id, u8 name_len, name bytes, u64 quantity }
//!         ...         datum_len  datum bytes (inline CBOR, or the 32-byte hash)
//! ```
//!
//! # Two readers, one layout
//!
//! The builder writes these bytes, the verifier spots-checks them and the reader
//! hands them over — and before this module existed all three held their own
//! offsets. A layout with three transcriptions is a layout that drifts, so the
//! offsets are named here once and [`Row`] is the only thing that walks them.
//!
//! # Nothing here allocates
//!
//! A row is a borrowed view: the address, each asset name and the datum are
//! slices of the mapped file. That is the whole point of the format being
//! byte-addressed — a consumer that wants a bech32 string or a `Value` makes one
//! itself, and a consumer that only wants to check a credential pays nothing.
//! [`Row::parse`] walks the asset records once (their lengths are what locate the
//! datum), which is O(assets) with no allocation and is also the row's bounds
//! check.

use crate::format::POLICY_ID_BYTES;

/// The fixed header `B2` specifies, 8-aligned. The rows are variable-length and
/// byte-addressed, so nothing here needs alignment beyond this.
pub const ROW_HEADER: usize = 56;

// Field offsets. Named rather than inlined because three callers used to hold
// their own copy of these numbers.
pub const OFF_OUTREF: usize = 0;
pub const OFF_OUT_INDEX: usize = 32;
pub const OFF_LOVELACE: usize = 36;
pub const OFF_DATUM_KIND: usize = 48;
pub const OFF_ADDR_LEN: usize = 49;
pub const OFF_N_ASSETS: usize = 51;
pub const OFF_ERA: usize = 53;
pub const OFF_DATUM_LEN: usize = 54;

/// The smallest an asset record can be: a policy id, a zero-length name and a
/// quantity. Used as a lower bound where a full parse is not wanted.
pub const MIN_ASSET_RECORD: usize = 4 + 1 + 8;

/// A datum as the walk sees it, and as a row hands it back.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Datum<'a> {
    None,
    Inline(&'a [u8]),
    /// Hash-referenced: the bytes are the 32-byte hash, and phase 1 keeps them
    /// rather than pointing at a table.
    Hash(&'a [u8]),
}

/// The `datum_kind` tags. Written by the builder, read by the reader, and named
/// here so those two cannot disagree about which number means which.
pub const DATUM_NONE: u8 = 0;
pub const DATUM_INLINE: u8 = 1;
pub const DATUM_HASH: u8 = 2;

impl<'a> Datum<'a> {
    pub fn tag(&self) -> u8 {
        match self {
            Datum::None => DATUM_NONE,
            Datum::Inline(_) => DATUM_INLINE,
            Datum::Hash(_) => DATUM_HASH,
        }
    }

    /// The bytes that follow the asset records — empty for `None`.
    pub fn payload(&self) -> &'a [u8] {
        match self {
            Datum::None => &[],
            Datum::Inline(b) | Datum::Hash(b) => b,
        }
    }

    /// From a tag and the bytes after the assets. An unknown tag is `None`: a
    /// future kind this reader does not know about is not a kind it may interpret.
    pub fn from_tag(tag: u8, payload: &'a [u8]) -> Datum<'a> {
        match tag {
            DATUM_INLINE => Datum::Inline(payload),
            DATUM_HASH => Datum::Hash(payload),
            _ => Datum::None,
        }
    }
}

/// Encode the fixed header of a row. The variable parts follow it in one order —
/// address, asset records, datum — which is the order [`Row::parse`] walks and
/// the only place that order is written down.
///
/// Here rather than in the builder so that the writer and the reader share the
/// offsets: a test that encodes a row by hand is then encoding it through the
/// same function the builder uses, and the layout has one transcription.
#[allow(clippy::too_many_arguments)]
pub fn write_header(
    head: &mut [u8; ROW_HEADER],
    outref: (&[u8; 32], u32),
    era: u8,
    lovelace: u64,
    addr_len: u16,
    n_assets: u16,
    datum: &Datum<'_>,
) {
    head[OFF_OUTREF..OFF_OUTREF + 32].copy_from_slice(outref.0);
    head[OFF_OUT_INDEX..OFF_OUT_INDEX + 4].copy_from_slice(&outref.1.to_le_bytes());
    head[OFF_LOVELACE..OFF_LOVELACE + 8].copy_from_slice(&lovelace.to_le_bytes());
    // 44..48 datum_ref: unused in phase 1.
    head[OFF_DATUM_KIND] = datum.tag();
    head[OFF_ADDR_LEN..OFF_ADDR_LEN + 2].copy_from_slice(&addr_len.to_le_bytes());
    head[OFF_N_ASSETS..OFF_N_ASSETS + 2].copy_from_slice(&n_assets.to_le_bytes());
    head[OFF_ERA] = era;
    head[OFF_DATUM_LEN..OFF_DATUM_LEN + 2]
        .copy_from_slice(&(datum.payload().len() as u16).to_le_bytes());
}

/// One asset record, encoded as it appears inside a row.
pub fn write_asset_record(out: &mut Vec<u8>, policy_id: u32, name: &[u8], quantity: u64) {
    out.extend_from_slice(&policy_id.to_le_bytes());
    out.push(name.len() as u8);
    out.extend_from_slice(name);
    out.extend_from_slice(&quantity.to_le_bytes());
}

/// Why a row could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowRead {
    /// The row does not fit inside `rows.bin`.
    OutOfRange,
    /// The bytes are inside the file but are not a row.
    Short,
}

/// One live output, borrowed from the mapped row store.
#[derive(Clone, Copy, Debug)]
pub struct Row<'a> {
    head: &'a [u8],
    address: &'a [u8],
    assets: &'a [u8],
    datum: Datum<'a>,
}

impl<'a> Row<'a> {
    /// Parse the bytes at a row offset. `bytes` is the file from that offset
    /// onwards, so every read here is bounded by the mapping and a mis-pointed
    /// offset cannot run past it.
    pub fn parse(bytes: &'a [u8]) -> Result<Row<'a>, RowRead> {
        let head = bytes.get(..ROW_HEADER).ok_or(RowRead::Short)?;
        let addr_len = addr_len(head) as usize;
        let n_assets = n_assets(head) as usize;
        let datum_len = datum_len(head) as usize;

        let addr_at = ROW_HEADER;
        let address = bytes
            .get(addr_at..addr_at + addr_len)
            .ok_or(RowRead::OutOfRange)?;

        // The asset records are what locate the datum, so they are walked rather
        // than skipped: each is `4 + 1 + name_len + 8`, and the sum is the only
        // way to know where the variable part ends.
        let mut at = addr_at + addr_len;
        for _ in 0..n_assets {
            let rec = bytes.get(at..).ok_or(RowRead::OutOfRange)?;
            let name_len = *rec.get(4).ok_or(RowRead::OutOfRange)? as usize;
            at = at
                .checked_add(MIN_ASSET_RECORD + name_len)
                .ok_or(RowRead::OutOfRange)?;
            if at > bytes.len() {
                return Err(RowRead::OutOfRange);
            }
        }
        let assets = &bytes[addr_at + addr_len..at];
        let datum_bytes = bytes.get(at..at + datum_len).ok_or(RowRead::OutOfRange)?;
        let datum = Datum::from_tag(head[OFF_DATUM_KIND], datum_bytes);
        Ok(Row {
            head,
            address,
            assets,
            datum,
        })
    }

    /// The row's own key. The row carries it in full — not a prefix — because a
    /// fold resolves ~138 M spends and a confirm read is ~2 ms against ~µs for a
    /// row already in hand (`B2`).
    pub fn outref(&self) -> ([u8; 32], u32) {
        let mut h = [0u8; 32];
        h.copy_from_slice(&self.head[OFF_OUTREF..OFF_OUTREF + 32]);
        let index = u32::from_le_bytes(
            self.head[OFF_OUT_INDEX..OFF_OUT_INDEX + 4]
                .try_into()
                .expect("4 bytes"),
        );
        (h, index)
    }

    pub fn lovelace(&self) -> u64 {
        u64::from_le_bytes(
            self.head[OFF_LOVELACE..OFF_LOVELACE + 8]
                .try_into()
                .expect("8 bytes"),
        )
    }

    pub fn era(&self) -> u8 {
        self.head[OFF_ERA]
    }

    /// The address bytes, exactly as they were written — the raw encoding, not a
    /// bech32 string. A consumer that wants the string encodes it; a consumer
    /// matching a credential reads it through `crate::address`.
    pub fn address(&self) -> &'a [u8] {
        self.address
    }

    pub fn datum(&self) -> Datum<'a> {
        self.datum
    }

    pub fn n_assets(&self) -> usize {
        n_assets(self.head) as usize
    }

    /// The row's asset records, borrowed. Each yields the *interned* policy id,
    /// so a caller resolves it through the artifact's policy table — which is
    /// what keeps a 28-byte policy off every one of 18 million asset entries.
    pub fn assets(&self) -> Assets<'a> {
        Assets {
            bytes: self.assets,
            at: 0,
        }
    }
}

/// One asset record: an interned policy id, a name and a quantity.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Asset<'a> {
    pub policy: u32,
    pub name: &'a [u8],
    pub quantity: u64,
}

impl Asset<'_> {
    /// The policy's leading 8 bytes — the prefix `by_policy`'s entries carry —
    /// once the id is resolved through the table.
    pub fn prefix(policy: &[u8; POLICY_ID_BYTES]) -> u64 {
        u64::from_le_bytes(policy[0..8].try_into().expect("8 bytes"))
    }
}

/// The asset records of one row, in file order. A value's assets are grouped by
/// policy (it is a `BTreeMap` keyed by one), so consecutive records repeat a
/// policy id and a caller that wants policies rather than names can dedup by
/// comparing with the previous — which is what both the ordering and the policy
/// confirm do.
#[derive(Clone, Copy, Debug)]
pub struct Assets<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Iterator for Assets<'a> {
    type Item = Asset<'a>;

    fn next(&mut self) -> Option<Asset<'a>> {
        let rec = self.bytes.get(self.at..)?;
        if rec.len() < MIN_ASSET_RECORD {
            return None;
        }
        let name_len = rec[4] as usize;
        let name = rec.get(5..5 + name_len)?;
        let qty_at = 5 + name_len;
        let quantity =
            u64::from_le_bytes(rec.get(qty_at..qty_at + 8)?.try_into().expect("8 bytes"));
        self.at += MIN_ASSET_RECORD + name_len;
        Some(Asset {
            policy: u32::from_le_bytes(rec[0..4].try_into().expect("4 bytes")),
            name,
            quantity,
        })
    }
}

pub fn addr_len(head: &[u8]) -> u16 {
    u16::from_le_bytes(
        head[OFF_ADDR_LEN..OFF_ADDR_LEN + 2]
            .try_into()
            .expect("2 bytes"),
    )
}

pub fn n_assets(head: &[u8]) -> u16 {
    u16::from_le_bytes(
        head[OFF_N_ASSETS..OFF_N_ASSETS + 2]
            .try_into()
            .expect("2 bytes"),
    )
}

pub fn datum_len(head: &[u8]) -> u16 {
    u16::from_le_bytes(
        head[OFF_DATUM_LEN..OFF_DATUM_LEN + 2]
            .try_into()
            .expect("2 bytes"),
    )
}

// A row's `addr_len` is bounded by the `u16` the header holds, which is the same
// bound the writer refuses past — see `format::LAYOUT_VERSION`. Nothing here needs
// to enforce it again: `Row::parse` is bounds-checked against the mapping, so a
// claim larger than the file is `OutOfRange` whatever the header says.

#[cfg(test)]
mod tests {
    use super::*;

    /// A row built by hand, so the parse is checked against bytes rather than
    /// against the builder that produced them. The builder is the other half of
    /// this agreement and a test that used it would be testing it.
    fn a_row() -> Vec<u8> {
        let mut v = vec![0u8; ROW_HEADER];
        v[OFF_OUTREF..OFF_OUTREF + 32].copy_from_slice(&[0x11; 32]);
        v[OFF_OUT_INDEX..OFF_OUT_INDEX + 4].copy_from_slice(&7u32.to_le_bytes());
        v[OFF_LOVELACE..OFF_LOVELACE + 8].copy_from_slice(&1_000_000u64.to_le_bytes());
        v[OFF_DATUM_KIND] = 1;
        v[OFF_ADDR_LEN..OFF_ADDR_LEN + 2].copy_from_slice(&29u16.to_le_bytes());
        v[OFF_N_ASSETS..OFF_N_ASSETS + 2].copy_from_slice(&2u16.to_le_bytes());
        v[OFF_ERA] = 5;
        v[OFF_DATUM_LEN..OFF_DATUM_LEN + 2].copy_from_slice(&3u16.to_le_bytes());
        v.extend_from_slice(&[0x61]);
        v.extend_from_slice(&[0xAB; 28]);
        for (id, name, qty) in [(9u32, &b"one"[..], 5u64), (9, b"two", 6)] {
            v.extend_from_slice(&id.to_le_bytes());
            v.push(name.len() as u8);
            v.extend_from_slice(name);
            v.extend_from_slice(&qty.to_le_bytes());
        }
        v.extend_from_slice(&[0xDE, 0xAD, 0xBE]);
        v
    }

    #[test]
    fn a_row_parses_to_its_parts() {
        let bytes = a_row();
        let row = Row::parse(&bytes).expect("parses");
        assert_eq!(row.outref().1, 7);
        assert_eq!(row.outref().0, [0x11; 32]);
        assert_eq!(row.lovelace(), 1_000_000);
        assert_eq!(row.era(), 5);
        assert_eq!(row.address().len(), 29);
        assert_eq!(row.address()[0], 0x61);
        assert_eq!(row.n_assets(), 2);
        assert!(matches!(row.datum(), Datum::Inline(&[0xDE, 0xAD, 0xBE])));
        // The parse stops at the row's end: the sentinel past the datum is not
        // read, which is the only thing that proves the length arithmetic landed
        // where the writer put the datum rather than a byte either side of it.
        let mut extended = bytes.clone();
        extended.push(0xFF);
        let row = Row::parse(&extended).expect("parses with a trailing byte");
        assert!(matches!(row.datum(), Datum::Inline(&[0xDE, 0xAD, 0xBE])));

        let assets: Vec<Asset<'_>> = row.assets().collect();
        assert_eq!(assets.len(), 2);
        assert_eq!(assets[0].policy, 9);
        assert_eq!(assets[0].name, b"one");
        assert_eq!(assets[0].quantity, 5);
        assert_eq!(assets[1].name, b"two");
        assert_eq!(assets[1].quantity, 6);
    }

    #[test]
    fn a_row_that_runs_off_the_end_is_not_a_row() {
        let bytes = a_row();
        // Every truncation must be refused, not read as a shorter row: the datum
        // is at the end, so a short read would otherwise silently yield a row
        // whose datum is some other row's address bytes.
        for cut in 1..bytes.len() {
            assert!(
                Row::parse(&bytes[..cut]).is_err(),
                "{} bytes of a {}-byte row parsed",
                cut,
                bytes.len()
            );
        }
        assert!(Row::parse(&bytes).is_ok());
        assert!(Row::parse(&[]).is_err());
    }

    #[test]
    fn a_mis_pointed_offset_cannot_run_past_the_mapping() {
        // A header that claims more than exists is `OutOfRange`, not a panic and
        // not a silent short read.
        let mut bytes = vec![0u8; ROW_HEADER];
        bytes[OFF_ADDR_LEN..OFF_ADDR_LEN + 2].copy_from_slice(&29u16.to_le_bytes());
        bytes[OFF_N_ASSETS..OFF_N_ASSETS + 2].copy_from_slice(&1u16.to_le_bytes());
        let parsed = Row::parse(&bytes);
        assert!(matches!(parsed, Err(RowRead::OutOfRange)), "{parsed:?}");
    }
}
