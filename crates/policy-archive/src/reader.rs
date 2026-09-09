//! The reader — over byte ranges a caller fetches, never over a file handle.
//!
//! # The protocol
//!
//! 1. Fetch the tail: [`footer_request`] says which bytes. Hand them to a
//!    [`SparseBytes`] and ask [`footer_length`] whether they reach the whole
//!    footer; if not, fetch the exact tail it names.
//! 2. [`Archive::open`] parses the footer: stamp, groups, density. For the
//!    density tier this is the END — nothing else is fetched.
//! 3. For rows: [`Archive::groups_overlapping`] picks groups by slot,
//!    [`Archive::group_range`] says which bytes each needs, the caller fetches
//!    them into the same `SparseBytes`, and [`Archive::read_group`] decodes.
//! 4. For one transaction: [`Archive::bloom_range`] per group, fetched, then
//!    [`Archive::candidate_groups`] narrows to the groups whose filter says
//!    maybe, and step 3 finishes it.
//!
//! Everything here is synchronous and allocation-light. The caller owns the
//! transport — `fetch` in a Worker, an R2 binding, `std::fs` on the box — and
//! this crate owns the format.

use std::collections::BTreeMap;
use std::io::Cursor;

use bytes::Bytes;
use parquet::column::reader::get_typed_column_reader;
use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::ReaderProperties;
use parquet::file::reader::{ChunkReader, FileReader, Length};
use parquet::file::serialized_reader::{ReadOptionsBuilder, SerializedFileReader};
use parquet::file::statistics::Statistics;

use crate::density::{DensityBucket, from_groups};
use crate::groups::GroupSummary;
use crate::schema::{Column, Movement, Stamp};
use crate::{Error, Result};

/// Parquet's fixed tail: 4-byte footer length + `PAR1`.
pub const FOOTER_TAIL: u64 = 8;

/// A sensible first tail request. Measured footers for a five-year policy at
/// daily granularity sit well under this, so one request usually suffices.
pub const FOOTER_HINT: u64 = 64 * 1024;

/// Which bytes to fetch first: the last `hint` of the file, or all of it.
pub fn footer_request(file_len: u64, hint: u64) -> (u64, u64) {
    let len = hint.min(file_len);
    (file_len - len, len)
}

/// The footer's total length — metadata plus the 8-byte tail — from the last
/// eight bytes of the file. If this exceeds what was fetched, fetch exactly
/// this much from the end.
pub fn footer_length(last_eight: &[u8]) -> Result<u64> {
    let tail: [u8; 8] = last_eight
        .get(last_eight.len().saturating_sub(8)..)
        .and_then(|s| s.try_into().ok())
        .ok_or(Error::TooShort(last_eight.len() as u64))?;
    let meta = ParquetMetaDataReader::decode_footer_tail(&tail)?.metadata_length() as u64;
    Ok(meta + FOOTER_TAIL)
}

/// Bytes of a remote file, present only where they have been fetched.
///
/// Implements parquet's [`ChunkReader`], so the crate's own reader runs over
/// it unchanged; a read outside a fetched range is an error naming the range,
/// not a silent zero.
#[derive(Debug, Clone)]
pub struct SparseBytes {
    len: u64,
    /// start → bytes. Non-overlapping; adjacent ranges are merged on insert.
    ranges: BTreeMap<u64, Bytes>,
}

impl SparseBytes {
    pub fn new(len: u64) -> Self {
        Self {
            len,
            ranges: BTreeMap::new(),
        }
    }

    /// A whole file in memory — the on-box case, and tests.
    pub fn whole(bytes: Bytes) -> Self {
        let mut s = Self::new(bytes.len() as u64);
        s.insert(0, bytes);
        s
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Bytes fetched so far — what a caller reports as "bytes read".
    pub fn fetched(&self) -> u64 {
        self.ranges.values().map(|b| b.len() as u64).sum()
    }

    /// Record a fetched range. Overlaps with existing ranges are tolerated
    /// (the new bytes win); adjacency is merged so a read spanning two
    /// requests still succeeds.
    pub fn insert(&mut self, start: u64, bytes: Bytes) {
        if bytes.is_empty() {
            return;
        }
        let mut start = start;
        let mut buf = bytes.to_vec();
        // Absorb anything overlapping or touching.
        let end = start + buf.len() as u64;
        let overlapping: Vec<u64> = self
            .ranges
            .range(..=end)
            .filter(|(s, b)| **s + b.len() as u64 >= start)
            .map(|(s, _)| *s)
            .collect();
        for s in overlapping {
            let existing = self.ranges.remove(&s).expect("listed");
            let e_end = s + existing.len() as u64;
            if s < start {
                let mut merged = existing[..(start - s) as usize].to_vec();
                merged.extend_from_slice(&buf);
                buf = merged;
                start = s;
            }
            if e_end > start + buf.len() as u64 {
                let keep_from = (start + buf.len() as u64 - s) as usize;
                buf.extend_from_slice(&existing[keep_from..]);
            }
        }
        self.ranges.insert(start, Bytes::from(buf));
    }

    /// Is `[start, start+len)` fully present?
    pub fn has(&self, start: u64, len: u64) -> bool {
        self.slice(start, len).is_ok()
    }

    fn slice(&self, start: u64, len: u64) -> Result<Bytes> {
        let end = start + len;
        let Some((s, b)) = self.ranges.range(..=start).next_back() else {
            return Err(Error::NotFetched { start, end });
        };
        if s + b.len() as u64 >= end {
            let off = (start - s) as usize;
            Ok(b.slice(off..off + len as usize))
        } else {
            Err(Error::NotFetched { start, end })
        }
    }

    fn from(&self, start: u64) -> Result<Bytes> {
        let Some((s, b)) = self.ranges.range(..=start).next_back() else {
            return Err(Error::NotFetched {
                start,
                end: start + 1,
            });
        };
        let off = (start - s) as usize;
        if off > b.len() {
            return Err(Error::NotFetched {
                start,
                end: start + 1,
            });
        }
        Ok(b.slice(off..))
    }
}

impl Length for SparseBytes {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for SparseBytes {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        self.from(start)
            .map(Cursor::new)
            .map_err(|e| parquet::errors::ParquetError::General(e.to_string()))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        self.slice(start, length as u64)
            .map_err(|e| parquet::errors::ParquetError::General(e.to_string()))
    }
}

/// An opened archive: the footer, decoded. Holds no data pages.
pub struct Archive {
    metadata: ParquetMetaData,
    stamp: Stamp,
    bucket_secs: u64,
    groups: Vec<GroupSummary>,
}

/// Whether a file reader should load the `tx_hash` bloom filters when it
/// opens a row group. They live outside the group's data range, so a reader
/// that only fetched the data must not ask for them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BloomRead {
    Load,
    Skip,
}

impl Archive {
    /// Parse the footer. Needs only the tail bytes fetched.
    pub fn open(bytes: &SparseBytes) -> Result<Self> {
        let metadata = ParquetMetaDataReader::new().parse_and_finish(bytes)?;
        let kvs = metadata
            .file_metadata()
            .key_value_metadata()
            .cloned()
            .unwrap_or_default();
        let stamp = Stamp::from_key_values(&kvs)?;
        let bucket_secs: u64 = crate::schema::required(&kvs, crate::schema::kv::BUCKET_SECS)?;
        let rows: Vec<u64> = metadata
            .row_groups()
            .iter()
            .map(|g| g.num_rows() as u64)
            .collect();
        let groups = GroupSummary::decode(&kvs, &rows, bucket_secs)?;
        Ok(Self {
            metadata,
            stamp,
            bucket_secs,
            groups,
        })
    }

    pub fn stamp(&self) -> &Stamp {
        &self.stamp
    }

    /// The histogram's resolution, seconds.
    pub fn bucket_secs(&self) -> u64 {
        self.bucket_secs
    }

    pub fn groups(&self) -> &[GroupSummary] {
        &self.groups
    }

    pub fn num_groups(&self) -> usize {
        self.metadata.num_row_groups()
    }

    pub fn num_rows(&self) -> u64 {
        self.metadata.file_metadata().num_rows() as u64
    }

    /// The density tier. Nothing beyond the footer is read.
    pub fn density(&self) -> Vec<DensityBucket> {
        from_groups(&self.groups, self.bucket_secs)
    }

    /// Slot range of one group, from its column statistics.
    pub fn slot_range(&self, group: usize) -> Option<(u64, u64)> {
        let col = self.metadata.row_group(group).column(Column::Slot.index());
        match col.statistics()? {
            Statistics::Int64(s) => Some((*s.min_opt()? as u64, *s.max_opt()? as u64)),
            _ => None,
        }
    }

    /// Groups whose slot range intersects `[from, to]`, ascending.
    pub fn groups_overlapping(&self, from: u64, to: u64) -> Vec<usize> {
        (0..self.num_groups())
            .filter(|&g| {
                self.slot_range(g)
                    .is_some_and(|(lo, hi)| lo <= to && hi >= from)
            })
            .collect()
    }

    /// The bytes one group's data pages occupy: `(start, len)`. Column chunks
    /// of a row group are written contiguously, so this is one range.
    pub fn group_range(&self, group: usize) -> (u64, u64) {
        let rg = self.metadata.row_group(group);
        let (mut lo, mut hi) = (u64::MAX, 0u64);
        for c in rg.columns() {
            let (start, len) = c.byte_range();
            lo = lo.min(start);
            hi = hi.max(start + len);
        }
        (lo, hi.saturating_sub(lo))
    }

    /// Where one group's `tx_hash` bloom filter lives, if it has one.
    pub fn bloom_range(&self, group: usize) -> Option<(u64, u64)> {
        let col = self
            .metadata
            .row_group(group)
            .column(Column::TxHash.index());
        let start = col.bloom_filter_offset()? as u64;
        let len = col.bloom_filter_length()? as u64;
        Some((start, len))
    }

    /// Groups that MIGHT hold `tx_hash`, by bloom filter. Needs each group's
    /// [`Self::bloom_range`] fetched; a group with no filter is always a
    /// candidate, because absence of a filter is not absence of the row.
    pub fn candidate_groups(&self, bytes: &SparseBytes, tx_hash: &[u8]) -> Result<Vec<usize>> {
        let reader = self.file_reader(bytes, BloomRead::Load)?;
        let needle = ByteArray::from(tx_hash.to_vec());
        let mut out = Vec::new();
        for g in 0..self.num_groups() {
            if self.bloom_range(g).is_none() {
                out.push(g);
                continue;
            }
            let rg = reader.get_row_group(g)?;
            match rg.get_column_bloom_filter(Column::TxHash.index()) {
                Some(sbbf) if !sbbf.check(&needle) => {}
                _ => out.push(g),
            }
        }
        Ok(out)
    }

    /// Decode one group's rows. Needs [`Self::group_range`] fetched.
    pub fn read_group(&self, bytes: &SparseBytes, group: usize) -> Result<Vec<Movement>> {
        // The data range only. Asking for the filters here would demand
        // bytes a window read never fetched.
        let reader = self.file_reader(bytes, BloomRead::Skip)?;
        let rg = reader.get_row_group(group)?;
        let n = rg.metadata().num_rows() as usize;

        let mut slot = Vec::with_capacity(n);
        let mut block_time = Vec::with_capacity(n);
        let mut tx_hash: Vec<ByteArray> = Vec::with_capacity(n);
        let mut unit_name: Vec<ByteArray> = Vec::with_capacity(n);
        let mut address: Vec<ByteArray> = Vec::with_capacity(n);
        let mut amount = Vec::with_capacity(n);
        let mut net_mint = Vec::with_capacity(n);

        macro_rules! read_i64 {
            ($col:expr, $into:expr) => {{
                let mut r =
                    get_typed_column_reader::<Int64Type>(rg.get_column_reader($col.index())?);
                r.read_records(n, None, None, &mut $into)?;
            }};
        }
        macro_rules! read_bytes {
            ($col:expr, $into:expr) => {{
                let mut r =
                    get_typed_column_reader::<ByteArrayType>(rg.get_column_reader($col.index())?);
                r.read_records(n, None, None, &mut $into)?;
            }};
        }
        read_i64!(Column::Slot, slot);
        read_i64!(Column::BlockTime, block_time);
        read_bytes!(Column::TxHash, tx_hash);
        read_bytes!(Column::UnitName, unit_name);
        read_bytes!(Column::Address, address);
        read_i64!(Column::Amount, amount);
        read_i64!(Column::NetMint, net_mint);

        Ok((0..n)
            .map(|i| Movement {
                slot: slot[i] as u64,
                block_time: block_time[i] as u64,
                tx_hash: tx_hash[i].data().to_vec(),
                unit_name: unit_name[i].data().to_vec(),
                address: String::from_utf8_lossy(address[i].data()).into_owned(),
                amount: amount[i],
                net_mint: net_mint[i],
            })
            .collect())
    }

    /// One transaction's rows, from the groups the bloom filters allow.
    /// Needs the candidate groups' [`Self::group_range`] fetched — call
    /// [`Self::candidate_groups`] first to learn which.
    pub fn find_tx(&self, bytes: &SparseBytes, tx_hash: &[u8]) -> Result<Vec<Movement>> {
        let mut out = Vec::new();
        for g in self.candidate_groups(bytes, tx_hash)? {
            out.extend(
                self.read_group(bytes, g)?
                    .into_iter()
                    .filter(|m| m.tx_hash == tx_hash),
            );
        }
        Ok(out)
    }

    fn file_reader(
        &self,
        bytes: &SparseBytes,
        bloom: BloomRead,
    ) -> Result<SerializedFileReader<SparseBytes>> {
        let props = ReaderProperties::builder()
            .set_read_bloom_filter(bloom == BloomRead::Load)
            .build();
        let options = ReadOptionsBuilder::new()
            .with_reader_properties(props)
            .build();
        Ok(SerializedFileReader::new_with_options(
            bytes.clone(),
            options,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::groups::GroupPolicy;
    use crate::schema::Completeness;
    use crate::writer::ArchiveWriter;
    use crate::writer::fixtures::{hash, policy};

    fn sealed(rows: &[Movement]) -> Vec<u8> {
        let stamp = Stamp {
            policy_hex: "ef".repeat(28),
            completeness: Completeness::Complete,
            walk_from: Some(0),
            walk_to: Some(1),
            covered_from: 0,
            covered_to: 1,
            sealed_unix: 0,
        };
        // A floor small enough that a synthetic policy of a few hundred rows
        // still yields many groups — the production floor would fold the
        // whole fixture into one and prove nothing about pruning.
        let policy = GroupPolicy {
            min_rows: 64,
            max_rows: 1_000,
            ..GroupPolicy::DAILY
        };
        let mut sink = Vec::new();
        let mut w = ArchiveWriter::new(&mut sink, &stamp, policy).unwrap();
        for r in rows {
            w.push(r.clone()).unwrap();
        }
        w.finish().unwrap();
        sink
    }

    /// A pretend remote: hands out ranges and counts what was asked for.
    struct Remote {
        file: Bytes,
        requests: std::cell::Cell<usize>,
    }

    impl Remote {
        fn fetch(&self, into: &mut SparseBytes, start: u64, len: u64) {
            self.requests.set(self.requests.get() + 1);
            into.insert(
                start,
                self.file.slice(start as usize..(start + len) as usize),
            );
        }
    }

    /// THE RANGE PROTOCOL, END TO END, with the transport simulated. Footer
    /// first, density from it alone, then only the groups a window needs, then
    /// one transaction through the bloom filters.
    #[test]
    fn the_range_protocol_reads_a_window_and_a_transaction() {
        let rows = policy(1_700_000_000, 60, |d| if d == 0 { 400 } else { 8 });
        let remote = Remote {
            file: sealed(&rows).into(),
            requests: std::cell::Cell::new(0),
        };
        let file_len = remote.file.len() as u64;
        let mut bytes = SparseBytes::new(file_len);

        // 1. the tail
        let (start, len) = footer_request(file_len, FOOTER_HINT);
        remote.fetch(&mut bytes, start, len);
        let need = footer_length(&remote.file[remote.file.len() - 8..]).unwrap();
        if need > len {
            let (start, len) = footer_request(file_len, need);
            remote.fetch(&mut bytes, start, len);
        }

        // 2. density from the footer, nothing else fetched
        let archive = Archive::open(&bytes).unwrap();
        let density = archive.density();
        assert_eq!(density.iter().map(|b| b.txs).sum::<u64>(), 400 + 59 * 8 + 1);
        assert_eq!(density[0].mints, 400, "day 0 is the mint");
        assert_eq!(density.last().unwrap().burns, 1, "the unattributed burn");
        // Exactly the tail was fetched — the hint, or the footer if larger —
        // and it was enough.
        let after_footer = bytes.fetched();
        assert_eq!(after_footer, len.max(need));
        assert!(
            need <= after_footer,
            "the footer is {need} of {file_len} bytes"
        );

        // 3. a window: the last ten days
        let last = rows.last().unwrap().slot;
        let from = last - 10 * 86_400;
        let groups = archive.groups_overlapping(from, last);
        assert!(!groups.is_empty());
        assert!(
            groups.len() < archive.num_groups(),
            "pruned by slot statistics"
        );
        for &g in &groups {
            let (s, l) = archive.group_range(g);
            remote.fetch(&mut bytes, s, l);
        }
        let mut window = Vec::new();
        for &g in &groups {
            window.extend(archive.read_group(&bytes, g).unwrap());
        }
        let expected: Vec<&Movement> = rows.iter().filter(|r| r.slot >= from).collect();
        let got: Vec<&Movement> = window.iter().filter(|r| r.slot >= from).collect();
        assert_eq!(got, expected);

        // 4. one transaction, by hash, through the bloom filters
        let wanted = hash(200); // a mint-day transaction
        for g in 0..archive.num_groups() {
            if let Some((s, l)) = archive.bloom_range(g) {
                remote.fetch(&mut bytes, s, l);
            }
        }
        let candidates = archive.candidate_groups(&bytes, &wanted).unwrap();
        assert!(
            candidates.len() < archive.num_groups(),
            "the filters ruled groups out: {candidates:?} of {}",
            archive.num_groups()
        );
        for &g in &candidates {
            let (s, l) = archive.group_range(g);
            remote.fetch(&mut bytes, s, l);
        }
        let found = archive.find_tx(&bytes, &wanted).unwrap();
        let expected: Vec<&Movement> = rows.iter().filter(|r| r.tx_hash == wanted).collect();
        assert_eq!(found.iter().collect::<Vec<_>>(), expected);
        assert!(!found.is_empty());

        // A hash that is not there reads as nothing, from no data pages
        // beyond what the filters allow.
        let absent = archive.find_tx(&bytes, &hash(999_999)).unwrap();
        assert!(absent.is_empty());
        println!(
            "range-protocol: {} requests, {} of {} bytes fetched",
            remote.requests.get(),
            bytes.fetched(),
            file_len
        );
    }

    /// Reading bytes nobody fetched is an ERROR naming the range — never a
    /// zero-filled page decoded into plausible rows.
    #[test]
    fn an_unfetched_range_is_refused_by_name() {
        let rows = policy(1_700_000_000, 3, |_| 5);
        let file: Bytes = sealed(&rows).into();
        let mut bytes = SparseBytes::new(file.len() as u64);
        let (s, l) = footer_request(file.len() as u64, FOOTER_HINT);
        bytes.insert(s, file.slice(s as usize..(s + l) as usize));
        let archive = Archive::open(&bytes).unwrap();
        if archive.group_range(0).0 + archive.group_range(0).1 <= s {
            let err = archive.read_group(&bytes, 0).unwrap_err();
            assert!(matches!(err, Error::Parquet(_)), "{err}");
            assert!(err.to_string().contains("not been fetched"), "{err}");
        }
    }

    /// Adjacent and overlapping inserts merge, so a read spanning two
    /// requests works.
    #[test]
    fn sparse_ranges_merge_when_they_touch() {
        let mut s = SparseBytes::new(10);
        s.insert(0, Bytes::from_static(b"abc"));
        s.insert(3, Bytes::from_static(b"def"));
        assert_eq!(s.slice(1, 4).unwrap().as_ref(), b"bcde");
        s.insert(2, Bytes::from_static(b"XYZW"));
        assert_eq!(s.slice(0, 6).unwrap().as_ref(), b"abXYZW");
        s.insert(8, Bytes::from_static(b"zz"));
        assert!(!s.has(6, 2));
        assert!(s.has(8, 2));
        assert_eq!(s.fetched(), 8);
    }

    #[test]
    fn the_footer_request_never_overshoots_a_small_file() {
        assert_eq!(footer_request(100, FOOTER_HINT), (0, 100));
        assert_eq!(
            footer_request(1 << 20, 1 << 16),
            ((1 << 20) - (1 << 16), 1 << 16)
        );
    }
}
