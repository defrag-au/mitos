//! The writer — rows in, one stamped file out, never holding more than one row
//! group in memory.

use std::io::Write;
use std::sync::Arc;

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::format::KeyValue;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::ColumnPath;

use crate::Result;
use crate::groups::{Bloom, GroupPolicy, GroupSummary, Grouper, Placement, Row, RowKind};
use crate::schema::{Column, MESSAGE_TYPE, Movement, Stamp, kv};

/// What a finished file holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Written {
    pub rows: u64,
    pub groups: Vec<GroupSummary>,
    /// `None` when no rows were written — an empty file is legal and carries
    /// its stamp, so a partition with nothing in its range still says so.
    pub min_slot: Option<u64>,
    pub max_slot: Option<u64>,
}

/// Streams [`Movement`]s into a stamped archive.
///
/// Rows must arrive in slot order and grouped by transaction; the [`Grouper`]
/// refuses anything else, because a row placed in the wrong group is a
/// statistic that lies.
pub struct ArchiveWriter<W: Write + Send> {
    inner: SerializedFileWriter<W>,
    grouper: Grouper,
    buf: Vec<Movement>,
    groups: Vec<GroupSummary>,
    rows: u64,
    min_slot: Option<u64>,
    max_slot: Option<u64>,
}

impl<W: Write + Send> ArchiveWriter<W> {
    pub fn new(sink: W, stamp: &Stamp, policy: GroupPolicy) -> Result<Self> {
        let schema = Arc::new(parse_message_type(MESSAGE_TYPE)?);
        let mut kvs = stamp.to_key_values();
        kvs.push(KeyValue::new(
            kv::BUCKET_SECS.to_string(),
            policy.bucket_secs.to_string(),
        ));
        let mut props = WriterProperties::builder()
            // SNAPPY, because every READER has to link the codec — including
            // the wasm bundle, where zstd is a C library. See the crate docs.
            .set_compression(Compression::SNAPPY)
            // Chunk-level statistics are what make the footer a histogram;
            // page-level ones would only inflate it.
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_key_value_metadata(Some(kvs));
        if policy.bloom == Bloom::PerGroup {
            // COLUMN-level ndv and fpp, not the file-level setters. Enabling
            // the filter per column installs that column's own bloom
            // properties, which then IGNORE the file-level ndv — measured:
            // the default 1,000,000 ndv put a 1 MB filter on every group,
            // 1.9 GB of filters on a 38k-row file.
            let col = ColumnPath::from(Column::TxHash.name());
            props = props
                .set_column_bloom_filter_enabled(col.clone(), true)
                .set_column_bloom_filter_ndv(col.clone(), (2 * policy.min_rows).max(1_024) as u64)
                .set_column_bloom_filter_fpp(col, 0.05);
        }
        let inner = SerializedFileWriter::new(sink, schema, Arc::new(props.build()))?;
        Ok(Self {
            inner,
            grouper: Grouper::new(policy),
            buf: Vec::new(),
            groups: Vec::new(),
            rows: 0,
            min_slot: None,
            max_slot: None,
        })
    }

    pub fn push(&mut self, m: Movement) -> Result<()> {
        let row = Row {
            block_time: m.block_time,
            tx_hash: &m.tx_hash,
            net_mint: m.net_mint,
            kind: match m.is_placeholder() {
                true => RowKind::Placeholder,
                false => RowKind::Movement,
            },
        };
        if let Placement::Close(done) = self.grouper.place(row)? {
            self.flush()?;
            self.groups.push(done);
        }
        self.min_slot = Some(self.min_slot.map_or(m.slot, |s| s.min(m.slot)));
        self.max_slot = Some(self.max_slot.map_or(m.slot, |s| s.max(m.slot)));
        self.rows += 1;
        self.buf.push(m);
        Ok(())
    }

    /// Write the buffered rows as one row group.
    ///
    /// Columnar, so each column is handed over as a contiguous batch — the
    /// pivot is why rows are buffered at all, and the group is the only thing
    /// ever buffered.
    fn flush(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.buf);
        let mut group = self.inner.next_row_group()?;
        let mut col = 0usize;
        while let Some(mut w) = group.next_column()? {
            match Column::ALL[col] {
                Column::Slot => {
                    let v: Vec<i64> = rows.iter().map(|r| r.slot as i64).collect();
                    w.typed::<Int64Type>().write_batch(&v, None, None)?;
                }
                Column::BlockTime => {
                    let v: Vec<i64> = rows.iter().map(|r| r.block_time as i64).collect();
                    w.typed::<Int64Type>().write_batch(&v, None, None)?;
                }
                Column::TxHash => {
                    let v: Vec<ByteArray> = rows
                        .iter()
                        .map(|r| ByteArray::from(r.tx_hash.clone()))
                        .collect();
                    w.typed::<ByteArrayType>().write_batch(&v, None, None)?;
                }
                Column::UnitName => {
                    let v: Vec<ByteArray> = rows
                        .iter()
                        .map(|r| ByteArray::from(r.unit_name.clone()))
                        .collect();
                    w.typed::<ByteArrayType>().write_batch(&v, None, None)?;
                }
                Column::Address => {
                    let v: Vec<ByteArray> = rows
                        .iter()
                        .map(|r| ByteArray::from(r.address.as_bytes().to_vec()))
                        .collect();
                    w.typed::<ByteArrayType>().write_batch(&v, None, None)?;
                }
                Column::Amount => {
                    let v: Vec<i64> = rows.iter().map(|r| r.amount).collect();
                    w.typed::<Int64Type>().write_batch(&v, None, None)?;
                }
                Column::NetMint => {
                    let v: Vec<i64> = rows.iter().map(|r| r.net_mint).collect();
                    w.typed::<Int64Type>().write_batch(&v, None, None)?;
                }
            }
            w.close()?;
            col += 1;
        }
        group.close()?;
        Ok(())
    }

    /// Close the last group, append the side tables, write the footer.
    pub fn finish(mut self) -> Result<Written> {
        let grouper = std::mem::replace(&mut self.grouper, Grouper::new(GroupPolicy::DAILY));
        if let Some(last) = grouper.finish() {
            self.flush()?;
            self.groups.push(last);
        }
        // Known only now, which is why they are appended rather than set in
        // the properties with the stamp.
        for (k, v) in GroupSummary::encode(&self.groups) {
            self.inner
                .append_key_value_metadata(KeyValue::new(k.to_string(), v));
        }
        self.inner.close()?;
        Ok(Written {
            rows: self.rows,
            groups: self.groups,
            min_slot: self.min_slot,
            max_slot: self.max_slot,
        })
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    //! Synthetic policies with the awkward shapes real ones have: a mint
    //! burst, a long quiet tail, a busy sale day, a burn from nowhere.

    use crate::schema::Movement;

    pub const DAY: u64 = 86_400;
    /// Mainnet: slot = unix − this.
    pub const SLOT_OFFSET: u64 = 1_596_059_091 - 4_492_800;

    pub fn hash(seed: u64) -> Vec<u8> {
        // Deterministic 32 bytes with no repeats across seeds we use.
        let mut h = Vec::with_capacity(32);
        let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        for _ in 0..4 {
            x ^= x >> 33;
            x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
            h.extend_from_slice(&x.to_le_bytes());
        }
        h
    }

    pub fn addr(n: u64) -> String {
        format!("addr1q{n:0>50}")
    }

    /// `days` of activity starting at `start_unix`; day `d` has
    /// `per_day(d)` transactions, each moving one unit between two parties.
    /// Day 0 mints everything; the last day burns one unit unattributed.
    pub fn policy(start_unix: u64, days: u64, per_day: impl Fn(u64) -> u64) -> Vec<Movement> {
        // Day-aligned, so "day d" of the fixture is one bucket.
        let start_unix = start_unix / DAY * DAY;
        let mut rows = Vec::new();
        let mut seq = 0u64;
        for d in 0..days {
            let n = per_day(d);
            for i in 0..n {
                seq += 1;
                let t = start_unix + d * DAY + i * 7;
                let tx = hash(seq);
                let unit = format!("Unit{:04}", seq % 500).into_bytes();
                let mint = if d == 0 { 1 } else { 0 };
                if d != 0 {
                    rows.push(Movement {
                        slot: t - SLOT_OFFSET,
                        block_time: t,
                        tx_hash: tx.clone(),
                        unit_name: unit.clone(),
                        address: addr(seq % 37),
                        amount: -1,
                        net_mint: mint,
                    });
                }
                rows.push(Movement {
                    slot: t - SLOT_OFFSET,
                    block_time: t,
                    tx_hash: tx,
                    unit_name: unit,
                    address: addr((seq + 1) % 37),
                    amount: 1,
                    net_mint: mint,
                });
            }
        }
        // The unattributed burn: a transaction the ledger knows only from its
        // mint field.
        let t = start_unix + (days - 1) * DAY + DAY / 2;
        rows.push(Movement {
            slot: t - SLOT_OFFSET,
            block_time: t,
            tx_hash: hash(u64::MAX),
            unit_name: b"Unit0001".to_vec(),
            address: String::new(),
            amount: 0,
            net_mint: -1,
        });
        rows.sort_by_key(|r| (r.block_time, r.tx_hash.clone()));
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;
    use crate::reader::{Archive, SparseBytes};
    use crate::schema::Completeness;

    fn stamp() -> Stamp {
        Stamp {
            policy_hex: "cd".repeat(28),
            completeness: Completeness::Partial,
            walk_from: Some(1),
            walk_to: Some(2),
            covered_from: 1,
            covered_to: 2,
            sealed_unix: 3,
        }
    }

    fn write(rows: &[Movement], policy: GroupPolicy) -> (Vec<u8>, Written) {
        let mut sink = Vec::new();
        let mut w = ArchiveWriter::new(&mut sink, &stamp(), policy).unwrap();
        for r in rows {
            w.push(r.clone()).unwrap();
        }
        let written = w.finish().unwrap();
        (sink, written)
    }

    /// Everything written comes back: stamp, groups, and every row in order.
    #[test]
    fn a_file_round_trips_through_the_reader() {
        let rows = policy(1_700_000_000, 40, |d| if d == 0 { 300 } else { 3 + d % 5 });
        let (bytes, written) = write(&rows, GroupPolicy::DAILY);
        let sparse = SparseBytes::whole(bytes.into());
        let archive = Archive::open(&sparse).unwrap();
        assert_eq!(archive.stamp(), &stamp());
        assert_eq!(archive.groups(), written.groups.as_slice());
        assert_eq!(written.rows as usize, rows.len());

        let mut back = Vec::new();
        for g in 0..archive.num_groups() {
            back.extend(archive.read_group(&sparse, g).unwrap());
        }
        assert_eq!(back, rows);
        assert!(back.iter().any(Movement::is_placeholder));
    }

    /// An empty partition is a legal file: it carries its stamp and no groups.
    #[test]
    fn an_empty_file_still_carries_its_stamp() {
        let (bytes, written) = write(&[], GroupPolicy::DAILY);
        assert_eq!(written.rows, 0);
        assert_eq!(written.min_slot, None);
        let sparse = SparseBytes::whole(bytes.into());
        let archive = Archive::open(&sparse).unwrap();
        assert_eq!(archive.stamp(), &stamp());
        assert_eq!(archive.num_groups(), 0);
        assert!(archive.density().is_empty());
    }

    /// THE FOOTER-SIZE MEASUREMENT — the third unknown in the design doc.
    ///
    /// Five years of daily activity, 7 columns. Strict per-day groups against
    /// the adaptive floor. The numbers are printed so a run records them; the
    /// assertions bound them so a regression that made the footer balloon
    /// fails here rather than in a Worker's memory limit.
    #[test]
    fn five_years_of_daily_groups_footer_size() {
        let days = 5 * 365;
        // ~10 tx/day, one 2,000-tx mint day: ~ the measured 41 MB sqlite ledger.
        let rows = policy(1_600_000_000, days, |d| if d == 0 { 2_000 } else { 10 });
        for (label, pol) in [
            ("strict daily", GroupPolicy::STRICT_DAILY),
            ("adaptive 5k floor", GroupPolicy::DAILY),
            (
                "adaptive, no bloom",
                GroupPolicy {
                    bloom: Bloom::None,
                    ..GroupPolicy::DAILY
                },
            ),
        ] {
            let (bytes, written) = write(&rows, pol);
            let footer = crate::reader::footer_length(&bytes[bytes.len() - 8..]).unwrap();
            let sparse = SparseBytes::whole(bytes.clone().into());
            let a = Archive::open(&sparse).unwrap();
            let bloom_bytes: u64 = (0..a.num_groups())
                .filter_map(|g| a.bloom_range(g))
                .map(|(_, len)| len)
                .sum();
            let side_tables: usize = GroupSummary::encode(&written.groups)
                .iter()
                .map(|(_, v)| v.len())
                .sum();
            let buckets = a.density().len();
            println!(
                "footer-size: {label}: rows={} groups={} buckets={buckets} file={} B \
                 footer={footer} B (side tables {side_tables} B) bloom={bloom_bytes} B",
                written.rows,
                written.groups.len(),
                bytes.len()
            );
            assert!(
                footer < 2 * 1024 * 1024,
                "{label}: footer {footer} B exceeds a Worker-friendly bound"
            );
            assert_eq!(
                buckets, days as usize,
                "{label}: the histogram is daily whatever the grouping"
            );
        }
    }
}
