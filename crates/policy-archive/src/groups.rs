//! Row-group placement — time-aligned, with an adaptive row-count floor — and
//! the per-bucket counts that make the footer a histogram.
//!
//! # Two units, deliberately
//!
//! A **bucket** is the histogram's unit: one calendar day (or whatever
//! [`GroupPolicy::bucket_secs`] says). A **row group** is Parquet's unit: what
//! a reader fetches and decodes as one range. They are not the same thing and
//! the first version of this module conflated them, which measured badly in
//! both directions — one group per day put ~1 KB of column metadata in the
//! footer per day (1.8 MB for five years), while merging days to a 5,000-row
//! floor folded a quiet policy into 250-day groups and destroyed the daily
//! shape the footer was meant to carry.
//!
//! So groups are sized for READING — quiet buckets merged up to a floor, busy
//! ones split at a cap, always on a transaction boundary — and the footer's
//! side tables count per BUCKET, whatever grouping the buckets landed in. A
//! reader gets the daily histogram at full resolution from a footer whose
//! size follows the number of groups, and the two concerns never trade off
//! against each other again.

use crate::{Error, Result};

/// How rows are placed into groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupPolicy {
    /// The histogram's resolution, in seconds of block time. A group never
    /// starts mid-bucket.
    pub bucket_secs: u64,
    /// Quiet buckets are merged into the open group until it holds this many.
    pub min_rows: usize,
    /// A group holding this many rows closes at the next transaction boundary
    /// even inside its bucket. A single transaction larger than this is not
    /// split; the group simply runs over.
    pub max_rows: usize,
    /// Write a bloom filter over `tx_hash` per group.
    pub bloom: Bloom,
}

/// Whether groups carry a `tx_hash` bloom filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bloom {
    /// Sized for `2 × min_rows` distinct hashes at 5% false positives — a few
    /// KB per group. A mint-day group far larger than that saturates its
    /// filter, which costs a lookup one extra group read exactly where it
    /// would have landed anyway.
    PerGroup,
    None,
}

impl GroupPolicy {
    /// Daily buckets, 5k-row floor, 200k-row cap, bloom filters on.
    pub const DAILY: GroupPolicy = GroupPolicy {
        bucket_secs: 86_400,
        min_rows: 5_000,
        max_rows: 200_000,
        bloom: Bloom::PerGroup,
    };

    /// One group per bucket, never merged — what the footer-size measurement
    /// compares against.
    pub const STRICT_DAILY: GroupPolicy = GroupPolicy {
        min_rows: 0,
        ..GroupPolicy::DAILY
    };

    /// The bucket a block time falls in — its first second.
    pub fn bucket_of(&self, block_time: u64) -> u64 {
        block_time / self.bucket_secs * self.bucket_secs
    }
}

/// What one bucket holds. The histogram's bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BucketSummary {
    /// First second of the bucket.
    pub from_unix: u64,
    /// Every row, placeholders included — what the row group's `num_rows`
    /// adds up to.
    pub rows: u64,
    /// Rows standing in for a `(transaction, unit)` attributed to nobody.
    /// `rows − placeholders` is the movement count.
    pub placeholders: u64,
    /// Distinct transactions.
    pub txs: u64,
    /// Transactions that minted any unit.
    pub mints: u64,
    /// Transactions that burned any unit.
    pub burns: u64,
}

impl BucketSummary {
    fn add(&mut self, other: &BucketSummary) {
        self.rows += other.rows;
        self.placeholders += other.placeholders;
        self.txs += other.txs;
        self.mints += other.mints;
        self.burns += other.burns;
    }
}

/// Whether a row is a movement or a placeholder — see
/// [`crate::schema::Movement::is_placeholder`]. An enum rather than a bool
/// so the call site says which it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKind {
    Movement,
    Placeholder,
}

/// What one row group holds: its span, its totals, and its buckets.
///
/// `rows` is in the footer's `num_rows` already and is checked against the
/// buckets on decode; everything else travels as side tables in the
/// key-value metadata — see [`GroupSummary::encode`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GroupSummary {
    /// First second of the group's first bucket.
    pub from_unix: u64,
    /// First second AFTER the group's last bucket.
    pub to_unix: u64,
    pub rows: u64,
    pub txs: u64,
    pub mints: u64,
    pub burns: u64,
    /// Non-empty buckets, ascending. Empty buckets inside a merged span are
    /// simply absent.
    pub buckets: Vec<BucketSummary>,
}

impl GroupSummary {
    /// The side tables, as footer entries.
    ///
    /// Per group: how many buckets it holds. Per bucket, flat across all
    /// groups in order: start, rows, transactions, mints, burns. Comma-joined
    /// decimals — a five-year policy at daily resolution is ~1,800 entries
    /// per list, tens of KB in all, and a reader that decodes the footer has
    /// the whole daily histogram without touching a data page.
    pub fn encode(groups: &[GroupSummary]) -> Vec<(&'static str, String)> {
        use crate::schema::kv;
        fn join(it: impl Iterator<Item = u64>) -> String {
            it.map(|v| v.to_string()).collect::<Vec<_>>().join(",")
        }
        let buckets = || groups.iter().flat_map(|g| g.buckets.iter());
        vec![
            (
                kv::GROUP_BUCKETS,
                join(groups.iter().map(|g| g.buckets.len() as u64)),
            ),
            (kv::BUCKET_FROM, join(buckets().map(|b| b.from_unix))),
            (kv::BUCKET_ROWS, join(buckets().map(|b| b.rows))),
            (
                kv::BUCKET_PLACEHOLDERS,
                join(buckets().map(|b| b.placeholders)),
            ),
            (kv::BUCKET_TXS, join(buckets().map(|b| b.txs))),
            (kv::BUCKET_MINTS, join(buckets().map(|b| b.mints))),
            (kv::BUCKET_BURNS, join(buckets().map(|b| b.burns))),
        ]
    }

    /// Rebuild the summaries from the footer.
    ///
    /// `rows_per_group` comes from the row groups themselves; the side tables
    /// must describe exactly that many groups, and each group's buckets must
    /// sum to its `num_rows`, or the tables describe a different file.
    pub fn decode(
        kvs: &[parquet::format::KeyValue],
        rows_per_group: &[u64],
        bucket_secs: u64,
    ) -> Result<Vec<Self>> {
        use crate::schema::{kv, lookup};
        fn list(kvs: &[parquet::format::KeyValue], key: &'static str) -> Result<Vec<u64>> {
            let raw = lookup(kvs, key).ok_or(Error::MissingStamp(key))?;
            if raw.is_empty() {
                return Ok(Vec::new());
            }
            raw.split(',')
                .map(|s| {
                    s.parse::<u64>().map_err(|e| Error::BadStamp {
                        key,
                        detail: e.to_string(),
                    })
                })
                .collect()
        }
        let counts = list(kvs, kv::GROUP_BUCKETS)?;
        if counts.len() != rows_per_group.len() {
            return Err(Error::GroupCountMismatch {
                listed: counts.len(),
                actual: rows_per_group.len(),
            });
        }
        let from = list(kvs, kv::BUCKET_FROM)?;
        let rows = list(kvs, kv::BUCKET_ROWS)?;
        let placeholders = list(kvs, kv::BUCKET_PLACEHOLDERS)?;
        let txs = list(kvs, kv::BUCKET_TXS)?;
        let mints = list(kvs, kv::BUCKET_MINTS)?;
        let burns = list(kvs, kv::BUCKET_BURNS)?;
        let total: u64 = counts.iter().sum();
        for (key, l) in [
            (kv::BUCKET_FROM, &from),
            (kv::BUCKET_ROWS, &rows),
            (kv::BUCKET_PLACEHOLDERS, &placeholders),
            (kv::BUCKET_TXS, &txs),
            (kv::BUCKET_MINTS, &mints),
            (kv::BUCKET_BURNS, &burns),
        ] {
            if l.len() as u64 != total {
                return Err(Error::BadStamp {
                    key,
                    detail: format!("{} entries for {total} buckets", l.len()),
                });
            }
        }

        let mut out = Vec::with_capacity(counts.len());
        let mut at = 0usize;
        for (g, &n) in counts.iter().enumerate() {
            let n = n as usize;
            let mut summary = GroupSummary::default();
            for i in at..at + n {
                let b = BucketSummary {
                    from_unix: from[i],
                    rows: rows[i],
                    placeholders: placeholders[i],
                    txs: txs[i],
                    mints: mints[i],
                    burns: burns[i],
                };
                summary.rows += b.rows;
                summary.txs += b.txs;
                summary.mints += b.mints;
                summary.burns += b.burns;
                summary.buckets.push(b);
            }
            at += n;
            if summary.rows != rows_per_group[g] {
                return Err(Error::BadStamp {
                    key: kv::BUCKET_ROWS,
                    detail: format!(
                        "group {g} buckets sum to {} rows, the row group has {}",
                        summary.rows, rows_per_group[g]
                    ),
                });
            }
            summary.from_unix = summary.buckets.first().map_or(0, |b| b.from_unix);
            summary.to_unix = summary
                .buckets
                .last()
                .map_or(0, |b| b.from_unix + bucket_secs);
            out.push(summary);
        }
        Ok(out)
    }
}

/// Where the row about to be written goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    /// Into the open group.
    Continue,
    /// The open group is finished — here is its summary — and this row opens
    /// the next one.
    Close(GroupSummary),
}

/// What one row does to the open group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Decision {
    /// Into the open group as it stands.
    Admit,
    /// Into the open group, which first widens to include this row's bucket.
    Merge,
    /// The open group closes on the cap; this row starts the next group of
    /// the same span.
    SplitSpan,
    /// The open span ends; this row starts a new one.
    NewSpan,
}

/// The streaming placer. Feed rows in slot order, grouped by transaction;
/// it answers, per row, whether the open group closes first.
pub struct Grouper {
    policy: GroupPolicy,
    open: Option<Open>,
}

struct Open {
    summary: GroupSummary,
    /// The bucket rows are currently landing in.
    bucket: BucketSummary,
    /// Rows across EVERY group of this span, this one included.
    ///
    /// A span is the run of buckets merged together; a cap split inside it
    /// starts a new group but not a new span. The merge decision reads this,
    /// not the group's own rows, so the decision is a function of
    /// whole-bucket totals rather than of where a split happened to fall.
    span_rows: u64,
    last_tx: Vec<u8>,
    tx_minted: bool,
    tx_burned: bool,
    /// Highest block time admitted. Rows must not go backwards.
    high_water: u64,
}

impl Open {
    fn start(policy: &GroupPolicy, block_time: u64) -> Self {
        let from = policy.bucket_of(block_time);
        Open {
            summary: GroupSummary {
                from_unix: from,
                to_unix: from + policy.bucket_secs,
                ..GroupSummary::default()
            },
            bucket: BucketSummary {
                from_unix: from,
                ..BucketSummary::default()
            },
            span_rows: 0,
            last_tx: Vec::new(),
            tx_minted: false,
            tx_burned: false,
            high_water: block_time,
        }
    }

    /// The next group of the SAME span, after a cap split — same span bounds,
    /// same open bucket, fresh counts.
    fn continuation(&self) -> Self {
        Open {
            summary: GroupSummary {
                from_unix: self.summary.from_unix,
                to_unix: self.summary.to_unix,
                ..GroupSummary::default()
            },
            bucket: BucketSummary {
                from_unix: self.bucket.from_unix,
                ..BucketSummary::default()
            },
            span_rows: self.span_rows,
            last_tx: Vec::new(),
            tx_minted: false,
            tx_burned: false,
            high_water: self.high_water,
        }
    }

    /// Fold the transaction in progress into the counts — the group's and
    /// the open bucket's, since a transaction sits in exactly one bucket.
    fn settle_tx(&mut self) {
        if self.last_tx.is_empty() {
            return;
        }
        let one = BucketSummary {
            from_unix: 0,
            rows: 0,
            placeholders: 0,
            txs: 1,
            mints: u64::from(self.tx_minted),
            burns: u64::from(self.tx_burned),
        };
        self.bucket.add(&one);
        self.summary.txs += one.txs;
        self.summary.mints += one.mints;
        self.summary.burns += one.burns;
        self.tx_minted = false;
        self.tx_burned = false;
    }

    /// Close the open bucket into the group and open the next.
    fn roll_bucket(&mut self, from: u64) {
        if self.bucket.rows > 0 {
            self.summary.buckets.push(self.bucket);
        }
        self.bucket = BucketSummary {
            from_unix: from,
            ..BucketSummary::default()
        };
    }

    fn admit(&mut self, policy: &GroupPolicy, row: Row<'_>) {
        let Row {
            block_time,
            tx_hash,
            net_mint,
            kind,
        } = row;
        if self.last_tx != tx_hash {
            self.settle_tx();
            self.last_tx = tx_hash.to_vec();
        }
        let bucket = policy.bucket_of(block_time);
        if bucket != self.bucket.from_unix {
            self.roll_bucket(bucket);
        }
        self.summary.rows += 1;
        self.bucket.rows += 1;
        if kind == RowKind::Placeholder {
            self.bucket.placeholders += 1;
        }
        self.span_rows += 1;
        self.tx_minted |= net_mint > 0;
        self.tx_burned |= net_mint < 0;
        self.high_water = block_time;
    }

    fn close(mut self) -> GroupSummary {
        self.settle_tx();
        if self.bucket.rows > 0 {
            self.summary.buckets.push(self.bucket);
        }
        self.summary
    }
}

impl Grouper {
    pub fn new(policy: GroupPolicy) -> Self {
        Grouper { policy, open: None }
    }

    pub fn policy(&self) -> &GroupPolicy {
        &self.policy
    }

    /// Place one row. Call BEFORE writing it, and act on
    /// [`Placement::Close`] before buffering the row into the next group.
    pub fn place(&mut self, row: Row<'_>) -> Result<Placement> {
        let policy = self.policy;
        let block_time = row.block_time;
        let Some(open) = self.open.as_mut() else {
            let mut o = Open::start(&policy, block_time);
            o.admit(&policy, row);
            self.open = Some(o);
            return Ok(Placement::Continue);
        };
        if block_time < open.high_water {
            return Err(Error::OutOfOrder {
                had: open.high_water,
                got: block_time,
            });
        }

        let same_tx = open.last_tx == row.tx_hash;
        let inside = block_time < open.summary.to_unix;
        let decision = match (inside, same_tx) {
            // Still in the span. Only a cap breach at a transaction boundary
            // closes the group — a transaction is never split — and the span
            // carries on into the next group.
            (true, false) if open.summary.rows as usize >= policy.max_rows => Decision::SplitSpan,
            (true, _) => Decision::Admit,
            // A new bucket. A span below the floor absorbs it; one at or
            // above the floor ends, and this row starts the next.
            (false, _) if (open.span_rows as usize) < policy.min_rows => Decision::Merge,
            (false, _) => Decision::NewSpan,
        };

        match decision {
            Decision::Admit => {}
            Decision::Merge => {
                // The quiet bucket(s) between the span's end and this row.
                open.summary.to_unix = policy.bucket_of(block_time) + policy.bucket_secs;
            }
            Decision::SplitSpan => {
                let next = open.continuation();
                let done = std::mem::replace(open, next).close();
                open.admit(&policy, row);
                return Ok(Placement::Close(done));
            }
            Decision::NewSpan => {
                let done = self.open.take().expect("open group").close();
                let mut next = Open::start(&policy, block_time);
                next.admit(&policy, row);
                self.open = Some(next);
                return Ok(Placement::Close(done));
            }
        }
        open.admit(&policy, row);
        Ok(Placement::Continue)
    }

    /// The open group, if any. Nothing may be placed after this.
    pub fn finish(self) -> Option<GroupSummary> {
        self.open.map(Open::close)
    }
}

/// What the placer needs to know about a row.
#[derive(Debug, Clone, Copy)]
pub struct Row<'a> {
    pub block_time: u64,
    pub tx_hash: &'a [u8],
    pub net_mint: i64,
    pub kind: RowKind,
}

/// Group a whole sequence of movements at once — the non-streaming face, for
/// tests.
pub fn plan<'a>(
    policy: GroupPolicy,
    rows: impl IntoIterator<Item = (u64, &'a [u8], i64)>,
) -> Result<Vec<GroupSummary>> {
    let mut g = Grouper::new(policy);
    let mut out = Vec::new();
    for (block_time, tx_hash, net_mint) in rows {
        let row = Row {
            block_time,
            tx_hash,
            net_mint,
            kind: RowKind::Movement,
        };
        if let Placement::Close(s) = g.place(row)? {
            out.push(s);
        }
    }
    out.extend(g.finish());
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: u64 = 86_400;

    fn tx(n: u8) -> Vec<u8> {
        vec![n; 32]
    }

    fn policy(min_rows: usize, max_rows: usize) -> GroupPolicy {
        GroupPolicy {
            bucket_secs: DAY,
            min_rows,
            max_rows,
            bloom: Bloom::None,
        }
    }

    fn spans(groups: &[GroupSummary]) -> Vec<(u64, u64)> {
        groups.iter().map(|g| (g.from_unix, g.to_unix)).collect()
    }

    /// Strictly daily: each day is its own group, whatever it holds.
    #[test]
    fn a_zero_floor_gives_one_group_per_day() {
        let a = tx(1);
        let b = tx(2);
        let c = tx(3);
        let rows = vec![
            (DAY * 10 + 5, a.as_slice(), 0),
            (DAY * 10 + 9, a.as_slice(), 0),
            (DAY * 11 + 1, b.as_slice(), 1),
            (DAY * 13 + 1, c.as_slice(), -1),
        ];
        let groups = plan(policy(0, 1_000), rows).unwrap();
        assert_eq!(groups.len(), 3);
        assert_eq!(spans(&groups)[0], (DAY * 10, DAY * 11));
        assert_eq!((groups[0].rows, groups[0].txs), (2, 1));
        assert_eq!((groups[1].mints, groups[1].burns), (1, 0));
        // Day 12 was empty and is NOT a group: an empty row group is a
        // statistic with no rows to describe.
        assert_eq!(spans(&groups)[2], (DAY * 13, DAY * 14));
        assert_eq!((groups[2].mints, groups[2].burns), (0, 1));
        assert!(groups.iter().all(|g| g.buckets.len() == 1));
    }

    /// Quiet days merge until the floor is met — the GROUP widens, and its
    /// buckets keep their own day-level counts.
    #[test]
    fn quiet_days_merge_up_to_the_floor_and_keep_their_buckets() {
        let a = tx(1);
        let b = tx(2);
        let c = tx(3);
        let d = tx(4);
        let rows = vec![
            (DAY * 10, a.as_slice(), 0),
            (DAY * 11, b.as_slice(), 0),
            (DAY * 14, c.as_slice(), 0),
            (DAY * 15, d.as_slice(), 0),
        ];
        let groups = plan(policy(3, 1_000), rows).unwrap();
        assert_eq!(
            spans(&groups),
            vec![(DAY * 10, DAY * 15), (DAY * 15, DAY * 16)]
        );
        assert_eq!(groups[0].rows, 3);
        let days: Vec<u64> = groups[0]
            .buckets
            .iter()
            .map(|b| b.from_unix / DAY)
            .collect();
        assert_eq!(
            days,
            vec![10, 11, 14],
            "empty day 12/13 are absent, not zero"
        );
        assert!(groups[0].buckets.iter().all(|b| b.rows == 1 && b.txs == 1));
    }

    /// A busy day splits at the cap — but only between transactions. A
    /// transaction wider than the cap runs the group over rather than being
    /// torn in two.
    #[test]
    fn a_busy_day_splits_on_a_transaction_boundary() {
        let a = tx(1);
        let b = tx(2);
        let c = tx(3);
        let mut rows = Vec::new();
        for _ in 0..5 {
            rows.push((DAY * 10, a.as_slice(), 0));
        }
        for _ in 0..3 {
            rows.push((DAY * 10, b.as_slice(), 0));
        }
        rows.push((DAY * 10 + 1, c.as_slice(), 0));
        let groups = plan(policy(0, 3), rows).unwrap();
        assert_eq!(groups.len(), 3, "{groups:?}");
        assert_eq!(groups[0].rows, 5, "tx a overran the cap whole");
        assert_eq!(groups[1].rows, 3);
        assert_eq!(groups[2].rows, 1);
        // Split groups share their span, and each carries its share of the
        // day's bucket.
        assert!(spans(&groups).iter().all(|s| *s == (DAY * 10, DAY * 11)));
        assert!(
            groups
                .iter()
                .all(|g| g.buckets.len() == 1 && g.buckets[0].from_unix == DAY * 10)
        );
        assert_eq!(groups.iter().map(|g| g.buckets[0].txs).sum::<u64>(), 3);
    }

    /// A cap split inside a MERGED span must not restart the merge
    /// accounting: the next bucket is judged against the whole span's rows.
    #[test]
    fn a_cap_split_inside_a_merged_span_keeps_the_span() {
        let hashes: Vec<Vec<u8>> = (0..8u8).map(|n| vec![n; 32]).collect();
        // Day 10: 1 row. Day 11: 4 rows across 4 txs (cap 2 splits it).
        // Day 12: 1 row.
        let mut rows: Vec<(u64, &[u8], i64)> = vec![(DAY * 10, &hashes[0], 0)];
        for h in &hashes[1..5] {
            rows.push((DAY * 11, h, 0));
        }
        rows.push((DAY * 12, &hashes[6], 0));
        let groups = plan(policy(4, 2), rows).unwrap();
        // Span [10, 12) holds 5 rows ≥ 4, so day 12 opens its own span.
        let s = spans(&groups);
        assert!(
            s.iter()
                .take(s.len() - 1)
                .all(|x| *x == (DAY * 10, DAY * 12)),
            "{s:?}"
        );
        assert_eq!(*s.last().unwrap(), (DAY * 12, DAY * 13));
        assert_eq!(groups.iter().map(|g| g.rows).sum::<u64>(), 6);
    }

    /// A transaction minting one unit and burning another counts once in
    /// each — the counts are of TRANSACTIONS, not rows.
    #[test]
    fn mint_and_burn_counts_are_per_transaction() {
        let a = tx(1);
        let rows = vec![
            (DAY, a.as_slice(), 1),
            (DAY, a.as_slice(), 1),
            (DAY, a.as_slice(), -1),
        ];
        let g = plan(policy(0, 100), rows).unwrap();
        assert_eq!((g[0].txs, g[0].mints, g[0].burns, g[0].rows), (1, 1, 1, 3));
        assert_eq!(g[0].buckets[0].mints, 1);
    }

    fn row(block_time: u64, tx_hash: &[u8], kind: RowKind) -> Row<'_> {
        Row {
            block_time,
            tx_hash,
            net_mint: 0,
            kind,
        }
    }

    /// Rows out of slot order are refused, not misfiled.
    #[test]
    fn a_row_going_backwards_is_an_error() {
        let mut g = Grouper::new(policy(0, 100));
        let (a, b) = (tx(1), tx(2));
        g.place(row(DAY * 2, &a, RowKind::Movement)).unwrap();
        assert!(matches!(
            g.place(row(DAY, &b, RowKind::Movement)),
            Err(Error::OutOfOrder { .. })
        ));
    }

    /// A placeholder is a row and a transaction, but not a movement.
    #[test]
    fn a_placeholder_counts_as_a_row_but_not_a_movement() {
        let mut g = Grouper::new(policy(0, 100));
        let (a, b) = (tx(1), tx(2));
        g.place(row(DAY, &a, RowKind::Movement)).unwrap();
        g.place(row(DAY, &b, RowKind::Placeholder)).unwrap();
        let s = g.finish().unwrap();
        assert_eq!((s.rows, s.txs), (2, 2));
        assert_eq!(s.buckets[0].placeholders, 1);
    }

    fn kvs_of(groups: &[GroupSummary]) -> Vec<parquet::format::KeyValue> {
        GroupSummary::encode(groups)
            .into_iter()
            .map(|(k, v)| parquet::format::KeyValue::new(k.to_string(), v))
            .collect()
    }

    #[test]
    fn summaries_round_trip_through_the_footer() {
        let a = tx(1);
        let b = tx(2);
        let c = tx(3);
        let rows = vec![
            (DAY * 10, a.as_slice(), 1),
            (DAY * 11, b.as_slice(), 0),
            (DAY * 14, c.as_slice(), -1),
            (DAY * 14, c.as_slice(), -1),
        ];
        let groups = plan(policy(3, 100), rows).unwrap();
        let rows_per_group: Vec<u64> = groups.iter().map(|g| g.rows).collect();
        let back = GroupSummary::decode(&kvs_of(&groups), &rows_per_group, DAY).unwrap();
        assert_eq!(back, groups);
    }

    /// Side tables that disagree with the row groups are refused — a footer
    /// describing a different file is worse than no footer.
    #[test]
    fn side_tables_must_agree_with_the_row_groups() {
        let a = tx(1);
        let groups = plan(policy(0, 100), vec![(DAY, a.as_slice(), 0)]).unwrap();
        let kvs = kvs_of(&groups);
        assert!(matches!(
            GroupSummary::decode(&kvs, &[1, 1], DAY),
            Err(Error::GroupCountMismatch {
                listed: 1,
                actual: 2
            })
        ));
        assert!(matches!(
            GroupSummary::decode(&kvs, &[2], DAY),
            Err(Error::BadStamp { .. })
        ));
    }

    #[test]
    fn an_empty_file_has_empty_side_tables() {
        assert_eq!(
            GroupSummary::decode(&kvs_of(&[]), &[], DAY).unwrap(),
            Vec::<GroupSummary>::new()
        );
    }
}
