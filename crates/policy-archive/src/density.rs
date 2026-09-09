//! The density tier — how much happened when, from the footer alone.
//!
//! The same shape whether it comes from a `GROUP BY` over a live sqlite
//! ledger or from a sealed file's footer, which is what lets the origin serve
//! it from the database today and from the archive later with no change on
//! the wire. The footer's side tables count per BUCKET, not per row group, so
//! the comparison is bucket-for-bucket exact whatever grouping the writer
//! chose — see [`crate::groups`].

use crate::groups::GroupSummary;

/// One bar of the histogram.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DensityBucket {
    pub from_unix: u64,
    /// Exclusive.
    pub to_unix: u64,
    /// Attributed movements — rows with a party. Placeholders for a
    /// transaction whose unit reached nobody we could attribute are not
    /// movements.
    pub movements: u64,
    pub txs: u64,
    pub mints: u64,
    pub burns: u64,
}

impl DensityBucket {
    fn absorb(&mut self, other: &DensityBucket) {
        self.to_unix = self.to_unix.max(other.to_unix);
        self.movements += other.movements;
        self.txs += other.txs;
        self.mints += other.mints;
        self.burns += other.burns;
    }
}

/// Buckets from the footer's groups. A bucket split across two groups (a
/// busy day over the cap) is re-joined, so a reader sees buckets, not the
/// writer's row-group mechanics.
///
/// `rows` per bucket counts every row, placeholders included; the writer
/// records how many of those were placeholders so `movements` can exclude
/// them — see [`GroupSummary`].
pub fn from_groups(groups: &[GroupSummary], bucket_secs: u64) -> Vec<DensityBucket> {
    let mut out: Vec<DensityBucket> = Vec::new();
    for b in groups.iter().flat_map(|g| g.buckets.iter()) {
        let bucket = DensityBucket {
            from_unix: b.from_unix,
            to_unix: b.from_unix + bucket_secs,
            movements: b.rows - b.placeholders,
            txs: b.txs,
            mints: b.mints,
            burns: b.burns,
        };
        match out.last_mut() {
            Some(last) if last.from_unix == bucket.from_unix => last.absorb(&bucket),
            _ => out.push(bucket),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::groups::{Bloom, GroupPolicy, plan};

    const DAY: u64 = 86_400;

    /// A bucket the cap split across groups comes back as ONE bar.
    #[test]
    fn a_split_bucket_is_one_bar() {
        let policy = GroupPolicy {
            bucket_secs: DAY,
            min_rows: 4,
            max_rows: 3,
            bloom: Bloom::None,
        };
        let hashes: Vec<Vec<u8>> = (0..10u8).map(|n| vec![n; 32]).collect();
        let mut rows: Vec<(u64, &[u8], i64)> = vec![
            (DAY * 10, &hashes[0], 0),
            (DAY * 10, &hashes[0], 0),
            (DAY * 11, &hashes[1], 1),
        ];
        for h in &hashes[2..8] {
            rows.push((DAY * 13, h, 0));
        }
        rows.push((DAY * 14, &hashes[9], -1));

        let groups = plan(policy, rows.iter().copied()).unwrap();
        assert!(groups.len() > 3, "the busy day split: {groups:?}");
        let density = from_groups(&groups, DAY);
        let days: Vec<u64> = density.iter().map(|b| b.from_unix / DAY).collect();
        assert_eq!(days, vec![10, 11, 13, 14]);
        assert_eq!((density[0].movements, density[0].txs), (2, 1));
        assert_eq!(density[1].mints, 1);
        assert_eq!((density[2].movements, density[2].txs), (6, 6));
        assert_eq!(density[3].burns, 1);
        assert!(density.iter().all(|b| b.to_unix == b.from_unix + DAY));
    }
}
