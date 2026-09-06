//! The archive's own record of itself — one JSON object per policy, written
//! LAST by whatever changed the archive, read FIRST by whatever reads it.
//!
//! Lives in this crate rather than in the walker because a Worker reading
//! the archive out of R2 has to parse the same shape the box wrote, and a
//! drift between two copies of this struct would be a manifest that names
//! files a reader cannot find.
//!
//! # Layout it describes
//!
//! ```text
//! <policy_hex>/
//!   manifest.json
//!   archive-NNNN.parquet          — a ROLLUP: every pass so far, one file
//!   pending.bin                   — carried state, after a rollup
//!   pass-NNNN/movements.parquet   — a pass not yet rolled up
//!   pass-NNNN/corrections.parquet
//!   pass-NNNN/seg-NNNN.parquet    — a pass left uncompacted
//!   pass-NNNN/pending.bin
//! ```
//!
//! A reader merges EVERY file the manifest names by summing rows per
//! `(transaction, unit, party)`; rollup and compaction are the same sum
//! applied ahead of time so readers open fewer files.
//!
//! # Coverage is a SET OF RANGES
//!
//! Each pass records the stretch it read — `[floor, ceiling)` plus any
//! `windows` it read outside that (a seek's detour) — and everything about
//! coverage is DERIVED from those: [`Manifest::ranges`] merges them,
//! [`Manifest::walk_from`]/[`Manifest::walk_to`] are their extremes,
//! [`Manifest::completeness`] is whether they form one stretch from the
//! first mint up. The `walk_from`/`walk_to`/`completeness` FIELDS are a
//! cache of those derivations, refreshed by [`Manifest::to_json`] so a
//! reader of the JSON sees the same answer the methods give. Nothing
//! requires passes to be contiguous or in order; the walker keeps its
//! descent contiguous because resolution is cheaper that way, not because
//! the manifest needs it. See `docs/design/POLICY_WALK_SCHEDULER.md`.

use serde::{Deserialize, Serialize};

use crate::schema::Completeness;

/// A stretch of slots, `[from, to)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SlotRange {
    pub from: u64,
    pub to: u64,
}

impl SlotRange {
    pub fn new(from: u64, to: u64) -> Self {
        Self { from, to }
    }

    pub fn contains(&self, slot: u64) -> bool {
        slot >= self.from && slot < self.to
    }

    pub fn is_empty(&self) -> bool {
        self.to <= self.from
    }
}

/// Merge overlapping and touching ranges, ascending.
pub fn merge_ranges(mut ranges: Vec<SlotRange>) -> Vec<SlotRange> {
    ranges.retain(|r| !r.is_empty());
    ranges.sort();
    let mut out: Vec<SlotRange> = Vec::with_capacity(ranges.len());
    for r in ranges {
        match out.last_mut() {
            Some(last) if r.from <= last.to => last.to = last.to.max(r.to),
            _ => out.push(r),
        }
    }
    out
}

/// What kind of chain a range was read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeKind {
    /// The Mithril immutable directory: settled, never rolls back, summed
    /// with everything else.
    #[default]
    Immutable,
    /// The stretch between the immutable tip and the live tip: may roll
    /// back, so its file is REPLACED on each refresh rather than summed.
    Volatile,
}

pub const MANIFEST: &str = "manifest.json";
pub const MOVEMENTS: &str = "movements.parquet";
pub const CORRECTIONS: &str = "corrections.parquet";
pub const PENDING: &str = "pending.bin";
pub const MANIFEST_FORMAT: u32 = 1;

/// Which stream a file belongs to. Movements files count transactions in
/// their footers; corrections files only ever add movements to transactions
/// counted elsewhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Movements,
    Corrections,
}

/// Kind by filename. `FileEntry` carries no kind because the name already
/// says it and a manifest edit cannot disagree with the file it names.
pub fn kind_of(file: &str) -> FileKind {
    match file.starts_with("corr-") || file == CORRECTIONS {
        true => FileKind::Corrections,
        false => FileKind::Movements,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub format: u32,
    pub policy: String,
    /// The first-mint floor, if any pass knew it. The bound below which no
    /// pass will look.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_mint_slot: Option<u64>,
    /// `complete` | `partial` | `unrecorded` — a CACHE of
    /// [`Manifest::completeness`], refreshed on write. Read the method.
    pub completeness: String,
    /// A cache of [`Manifest::walk_from`], refreshed on write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub walk_from: Option<u64>,
    /// A cache of [`Manifest::walk_to`], refreshed on write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub walk_to: Option<u64>,
    /// THE ROLLUP: every pass up to `rolled_up_through`, in one file at the
    /// policy's root. Passes with a `seq` at or below that hold no files of
    /// their own any more; their entries stay for the record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollup: Option<FileEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rolled_up_through: Option<u32>,
    /// The carried pending set at the policy's root, once a rollup has
    /// removed the pass directory that held it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending: Option<String>,
    pub passes: Vec<PassEntry>,
    pub updated_unix: u64,
}

impl Manifest {
    pub fn new(policy_hex: &str) -> Self {
        Manifest {
            format: MANIFEST_FORMAT,
            policy: policy_hex.to_string(),
            first_mint_slot: None,
            completeness: Completeness::Unrecorded.as_wire().to_string(),
            walk_from: None,
            walk_to: None,
            rollup: None,
            rolled_up_through: None,
            pending: None,
            passes: Vec::new(),
            updated_unix: 0,
        }
    }

    /// Every stretch any pass has read, merged, ascending. THE coverage;
    /// everything else here is a view of it.
    pub fn ranges(&self) -> Vec<SlotRange> {
        let mut all = Vec::new();
        for p in &self.passes {
            all.push(SlotRange::new(p.floor, p.ceiling));
            all.extend(p.windows.iter().copied());
        }
        merge_ranges(all)
    }

    /// Lowest slot any pass has read.
    pub fn walk_from(&self) -> Option<u64> {
        self.ranges().first().map(|r| r.from)
    }

    /// Highest slot any pass has read.
    pub fn walk_to(&self) -> Option<u64> {
        self.ranges().last().map(|r| r.to)
    }

    /// Has any pass read `slot`?
    pub fn covers(&self, slot: u64) -> bool {
        self.ranges().iter().any(|r| r.contains(slot))
    }

    /// The parts of `[from, to)` no pass has read, ascending.
    pub fn uncovered(&self, from: u64, to: u64) -> Vec<SlotRange> {
        let mut out = Vec::new();
        let mut cursor = from;
        for r in self.ranges() {
            if r.to <= cursor {
                continue;
            }
            if r.from >= to {
                break;
            }
            if r.from > cursor {
                out.push(SlotRange::new(cursor, r.from.min(to)));
            }
            cursor = cursor.max(r.to);
            if cursor >= to {
                break;
            }
        }
        if cursor < to {
            out.push(SlotRange::new(cursor, to));
        }
        out
    }

    /// DERIVED: does the coverage form ONE stretch that reaches the
    /// policy's beginning? Reaching is genesis, or the first mint when it
    /// is known. A stretch that stops short, or a second stretch below the
    /// first — a seek window the descent has not joined up with yet — is
    /// `Partial`. No ranges at all is `Unrecorded`.
    ///
    /// Never demoted by a later pass in practice: a pass on a complete
    /// archive can only extend the one stretch upward. It CAN be demoted
    /// by a window read below a hole, which is the truthful answer.
    pub fn completeness(&self) -> Completeness {
        let ranges = self.ranges();
        let Some(lowest) = ranges.first() else {
            return Completeness::Unrecorded;
        };
        let reached = lowest.from == 0 || self.first_mint_slot.is_some_and(|m| lowest.from <= m);
        match reached && ranges.len() == 1 {
            true => Completeness::Complete,
            false => Completeness::Partial,
        }
    }

    /// Bring the cached fields in line with the derivations. Called by
    /// [`Manifest::to_json`]; call it after editing passes if the fields
    /// are read directly before a write.
    pub fn refresh_derived(&mut self) {
        self.walk_from = self.walk_from();
        self.walk_to = self.walk_to();
        self.completeness = self.completeness().as_wire().to_string();
    }

    /// The newest pass — the one whose pending set is current.
    pub fn latest_pass(&self) -> Option<&PassEntry> {
        self.passes.iter().max_by_key(|p| p.seq)
    }

    /// Every pending sidecar still on disk, relative to the policy's
    /// directory: one per un-rolled-up pass, plus the root's after a
    /// rollup. A job loads the UNION; a spender resolved by one pass is
    /// harmless to load again, since its outref is spent once on chain.
    pub fn pending_files(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .passes
            .iter()
            .filter(|p| !self.rolled_up_through.is_some_and(|t| p.seq <= t))
            .map(|p| format!("{}/{}", p.dir, PENDING))
            .collect();
        if let Some(root) = &self.pending {
            out.push(root.clone());
        }
        out
    }

    pub fn next_seq(&self) -> u32 {
        self.latest_pass().map_or(0, |p| p.seq + 1)
    }

    /// Every Parquet file a reader must merge, as `(relative path, kind)`,
    /// relative to the policy's directory or key prefix.
    pub fn files(&self) -> Vec<(String, FileKind)> {
        let mut out = Vec::new();
        if let Some(r) = &self.rollup {
            out.push((r.file.clone(), FileKind::Movements));
        }
        for p in &self.passes {
            if self.rolled_up_through.is_some_and(|t| p.seq <= t) {
                continue;
            }
            if let Some(f) = &p.movements {
                out.push((format!("{}/{}", p.dir, f.file), FileKind::Movements));
            }
            if let Some(f) = &p.corrections {
                out.push((format!("{}/{}", p.dir, f.file), FileKind::Corrections));
            }
            for f in &p.segments {
                out.push((format!("{}/{}", p.dir, f.file), kind_of(&f.file)));
            }
        }
        out
    }

    /// Where the current pending set is, relative to the policy's directory.
    pub fn pending_file(&self) -> Option<String> {
        match self.latest_pass() {
            Some(p) if !self.rolled_up_through.is_some_and(|t| p.seq <= t) => {
                Some(format!("{}/{}", p.dir, PENDING))
            }
            _ => self.pending.clone(),
        }
    }

    /// A LOWER BOUND on the policy's distinct units: the most any one pass
    /// or rollup saw.
    pub fn units(&self) -> u64 {
        self.passes
            .iter()
            .map(|p| p.units)
            .chain(self.rollup.as_ref().map(|r| r.units))
            .max()
            .unwrap_or(0)
    }

    /// With the cached fields refreshed, so the JSON says what the methods
    /// say.
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut fresh = self.clone();
        fresh.refresh_derived();
        serde_json::to_vec_pretty(&fresh)
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// One pass, as the manifest records it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PassEntry {
    pub seq: u32,
    /// Directory under the policy's, e.g. `pass-0003`.
    pub dir: String,
    /// The range this pass walked: `[floor, ceiling)`.
    pub ceiling: u64,
    pub floor: u64,
    /// Stretches this pass ALSO read, outside `[floor, ceiling)` — the
    /// windows its detours took for seeks below the floor. Their rows are
    /// in this pass's files like any other.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub windows: Vec<SlotRange>,
    /// Which chain the range came from. Everything so far is immutable;
    /// the volatile tail is the design's next step.
    #[serde(default)]
    pub kind: RangeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub movements: Option<FileEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub corrections: Option<FileEntry>,
    /// A pass left UNCOMPACTED: the segments it spilled while walking, in
    /// order, kind by filename (`seg-`/`corr-`). Empty once compacted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub segments: Vec<FileEntry>,
    /// Spenders still waiting on a source after this pass.
    pub pending: u64,
    /// Transactions the walk FOUND touching the policy in the pass's range.
    #[serde(default)]
    pub found: u64,
    /// Transactions with rows in `movements` — fewer than `found`, because
    /// a transaction the asset only rode through as change moved nothing.
    pub written: u64,
    /// Delta rows resolved for rows written earlier — here or by an earlier
    /// pass.
    pub backfilled: u64,
    /// Distinct units seen in this pass.
    pub units: u64,
    pub secs: f64,
    pub written_unix: u64,
}

impl PassEntry {
    pub fn dir_name(seq: u32) -> String {
        format!("pass-{seq:04}")
    }
}

/// One Parquet file, as the manifest records it — enough to decide whether
/// to open it without opening it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileEntry {
    pub file: String,
    pub rows: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_slot: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_slot: Option<u64>,
    /// Distinct units in the file, where the writer counted them (rollups).
    #[serde(default)]
    pub units: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(file: &str) -> FileEntry {
        FileEntry {
            file: file.into(),
            rows: 1,
            min_slot: None,
            max_slot: None,
            units: 0,
        }
    }

    fn pass(seq: u32) -> PassEntry {
        PassEntry {
            seq,
            dir: PassEntry::dir_name(seq),
            ceiling: 10,
            floor: 5,
            windows: Vec::new(),
            kind: RangeKind::Immutable,
            movements: Some(entry(MOVEMENTS)),
            corrections: (seq > 0).then(|| entry(CORRECTIONS)),
            segments: Vec::new(),
            pending: 0,
            found: 0,
            written: 0,
            backfilled: 0,
            units: seq as u64,
            secs: 0.0,
            written_unix: 0,
        }
    }

    /// A rollup REPLACES the passes it covers in the file list and takes
    /// over the pending set; later passes still list their own files.
    #[test]
    fn a_rollup_stands_in_for_the_passes_it_covers() {
        let mut m = Manifest::new("ab");
        m.passes = vec![pass(0), pass(1), pass(2)];
        let before = m.files();
        assert_eq!(before.len(), 5, "3 movements + 2 corrections");
        assert_eq!(m.pending_file().as_deref(), Some("pass-0002/pending.bin"));

        m.rollup = Some(entry("archive-0001.parquet"));
        m.rolled_up_through = Some(1);
        m.pending = Some(PENDING.into());
        let after = m.files();
        assert_eq!(after.len(), 3, "rollup + pass 2's two files");
        assert_eq!(
            after[0],
            ("archive-0001.parquet".to_string(), FileKind::Movements)
        );
        assert_eq!(m.pending_file().as_deref(), Some("pass-0002/pending.bin"));

        m.rolled_up_through = Some(2);
        assert_eq!(m.files().len(), 1);
        assert_eq!(m.pending_file().as_deref(), Some(PENDING));
        assert_eq!(m.next_seq(), 3, "sequence numbers never reuse");
    }

    #[test]
    fn the_manifest_round_trips_through_json() {
        let mut m = Manifest::new("ab");
        m.passes.push(pass(0));
        m.rollup = Some(entry("archive-0000.parquet"));
        let back = Manifest::from_json(&m.to_json().unwrap()).unwrap();
        // The JSON carries the derivations, so the round trip refreshes
        // the cache: compare after refreshing the original too.
        m.refresh_derived();
        assert_eq!(back, m);
        assert_eq!(back.walk_from, Some(5));
        assert_eq!(back.walk_to, Some(10));
        assert_eq!(
            back.completeness(),
            Completeness::Partial,
            "5 is not the mint"
        );
    }

    /// COVERAGE IS A SET OF RANGES. Passes need not be contiguous or in
    /// order; a detour's window counts; the derivations say what has been
    /// read, what has not, and whether it reaches the beginning.
    #[test]
    fn coverage_is_derived_from_the_ranges_read() {
        let mut m = Manifest::new("ab");
        assert_eq!(m.completeness(), Completeness::Unrecorded);
        assert!(m.ranges().is_empty());

        // The descent read [500, 1000), and a seek read [100, 150) below it.
        let mut p0 = pass(0);
        p0.floor = 500;
        p0.ceiling = 1_000;
        p0.windows = vec![SlotRange::new(100, 150)];
        m.passes.push(p0);
        assert_eq!(
            m.ranges(),
            vec![SlotRange::new(100, 150), SlotRange::new(500, 1_000)]
        );
        assert_eq!(m.walk_from(), Some(100));
        assert_eq!(m.walk_to(), Some(1_000));
        assert!(m.covers(120) && m.covers(999) && !m.covers(150) && !m.covers(300));
        assert_eq!(
            m.uncovered(0, 1_200),
            vec![
                SlotRange::new(0, 100),
                SlotRange::new(150, 500),
                SlotRange::new(1_000, 1_200)
            ]
        );
        assert_eq!(m.uncovered(600, 900), Vec::new(), "inside a range");
        m.first_mint_slot = Some(100);
        assert_eq!(
            m.completeness(),
            Completeness::Partial,
            "two stretches: the hole between them is not read"
        );

        // The descent joins them up: [100, 500) as pass 1. One stretch,
        // from the mint: complete.
        let mut p1 = pass(1);
        p1.floor = 100;
        p1.ceiling = 500;
        m.passes.push(p1);
        assert_eq!(m.ranges(), vec![SlotRange::new(100, 1_000)]);
        assert_eq!(m.completeness(), Completeness::Complete);
        assert!(m.uncovered(100, 1_000).is_empty());
        // A later top-up above extends the one stretch.
        let mut p2 = pass(2);
        p2.floor = 1_000;
        p2.ceiling = 1_300;
        m.passes.push(p2);
        assert_eq!(m.completeness(), Completeness::Complete);
        assert_eq!(m.walk_to(), Some(1_300));
        assert_eq!(m.pending_files().len(), 3);
    }

    #[test]
    fn ranges_merge_when_they_touch() {
        let merged = merge_ranges(vec![
            SlotRange::new(10, 20),
            SlotRange::new(20, 30),
            SlotRange::new(5, 12),
            SlotRange::new(40, 40),
            SlotRange::new(50, 60),
        ]);
        assert_eq!(merged, vec![SlotRange::new(5, 30), SlotRange::new(50, 60)]);
    }

    #[test]
    fn kinds_come_from_names() {
        assert_eq!(kind_of("seg-0003.parquet"), FileKind::Movements);
        assert_eq!(kind_of("corr-0003.parquet"), FileKind::Corrections);
        assert_eq!(kind_of(MOVEMENTS), FileKind::Movements);
        assert_eq!(kind_of(CORRECTIONS), FileKind::Corrections);
        assert_eq!(kind_of("archive-0001.parquet"), FileKind::Movements);
    }
}
