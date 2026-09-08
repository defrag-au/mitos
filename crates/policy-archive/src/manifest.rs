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

fn is_zero(n: &u32) -> bool {
    *n == 0
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

/// A stretch read, and which chain it came from. The two never merge with
/// each other: an immutable file is SUMMED with everything else, a volatile
/// one is REPLACED whole on every refresh, so "read" means a different thing
/// on each side of the immutable tip and a reader has to be told which.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Span {
    pub from: u64,
    pub to: u64,
    pub kind: RangeKind,
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
    /// THE ROLLUP: the passes marked [`PassEntry::rolled_up`], folded into
    /// one file at the policy's root. Those passes hold no files of their
    /// own any more; their entries stay for the record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollup: Option<FileEntry>,
    /// The highest `seq` the rollup folded — a RECORD, not the test.
    ///
    /// It used to be the test, and that was a defect the moment jobs began
    /// landing out of order: four walk workers on one policy land seq 10
    /// before seq 8, a rollup at seq 10 wrote `rolled_up_through: 10`, and
    /// seq 8 then landed into a manifest that no longer named its files —
    /// which the next prune deleted. Measured on a ClayNation re-walk: 13
    /// out-of-order landings, 6,641 transactions and 72 whole days gone from
    /// an archive that called itself complete. The test is the per-pass flag;
    /// this is read only to migrate a manifest written before it existed
    /// (see [`Manifest::from_json`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rolled_up_through: Option<u32>,
    /// The rollup file's OWN sequence — see [`Manifest::next_rollup_seq`].
    #[serde(default, skip_serializing_if = "is_zero")]
    pub rollup_seq: u32,
    /// The carried pending set at the policy's root, once a rollup has
    /// removed the pass directory that held it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending: Option<String>,
    /// What this policy's units ARE, accumulated by the walks.
    ///
    /// Stamped here because it records what the archive ASSUMED, not merely
    /// what it found — in particular whether undecoded candidates were worth
    /// keeping. A reader that finds no candidates must be able to tell "there
    /// were none" from "this is a collection and we did not keep them".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<crate::profile::Profile>,
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
            rollup_seq: 0,
            pending: None,
            profile: None,
            passes: Vec::new(),
            updated_unix: 0,
        }
    }

    /// Every stretch any pass has read, merged, ascending. THE coverage;
    /// everything else here is a view of it.
    pub fn ranges(&self) -> Vec<SlotRange> {
        merge_ranges(
            self.spans()
                .into_iter()
                .map(|s| SlotRange::new(s.from, s.to))
                .collect(),
        )
    }

    /// The stretches read from the IMMUTABLE chain alone — the settled ones.
    ///
    /// This, not [`Self::ranges`], is what completeness and the descent are
    /// about: the volatile tail sits above the immutable tip, can roll back,
    /// and is replaced rather than extended. A walk that treated it as
    /// coverage would think it had already read to the live tip and stop
    /// generating the top-ups that make the tail shrink.
    pub fn immutable_ranges(&self) -> Vec<SlotRange> {
        merge_ranges(self.ranges_of(RangeKind::Immutable))
    }

    /// Every stretch read, merged WITHIN each kind and tagged with it.
    pub fn spans(&self) -> Vec<Span> {
        let mut out: Vec<Span> = Vec::new();
        for kind in [RangeKind::Immutable, RangeKind::Volatile] {
            out.extend(
                merge_ranges(self.ranges_of(kind))
                    .into_iter()
                    .map(|r| Span {
                        from: r.from,
                        to: r.to,
                        kind,
                    }),
            );
        }
        out.sort_by_key(|s| (s.from, s.to));
        out
    }

    fn ranges_of(&self, kind: RangeKind) -> Vec<SlotRange> {
        let mut all = Vec::new();
        for p in self.passes.iter().filter(|p| p.kind == kind) {
            all.push(SlotRange::new(p.floor, p.ceiling));
            all.extend(p.windows.iter().copied());
        }
        all
    }

    /// The one volatile pass, if the tail has been read. At most one by
    /// construction: each refresh replaces it.
    pub fn volatile(&self) -> Option<&PassEntry> {
        self.passes.iter().find(|p| p.kind == RangeKind::Volatile)
    }

    /// The top of the settled coverage — what "complete to" means. The
    /// volatile tail rides above it.
    pub fn immutable_walk_to(&self) -> Option<u64> {
        self.immutable_ranges().last().map(|r| r.to)
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

    /// The parts of `[from, to)` no IMMUTABLE pass has read, ascending —
    /// what is left for a job to do.
    ///
    /// Immutable only, because this is what a job asks before deciding to
    /// skip a range. The volatile tail is replaced on its next refresh, so
    /// a top-up that read the stretch it covers is not doing work twice —
    /// it is the only thing making that stretch settled.
    pub fn uncovered(&self, from: u64, to: u64) -> Vec<SlotRange> {
        let mut out = Vec::new();
        let mut cursor = from;
        for r in self.immutable_ranges() {
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
        // IMMUTABLE only. A volatile tail is a live tail, not settled
        // history: an archive whose immutable stretch stops short is
        // partial however far above the tip it can also see.
        let ranges = self.immutable_ranges();
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

    /// Every pending sidecar still on disk, as `(landed_unix, path)`
    /// relative to the policy's directory: one per un-rolled-up pass, plus
    /// the root's after a rollup (the oldest, at time zero). A job merges
    /// them latest-first; see the walker's `load_pending_union`.
    pub fn pending_files(&self) -> Vec<(u64, String)> {
        let mut out: Vec<(u64, String)> = self
            .passes
            .iter()
            .filter(|p| !p.rolled_up && p.kind == RangeKind::Immutable)
            .map(|p| (p.written_unix, format!("{}/{}", p.dir, PENDING)))
            .collect();
        if let Some(root) = &self.pending {
            out.push((0, root.clone()));
        }
        out
    }

    pub fn next_seq(&self) -> u32 {
        self.latest_pass().map_or(0, |p| p.seq + 1)
    }

    /// The next ROLLUP file's sequence — its own counter, never the pass
    /// sequence.
    ///
    /// It used to be `next_seq()`, and that was a silent data-loss bug the
    /// moment jobs began landing out of order. `next_seq()` is `max(seq)+1`,
    /// so two rollups at the same high-water mark computed the SAME name:
    /// seq 104 lands → rollup writes `archive-0105`; seq 103 lands later →
    /// rollup again, max seq still 104 → `archive-0105` again. The second
    /// merge read that file as an INPUT, wrote its output over the same
    /// name, and then the prune — which deletes every input — removed the
    /// file the manifest had just been pointed at. Policy `f7f5a12b…` lost
    /// its whole rolled-up history that way on 2026-09-06, and every read
    /// 500'd on the missing file.
    ///
    /// Monotonic across the migration: a manifest written before this field
    /// existed carries the number in its rollup's FILENAME, so that is read
    /// as the floor and no name is ever reused.
    pub fn next_rollup_seq(&self) -> u32 {
        let from_name = self
            .rollup
            .as_ref()
            .and_then(|r| r.file.strip_prefix("archive-"))
            .and_then(|s| s.strip_suffix(".parquet"))
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(0);
        self.rollup_seq.max(from_name).saturating_add(1)
    }

    /// Every Parquet file a reader must merge, as `(relative path, kind)`,
    /// relative to the policy's directory or key prefix.
    pub fn files(&self) -> Vec<(String, FileKind)> {
        self.files_from(&self.passes.iter().collect::<Vec<_>>())
    }

    /// The files a ROLLUP may fold: the immutable ones only.
    ///
    /// A volatile file is replaced whole on the next refresh, so folding it
    /// into the rollup would bake a stretch that can roll back into a file
    /// that nothing ever rewrites.
    pub fn immutable_files(&self) -> Vec<(String, FileKind)> {
        self.files_from(
            &self
                .passes
                .iter()
                .filter(|p| p.kind == RangeKind::Immutable)
                .collect::<Vec<_>>(),
        )
    }

    fn files_from(&self, passes: &[&PassEntry]) -> Vec<(String, FileKind)> {
        let mut out = Vec::new();
        if let Some(r) = &self.rollup {
            out.push((r.file.clone(), FileKind::Movements));
        }
        for p in passes {
            if p.rolled_up {
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
    ///
    /// IMMUTABLE passes only. The volatile tail is re-derived whole on every
    /// refresh, so it carries nothing forward and writes no sidecar — and a
    /// manifest that named one sent the publisher to `stat` a file that has
    /// never existed, which failed the whole publish before it reached the
    /// manifest flip and quietly froze R2 and KV at the last good tick.
    pub fn pending_file(&self) -> Option<String> {
        match self.latest_immutable_pass() {
            Some(p) if !p.rolled_up => Some(format!("{}/{}", p.dir, PENDING)),
            _ => self.pending.clone(),
        }
    }

    /// The newest pass read from the settled chain — whose pending set is
    /// the one a later job carries.
    pub fn latest_immutable_pass(&self) -> Option<&PassEntry> {
        self.passes
            .iter()
            .filter(|p| p.kind == RangeKind::Immutable)
            .max_by_key(|p| p.seq)
    }

    /// A LOWER BOUND on the policy's distinct units: the most any one pass
    /// or rollup saw.
    ///
    /// # A ROLLED-UP PASS DOES NOT VOTE
    ///
    /// Its rows are in the rollup and its file is gone, so its count is
    /// subsumed — and taking a `max` across both is how a stale number
    /// outlived the thing that produced it.
    ///
    /// This was harmless while every count was computed the same way: a
    /// rollup merges strictly more rows than any single pass, so it always
    /// won the `max` anyway. It stopped being harmless the moment `units`
    /// gained a meaning — CIP-68 reference twins and CIP-27 royalty tokens
    /// are no longer counted — because a rollup written by the new code
    /// (5,000 for Mekka S1) then lost the `max` to a pass entry written by
    /// the old code (9,690), and re-folding the archive could not shift it.
    /// A number nothing can correct is worse than one that is merely wrong.
    ///
    /// LOOSE passes still vote: their rows are genuinely not in the rollup.
    pub fn units(&self) -> u64 {
        self.passes
            .iter()
            .filter(|p| !p.rolled_up)
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
        let mut m: Manifest = serde_json::from_slice(bytes)?;
        // A manifest written before [`PassEntry::rolled_up`] existed says
        // which passes were folded only as a threshold. Read it once, here,
        // so nothing downstream has to know the field ever meant that. A
        // manifest this code wrote always flags at least one pass, so this
        // never fires on one of ours.
        if let Some(through) = m.rolled_up_through
            && !m.passes.iter().any(|p| p.rolled_up)
        {
            for p in m.passes.iter_mut().filter(|p| p.seq <= through) {
                p.rolled_up = true;
            }
        }
        Ok(m)
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
    /// A rollup has folded this pass's files into the policy's rollup file
    /// and pruned them. Per pass, never a threshold over `seq`: jobs land
    /// out of order, so "every pass up to N" is not a statement anyone can
    /// make — see [`Manifest::rolled_up_through`].
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub rolled_up: bool,
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

    /// The VOLATILE tail's directory. Its own name, because it is the one
    /// entry in the archive that is replaced rather than added to, and an
    /// operator looking at the directory should be able to see that. A
    /// sequence still, so no name is ever reused — the publisher skips an
    /// R2 object it already holds at the same size, which is only safe
    /// while names are unique.
    pub fn tip_dir_name(seq: u32) -> String {
        format!("tip-{seq:04}")
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
            rolled_up: false,
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

    /// A CORRECTED ROLLUP MUST BE ABLE TO LOWER THE COUNT.
    ///
    /// The exact shape that made a wrong number uncorrectable: `units` stopped
    /// counting CIP-68 reference twins and CIP-27 royalty tokens, a forced
    /// rollup recomputed Mekka S1 from 9,690 to 5,000 — and the old pass
    /// entries, still carrying 9,690 for files that no longer exist, won the
    /// `max` and put the stale number straight back on the page.
    ///
    /// A rolled-up pass's rows are IN the rollup, so its count is subsumed.
    /// A loose pass's are not, so its count still counts.
    #[test]
    fn a_rolled_up_pass_cannot_outvote_the_rollup_that_replaced_it() {
        let mut m = Manifest::new("ab");
        let mut stale = pass(0);
        stale.units = 9_690;
        stale.rolled_up = true;
        m.passes = vec![stale];
        m.rollup = Some(FileEntry {
            units: 5_000,
            ..entry("archive-0001.parquet")
        });
        assert_eq!(
            m.units(),
            5_000,
            "the rollup counted every row and is the only file left"
        );

        // A pass still waiting to be folded genuinely holds rows the rollup
        // does not, so it is still evidence of a lower bound.
        let mut loose = pass(1);
        loose.units = 6_100;
        m.passes.push(loose);
        assert_eq!(m.units(), 6_100, "a LOOSE pass still votes");
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
        for p in m.passes.iter_mut().filter(|p| p.seq <= 1) {
            p.rolled_up = true;
        }
        m.pending = Some(PENDING.into());
        let after = m.files();
        assert_eq!(after.len(), 3, "rollup + pass 2's two files");
        assert_eq!(
            after[0],
            ("archive-0001.parquet".to_string(), FileKind::Movements)
        );
        assert_eq!(m.pending_file().as_deref(), Some("pass-0002/pending.bin"));

        m.passes[2].rolled_up = true;
        assert_eq!(m.files().len(), 1);
        assert_eq!(m.pending_file().as_deref(), Some(PENDING));
        assert_eq!(m.next_seq(), 3, "sequence numbers never reuse");
    }

    /// A PASS THAT LANDS AFTER A ROLLUP WITH A LOWER `seq` IS STILL LOOSE.
    ///
    /// Four walk workers on one policy land out of order — seq 10 before
    /// seq 8 — and the rollup at seq 10 folded only what the manifest named
    /// at the time. Marking "everything up to 10" instead deleted 6,641
    /// transactions and 72 whole days from a ClayNation archive that went
    /// on calling itself complete.
    #[test]
    fn a_pass_that_lands_after_a_rollup_is_not_folded_by_its_sequence() {
        let mut m = Manifest::new("ab");
        m.passes = vec![pass(0), pass(2)];
        for p in m.passes.iter_mut() {
            p.rolled_up = true;
        }
        m.rollup = Some(entry("archive-0003.parquet"));
        m.rolled_up_through = Some(2);
        m.pending = Some(PENDING.into());
        assert_eq!(m.files().len(), 1, "only the rollup, so far");

        // Seq 1 was in flight while that rollup ran, and lands now.
        m.passes.push(pass(1));
        m.passes.sort_by_key(|p| p.seq);
        let files = m.files();
        assert_eq!(files.len(), 3, "the rollup AND the late pass's two files");
        assert!(
            files
                .iter()
                .any(|(f, _)| f == "pass-0001/movements.parquet")
        );
        assert_eq!(
            m.pending_files().len(),
            2,
            "the late pass's sidecar and the root's"
        );
    }

    /// THE ROLLUP FILE'S NAME MUST NEVER REPEAT, and the pass sequence
    /// cannot promise that.
    ///
    /// `next_seq()` is `max(seq)+1`, so two rollups at the same high-water
    /// mark — ordinary once jobs land out of order — computed the same name.
    /// The second read that file as an input, wrote its output over it, and
    /// the prune then deleted every input, taking the file the manifest had
    /// just been pointed at. Policy `f7f5a12b…` lost its entire rolled-up
    /// history to this on 2026-09-06 and every read 500'd.
    #[test]
    fn the_rollup_sequence_never_repeats_when_passes_land_out_of_order() {
        let mut m = Manifest::new("ab");
        m.passes = vec![pass(0), pass(1), pass(2)];
        // Three passes landed, seq 2 highest: the old rule said "0003".
        assert_eq!(m.next_seq(), 3);
        let first = m.next_rollup_seq();
        m.rollup = Some(entry(&format!("archive-{first:04}.parquet")));
        m.rollup_seq = first;

        // Seq 1 was in flight and lands now: the high-water mark has NOT
        // moved, so `next_seq()` is unchanged — and the rollup name must be
        // anyway.
        assert_eq!(m.next_seq(), 3, "the pass sequence really does repeat");
        let second = m.next_rollup_seq();
        assert!(second > first, "{second} must be past {first}");
        assert_ne!(
            format!("archive-{second:04}.parquet"),
            m.rollup.as_ref().unwrap().file,
            "a rollup must never write over its own input"
        );
    }

    /// A manifest written before `rollup_seq` existed carries the number in
    /// its rollup's FILENAME. Read it as the floor, or the first rollup
    /// after the upgrade reuses a name R2 may still hold.
    #[test]
    fn the_rollup_sequence_migrates_from_the_filename() {
        let mut m = Manifest::new("ab");
        m.rollup = Some(entry("archive-0105.parquet"));
        assert_eq!(m.rollup_seq, 0, "the field is absent in the old shape");
        assert_eq!(m.next_rollup_seq(), 106);
    }

    /// THE VOLATILE TAIL CARRIES NOTHING FORWARD, so it names no sidecar.
    ///
    /// It is re-derived whole on every refresh, so there is no pending set
    /// to hand to a later job. A manifest that named one sent the publisher
    /// to `stat` a file that has never existed; that failed the publish
    /// before it reached the manifest flip, and R2 and KV silently froze at
    /// the last tick before the tail started running.
    #[test]
    fn the_volatile_tail_names_no_pending_sidecar() {
        let mut m = Manifest::new("ab");
        m.passes = vec![pass(0), pass(1)];
        let mut tail = pass(2);
        tail.kind = RangeKind::Volatile;
        tail.dir = PassEntry::tip_dir_name(2);
        m.passes.push(tail);

        let sidecars = m.pending_files();
        assert_eq!(sidecars.len(), 2, "the two immutable passes only");
        assert!(
            !sidecars.iter().any(|(_, f)| f.starts_with("tip-")),
            "{sidecars:?}"
        );
        // And the CURRENT one is the newest immutable pass, not the tail,
        // even though the tail has the highest sequence.
        assert_eq!(
            m.latest_pass().map(|p| p.seq),
            Some(2),
            "the tail is newest"
        );
        assert_eq!(m.pending_file().as_deref(), Some("pass-0001/pending.bin"));
    }

    /// A manifest written before the per-pass flag says which passes were
    /// folded only as a threshold. It is read once, on load, and never
    /// meant again.
    #[test]
    fn a_manifest_from_before_the_flag_migrates_on_load() {
        let mut m = Manifest::new("ab");
        m.passes = vec![pass(0), pass(1), pass(2)];
        m.rollup = Some(entry("archive-0002.parquet"));
        m.rolled_up_through = Some(1);
        let raw = serde_json::to_vec(&m).unwrap();
        let back = Manifest::from_json(&raw).unwrap();
        assert_eq!(
            back.passes.iter().map(|p| p.rolled_up).collect::<Vec<_>>(),
            vec![true, true, false]
        );
        assert_eq!(back.files().len(), 3, "rollup + pass 2's two files");
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
