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

use serde::{Deserialize, Serialize};

use crate::schema::Completeness;

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
    /// `complete` | `partial` | `unrecorded` — the feed's own words.
    pub completeness: String,
    /// Lowest slot covered, across passes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub walk_from: Option<u64>,
    /// Highest slot covered.
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

    pub fn completeness(&self) -> Completeness {
        Completeness::from_wire(&self.completeness).unwrap_or(Completeness::Unrecorded)
    }

    /// The newest pass — the one whose pending set is current.
    pub fn latest_pass(&self) -> Option<&PassEntry> {
        self.passes.iter().max_by_key(|p| p.seq)
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

    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec_pretty(self)
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
        assert_eq!(back, m);
        assert_eq!(back.completeness(), Completeness::Unrecorded);
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
