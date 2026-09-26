//! Validating an unpacked Mithril snapshot before it is published.
//!
//! # A directory cannot tell you its own boundary
//!
//! cardano-node keeps appending to the highest-numbered immutable file until the next
//! one seals. Mithril expresses that rule *relative to the directory* —
//! `ImmutableFile::list_completed_in_dir` is `filter(|f| f.number < last_number)`
//! (`mithril-cardano-node-internal-database/src/entities/immutable_file.rs`) — so it
//! names a different file in every tree, and `pallas_hardano::storage::immutable` and
//! `chain_sieve::list_chunks` both replicate it on the read side. That is correct
//! there: a reader of a *running node* must not touch the file being written.
//!
//! It is the wrong rule for a published tree. A snapshot is `0..=beacon`, the beacon
//! is signed, and the aggregator applied the drop-the-last rule *before* archiving:
//! `get_last_immutable_number()` is `list_completed_in_dir(..).next_back()`, the digest
//! covers `number <= up_to_file_number` inclusive, and the archive uploads `0..=beacon`.
//! So holding back "the highest local number" of a restored snapshot loses a sealed
//! chunk rather than protecting against an unsealed one — and it has no fixed point,
//! because removing the top file makes the next one the top in turn.
//!
//! # So the boundary is published, and so is the proof
//!
//! `GET /artifact/cardano-database/digests` returns the aggregator's whole digest map
//! rather than one snapshot's. Sealed bytes never change, so a digest published today
//! still describes a file written years ago, which is what makes the map usable for a
//! snapshot long past Mithril's 28-day publication window. A local part is
//! **publishable** exactly when the map names it, and **canonical** exactly when its
//! SHA-256 matches.
//!
//! That is the whole model: [`Reconciliation`] joins a directory against the map, and
//! it is why this crate is no longer offline-only. `validate` reads the directory,
//! `verify` asks Mithril.
//!
//! # What this is not
//!
//! Not a re-derivation of Mithril's cryptography. `mithril-client` verified the archive
//! against the certificate chain at download; what is unproven is whether the bytes *on
//! this disk* are the ones it verified. This is a media check, and the digest map is
//! what makes it a check rather than a hope.
//!
//! # Why nothing here deletes
//!
//! The publishable set is a function of what Mithril has published, so it can only grow
//! as the aggregator advances. Deleting a local file cannot add to it — it can only
//! remove a chunk that a later run would have published.

pub mod aggregator;
pub mod ingest;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result, bail};
use sha2::{Digest as _, Sha256};

/// The three files that make one immutable file.
///
/// `.chunk` holds the blocks; `.primary` and `.secondary` are cardano-node's own index
/// into it. All three are immutable, and the aggregator digests all three.
pub const PART_EXTS: [&str; 3] = ["chunk", "primary", "secondary"];

/// The immutable file number a part name encodes: `09193.chunk` → `9193`.
///
/// The join between a directory and the digest map, which keys on names while
/// [`Inventory`] keys on numbers. One parser, shared by both, so a name that cannot be
/// read one way cannot be read the other either.
pub fn part_number(name: &str) -> Option<u64> {
    let (stem, ext) = name.split_once('.')?;
    if !PART_EXTS.contains(&ext) {
        return None;
    }

    stem.parse().ok()
}

/// Coalesce ascending numbers into inclusive runs.
fn runs_of(absent: impl IntoIterator<Item = u64>) -> Vec<Gap> {
    let mut runs: Vec<Gap> = Vec::new();
    for number in absent {
        match runs.last_mut() {
            Some(run) if run.to + 1 == number => run.to = number,
            _ => runs.push(Gap {
                from: number,
                to: number,
            }),
        }
    }
    runs
}

/// Which of the three parts a number is missing.
fn missing_parts(parts: &BTreeMap<String, PathBuf>) -> Vec<&'static str> {
    PART_EXTS
        .iter()
        .copied()
        .filter(|ext| !parts.contains_key(*ext))
        .collect()
}

/// What is on disk, grouped by immutable file number.
#[derive(Debug, Default, Clone)]
pub struct Inventory {
    /// Number → extension → path.
    pub parts: BTreeMap<u64, BTreeMap<String, PathBuf>>,
    /// Files that belong to no immutable file.
    pub strays: Vec<PathBuf>,
}

impl Inventory {
    /// Read `dir` and group its files by immutable file number.
    pub fn scan(dir: &Path) -> Result<Self> {
        let mut inv = Inventory::default();

        let entries = fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))?;
        for entry in entries {
            let path = entry
                .with_context(|| format!("reading {}", dir.display()))?
                .path();
            if !path.is_file() {
                continue;
            }

            let name = path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default();

            match part_number(&name) {
                Some(number) => {
                    let ext = path
                        .extension()
                        .and_then(|ext| ext.to_str())
                        .expect("part_number checked the extension")
                        .to_string();
                    inv.parts.entry(number).or_default().insert(ext, path);
                }
                None => inv.strays.push(path),
            }
        }

        inv.strays.sort();
        Ok(inv)
    }

    /// Every immutable file number present, ascending.
    pub fn numbers(&self) -> Vec<u64> {
        self.parts.keys().copied().collect()
    }

    /// Every part's file name, exactly as the aggregator names it (`09193.chunk`).
    pub fn part_names(&self) -> BTreeSet<String> {
        self.parts
            .values()
            .flat_map(|parts| parts.values())
            .filter_map(|path| path.file_name())
            .map(|name| name.to_string_lossy().into_owned())
            .collect()
    }

    /// How many part files are present.
    pub fn part_count(&self) -> usize {
        self.parts.values().map(BTreeMap::len).sum()
    }

    /// The last immutable file number present — the only one with no successor, and so
    /// the only one that might still be being written.
    ///
    /// A `max`, not a re-derivation: `pallas_hardano` reaches the same conclusion by
    /// sorting names and popping, and `chain_sieve::list_chunks` by parsing and popping.
    /// Both land here. This is a fact about the *directory*: it does not say whether the
    /// file is finished, only that nothing here can prove it is. For a restored
    /// snapshot the proof is in the digest map — see [`Reconciliation`].
    pub fn highest(&self) -> Option<u64> {
        self.parts.keys().next_back().copied()
    }

    /// The file names of the last immutable file — what an offline run holds back.
    ///
    /// Sorted by extension, so `chunk`, `primary`, `secondary`. This is the
    /// conservative answer when Mithril cannot be asked; `publish` asks, and uses
    /// [`Reconciliation::hold_back`] instead.
    pub fn held_back(&self) -> Vec<String> {
        self.highest()
            .map(|number| self.paths_for(number))
            .unwrap_or_default()
            .into_iter()
            .filter_map(|path| path.file_name())
            .map(|name| name.to_string_lossy().into_owned())
            .collect()
    }

    /// The paths present for `number`.
    pub fn paths_for(&self, number: u64) -> Vec<&PathBuf> {
        self.parts
            .get(&number)
            .map(|m| m.values().collect())
            .unwrap_or_default()
    }
}

/// A finalized immutable file that is missing parts of its triple.
#[derive(Debug, PartialEq, Eq)]
pub struct Incomplete {
    pub number: u64,
    pub missing: Vec<&'static str>,
}

/// An inclusive run of absent numbers between two present ones.
#[derive(Debug, PartialEq, Eq)]
pub struct Gap {
    pub from: u64,
    pub to: u64,
}

/// The outcome of checking an [`Inventory`] on its own.
///
/// This is the *offline* view, and its bias is deliberate: with no map to consult it
/// cannot know where Mithril's boundary is, so it treats the highest number present as
/// possibly-unsealed. `verify` replaces that guess with the published boundary, and
/// `publish` uses the published boundary — never this.
#[derive(Debug)]
pub struct Report {
    /// The numbers a reader would serve: everything except the possibly-unsealed top.
    pub finalized: Vec<u64>,
    /// The last immutable file number present. No successor exists to prove it closed,
    /// so the offline view holds it back — never removes it.
    /// [`Inventory::held_back`] names it.
    pub highest: Option<u64>,
    /// Finalized numbers missing one or more of their three parts.
    pub incomplete: Vec<Incomplete>,
    /// Absent numbers between the lowest and highest present.
    pub gaps: Vec<Gap>,
    /// Files that belong to no immutable file.
    pub strays: Vec<PathBuf>,
}

impl Report {
    pub fn build(inv: &Inventory) -> Self {
        let numbers = inv.numbers();
        let highest = inv.highest();

        let mut incomplete = Vec::new();
        for (&number, parts) in &inv.parts {
            // The offline view takes the top number as unsealed, so a part missing
            // there is expected rather than a fault. `Reconciliation` makes the same
            // judgement against the published boundary instead.
            if Some(number) == highest {
                continue;
            }
            let missing = missing_parts(parts);
            if !missing.is_empty() {
                incomplete.push(Incomplete { number, missing });
            }
        }

        let gaps = match (numbers.first(), numbers.last()) {
            (Some(&first), Some(&last)) => {
                runs_of((first..=last).filter(|n| !inv.parts.contains_key(n)))
            }
            _ => Vec::new(),
        };

        let finalized = numbers
            .iter()
            .copied()
            .filter(|n| Some(*n) != highest)
            .collect();

        Report {
            finalized,
            highest,
            incomplete,
            gaps,
            strays: inv.strays.clone(),
        }
    }

    /// A tree that can be published: no gaps, no incomplete triples among the finalized
    /// range, and nothing that belongs to no immutable file.
    pub fn ok(&self) -> bool {
        self.incomplete.is_empty() && self.gaps.is_empty() && self.strays.is_empty()
    }
}

/// The aggregator's digest map: immutable file name → SHA-256, hex-encoded.
#[derive(Debug, Clone, Default)]
pub struct Digests {
    by_name: BTreeMap<String, String>,
}

impl Digests {
    /// Build from `(immutable_file_name, digest)` pairs.
    pub fn from_parts(parts: impl IntoIterator<Item = (String, String)>) -> Self {
        Digests {
            by_name: parts.into_iter().collect(),
        }
    }

    /// How many parts the map names.
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    /// Whether the map names nothing.
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    /// The digest published for `name`, if any.
    ///
    /// Presence is the claim that Mithril has this file, sealed and canonical.
    pub fn digest(&self, name: &str) -> Option<&str> {
        self.by_name.get(name).map(String::as_str)
    }

    /// Immutable file numbers with all three parts published.
    pub fn sealed_numbers(&self) -> BTreeSet<u64> {
        let mut counts: BTreeMap<u64, usize> = BTreeMap::new();
        for name in self.by_name.keys() {
            if let Some(number) = part_number(name) {
                *counts.entry(number).or_default() += 1;
            }
        }

        counts
            .into_iter()
            .filter(|(_, count)| *count == PART_EXTS.len())
            .map(|(number, _)| number)
            .collect()
    }

    /// The highest number Mithril publishes a full triple for — its boundary.
    ///
    /// This is the number to compare a local tree against, and the number a `validate`
    /// run cannot know. Note it is the map's top, not the latest snapshot's beacon: the
    /// beacon describes one snapshot, while the map spans everything the aggregator has
    /// digested.
    pub fn top(&self) -> Option<u64> {
        self.sealed_numbers().into_iter().next_back()
    }
}

/// A local tree reconciled against what Mithril has published.
#[derive(Debug)]
pub struct Reconciliation {
    /// Local part names the aggregator publishes a digest for.
    pub published: BTreeSet<String>,
    /// Local part names with no published digest — held back from an upload.
    pub unpublished: BTreeSet<String>,
    /// The highest local immutable file number.
    pub local_top: Option<u64>,
    /// How many parts are on disk.
    pub local_parts: usize,
    /// Numbers inside Mithril's published set that are missing a part locally.
    ///
    /// [`Report::incomplete`] cannot see these when they are also the highest local
    /// number, which is exactly the shape a partial or interrupted restore leaves.
    pub incomplete_published: Vec<Incomplete>,
    /// Numbers Mithril publishes that are absent locally, as inclusive runs.
    pub fetchable: Vec<Gap>,
}

impl Reconciliation {
    pub fn build(inv: &Inventory, digests: &Digests) -> Self {
        let local = inv.part_names();
        let published: BTreeSet<String> = local
            .iter()
            .filter(|name| digests.digest(name).is_some())
            .cloned()
            .collect();
        let unpublished: BTreeSet<String> = local.difference(&published).cloned().collect();

        let local_numbers: BTreeSet<u64> = inv.numbers().into_iter().collect();
        let sealed = digests.sealed_numbers();

        let incomplete_published = inv
            .parts
            .iter()
            .filter(|(number, _)| sealed.contains(number))
            .filter_map(|(number, parts)| {
                let missing = missing_parts(parts);
                (!missing.is_empty()).then_some(Incomplete {
                    number: *number,
                    missing,
                })
            })
            .collect();

        Reconciliation {
            published,
            unpublished,
            local_top: inv.highest(),
            local_parts: inv.part_count(),
            incomplete_published,
            fetchable: runs_of(
                sealed
                    .iter()
                    .copied()
                    .filter(|n| !local_numbers.contains(n)),
            ),
        }
    }

    /// Local parts with no published digest, in name order — the exclusion set for a
    /// transfer.
    pub fn hold_back(&self) -> Vec<&str> {
        self.unpublished.iter().map(String::as_str).collect()
    }

    /// Numbers whose parts Mithril does not publish, excluding the local top.
    ///
    /// The top number legitimately holds: a live node is still appending to it, and it
    /// is the one number a snapshot's restore might not have brought along. Anything
    /// *below* the top with no published digest is not a shape a publish should paper
    /// over — the tree is ahead of Mithril, or mixed from two sources.
    pub fn unexplained_holds(&self) -> Vec<u64> {
        self.unpublished
            .iter()
            .filter_map(|name| part_number(name))
            .filter(|number| Some(*number) != self.local_top)
            .collect::<BTreeSet<u64>>()
            .into_iter()
            .collect()
    }

    /// Numbers inside Mithril's published set that are absent locally.
    pub fn missing_count(&self) -> u64 {
        self.fetchable.iter().map(|run| run.to - run.from + 1).sum()
    }

    /// The runs worth fetching, clamped to `boundary`.
    ///
    /// A snapshot covers `0..=beacon`, so nothing above the latest beacon can be
    /// asked for however far the digest map reaches. When the two agree — measured
    /// 2026-09-26, map top 09202 and beacon 09202 — the clamp is a no-op.
    pub fn fetch_runs(&self, boundary: u64) -> Vec<Gap> {
        self.fetchable
            .iter()
            .filter(|run| run.from <= boundary)
            .map(|run| Gap {
                from: run.from,
                to: run.to.min(boundary),
            })
            .collect()
    }
}

/// Move every immutable part out of a staging directory into `db_immutable`.
///
/// Why stage at all: `mithril-client cardano-db download` verifies the certificate
/// against the range it fetched and, on a mismatch, **deletes the whole download
/// directory** (`shared_steps::verify_message_matches_certificate`:
/// `remove_dir_all(db_dir)`). Pointed straight at a database holding 232 GB of good
/// files, that is a way to lose all of them to one bad transfer. Pointed at a staging
/// directory that holds only what was just fetched, it costs the transfer.
///
/// A rename, not a copy — the staging directory is a sibling of the database, so this
/// stays within one filesystem. Refuses to overwrite: a name already in the database
/// means the two disagree about what is missing, which is not something to resolve by
/// guessing.
///
/// Returns the names moved, sorted.
pub fn lift(staging_immutable: &Path, db_immutable: &Path) -> Result<Vec<String>> {
    // Two passes — decide, then move — so a name already in the database does not
    // leave half a chunk moved. `read_dir` order is unspecified, so the plan is
    // sorted to make the move deterministic.
    let mut planned: Vec<(PathBuf, String)> = Vec::new();
    let entries = fs::read_dir(staging_immutable)
        .with_context(|| format!("reading {}", staging_immutable.display()))?;
    for entry in entries {
        let path = entry
            .with_context(|| format!("reading {}", staging_immutable.display()))?
            .path();
        if !path.is_file() {
            continue;
        }

        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        let destination = db_immutable.join(&name);
        if destination.exists() {
            bail!(
                "refusing to overwrite {} — the database already has it, so the fetch \
                 range and the database disagree",
                destination.display()
            );
        }

        planned.push((path, name));
    }
    planned.sort_by(|a, b| a.1.cmp(&b.1));

    fs::create_dir_all(db_immutable)
        .with_context(|| format!("creating {}", db_immutable.display()))?;

    let mut moved = Vec::with_capacity(planned.len());
    for (path, name) in planned {
        let destination = db_immutable.join(&name);
        fs::rename(&path, &destination).with_context(|| {
            format!(
                "moving {} to {} (a staging directory on another filesystem cannot be \
                 renamed across)",
                path.display(),
                destination.display()
            )
        })?;
        moved.push(name);
    }

    Ok(moved)
}

/// A local part whose bytes disagree with the digest Mithril publishes for it.
#[derive(Debug, PartialEq, Eq)]
pub struct Mismatch {
    pub name: String,
    pub published: String,
    pub local: String,
}

/// SHA-256 of a file's contents, hex-encoded.
///
/// What `ImmutableFile::compute_raw_hash::<Sha256>()` does on the aggregator's side,
/// streamed rather than read into memory — a chunk reaches 128 MiB.
pub fn part_digest(path: &Path) -> Result<String> {
    let mut hasher = Sha256::new();
    let mut file = fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    io::copy(&mut file, &mut hasher).with_context(|| format!("hashing {}", path.display()))?;

    Ok(hex::encode(hasher.finalize()))
}

/// Check every local part that has a published digest, `jobs` files at a time.
///
/// Returns the mismatches in name order, empty when the tree is the published one. A
/// part with no published digest is not checked — nothing exists to check it against,
/// and [`Reconciliation::unpublished`] names it instead.
pub fn verify_parts(inv: &Inventory, digests: &Digests, jobs: usize) -> Result<Vec<Mismatch>> {
    let mut work: Vec<(String, &Path)> = Vec::new();
    for path in inv.parts.values().flat_map(|parts| parts.values()) {
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        if digests.digest(&name).is_some() {
            work.push((name, path.as_path()));
        }
    }
    work.sort();

    let jobs = jobs.max(1).min(work.len().max(1));
    let next = AtomicUsize::new(0);
    let work = &work;

    let checked: Vec<Result<Vec<Mismatch>>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..jobs)
            .map(|_| {
                let next = &next;
                scope.spawn(move || {
                    let mut mine = Vec::new();
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some((name, path)) = work.get(index) else {
                            break;
                        };
                        let published = digests
                            .digest(name)
                            .expect("work is filtered on having a published digest");
                        let local = part_digest(path)?;
                        if local != published {
                            mine.push(Mismatch {
                                name: name.clone(),
                                published: published.to_string(),
                                local,
                            });
                        }
                    }

                    Ok(mine)
                })
            })
            .collect();

        handles
            .into_iter()
            .map(|handle| handle.join().expect("a hashing worker panicked"))
            .collect()
    });

    let mut mismatches = Vec::new();
    for result in checked {
        mismatches.extend(result?);
    }
    mismatches.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(mismatches)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scratch directory that cleans up after itself. No `tempfile` dependency
    /// for three tests.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(tag: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("mithril-snapshot-{tag}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).expect("create scratch");
            Scratch(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        fn write(&self, name: &str) {
            fs::write(self.0.join(name), b"x").expect("write");
        }

        /// A sealed immutable file: all three parts present.
        fn write_triple(&self, number: u64) {
            for ext in PART_EXTS {
                self.write(&format!("{number:05}.{ext}"));
            }
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn the_last_file_is_the_one_with_no_successor() {
        let scratch = Scratch::new("highest");
        // The last file carries all three parts too — measured on the mainnet
        // store 2026-09-26, where the highest number had a complete triple. It is
        // held back because of its position, not its shape.
        for number in 0..4 {
            scratch.write_triple(number);
        }

        let inv = Inventory::scan(scratch.path()).unwrap();
        assert_eq!(inv.highest(), Some(3));

        let report = Report::build(&inv);
        assert_eq!(report.finalized, vec![0, 1, 2]);
        assert_eq!(report.highest, Some(3));
        assert!(report.incomplete.is_empty());
        assert!(report.ok());
    }

    #[test]
    fn a_finalized_file_missing_its_sidecars_is_incomplete() {
        let scratch = Scratch::new("incomplete");
        scratch.write_triple(0);
        scratch.write_triple(1);
        scratch.write("00002.chunk"); // sealed, but its sidecars are missing
        scratch.write_triple(3); // last is 3; 2 is finalized and bare

        let inv = Inventory::scan(scratch.path()).unwrap();
        let report = Report::build(&inv);
        assert_eq!(
            report.incomplete,
            vec![Incomplete {
                number: 2,
                missing: vec!["primary", "secondary"],
            }]
        );
        assert!(!report.ok());
    }

    #[test]
    fn absent_numbers_coalesce_into_runs() {
        let scratch = Scratch::new("gaps");
        for number in [0, 1, 4, 5, 6, 9] {
            scratch.write_triple(number);
        }

        let inv = Inventory::scan(scratch.path()).unwrap();
        let report = Report::build(&inv);
        assert_eq!(
            report.gaps,
            vec![Gap { from: 2, to: 3 }, Gap { from: 7, to: 8 }]
        );
    }

    #[test]
    fn the_last_file_is_what_gets_held_back() {
        let scratch = Scratch::new("held-back");
        scratch.write_triple(0);
        scratch.write_triple(1);
        scratch.write_triple(2);

        let inv = Inventory::scan(scratch.path()).unwrap();
        assert_eq!(
            inv.held_back(),
            vec!["00002.chunk", "00002.primary", "00002.secondary"]
        );
    }

    #[test]
    fn a_non_numeric_name_is_a_stray() {
        let scratch = Scratch::new("stray");
        scratch.write_triple(0);
        scratch.write("notes.txt");

        let inv = Inventory::scan(scratch.path()).unwrap();
        assert_eq!(inv.strays.len(), 1);
        assert!(!Report::build(&inv).ok());
    }

    #[test]
    fn a_part_number_is_what_the_aggregator_publishes_a_digest_for() {
        assert_eq!(part_number("09193.chunk"), Some(9193));
        assert_eq!(part_number("00000.secondary"), Some(0));
        assert_eq!(part_number("09193"), None); // no extension
        assert_eq!(part_number("09193.chunk.bak"), None); // not a part extension
        assert_eq!(part_number("latest.chunk"), None); // not a number
    }

    #[test]
    fn the_map_needs_all_three_parts_before_it_calls_a_number_sealed() {
        let digests = Digests::from_parts([
            ("00000.chunk".to_string(), "a".to_string()),
            ("00000.primary".to_string(), "b".to_string()),
            ("00000.secondary".to_string(), "c".to_string()),
            // 00001 is a number the map knows, but only one of its parts
            ("00001.chunk".to_string(), "d".to_string()),
        ]);

        assert_eq!(digests.top(), Some(0));
        assert_eq!(
            digests.sealed_numbers().into_iter().collect::<Vec<_>>(),
            vec![0]
        );
    }

    /// The published set is what makes a part uploadable — not its position.
    #[test]
    fn a_part_mithril_publishes_is_publishable_wherever_it_sits() {
        let scratch = Scratch::new("reconcile");
        for number in 0..3 {
            scratch.write_triple(number);
        }

        // Mithril publishes 0 and 1; 2 is the local top and is unpublished.
        let names = (0..2)
            .flat_map(|number| {
                PART_EXTS
                    .iter()
                    .map(move |ext| (format!("{number:05}.{ext}"), "digest".to_string()))
            })
            .collect::<Vec<_>>();

        let inv = Inventory::scan(scratch.path()).unwrap();
        let recon = Reconciliation::build(&inv, &Digests::from_parts(names));

        assert_eq!(recon.published.len(), 6);
        assert_eq!(recon.local_parts, 9);
        assert_eq!(
            recon.hold_back(),
            vec!["00002.chunk", "00002.primary", "00002.secondary"]
        );
        // Holding back the top is the one hold that is explained.
        assert!(recon.unexplained_holds().is_empty());
        assert!(recon.fetchable.is_empty());
    }

    /// A bare triple below the top is what a positional rule cannot see.
    #[test]
    fn an_unpublished_part_below_the_top_is_unexplained() {
        let scratch = Scratch::new("unexplained");
        for number in 0..3 {
            scratch.write_triple(number);
        }

        // 1 is published only partially, so all of 1's local parts are held back
        // even though 2 — the top — is.
        let mut names = Vec::new();
        for ext in PART_EXTS {
            names.push((format!("00000.{ext}"), "digest".to_string()));
            names.push((format!("00002.{ext}"), "digest".to_string()));
        }
        names.push(("00001.chunk".to_string(), "digest".to_string()));

        let inv = Inventory::scan(scratch.path()).unwrap();
        let recon = Reconciliation::build(&inv, &Digests::from_parts(names));

        assert_eq!(recon.unexplained_holds(), vec![1]);
    }

    /// Mithril publishes numbers we do not hold — the fetch delta.
    #[test]
    fn numbers_published_above_the_local_top_coalesce_into_runs() {
        let scratch = Scratch::new("fetchable");
        for number in 0..3 {
            scratch.write_triple(number);
        }

        let names = (0..6)
            .flat_map(|number| {
                PART_EXTS
                    .iter()
                    .map(move |ext| (format!("{number:05}.{ext}"), "digest".to_string()))
            })
            .collect::<Vec<_>>();

        let inv = Inventory::scan(scratch.path()).unwrap();
        let recon = Reconciliation::build(&inv, &Digests::from_parts(names));

        assert!(recon.hold_back().is_empty());
        assert_eq!(recon.fetchable, vec![Gap { from: 3, to: 5 }]);
        assert_eq!(recon.missing_count(), 3);
    }

    /// Inside Mithril's published range, a missing part is a fault even at the top.
    #[test]
    fn a_published_number_missing_a_part_is_incomplete_at_the_top() {
        let scratch = Scratch::new("incomplete-published");
        scratch.write_triple(0);
        scratch.write("00001.chunk"); // the local top, and its sidecars are absent

        let names = (0..2)
            .flat_map(|number| {
                PART_EXTS
                    .iter()
                    .map(move |ext| (format!("{number:05}.{ext}"), "digest".to_string()))
            })
            .collect::<Vec<_>>();

        let inv = Inventory::scan(scratch.path()).unwrap();
        let recon = Reconciliation::build(&inv, &Digests::from_parts(names));

        // The offline view cannot see this, because it is the highest number.
        assert!(Report::build(&inv).incomplete.is_empty());
        assert_eq!(
            recon.incomplete_published,
            vec![Incomplete {
                number: 1,
                missing: vec!["primary", "secondary"],
            }]
        );
    }

    #[test]
    fn a_part_digest_is_sha256_of_the_file_contents() {
        let scratch = Scratch::new("digest");
        scratch.write("00000.chunk"); // b"x"

        // sha256("x"), from `printf 'x' | shasum -a 256`.
        assert_eq!(
            part_digest(&scratch.path().join("00000.chunk")).unwrap(),
            "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"
        );
    }

    #[test]
    fn a_published_digest_the_bytes_do_not_match_is_a_mismatch() {
        let scratch = Scratch::new("mismatch");
        scratch.write_triple(0);
        scratch.write_triple(1);

        let matching = part_digest(&scratch.path().join("00000.chunk")).unwrap();
        let mut names = vec![("00000.chunk".to_string(), matching)];
        for name in [
            "00000.primary",
            "00000.secondary",
            "00001.chunk",
            "00001.primary",
            "00001.secondary",
        ] {
            names.push((name.to_string(), "00".to_string()));
        }

        let inv = Inventory::scan(scratch.path()).unwrap();
        let mismatches = verify_parts(&inv, &Digests::from_parts(names), 2).unwrap();

        assert_eq!(
            mismatches
                .iter()
                .map(|m| m.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "00000.primary",
                "00000.secondary",
                "00001.chunk",
                "00001.primary",
                "00001.secondary",
            ]
        );
        assert_eq!(mismatches[0].published, "00");
        assert_eq!(mismatches[0].local.len(), 64);
    }

    /// The map can reach past the beacon; a snapshot cannot serve past it.
    #[test]
    fn the_fetch_is_clamped_to_what_the_beacon_covers() {
        let scratch = Scratch::new("fetch-runs");
        scratch.write_triple(0);

        let names = (0..6)
            .flat_map(|number| {
                PART_EXTS
                    .iter()
                    .map(move |ext| (format!("{number:05}.{ext}"), "digest".to_string()))
            })
            .collect::<Vec<_>>();

        let inv = Inventory::scan(scratch.path()).unwrap();
        let recon = Reconciliation::build(&inv, &Digests::from_parts(names));

        assert_eq!(recon.fetchable, vec![Gap { from: 1, to: 5 }]);
        assert_eq!(recon.fetch_runs(3), vec![Gap { from: 1, to: 3 }]);
        assert_eq!(recon.fetch_runs(5), vec![Gap { from: 1, to: 5 }]);
        assert!(recon.fetch_runs(0).is_empty());
    }

    #[test]
    fn staging_moves_every_part_in_and_leaves_the_database_alone() {
        let stage = Scratch::new("lift-stage");
        let database = Scratch::new("lift-db");
        database.write_triple(0);
        stage.write_triple(1);

        let moved = lift(stage.path(), database.path()).unwrap();

        assert_eq!(
            moved,
            vec!["00001.chunk", "00001.primary", "00001.secondary"]
        );
        assert!(database.path().join("00001.chunk").exists());
        assert!(!stage.path().join("00001.chunk").exists());
        assert!(database.path().join("00000.chunk").exists());
    }

    /// A name already present is a disagreement, and is reported rather than resolved.
    #[test]
    fn staging_refuses_to_overwrite_what_the_database_already_has() {
        let stage = Scratch::new("lift-clash-stage");
        let database = Scratch::new("lift-clash-db");
        for number in 0..2 {
            stage.write_triple(number);
            database.write_triple(number);
        }

        let error = lift(stage.path(), database.path()).unwrap_err();

        assert!(format!("{error}").contains("refusing to overwrite"));
        // The decision pass runs before the move, so nothing was half-moved.
        assert!(database.path().join("00001.chunk").exists());
        assert!(stage.path().join("00000.chunk").exists());
        assert!(stage.path().join("00001.chunk").exists());
    }
}
