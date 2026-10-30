//! What the store holds, and what Mithril publishes that it does not.
//!
//! The local half is a directory scan and needs no network. The published half
//! joins that inventory to the aggregator's digest map, and is computed after
//! the local answer so a screen can show what is on disk before (or without)
//! the aggregator answering.
//!
//! Coverage is counted in immutable files, not bytes: a number is present only
//! when all three parts are. A number missing a sidecar is `incomplete`, not
//! covered, because no reader can use it.

use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;

use anyhow::{Context, Result};
use mithril_snapshot::{Digests, Inventory, PART_EXTS};
use serde::{Deserialize, Serialize};

use crate::manifest::PolicyManifest;

/// How many missing runs a screen carries. The count is kept separately.
pub const MAX_GAPS: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRange {
    pub from: u64,
    pub to: u64,
}

impl std::fmt::Display for ChunkRange {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:05}–{:05}", self.from, self.to)
    }
}

/// One policy's requested range, and how much of it is on disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyCoverage {
    pub policy: String,
    pub earliest_asset: String,
    pub floor_chunk: u64,
    pub requested_to: Option<u64>,
    pub covered: u64,
    pub total: u64,
}

impl PolicyCoverage {
    pub fn percent(&self) -> u16 {
        if self.total == 0 {
            return 0;
        }
        (self.covered.saturating_mul(100) / self.total).min(100) as u16
    }
}

/// The store on its own — no aggregator call, no notion of published.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LocalCoverage {
    /// Complete immutable files (all three parts).
    pub files: u64,
    pub bytes: u64,
    pub first: Option<u64>,
    pub last: Option<u64>,
    /// Numbers on disk missing at least one part.
    pub incomplete: Vec<u64>,
    pub policies: Vec<PolicyCoverage>,
    /// Policy manifests that could not be read, named rather than dropped.
    pub policy_errors: Vec<String>,
    /// The complete numbers, for the published join. Not serialised: a fixture
    /// carries the counts and the screen does not need the set.
    #[serde(skip)]
    pub complete: BTreeSet<u64>,
}

/// The store joined against what the aggregator has signed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishedCoverage {
    /// The newest signed snapshot. Nothing above this can be fetched yet.
    pub beacon: u64,
    pub epoch: u64,
    pub created_at: String,
    /// From the snapshot's total, so an estimate rather than a measurement.
    pub per_file_bytes: u64,
    /// Sealed numbers up to and including the beacon.
    pub published_files: u64,
    pub covered_files: u64,
    pub missing_files: u64,
    pub missing_bytes: u64,
    /// Complete local numbers Mithril publishes no digest for.
    pub unpublished_files: u64,
    /// The first [`MAX_GAPS`] missing runs, ascending.
    pub gaps: Vec<ChunkRange>,
    pub gap_count: u64,
}

impl PublishedCoverage {
    pub fn percent(&self) -> u16 {
        if self.published_files == 0 {
            return 0;
        }
        (self
            .covered_files
            .saturating_mul(100)
            .checked_div(self.published_files)
            .unwrap_or(0))
        .min(100) as u16
    }

    /// `4 of 9,339` is 0% when truncated, which reads as nothing at all. Say
    /// `<1%` while there is something there.
    pub fn percent_label(&self) -> String {
        let percent = self.percent();
        if percent == 0 && self.covered_files > 0 {
            "<1%".to_string()
        } else {
            format!("{percent}%")
        }
    }
}

pub fn scan_local(destination: &Path) -> Result<LocalCoverage> {
    let immutable = destination.join("immutable");
    let inventory = if immutable.is_dir() {
        Inventory::scan(&immutable)?
    } else {
        Inventory::default()
    };

    let mut complete = BTreeSet::new();
    let mut incomplete = Vec::new();
    let mut bytes = 0u64;
    for (number, parts) in &inventory.parts {
        if parts.len() == PART_EXTS.len() {
            complete.insert(*number);
        } else {
            incomplete.push(*number);
        }
        for path in parts.values() {
            bytes += fs::metadata(path)
                .with_context(|| format!("reading the size of {}", path.display()))?
                .len();
        }
    }
    incomplete.sort_unstable();

    let (policies, policy_errors) = scan_policies(destination, &complete);

    Ok(LocalCoverage {
        files: complete.len() as u64,
        bytes,
        first: complete.first().copied(),
        last: complete.last().copied(),
        incomplete,
        policies,
        policy_errors,
        complete,
    })
}

/// Join a local scan to the published digest map.
///
/// Only numbers from 1 through `beacon` are counted. The map can name files
/// above the newest signed snapshot, and those cannot be fetched yet, so calling
/// them "missing" would make the coverage number permanently short. Number 0 is
/// excluded too: `ImmutableFileRange` starts at 1, so a backfill could never
/// fetch it and the coverage would never reach 100%.
pub fn scan_published(
    local: &LocalCoverage,
    digests: &Digests,
    beacon: u64,
    epoch: u64,
    created_at: String,
    per_file_bytes: u64,
) -> PublishedCoverage {
    let sealed: BTreeSet<u64> = digests
        .sealed_numbers()
        .into_iter()
        .filter(|number| *number >= 1 && *number <= beacon)
        .collect();
    let covered_files = local.complete.intersection(&sealed).count() as u64;
    let missing: BTreeSet<u64> = sealed.difference(&local.complete).copied().collect();
    let missing_files = missing.len() as u64;
    let gaps_all = runs_of(&missing);
    let gap_count = gaps_all.len() as u64;
    let gaps = gaps_all.into_iter().take(MAX_GAPS).collect();
    let unpublished_files = local.complete.difference(&sealed).count() as u64;

    PublishedCoverage {
        beacon,
        epoch,
        created_at,
        per_file_bytes,
        published_files: sealed.len() as u64,
        covered_files,
        missing_files,
        missing_bytes: missing_files.saturating_mul(per_file_bytes),
        unpublished_files,
        gaps,
        gap_count,
    }
}

/// Coalesce ascending numbers into inclusive runs.
fn runs_of(numbers: &BTreeSet<u64>) -> Vec<ChunkRange> {
    let mut runs: Vec<ChunkRange> = Vec::new();
    for number in numbers {
        match runs.last_mut() {
            Some(run) if run.to.checked_add(1) == Some(*number) => run.to = *number,
            _ => runs.push(ChunkRange {
                from: *number,
                to: *number,
            }),
        }
    }
    runs
}

fn scan_policies(
    destination: &Path,
    complete: &BTreeSet<u64>,
) -> (Vec<PolicyCoverage>, Vec<String>) {
    let dir = destination.join("policies");
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return (Vec::new(), Vec::new());
        }
        Err(error) => {
            return (
                Vec::new(),
                vec![format!("reading {}: {error}", dir.display())],
            );
        }
    };

    let mut policies = Vec::new();
    let mut errors = Vec::new();
    for entry in entries {
        let path = match entry {
            Ok(entry) => entry.path(),
            Err(error) => {
                errors.push(format!("reading {}: {error}", dir.display()));
                continue;
            }
        };
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }

        match PolicyManifest::load(&path) {
            Ok(Some(manifest)) => {
                // Manifests written before `requested_to` fall back to the
                // highest range they recorded, which is what a re-run would
                // have fetched.
                let requested_to = manifest
                    .requested_to
                    .or_else(|| manifest.fetched.iter().map(|range| range.to).max());
                let total = requested_to
                    .map(|to| to.saturating_sub(manifest.floor_chunk) + 1)
                    .unwrap_or(0);
                let covered = requested_to
                    .map(|to| complete.range(manifest.floor_chunk..=to).count() as u64)
                    .unwrap_or(0);
                policies.push(PolicyCoverage {
                    policy: manifest.policy,
                    earliest_asset: manifest.earliest_asset,
                    floor_chunk: manifest.floor_chunk,
                    requested_to,
                    covered,
                    total,
                });
            }
            Ok(None) => errors.push(format!("{} vanished while scanning", path.display())),
            Err(error) => errors.push(format!("{error:#}")),
        }
    }
    policies.sort_by(|a, b| a.policy.cmp(&b.policy));
    (policies, errors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use crate::manifest::FetchedRange;

    fn scratch(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "mithril-fetch-coverage-{name}-{}-{}",
            std::process::id(),
            crate::fmt::now_unix()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn part(destination: &Path, number: u64, extension: &str) {
        let immutable = destination.join("immutable");
        std::fs::create_dir_all(&immutable).unwrap();
        std::fs::write(immutable.join(format!("{number:05}.{extension}")), b"x").unwrap();
    }

    fn triple(destination: &Path, number: u64) {
        for extension in PART_EXTS {
            part(destination, number, extension);
        }
    }

    fn digests(numbers: impl IntoIterator<Item = u64>) -> Digests {
        Digests::from_parts(numbers.into_iter().flat_map(|number| {
            PART_EXTS.iter().map(move |extension| {
                (
                    format!("{number:05}.{extension}"),
                    format!("digest-{number}-{extension}"),
                )
            })
        }))
    }

    fn manifest(destination: &Path, floor: u64, to: Option<u64>) {
        let directory = destination.join("policies");
        std::fs::create_dir_all(&directory).unwrap();
        let policy = "16657df32ad8eaa8f8c628586ac6b8ba3771226c12bd69b582738fb7".to_string();
        let manifest = PolicyManifest {
            policy: policy.clone(),
            first_mint_unix: 1_600_000_000,
            first_mint_slot: 4_000_000,
            exact_chunk: floor,
            floor_chunk: floor,
            assets: 1,
            earliest_asset: "DONG".to_string(),
            minting_tx: None,
            requested_to: to,
            fetched: vec![FetchedRange {
                from: floor,
                to: to.unwrap_or(floor),
                snapshot_hash: "snapshot".to_string(),
                certificate_hash: "certificate".to_string(),
                beacon: to.unwrap_or(floor),
                files: 1,
                bytes: 1,
                at_unix: 1_700_000_000,
            }],
            updated_unix: 1_700_000_000,
        };
        manifest
            .save(&directory.join(format!("{policy}.json")))
            .unwrap();
    }

    #[test]
    fn an_empty_store_is_empty_not_an_error() {
        let destination = scratch("empty");
        let local = scan_local(&destination).unwrap();
        assert_eq!(local.files, 0);
        assert_eq!(local.bytes, 0);
        assert_eq!(local.first, None);
        assert!(local.complete.is_empty());
        std::fs::remove_dir_all(&destination).unwrap();
    }

    #[test]
    fn a_partial_triple_is_incomplete_not_covered() {
        let destination = scratch("partial");
        triple(&destination, 1);
        part(&destination, 2, "chunk");

        let local = scan_local(&destination).unwrap();
        assert_eq!(local.files, 1);
        assert_eq!(local.incomplete, vec![2]);
        assert_eq!(local.first, Some(1));
        assert_eq!(local.last, Some(1));
        assert_eq!(local.bytes, 4);

        std::fs::remove_dir_all(&destination).unwrap();
    }

    #[test]
    fn published_coverage_names_the_missing_runs_and_keeps_unpublished_apart() {
        let destination = scratch("published");
        for number in [1, 2, 5, 12] {
            triple(&destination, number);
        }
        let local = scan_local(&destination).unwrap();
        let digests = digests(1..=6);
        let published = scan_published(&local, &digests, 6, 500, "now".to_string(), 100);

        assert_eq!(published.published_files, 6);
        assert_eq!(published.covered_files, 3);
        assert_eq!(published.missing_files, 3);
        assert_eq!(published.missing_bytes, 300);
        assert_eq!(
            published.gaps,
            vec![ChunkRange { from: 3, to: 4 }, ChunkRange { from: 6, to: 6 }]
        );
        assert_eq!(published.gap_count, 2);
        // 12 is complete locally but beyond the published map.
        assert_eq!(published.unpublished_files, 1);
        assert_eq!(published.percent(), 50);

        std::fs::remove_dir_all(&destination).unwrap();
    }

    #[test]
    fn chunk_zero_is_not_counted_because_a_fetch_cannot_start_there() {
        let destination = scratch("zero");
        let local = scan_local(&destination).unwrap();
        let digests = digests(0..=2);
        let published = scan_published(&local, &digests, 2, 500, "now".to_string(), 100);
        assert_eq!(published.published_files, 2);
        assert_eq!(published.missing_files, 2);
        assert_eq!(published.gaps, vec![ChunkRange { from: 1, to: 2 }]);
        std::fs::remove_dir_all(&destination).unwrap();
    }

    #[test]
    fn a_small_covered_fraction_reads_as_less_than_one_percent() {
        let destination = scratch("percent-label");
        triple(&destination, 1);
        let local = scan_local(&destination).unwrap();
        let digests = digests(1..=1_000);
        let published = scan_published(&local, &digests, 1_000, 500, "now".to_string(), 100);
        assert_eq!(published.percent(), 0);
        assert_eq!(published.percent_label(), "<1%");
        std::fs::remove_dir_all(&destination).unwrap();
    }

    #[test]
    fn numbers_above_the_beacon_are_not_counted_as_published() {
        let destination = scratch("beacon");
        triple(&destination, 1);
        let local = scan_local(&destination).unwrap();
        let digests = digests(1..=10);
        let published = scan_published(&local, &digests, 5, 500, "now".to_string(), 100);
        assert_eq!(published.published_files, 5);
        assert_eq!(published.missing_files, 4);
        assert_eq!(published.unpublished_files, 0);
        std::fs::remove_dir_all(&destination).unwrap();
    }

    #[test]
    fn a_policy_manifest_contributes_its_requested_coverage() {
        let destination = scratch("policy");
        for number in [1, 2, 3, 7] {
            triple(&destination, number);
        }
        manifest(&destination, 1, Some(8));
        let local = scan_local(&destination).unwrap();
        assert_eq!(local.policies.len(), 1);
        let policy = &local.policies[0];
        assert_eq!(policy.covered, 4);
        assert_eq!(policy.total, 8);
        assert_eq!(policy.percent(), 50);
        assert!(local.policy_errors.is_empty());
        std::fs::remove_dir_all(&destination).unwrap();
    }

    #[test]
    fn a_manifest_without_a_requested_end_falls_back_to_its_fetched_ranges() {
        let destination = scratch("manifest-fallback");
        for number in [1, 2, 3, 4] {
            triple(&destination, number);
        }
        manifest(&destination, 1, None);
        let local = scan_local(&destination).unwrap();
        let policy = &local.policies[0];
        // The fixture manifest records one fetched range ending at its floor.
        assert_eq!(policy.requested_to, Some(1));
        assert_eq!(policy.covered, 1);
        assert_eq!(policy.total, 1);
        std::fs::remove_dir_all(&destination).unwrap();
    }

    #[test]
    fn a_broken_manifest_is_named_not_dropped() {
        let destination = scratch("broken-manifest");
        let directory = destination.join("policies");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("bad.json"), b"{").unwrap();
        let local = scan_local(&destination).unwrap();
        assert!(local.policies.is_empty());
        assert_eq!(local.policy_errors.len(), 1);
        std::fs::remove_dir_all(&destination).unwrap();
    }

    #[test]
    fn a_run_coalesces_contiguous_numbers_and_stops_at_u64_max() {
        let numbers = BTreeSet::from([3, 4, 5, 9, u64::MAX]);
        assert_eq!(
            runs_of(&numbers),
            vec![
                ChunkRange { from: 3, to: 5 },
                ChunkRange { from: 9, to: 9 },
                ChunkRange {
                    from: u64::MAX,
                    to: u64::MAX
                }
            ]
        );
    }
}
