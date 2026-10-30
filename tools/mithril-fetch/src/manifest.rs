//! What was fetched for a policy, beside the chunks it describes.
//!
//! The chunk store is shared: sealed immutable files are identical for every
//! policy, so a per-policy copy would duplicate tens of gigabytes. Provenance
//! is therefore per-policy and small — the floor it was resolved from, and the
//! verified ranges that have landed.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::fmt;
use crate::plan::{PolicyId, RangePlan};

/// One certificate-verified, digest-checked range that reached the store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchedRange {
    pub from: u64,
    pub to: u64,
    pub snapshot_hash: String,
    pub certificate_hash: String,
    pub beacon: u64,
    pub files: u64,
    pub bytes: u64,
    pub at_unix: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyManifest {
    pub policy: String,
    pub first_mint_unix: i64,
    pub first_mint_slot: u64,
    /// The chunk the slot falls in, and the one fetched from (one earlier).
    pub exact_chunk: u64,
    pub floor_chunk: u64,
    /// The end the fetch was asked for. Older manifests predate this and fall
    /// back to their highest fetched range when coverage is computed.
    #[serde(default)]
    pub requested_to: Option<u64>,
    pub assets: usize,
    pub earliest_asset: String,
    pub minting_tx: Option<String>,
    pub fetched: Vec<FetchedRange>,
    pub updated_unix: i64,
}

impl PolicyManifest {
    pub fn for_plan(plan: &RangePlan) -> Self {
        Self {
            policy: plan.policy.to_hex(),
            first_mint_unix: plan.floor.earliest_unix,
            first_mint_slot: plan.floor.slot,
            exact_chunk: plan.floor.exact_chunk,
            floor_chunk: plan.floor.floor_chunk,
            requested_to: Some(plan.to),
            assets: plan.floor.assets,
            earliest_asset: plan.floor.earliest_asset.clone(),
            minting_tx: plan.floor.minting_tx.clone(),
            fetched: Vec::new(),
            updated_unix: fmt::now_unix(),
        }
    }

    pub fn path(destination: &Path, policy: &PolicyId) -> PathBuf {
        destination
            .join("policies")
            .join(format!("{}.json", policy.to_hex()))
    }

    pub fn load(path: &Path) -> Result<Option<Self>> {
        match fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing {}", path.display()))
                .map(Some),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
        }
    }

    /// Append a range, replacing an identical one rather than growing the file
    /// every time a run is retried.
    pub fn record(&mut self, range: FetchedRange) {
        let duplicate = self.fetched.iter().position(|existing| {
            existing.from == range.from
                && existing.to == range.to
                && existing.snapshot_hash == range.snapshot_hash
        });
        match duplicate {
            Some(index) => self.fetched[index] = range,
            None => self.fetched.push(range),
        }
        self.updated_unix = fmt::now_unix();
    }

    /// Write beside the real file and rename, so a crash cannot leave half a
    /// manifest that the next run then fails to parse.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        let temporary = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(self).context("serialising the policy manifest")?;
        fs::write(&temporary, bytes).with_context(|| format!("writing {}", temporary.display()))?;
        fs::rename(&temporary, path)
            .with_context(|| format!("renaming {} to {}", temporary.display(), path.display()))?;
        Ok(())
    }
}

/// Load the policy's manifest, or start one from the plan.
pub fn load_or_new(plan: &RangePlan) -> Result<PolicyManifest> {
    let path = PolicyManifest::path(&plan.destination, &plan.policy);
    match PolicyManifest::load(&path)? {
        Some(manifest) => {
            if manifest.policy != plan.policy.to_hex() {
                bail!(
                    "{} describes policy {}, not {}",
                    path.display(),
                    manifest.policy,
                    plan.policy
                );
            }
            Ok(manifest)
        }
        None => Ok(PolicyManifest::for_plan(plan)),
    }
}

pub fn record(plan: &RangePlan, range: FetchedRange) -> Result<()> {
    let path = PolicyManifest::path(&plan.destination, &plan.policy);
    let mut manifest = load_or_new(plan)?;
    manifest.record(range);
    manifest.save(&path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{PolicyFloor, Tip};

    fn plan() -> RangePlan {
        let policy =
            PolicyId::parse("16657df32ad8eaa8f8c628586ac6b8ba3771226c12bd69b582738fb7").unwrap();
        let floor = PolicyFloor {
            assets: 3,
            earliest_unix: 1_600_000_000,
            earliest_asset: "first".to_string(),
            minting_tx: Some("aa".to_string()),
            slot: 4_000_000,
            exact_chunk: 185,
            floor_chunk: 184,
        };
        RangePlan::new(
            policy,
            floor,
            std::env::temp_dir(),
            Tip {
                beacon: 9_400,
                epoch: 500,
                created_at: "2026-10-29T00:00:00Z".to_string(),
                total_bytes: 1,
            },
            None,
        )
        .unwrap()
    }

    fn range(from: u64, to: u64, snapshot: &str) -> FetchedRange {
        FetchedRange {
            from,
            to,
            snapshot_hash: snapshot.to_string(),
            certificate_hash: "cert".to_string(),
            beacon: 9_400,
            files: to - from + 1,
            bytes: 1_000,
            at_unix: 1_700_000_000,
        }
    }

    #[test]
    fn a_manifest_names_the_floor_it_was_resolved_from() {
        let manifest = PolicyManifest::for_plan(&plan());
        assert_eq!(manifest.first_mint_slot, 4_000_000);
        assert_eq!(manifest.floor_chunk, 184);
        assert_eq!(manifest.assets, 3);
        assert!(manifest.fetched.is_empty());
    }

    #[test]
    fn recording_the_same_range_twice_replaces_rather_than_duplicates() {
        let mut manifest = PolicyManifest::for_plan(&plan());
        manifest.record(range(184, 200, "snapshot-a"));
        manifest.record(range(184, 200, "snapshot-a"));
        assert_eq!(manifest.fetched.len(), 1);
        manifest.record(range(201, 250, "snapshot-a"));
        assert_eq!(manifest.fetched.len(), 2);
    }

    #[test]
    fn a_manifest_round_trips_through_disk() {
        let scratch = std::env::temp_dir().join(format!(
            "mithril-fetch-manifest-{}-{}",
            std::process::id(),
            fmt::now_unix()
        ));
        std::fs::create_dir_all(&scratch).unwrap();
        let path = scratch.join("policy.json");

        let mut manifest = PolicyManifest::for_plan(&plan());
        manifest.record(range(184, 200, "snapshot-a"));
        manifest.save(&path).unwrap();

        let loaded = PolicyManifest::load(&path).unwrap().unwrap();
        assert_eq!(loaded.fetched, manifest.fetched);
        assert_eq!(loaded.policy, manifest.policy);

        std::fs::remove_dir_all(&scratch).unwrap();
    }
}
