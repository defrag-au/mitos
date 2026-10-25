//! `token-ledger sync` — make R2 and Workers KV match the archives on disk.
//!
//! # Why this is a process and not a step in the refresh
//!
//! It was a step. Every tick, the tail rebuild published each policy from the
//! thread that landed it, and MEASURED on 2026-10-25 that was **206 s of a
//! 300 s interval against 0.1 s of reading the spool**. The index work and the
//! upload work have nothing in common but a clock: the read is local and
//! cheap, the publish is network-bound and grows with the number of tracked
//! policies and the number of files in each archive. Sharing a thread meant a
//! slow publish delayed the next read, a failing one held a landing lock, and
//! neither could be retried without touching the other.
//!
//! Splitting them buys three things rather than one: the refresh can never be
//! delayed by R2, publishing gets its own concurrency (it was serial, so the
//! ceiling was ~50 policies), and a publish failure is isolated to a unit that
//! can be restarted on its own.
//!
//! # No queue, because the work is already on disk
//!
//! A publish records the manifest version it carried in `published.json`.
//! `manifest.json` carries the current one. **A policy needs publishing exactly
//! when those disagree** — or when there is no record at all — so a pass is a
//! directory scan, not a handoff from the landing thread.
//!
//! That is why a crash mid-publish loses nothing: the record is written last,
//! the next pass sees a stale one and tries again. It also means a publish is
//! idempotent, and that re-running a pass cannot miss a policy that landed
//! while the process was down.
//!
//! ⚠️ **`manifest.updated_unix` moves on every tail rebuild**, so a quiet
//! policy is not skipped — the archive genuinely gains a pass. Reducing that
//! work means changing what the tail writes, not changing this test.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::publish::{self, Publisher};

/// What `sync` is told on the command line.
#[derive(Args, Debug)]
pub struct SyncArgs {
    /// Root of the policy archives (`<dir>/<policy_hex>/manifest.json`).
    #[arg(long, default_value = "archive")]
    pub archive_dir: PathBuf,

    /// Seconds between passes. A pass publishes everything stale, so this is
    /// the upper bound on how far behind R2 can fall — not a batch size, and
    /// not tied to `--tip-refresh-secs`.
    #[arg(long, default_value_t = 15)]
    pub interval: u64,

    /// Archives published at once. Each publish is its own set of round trips
    /// and they share no state, so this is the only knob that mattered once
    /// publishing left the landing thread.
    #[arg(long, default_value_t = 8)]
    pub concurrency: usize,

    /// Run one pass and exit, for a timer or a manual catch-up.
    #[arg(long)]
    pub once: bool,
}

pub fn run(args: SyncArgs) -> Result<()> {
    let targets = publish::Targets::from_env();
    let Some(publisher) = Publisher::new(targets)? else {
        bail!(
            "R2 is not configured (R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_BUCKET)"
        );
    };
    tracing::info!(
        archive_dir = %args.archive_dir.display(),
        interval = args.interval,
        concurrency = args.concurrency,
        "sync: publishing stale archives to R2 and KV"
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building the sync runtime")?;
    let publisher = Arc::new(publisher);
    let concurrency = args.concurrency.max(1);

    loop {
        match runtime.block_on(pass(&publisher, &args.archive_dir, concurrency)) {
            Ok(0) => tracing::debug!("sync: nothing stale"),
            Ok(n) => tracing::info!(published = n, "sync: pass complete"),
            // Only the archive ROOT failing to read reaches here — a single
            // unreadable manifest is skipped inside the scan. Either way the
            // loop survives: the process is a poller, and the next pass is the
            // retry.
            Err(e) => tracing::warn!(error = %format!("{e:#}"), "sync: pass failed"),
        }
        if args.once {
            return Ok(());
        }
        std::thread::sleep(Duration::from_secs(args.interval.max(1)));
    }
}

/// Publish every stale archive under `root`, at most `concurrency` at a time.
async fn pass(publisher: &Arc<Publisher>, root: &Path, concurrency: usize) -> Result<usize> {
    let dirty = stale(root)?;
    if dirty.is_empty() {
        return Ok(0);
    }
    let mut queue = dirty.into_iter();
    let mut set = tokio::task::JoinSet::new();
    let mut published = 0usize;
    loop {
        while set.len() < concurrency {
            let Some(policy) = queue.next() else { break };
            let publisher = Arc::clone(publisher);
            let dir = root.join(&policy);
            set.spawn(async move {
                let outcome = publisher.publish(&dir, &policy).await;
                (policy, outcome)
            });
        }
        let Some(joined) = set.join_next().await else {
            break;
        };
        match joined {
            Ok((policy, Ok(rec))) if rec.all_done() => {
                published += 1;
                tracing::info!(policy, outcome = %rec.summary(), "sync: archive published");
            }
            Ok((policy, Ok(rec))) => {
                tracing::warn!(policy, outcome = %rec.summary(), "sync: publish INCOMPLETE");
            }
            Ok((policy, Err(e))) => {
                tracing::warn!(policy, error = %format!("{e:#}"), "sync: publish failed")
            }
            // The task itself panicked; the policy keeps its stale record, so
            // the next pass picks it up.
            Err(e) => tracing::warn!(error = %e, "sync: publish task did not finish"),
        }
    }
    Ok(published)
}

/// Every policy under `root` whose archive is not what R2 was last told about.
///
/// A policy whose manifest cannot be read is SKIPPED with a warning, not
/// returned as an error: the manifest is rewritten by another process, and one
/// unreadable archive must not stop the other thirty from publishing.
fn stale(root: &Path) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let entries = std::fs::read_dir(root).with_context(|| format!("read {}", root.display()))?;
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let dir = entry.path();
        let policy = entry.file_name().to_string_lossy().into_owned();
        let manifest = match crate::archive::load_manifest(&dir) {
            Ok(Some(m)) => m,
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!(policy, error = %format!("{e:#}"), "sync: manifest unreadable — left for the next pass");
                continue;
            }
        };
        let published = match publish::load_record(&dir) {
            Ok(r) => r.map(|r| r.manifest_updated_unix),
            Err(e) => {
                tracing::warn!(policy, error = %format!("{e:#}"), "sync: published.json unreadable — republishing");
                None
            }
        };
        if needs_publish(published, manifest.updated_unix) {
            out.push(policy);
        }
    }
    out.sort();
    Ok(out)
}

/// Does an archive need publishing?
///
/// `published` is the manifest version the last publish carried, or `None` when
/// the archive has never been published. Both matter: no record means the
/// archive exists and R2 has never seen it, which is exactly as stale as a
/// record naming an older version.
fn needs_publish(published: Option<u64>, manifest: u64) -> bool {
    published != Some(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_archive_is_stale_until_a_record_names_the_manifest_it_carries() {
        assert!(needs_publish(None, 7), "never published is stale");
        assert!(
            needs_publish(Some(6), 7),
            "a record behind the manifest is stale"
        );
        assert!(
            !needs_publish(Some(7), 7),
            "a record naming the current manifest is what published means"
        );
    }
}
