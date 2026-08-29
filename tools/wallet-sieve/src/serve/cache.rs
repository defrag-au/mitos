//! Keeping the row cache BOUNDED, and saying how big it is.
//!
//! `sieve-cache.db` grows every time somebody looks up an address that isn't
//! in it, and nothing ever removed anything: `tail.db` prunes itself, this one
//! did not. Left alone it is a slow leak with no ceiling and no gauge, which is
//! how a sibling service on this box reached 253 GB before anybody noticed.
//!
//! Two jobs, one thread:
//!
//! - **Evict** to a byte budget, oldest-REQUESTED first. See
//!   [`super::db::evict_to_budget`] for why it vacuums and why it would rather
//!   sit over budget than evict a wallet somebody is watching.
//! - **Publish** a Prometheus textfile for the Alloy `textfile` collector
//!   already running here, so the size is visible before it is a problem
//!   rather than after.
//!
//! Client interest is buffered in memory ([`Seen`]) and flushed on each pass
//! rather than written per request: the read path shares a database with the
//! scan lanes, and an UPDATE per HTTP call would put it in contention with a
//! sweep mid-transaction for a signal that only needs minute granularity.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};

use super::db;

/// Wallets requested since the last janitor pass.
///
/// A map rather than a log: a wallet polled two hundred times in a minute is
/// one entry, and the newest timestamp is the only one that matters.
#[derive(Clone, Default)]
pub struct Seen(Arc<Mutex<HashMap<String, u64>>>);

impl Seen {
    pub fn mark(&self, canonical: &str, at_unix: u64) {
        if let Ok(mut m) = self.0.lock() {
            let e = m.entry(canonical.to_string()).or_insert(at_unix);
            *e = (*e).max(at_unix);
        }
    }

    fn drain(&self) -> Vec<(String, u64)> {
        match self.0.lock() {
            Ok(mut m) => m.drain().collect(),
            Err(_) => Vec::new(),
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub db_path: PathBuf,
    /// Ceiling for the cache FILE. `0` disables eviction (the gauge still
    /// publishes, so an unbounded cache is at least a visible one).
    pub budget_bytes: u64,
    /// A wallet requested this recently is never evicted.
    pub protect: Duration,
    /// Where to write the Prometheus textfile. `None` disables publishing.
    pub textfile: Option<PathBuf>,
    pub interval: Duration,
}

/// Spawn the janitor. One thread, its own connection — `Connection` is not
/// `Sync`, and WAL plus the busy timeout absorb overlap with the scan lanes.
pub fn spawn(cfg: Config, seen: Seen) -> Result<()> {
    let mut conn = db::open_rw(&cfg.db_path)?;
    std::thread::Builder::new()
        .name("cache-janitor".into())
        .spawn(move || {
            loop {
                std::thread::sleep(cfg.interval);
                if let Err(e) = pass(&mut conn, &cfg, &seen) {
                    // A failed pass must not kill the thread: the next one
                    // retries, and a cache that stops being trimmed silently
                    // is the failure this module exists to prevent.
                    tracing::warn!("cache janitor pass failed: {e:#}");
                }
            }
        })
        .context("spawning cache janitor")?;
    Ok(())
}

fn pass(conn: &mut rusqlite::Connection, cfg: &Config, seen: &Seen) -> Result<()> {
    let now = now_unix();
    let marks = seen.drain();
    if !marks.is_empty() {
        db::touch_seen(conn, &marks).context("flushing seen marks")?;
    }
    let mut evictions = 0u64;
    if cfg.budget_bytes > 0 {
        let out = db::evict_to_budget(conn, cfg.budget_bytes, cfg.protect, now)?;
        evictions = out.wallets as u64;
        if out.wallets > 0 {
            tracing::info!(
                wallets = out.wallets,
                flows = out.flows,
                before_mb = out.before / 1_048_576,
                after_mb = out.after / 1_048_576,
                "cache trimmed to budget"
            );
        }
        if out.stuck {
            // Loud on purpose. Everything over budget is in active use, so
            // the budget is wrong for the traffic — a decision only the
            // operator can make, and one they cannot make unprompted.
            tracing::warn!(
                bytes = out.after,
                budget = cfg.budget_bytes,
                "cache over budget with nothing safe to evict — raise the budget or add capacity"
            );
        }
    }
    if let Some(path) = &cfg.textfile {
        let stats = db::cache_stats(conn)?;
        if let Err(e) = publish(path, &stats, cfg.budget_bytes, evictions) {
            tracing::warn!("writing {}: {e:#}", path.display());
        }
    }
    Ok(())
}

/// Write the gauges for the Alloy `textfile` collector.
///
/// Rendered to a temp file and renamed, because the collector reads this on
/// its own schedule: a partial file scrapes as a partial metric set, and a
/// disk gauge that occasionally reports nothing is worse than none at all.
fn publish(path: &Path, stats: &db::CacheStats, budget: u64, evictions: u64) -> Result<()> {
    let body = format!(
        "# HELP wallet_sieve_cache_bytes Size of the sieve row cache on disk.\n\
         # TYPE wallet_sieve_cache_bytes gauge\n\
         wallet_sieve_cache_bytes {}\n\
         # HELP wallet_sieve_cache_budget_bytes Configured ceiling; 0 = unbounded.\n\
         # TYPE wallet_sieve_cache_budget_bytes gauge\n\
         wallet_sieve_cache_budget_bytes {budget}\n\
         # HELP wallet_sieve_cache_wallets Wallets held in the cache.\n\
         # TYPE wallet_sieve_cache_wallets gauge\n\
         wallet_sieve_cache_wallets {}\n\
         # HELP wallet_sieve_cache_flows Flow rows held in the cache.\n\
         # TYPE wallet_sieve_cache_flows gauge\n\
         wallet_sieve_cache_flows {}\n\
         # HELP wallet_sieve_cache_largest_wallet_flows Rows held by the biggest single wallet.\n\
         # TYPE wallet_sieve_cache_largest_wallet_flows gauge\n\
         wallet_sieve_cache_largest_wallet_flows {}\n\
         # HELP wallet_sieve_cache_evicted_wallets Wallets dropped by the last janitor pass.\n\
         # TYPE wallet_sieve_cache_evicted_wallets gauge\n\
         wallet_sieve_cache_evicted_wallets {evictions}\n\
         # HELP wallet_sieve_capped_wallets Wallets whose backfill was abandoned for being oversize.\n\
         # TYPE wallet_sieve_capped_wallets gauge\n\
         wallet_sieve_capped_wallets {}\n",
        stats.bytes, stats.wallets, stats.flows, stats.largest_wallet_flows, stats.capped_wallets,
    );
    let tmp = path.with_extension("prom.tmp");
    std::fs::write(&tmp, body).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("renaming onto {}", path.display()))?;
    Ok(())
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Repeated polling of one wallet collapses to a single mark carrying the
    /// NEWEST timestamp — the read path calls this on every request, and the
    /// eviction order depends on the latest interest, not the first.
    #[test]
    fn interest_marks_coalesce_and_keep_the_newest() {
        let seen = Seen::default();
        seen.mark("stake1abc", 100);
        seen.mark("stake1abc", 300);
        seen.mark("stake1abc", 200);
        seen.mark("stake1xyz", 150);
        let mut drained = seen.drain();
        drained.sort();
        assert_eq!(
            drained,
            vec![
                ("stake1abc".to_string(), 300),
                ("stake1xyz".to_string(), 150)
            ]
        );
    }

    /// Draining empties: a mark flushed once must not keep re-flushing and
    /// hold a wallet artificially fresh forever.
    #[test]
    fn draining_clears_the_buffer() {
        let seen = Seen::default();
        seen.mark("stake1abc", 100);
        assert_eq!(seen.drain().len(), 1);
        assert!(seen.drain().is_empty());
    }
}
