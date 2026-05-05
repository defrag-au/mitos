//! Convergence diff harness: poll the existing `collection-ownership`
//! worker and the new `collections-mitos` worker, compare
//! their read-API outputs for one or more policies, and report
//! divergence.
//!
//! Run as a long-running process to gather hourly convergence
//! metrics during parallel-run validation. See
//! `mitos/docs/design/ROADMAP.md` Phase 4.5 success criteria.
//!
//! Sample invocation:
//!
//! ```sh
//! diff-collection-ownership \
//!     --baseline https://ownership.cnft.dev \
//!     --mitos https://ownership-mitos.cnft.dev \
//!     --policy abc123... \
//!     --probe-asset deadbeef... \
//!     --probe-stake stake1u... \
//!     --interval 3600
//! ```

use std::collections::HashSet;
use std::time::Duration;

use clap::Parser;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(
    version,
    about = "diff convergence between two collection-ownership workers"
)]
struct Args {
    /// Base URL of the existing classifier-fed worker.
    #[arg(long, env = "BASELINE_URL")]
    baseline: String,

    /// Base URL of the mitos-driven worker.
    #[arg(long, env = "MITOS_URL")]
    mitos: String,

    /// Policy ID (hex) to compare. Repeat for multiple policies.
    #[arg(long = "policy", required = true)]
    policies: Vec<String>,

    /// Optional asset_name_hex to probe via /api/check + /api/owner.
    /// If omitted, only /api/stats and /api/bundle are compared.
    /// Repeats are paired with `--probe-stake`.
    #[arg(long = "probe-asset")]
    probe_assets: Vec<String>,

    /// Optional stake addresses for paired /api/check probes and
    /// for /api/bundle. Repeats pair positionally with
    /// `--probe-asset` for /api/check; all of them are also used
    /// for /api/bundle.
    #[arg(long = "probe-stake")]
    probe_stakes: Vec<String>,

    /// Polling interval in seconds. Default: 3600 (one hour).
    #[arg(long, default_value = "3600")]
    interval: u64,

    /// Single-shot mode: compare once and exit. Useful for cron.
    #[arg(long)]
    once: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .compact()
        .init();

    let args = Args::parse();
    let client = Client::builder().timeout(Duration::from_secs(15)).build()?;

    if args.once {
        run_once(&client, &args).await?;
        return Ok(());
    }

    let interval = Duration::from_secs(args.interval);
    info!(
        baseline = %args.baseline,
        mitos = %args.mitos,
        policies = ?args.policies,
        interval_secs = args.interval,
        "diff harness started; comparing on each tick"
    );
    loop {
        if let Err(e) = run_once(&client, &args).await {
            error!(error = %e, "diff pass failed; will retry next tick");
        }
        tokio::time::sleep(interval).await;
    }
}

async fn run_once(client: &Client, args: &Args) -> anyhow::Result<()> {
    let mut overall_match = true;
    for policy in &args.policies {
        let policy_match = compare_policy(client, args, policy).await?;
        overall_match &= policy_match;
    }
    if overall_match {
        info!("all policies converged");
    } else {
        warn!("at least one policy diverged — see warnings above");
    }
    Ok(())
}

async fn compare_policy(client: &Client, args: &Args, policy: &str) -> anyhow::Result<bool> {
    let mut policy_match = true;

    // /api/stats — overall counts. Slight expected variance because
    // the existing worker's stats include CIP-68 reference NFTs and
    // the mitos worker (prototype scope) doesn't filter them. Still
    // useful as a sanity check on order of magnitude.
    let stats_baseline = fetch_stats(client, &args.baseline, policy).await;
    let stats_mitos = fetch_stats(client, &args.mitos, policy).await;
    match (&stats_baseline, &stats_mitos) {
        (Ok(b), Ok(m)) => {
            let drift_pct = if b.asset_count > 0 {
                ((m.asset_count as f64 - b.asset_count as f64).abs() / b.asset_count as f64) * 100.0
            } else {
                0.0
            };
            let acceptable = drift_pct < 5.0;
            if acceptable {
                info!(
                    policy = %policy,
                    baseline_assets = b.asset_count,
                    mitos_assets = m.asset_count,
                    drift_pct = format_args!("{drift_pct:.2}"),
                    "stats within tolerance"
                );
            } else {
                warn!(
                    policy = %policy,
                    baseline_assets = b.asset_count,
                    mitos_assets = m.asset_count,
                    drift_pct = format_args!("{drift_pct:.2}"),
                    "stats diverged > 5%"
                );
                policy_match = false;
            }
        }
        (Err(e), _) | (_, Err(e)) => {
            warn!(policy = %policy, error = %e, "stats fetch failed");
            policy_match = false;
        }
    }

    // /api/check — per-asset ownership probe. Pairs probe_assets with
    // probe_stakes positionally.
    for (asset, stake) in args.probe_assets.iter().zip(args.probe_stakes.iter()) {
        let baseline = fetch_check(client, &args.baseline, policy, asset, stake).await;
        let mitos = fetch_check(client, &args.mitos, policy, asset, stake).await;
        match (baseline, mitos) {
            (Ok(b), Ok(m)) if b.owns == m.owns => {
                info!(policy = %policy, asset = %asset, stake = %stake, owns = b.owns, "check matches");
            }
            (Ok(b), Ok(m)) => {
                warn!(
                    policy = %policy, asset = %asset, stake = %stake,
                    baseline_owns = b.owns, mitos_owns = m.owns,
                    "check DIVERGED"
                );
                policy_match = false;
            }
            (b, m) => {
                warn!(policy = %policy, asset = %asset, baseline_err = ?b.err(), mitos_err = ?m.err(), "check probe failed");
                policy_match = false;
            }
        }
    }

    // /api/bundle — assets-by-stake. Compare as sets (order may
    // differ for legitimate reasons; equivalence is what we need).
    for stake in &args.probe_stakes {
        let baseline = fetch_bundle(client, &args.baseline, policy, stake).await;
        let mitos = fetch_bundle(client, &args.mitos, policy, stake).await;
        match (baseline, mitos) {
            (Ok(b), Ok(m)) => {
                let b_set: HashSet<&String> = b.assets.iter().collect();
                let m_set: HashSet<&String> = m.assets.iter().collect();
                let only_baseline: Vec<_> = b_set.difference(&m_set).collect();
                let only_mitos: Vec<_> = m_set.difference(&b_set).collect();
                if only_baseline.is_empty() && only_mitos.is_empty() {
                    info!(
                        policy = %policy, stake = %stake, count = b.assets.len(),
                        "bundle matches"
                    );
                } else {
                    warn!(
                        policy = %policy, stake = %stake,
                        only_baseline = only_baseline.len(),
                        only_mitos = only_mitos.len(),
                        "bundle DIVERGED — see debug log for asset lists"
                    );
                    tracing::debug!(?only_baseline, ?only_mitos);
                    policy_match = false;
                }
            }
            (b, m) => {
                warn!(policy = %policy, stake = %stake, baseline_err = ?b.err(), mitos_err = ?m.err(), "bundle probe failed");
                policy_match = false;
            }
        }
    }

    Ok(policy_match)
}

#[derive(Debug, Deserialize, Serialize)]
struct StatsResp {
    asset_count: u64,
    holder_count: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct CheckResp {
    owns: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct BundleResp {
    assets: Vec<String>,
}

async fn fetch_stats(client: &Client, base: &str, policy: &str) -> anyhow::Result<StatsResp> {
    let url = format!("{base}/api/stats/{policy}");
    let resp = client.get(&url).send().await?.error_for_status()?;
    Ok(resp.json().await?)
}

async fn fetch_check(
    client: &Client,
    base: &str,
    policy: &str,
    asset: &str,
    stake: &str,
) -> anyhow::Result<CheckResp> {
    let url = format!("{base}/api/check/{policy}?asset={asset}&stake={stake}");
    let resp = client.get(&url).send().await?.error_for_status()?;
    Ok(resp.json().await?)
}

async fn fetch_bundle(
    client: &Client,
    base: &str,
    policy: &str,
    stake: &str,
) -> anyhow::Result<BundleResp> {
    let url = format!("{base}/api/bundle/{policy}?stake={stake}");
    let resp = client.get(&url).send().await?.error_for_status()?;
    Ok(resp.json().await?)
}
