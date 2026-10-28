//! `token-ledger tapes` — keep every registered token's published tape level
//! with its archive.
//!
//! A pass looks at each token's archive manifest. Where it moved since the
//! token's tape was last published (`TAPE_STAMP` beside the pack), it packs
//! the token again ([`crate::pack`]) and publishes it
//! ([`crate::publish_tape`]): every page, but only the pages whose bytes
//! changed go up, which on a quiet token is the open page and the book.
//!
//! Runs as its own service beside `sync`, like `sync` does beside `serve`:
//! a pack reads a token's whole archive and takes minutes, which must not
//! hold up anything that follows the tip. The arena asks the worker for
//! what came after the published tape, so the interval bounds how much a
//! reader fetches that way, not how stale what they see is.
//!
//! Every pack starts from an empty directory. `pack` refuses to write over
//! a party table that the new one does not extend, which protects a table
//! someone reads by number. A tape has no such reader: every page carries
//! its own people, and a build is read whole. And the table is not strictly
//! append-only anyway: the parties only the story names (a fill's trader no
//! movement row shows) are numbered after every movement's, so new movements
//! at the tip shift them. MEASURED on $NikePig, an ordinary pass later: 27,501
//! parties against 27,495, differing from party 27,495 on. Closed pages
//! still came out byte-identical (1 of 77 pages sent).

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use cf_publish::{Client, Targets};
use serde::{Deserialize, Serialize};

use crate::archive::{load_manifest, policy_dir};
use crate::pack::{PackArgs, pack};

/// Beside a token's pack: what the last publish was of.
pub const TAPE_STAMP: &str = "tape-published.json";

#[derive(clap::Args, Debug)]
pub struct TapesArgs {
    /// Root of the policy archives (`<dir>/<policy_hex>/manifest.json`).
    #[arg(long, default_value = "archive")]
    pub archive_dir: PathBuf,
    /// Where the packs are kept between passes: `<out>/<policy_hex>/…`.
    #[arg(long)]
    pub out: PathBuf,
    /// The token registry: every `[[token]]` in it is kept published.
    #[arg(long)]
    pub tokens: PathBuf,
    /// Only these policies, rather than every registered token.
    #[arg(long = "policy")]
    pub policies: Vec<String>,
    /// As for `pack`.
    #[arg(long)]
    pub handles_dir: Option<PathBuf>,
    #[arg(long, default_value = crate::names::HANDLE_POLICY)]
    pub handle_policy: String,
    #[arg(long, requires = "data_dir")]
    pub tx_index_dir: Option<PathBuf>,
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    /// Seconds between passes.
    #[arg(long, default_value_t = 1800)]
    pub interval: u64,
    /// Run one pass and exit, for a timer or a manual catch-up.
    #[arg(long)]
    pub once: bool,
}

/// What the last publish of a token's tape was of.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stamp {
    /// The archive manifest's `updated_unix` the pack read.
    pub manifest_updated_unix: u64,
    pub build: String,
    pub published_unix: u64,
}

pub fn run(args: TapesArgs) -> Result<()> {
    let targets = Targets::from_env();
    tracing::info!(targets = %targets.describe(), "tapes: keeping published tapes level");
    let Some(client) = Client::new(targets)? else {
        bail!(
            "R2 is not configured (R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_BUCKET)"
        );
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building the tapes runtime")?;
    loop {
        match pass(&args, &client, &runtime) {
            Ok(0) => tracing::debug!("tapes: nothing moved"),
            Ok(n) => tracing::info!(published = n, "tapes: pass complete"),
            // Only the registry failing to read reaches here; one token's
            // failure is logged in the pass and the next pass retries it.
            Err(e) => tracing::warn!(error = %format!("{e:#}"), "tapes: pass failed"),
        }
        if args.once {
            return Ok(());
        }
        std::thread::sleep(Duration::from_secs(args.interval.max(1)));
    }
}

/// One pass: every token whose archive moved, packed and published.
fn pass(args: &TapesArgs, client: &Client, runtime: &tokio::runtime::Runtime) -> Result<usize> {
    let policies: Vec<String> = match args.policies.is_empty() {
        true => crate::registry::tokens(&args.tokens)?
            .into_iter()
            .map(|t| t.policy.to_lowercase())
            .collect(),
        false => args.policies.iter().map(|p| p.to_lowercase()).collect(),
    };
    let mut published = 0;
    for policy in policies {
        match one(args, client, runtime, &policy) {
            Ok(Some(summary)) => {
                published += 1;
                tracing::info!(%policy, %summary, "tapes: published");
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(%policy, error = %format!("{e:#}"), "tapes: not published"),
        }
    }
    Ok(published)
}

/// Pack and publish `policy` if its archive moved since its last publish;
/// `None` when it did not, or when it has no archive.
fn one(
    args: &TapesArgs,
    client: &Client,
    runtime: &tokio::runtime::Runtime,
    policy: &str,
) -> Result<Option<String>> {
    let Some(manifest) = load_manifest(&policy_dir(&args.archive_dir, policy))? else {
        return Ok(None);
    };
    let dir = args.out.join(policy);
    if is_level(&dir, manifest.updated_unix) {
        return Ok(None);
    }
    let started = std::time::Instant::now();
    let pack_args = || PackArgs {
        archive_dir: args.archive_dir.clone(),
        policy: policy.to_string(),
        out: args.out.clone(),
        handles_dir: args.handles_dir.clone(),
        handle_policy: args.handle_policy.clone(),
        tx_index_dir: args.tx_index_dir.clone(),
        data_dir: args.data_dir.clone(),
        tokens: Some(args.tokens.clone()),
    };
    // From nothing, every time: see the module.
    if dir.exists() {
        std::fs::remove_dir_all(&dir).with_context(|| format!("clearing {}", dir.display()))?;
    }
    pack(pack_args())?;
    let done = runtime.block_on(crate::publish_tape::publish(client, &dir, policy))?;
    cf_publish::store_record(
        &dir.join(TAPE_STAMP),
        &Stamp {
            manifest_updated_unix: manifest.updated_unix,
            build: done.build.clone(),
            published_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
        },
    )?;
    Ok(Some(format!(
        "{}; in {:.0}s",
        done.summary(),
        started.elapsed().as_secs_f64()
    )))
}

/// Whether `dir` holds a pack already published at `manifest_updated_unix`.
/// An unreadable stamp is not level: the token is packed again.
fn is_level(dir: &Path, manifest_updated_unix: u64) -> bool {
    cf_publish::load_record::<Stamp>(&dir.join(TAPE_STAMP))
        .ok()
        .flatten()
        .is_some_and(|s| s.manifest_updated_unix == manifest_updated_unix)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A token is packed again only when its archive moved: the stamp names
    /// the manifest version the published tape read.
    #[test]
    fn a_tape_is_level_while_its_archive_has_not_moved() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(!is_level(tmp.path(), 10), "never published");
        cf_publish::store_record(
            &tmp.path().join(TAPE_STAMP),
            &Stamp {
                manifest_updated_unix: 10,
                build: "v6-0000000000000001".to_string(),
                published_unix: 11,
            },
        )
        .unwrap();
        assert!(is_level(tmp.path(), 10));
        assert!(!is_level(tmp.path(), 12), "the archive moved");
    }
}
