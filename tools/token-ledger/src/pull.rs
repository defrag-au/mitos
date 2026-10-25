//! `token-ledger pull` — a policy's published archive, from R2 to a local
//! archive directory. The mirror of [`crate::publish`].
//!
//! # Why R2 and not the box
//!
//! R2 is what the flow explorer's Worker reads, so a fold tested against a
//! pulled archive is tested against exactly what production serves. It runs
//! at most one `sync` interval behind the box (the manifest names the volatile
//! tail too), it needs no SSH and puts no load on the machine running the
//! walks, and it is small: $NIKEPIG's whole archive is 66 MB. Walks and
//! backfills still need the box, or a local Mithril snapshot: they read the
//! chunk store, which R2 does not hold.
//!
//! # The order, and why
//!
//! 1. **The manifest** is read first, and decides everything else: the same
//!    [`Manifest::files`] and [`Manifest::pending_file`] the publisher
//!    uploads, so the local layout cannot drift from what publish wrote. A
//!    hand-derived file list already got the pending sidecar's path wrong
//!    once (it lives under the newest immutable pass, not at the root).
//! 2. **Files**, each skipped when the local copy is already that size. They
//!    are immutable and never reuse a name, as the publisher relies on.
//! 3. **Derived** artefacts on request: the summary, its checkpoints, the
//!    graph. A local `summary` fold rebuilds them, so they are optional.
//! 4. **The manifest is written LAST**, so a pull that stops halfway leaves
//!    no manifest naming files that are not there, and a reader sees the old
//!    archive or the new one, never a mixture.
//!
//! # Credentials
//!
//! The same `R2_*` variables as `publish`. Use a READ-ONLY token on a laptop:
//! with one, a stray local `publish` fails instead of overwriting production.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use cf_publish::{Client, ObjPath, Targets};
use object_store::ObjectStore;

use crate::archive::{MANIFEST, Manifest};
use crate::publish::PREFIX;

/// Objects fetched at once.
const CONCURRENCY: usize = 8;

#[derive(clap::Args, Debug)]
pub struct PullArgs {
    /// Archive root to write into (`<root>/<policy_hex>/manifest.json`).
    #[arg(long, default_value = "archive")]
    pub archive_dir: PathBuf,
    /// 56-hex policy id.
    #[arg(long)]
    pub policy: String,
    /// Also pull the derived artefacts: summary, checkpoints and graph.
    #[arg(long)]
    pub derived: bool,
}

fn key(policy: &str, rel: &str) -> ObjPath {
    ObjPath::from(format!("{PREFIX}/{policy}/{rel}"))
}

/// One object, whole, or `None` when R2 has no such key.
async fn get(store: &Arc<dyn ObjectStore>, key: &ObjPath) -> Result<Option<Vec<u8>>> {
    match store.get(key).await {
        Ok(got) => Ok(Some(
            got.bytes()
                .await
                .with_context(|| format!("{key}"))?
                .to_vec(),
        )),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e).with_context(|| format!("{key}")),
    }
}

/// Write through a rename, so a reader never opens half a file.
fn write(path: &Path, body: &[u8]) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".pull");
    let tmp = PathBuf::from(tmp);
    std::fs::write(&tmp, body).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("renaming into {}", path.display()))
}

/// Which of `rels` the local directory lacks at the size R2 holds. A key R2
/// does not list is fetched anyway, and fails loudly there if it is missing.
fn wanted(
    dir: &Path,
    policy: &str,
    rels: Vec<String>,
    landed: &std::collections::BTreeMap<ObjPath, u64>,
) -> (Vec<String>, usize) {
    let mut out = Vec::new();
    let mut same = 0usize;
    for rel in rels {
        let local = std::fs::metadata(dir.join(&rel)).ok().map(|m| m.len());
        match (local, landed.get(&key(policy, &rel))) {
            (Some(l), Some(r)) if l == *r => same += 1,
            _ => out.push(rel),
        }
    }
    (out, same)
}

pub fn run(args: PullArgs) -> Result<()> {
    let policy = args.policy.to_lowercase();
    if policy.len() != 56 || !policy.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("--policy must be 56 hex characters");
    }
    let targets = Targets::from_env();
    let Some(client) = Client::new(targets.clone())? else {
        bail!(
            "R2 is not configured (R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_BUCKET); \
             a read-only token is enough"
        );
    };
    println!("source: {}", targets.describe());
    let dir = crate::archive::policy_dir(&args.archive_dir, &policy);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(pull(&client, &dir, &policy, args.derived))
}

async fn pull(client: &Client, dir: &Path, policy: &str, derived: bool) -> Result<()> {
    let started = std::time::Instant::now();
    let store = client.store().clone();

    // 1. The manifest decides everything else.
    let Some(manifest_bytes) = get(&store, &key(policy, MANIFEST)).await? else {
        bail!("R2 holds no archive for {policy}: it has not been walked and published");
    };
    let manifest = Manifest::from_json(&manifest_bytes).context("the manifest does not parse")?;
    let mut rels: Vec<String> = manifest.files().into_iter().map(|(rel, _)| rel).collect();
    if let Some(p) = manifest.pending_file() {
        rels.push(p);
    }

    // 3 (gathered now, fetched with the rest). The summary names its
    // checkpoints, so it is read before the list is final.
    let mut derived_rels: Vec<String> = Vec::new();
    if derived {
        if let Some(bytes) = get(&store, &key(policy, policy_archive::SUMMARY)).await? {
            match policy_archive::Summary::decode(&bytes) {
                Ok(s) => derived_rels.extend(s.checkpoints.iter().map(|c| c.rel.clone())),
                Err(e) => println!("summary in R2 is unusable here ({e}); pulling it raw"),
            }
            derived_rels.push(policy_archive::SUMMARY.to_string());
        } else {
            println!("no summary in R2 yet: `token-ledger summary` folds one locally");
        }
        derived_rels.push(policy_archive::GRAPH.to_string());
        derived_rels.push(format!("{}.gz", policy_archive::GRAPH));
    }

    // 2. One listing answers "what is already here at the right size".
    let landed = client
        .sizes_under(&ObjPath::from(format!("{PREFIX}/{policy}")))
        .await?;
    // A derived file R2 does not list simply does not exist (no graph yet, or
    // the raw graph when the gzipped one is live): not an error, skipped.
    derived_rels.retain(|rel| landed.contains_key(&key(policy, rel)));
    let all: Vec<String> = rels.into_iter().chain(derived_rels).collect();
    let (todo, same) = wanted(dir, policy, all, &landed);

    let mut fetched = 0usize;
    let mut bytes = 0u64;
    let mut queue = todo.into_iter();
    let mut running = tokio::task::JoinSet::new();
    loop {
        while running.len() < CONCURRENCY {
            let Some(rel) = queue.next() else { break };
            let store = store.clone();
            let k = key(policy, &rel);
            running.spawn(async move { (rel, get(&store, &k).await) });
        }
        let Some(done) = running.join_next().await else {
            break;
        };
        let (rel, got) = done.context("a fetch task panicked")?;
        let Some(body) = got? else {
            bail!(
                "the manifest names {rel}, and R2 has no such object: a publish in flight? try again"
            );
        };
        write(&dir.join(&rel), &body)?;
        fetched += 1;
        bytes += body.len() as u64;
    }

    // 4. The manifest last: the local archive flips in one rename.
    write(&dir.join(MANIFEST), &manifest_bytes)?;
    println!(
        "{policy}: {fetched} fetched ({bytes} bytes), {same} already here, {} completeness, in {:.1}s → {}",
        manifest.completeness().as_wire(),
        started.elapsed().as_secs_f64(),
        dir.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A local file at R2's size is skipped; a missing one, a different size,
    /// and a key R2 does not list are all fetched, which is the only safe
    /// direction: a stale listing costs a fetch, never a missing file.
    #[test]
    fn only_what_is_missing_or_different_is_fetched() {
        let dir = std::env::temp_dir().join(format!("pull-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("same.parquet"), [0u8; 4]).unwrap();
        std::fs::write(dir.join("changed.parquet"), [0u8; 3]).unwrap();
        let landed = std::collections::BTreeMap::from([
            (key("ab", "same.parquet"), 4),
            (key("ab", "changed.parquet"), 4),
            (key("ab", "absent.parquet"), 4),
        ]);
        let rels = ["same", "changed", "absent", "unlisted"]
            .map(|n| format!("{n}.parquet"))
            .to_vec();
        let (todo, same) = wanted(&dir, "ab", rels, &landed);
        assert_eq!(same, 1);
        assert_eq!(
            todo,
            vec!["changed.parquet", "absent.parquet", "unlisted.parquet"]
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
