//! Publishing a policy's archive — the Parquet and manifest to R2, the
//! bundle to Workers KV — in process, as typed steps with a record.
//!
//! # Why this is code and not a script
//!
//! The first cut shelled out to rclone and curl after every landed pass.
//! One exit code covered a four-step publish, so a KV failure after a
//! successful R2 copy read as "published"; nothing recorded when, or
//! whether, a policy had actually reached the edge; a transient API error
//! stayed unretried until the next pass happened to land; and the manifest
//! flip could not be conditional, so two writers on one policy would race
//! silently. The user's verdict: *an incredibly brittle surface*.
//!
//! # The order, and why it is fixed
//!
//! 1. **Files** the manifest names — Parquet and the pending sidecar —
//!    each skipped when R2 already holds an object of that size. Files are
//!    immutable once written and never reuse a name, so size is identity
//!    enough. Immutable cache headers, a year.
//! 2. **The manifest**, LAST of the data, `no-cache`, and CONDITIONAL on the
//!    version read before step 1: a reader never sees a manifest naming an
//!    object that is not there, and a second writer's flip fails loudly
//!    rather than winning quietly.
//! 3. **The bundle** to every KV namespace, so a Worker opens the archive
//!    in one read. After the files it points at exist.
//! 4. **Prune** what the manifest no longer names — a rollup's predecessors.
//!    Only after the flip succeeded, so a stale prune can never remove files
//!    a live manifest still names.
//!
//! A failed step stops the sequence there. Every step's outcome is written
//! to `published.json` beside the manifest and logged, so "landed" and
//! "published" are two facts, not one.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path as ObjPath;
use object_store::{
    Attribute, Attributes, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion,
};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

use crate::archive::{MANIFEST, Manifest};

/// Key prefix in the bucket: `policy-archive/<policy_hex>/<relative path>`,
/// the same relative paths the manifest carries.
pub const PREFIX: &str = "policy-archive";
/// The record of the last publish, beside the manifest.
pub const RECORD: &str = "published.json";

// ── Configuration ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct R2Target {
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub bucket: String,
}

#[derive(Debug, Clone)]
pub struct KvTarget {
    pub account_id: String,
    pub token: String,
    /// Every namespace gets the same bundle: dev and prod read one bucket.
    pub namespaces: Vec<String>,
}

/// Where to publish. Read from the environment the service unit loads
/// (`/etc/default/token-ledger`), by the names the box already uses.
#[derive(Debug, Clone, Default)]
pub struct Targets {
    pub r2: Option<R2Target>,
    pub kv: Option<KvTarget>,
}

impl Targets {
    pub fn from_env() -> Self {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
        let r2 = match (
            var("R2_ENDPOINT"),
            var("R2_ACCESS_KEY_ID"),
            var("R2_SECRET_ACCESS_KEY"),
            var("R2_BUCKET"),
        ) {
            (Some(endpoint), Some(access_key_id), Some(secret_access_key), Some(bucket)) => {
                Some(R2Target {
                    endpoint,
                    access_key_id,
                    secret_access_key,
                    bucket,
                })
            }
            _ => None,
        };
        let kv = match (
            var("CF_ACCOUNT_ID").or_else(|| var("R2_ACCOUNT_ID")),
            var("CF_KV_TOKEN"),
            var("KV_NAMESPACE_IDS"),
        ) {
            (Some(account_id), Some(token), Some(ids)) => {
                let namespaces: Vec<String> = ids.split_whitespace().map(String::from).collect();
                (!namespaces.is_empty()).then_some(KvTarget {
                    account_id,
                    token,
                    namespaces,
                })
            }
            _ => None,
        };
        Self { r2, kv }
    }

    /// One line for the startup log — what is configured, never a secret.
    pub fn describe(&self) -> String {
        let r2 = match &self.r2 {
            Some(r) => format!("r2 bucket {} at {}", r.bucket, r.endpoint),
            None => "r2 NOT configured".to_string(),
        };
        let kv = match &self.kv {
            Some(k) => format!("kv {} namespace(s)", k.namespaces.len()),
            None => "kv NOT configured (CF_KV_TOKEN / KV_NAMESPACE_IDS)".to_string(),
        };
        format!("{r2}; {kv}")
    }
}

// ── The record ───────────────────────────────────────────────────────────

/// How one step of a publish went.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Outcome {
    Done,
    /// No target for this step; the archive is published without it.
    NotConfigured,
    /// An earlier step failed, so this one never ran.
    NotAttempted,
    Failed {
        error: String,
    },
}

impl Outcome {
    fn failed(e: impl std::fmt::Display) -> Self {
        Outcome::Failed {
            error: format!("{e:#}"),
        }
    }

    pub fn is_done(&self) -> bool {
        matches!(self, Outcome::Done | Outcome::NotConfigured)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvOutcome {
    pub namespace: String,
    pub outcome: Outcome,
}

/// What the last publish did — `published.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub policy: String,
    /// The manifest this publish carried, by its own clock.
    pub manifest_updated_unix: u64,
    pub published_unix: u64,
    pub secs: f64,
    pub files: Outcome,
    pub uploaded: usize,
    pub skipped: usize,
    pub uploaded_bytes: u64,
    pub manifest: Outcome,
    pub bundle: Outcome,
    #[serde(default)]
    pub kv: Vec<KvOutcome>,
    pub prune: Outcome,
    pub pruned: usize,
}

impl Record {
    /// Did every configured step succeed?
    pub fn all_done(&self) -> bool {
        self.files.is_done()
            && self.manifest.is_done()
            && self.bundle.is_done()
            && self.prune.is_done()
    }

    pub fn summary(&self) -> String {
        let step = |name: &str, o: &Outcome| match o {
            Outcome::Done => format!("{name} ok"),
            Outcome::NotConfigured => format!("{name} n/a"),
            Outcome::NotAttempted => format!("{name} skipped"),
            Outcome::Failed { error } => format!("{name} FAILED: {error}"),
        };
        format!(
            "{} ({} up, {} same, {} B); {}; {}; {} ({} pruned); {:.1}s",
            step("files", &self.files),
            self.uploaded,
            self.skipped,
            self.uploaded_bytes,
            step("manifest", &self.manifest),
            step("bundle", &self.bundle),
            step("prune", &self.prune),
            self.pruned,
            self.secs
        )
    }
}

pub fn load_record(dir: &Path) -> Result<Option<Record>> {
    let path = dir.join(RECORD);
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read(&path)?;
    Ok(Some(
        serde_json::from_slice(&raw).with_context(|| format!("parsing {}", path.display()))?,
    ))
}

fn store_record(dir: &Path, r: &Record) -> Result<()> {
    let tmp = dir.join(format!("{RECORD}.tmp"));
    std::fs::write(&tmp, serde_json::to_vec_pretty(r)?)?;
    std::fs::rename(&tmp, dir.join(RECORD))?;
    Ok(())
}

// ── The publisher ────────────────────────────────────────────────────────

pub struct Publisher {
    store: Arc<dyn ObjectStore>,
    kv: Option<KvTarget>,
    http: reqwest::Client,
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn immutable() -> Attributes {
    let mut a = Attributes::new();
    a.insert(
        Attribute::CacheControl,
        "public, max-age=31536000, immutable".into(),
    );
    a
}

fn manifest_attributes() -> Attributes {
    let mut a = Attributes::new();
    a.insert(Attribute::CacheControl, "no-cache".into());
    a.insert(Attribute::ContentType, "application/json".into());
    a
}

struct Uploaded {
    uploaded: usize,
    skipped: usize,
    bytes: u64,
}

impl Publisher {
    /// `None` when R2 is not configured: the bundle names R2 keys, so KV on
    /// its own would publish pointers to nothing.
    pub fn new(targets: Targets) -> Result<Option<Self>> {
        let Some(r2) = targets.r2 else {
            return Ok(None);
        };
        let store = AmazonS3Builder::new()
            .with_endpoint(&r2.endpoint)
            .with_bucket_name(&r2.bucket)
            .with_access_key_id(&r2.access_key_id)
            .with_secret_access_key(&r2.secret_access_key)
            .with_region("auto")
            // The manifest flip: `If-Match` on the version read before the
            // publish began. R2's S3 surface honours it.
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .context("building the R2 client")?;
        Ok(Some(Self {
            store: Arc::new(store),
            kv: targets.kv,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .context("building the HTTP client")?,
        }))
    }

    fn key(policy: &str, rel: &str) -> ObjPath {
        ObjPath::from(format!("{PREFIX}/{policy}/{rel}"))
    }

    /// Publish `<dir>` — a policy's archive directory — as `policy`. Always
    /// writes `published.json`; returns the record. `Err` only when the
    /// archive itself cannot be read.
    pub async fn publish(&self, dir: &Path, policy: &str) -> Result<Record> {
        let started = Instant::now();
        let manifest = crate::archive::load_manifest(dir)?
            .with_context(|| format!("no archive at {}", dir.display()))?;
        // The bundle is written at landing; an archive from before it
        // existed gets one here, so a publish always carries it.
        let bundle_path = dir.join(policy_archive::BUNDLE);
        if !bundle_path.exists() {
            crate::archive::store_bundle(dir, &manifest)?;
        }

        let mut record = Record {
            policy: policy.to_string(),
            manifest_updated_unix: manifest.updated_unix,
            published_unix: 0,
            secs: 0.0,
            files: Outcome::NotAttempted,
            uploaded: 0,
            skipped: 0,
            uploaded_bytes: 0,
            manifest: Outcome::NotAttempted,
            bundle: Outcome::NotAttempted,
            kv: Vec::new(),
            prune: Outcome::NotAttempted,
            pruned: 0,
        };

        // What the archive is, by the manifest: every file it names plus
        // the pending sidecar. The bundle is KV's, not R2's.
        let mut wanted: Vec<(String, PathBuf)> = manifest
            .files()
            .into_iter()
            .map(|(rel, _)| (rel.clone(), dir.join(&rel)))
            .collect();
        if let Some(p) = manifest.pending_file() {
            wanted.push((p.clone(), dir.join(&p)));
        }
        let manifest_key = Self::key(policy, MANIFEST);

        // The version the flip will be conditional on, read BEFORE anything
        // changes. Absent means "create, and fail if someone beat us".
        let prior = match self.store.head(&manifest_key).await {
            Ok(m) => Some(UpdateVersion {
                e_tag: m.e_tag,
                version: m.version,
            }),
            Err(object_store::Error::NotFound { .. }) => None,
            Err(e) => {
                record.files = Outcome::failed(anyhow::Error::from(e).context("head manifest"));
                return self.finish(dir, record, started);
            }
        };

        // 1. Files.
        match self.upload_files(policy, &wanted).await {
            Ok(u) => {
                record.files = Outcome::Done;
                record.uploaded = u.uploaded;
                record.skipped = u.skipped;
                record.uploaded_bytes = u.bytes;
            }
            Err(e) => {
                record.files = Outcome::failed(e);
                return self.finish(dir, record, started);
            }
        }

        // 2. The manifest, conditionally.
        let mode = prior.map_or(PutMode::Create, PutMode::Update);
        let body = std::fs::read(dir.join(MANIFEST))?;
        let put = self
            .store
            .put_opts(
                &manifest_key,
                PutPayload::from(body),
                PutOptions {
                    mode,
                    attributes: manifest_attributes(),
                    ..Default::default()
                },
            )
            .await;
        match put {
            Ok(_) => record.manifest = Outcome::Done,
            Err(e @ object_store::Error::Precondition { .. })
            | Err(e @ object_store::Error::AlreadyExists { .. }) => {
                record.manifest = Outcome::failed(format!(
                    "the manifest in R2 changed under this publish — another writer? ({e})"
                ));
                return self.finish(dir, record, started);
            }
            Err(e) => {
                record.manifest = Outcome::failed(e);
                return self.finish(dir, record, started);
            }
        }

        // 3. The bundle, to every namespace.
        match &self.kv {
            None => record.bundle = Outcome::NotConfigured,
            Some(kv) => {
                let bytes = std::fs::read(&bundle_path)?;
                let metadata = serde_json::to_string(&BundleMetadata {
                    updated_unix: manifest.updated_unix,
                })?;
                let mut failed = 0usize;
                for ns in &kv.namespaces {
                    let outcome = match self.put_kv(kv, ns, policy, &bytes, &metadata).await {
                        Ok(()) => Outcome::Done,
                        Err(e) => {
                            failed += 1;
                            Outcome::failed(e)
                        }
                    };
                    record.kv.push(KvOutcome {
                        namespace: ns.clone(),
                        outcome,
                    });
                }
                record.bundle = match failed {
                    0 => Outcome::Done,
                    n => Outcome::failed(format!("{n} of {} namespace(s)", kv.namespaces.len())),
                };
            }
        }

        // 4. Prune. The flip succeeded, so nothing live names what goes.
        let keep: BTreeSet<ObjPath> = wanted
            .iter()
            .map(|(rel, _)| Self::key(policy, rel))
            .chain(std::iter::once(manifest_key))
            .collect();
        match self.prune(policy, &keep).await {
            Ok(n) => {
                record.prune = Outcome::Done;
                record.pruned = n;
            }
            Err(e) => record.prune = Outcome::failed(e),
        }

        self.finish(dir, record, started)
    }

    fn finish(&self, dir: &Path, mut record: Record, started: Instant) -> Result<Record> {
        record.published_unix = now_unix();
        record.secs = started.elapsed().as_secs_f64();
        store_record(dir, &record)?;
        Ok(record)
    }

    async fn upload_files(&self, policy: &str, wanted: &[(String, PathBuf)]) -> Result<Uploaded> {
        let mut out = Uploaded {
            uploaded: 0,
            skipped: 0,
            bytes: 0,
        };
        for (rel, path) in wanted {
            let len = std::fs::metadata(path)
                .with_context(|| format!("stat {}", path.display()))?
                .len();
            let key = Self::key(policy, rel);
            match self.store.head(&key).await {
                Ok(m) if m.size == len => {
                    out.skipped += 1;
                    continue;
                }
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(anyhow::Error::from(e).context(format!("head {key}"))),
            }
            let body = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
            self.store
                .put_opts(
                    &key,
                    PutPayload::from(body),
                    PutOptions {
                        attributes: immutable(),
                        ..Default::default()
                    },
                )
                .await
                .with_context(|| format!("put {key}"))?;
            out.uploaded += 1;
            out.bytes += len;
        }
        Ok(out)
    }

    /// One KV write, three attempts. The API wants multipart: the value
    /// and a JSON metadata part.
    async fn put_kv(
        &self,
        kv: &KvTarget,
        namespace: &str,
        policy: &str,
        bytes: &[u8],
        metadata: &str,
    ) -> Result<()> {
        let url = format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/storage/kv/namespaces/{namespace}/values/{policy}",
            kv.account_id
        );
        let mut last = None;
        for attempt in 1..=3u32 {
            let form = reqwest::multipart::Form::new()
                .part(
                    "value",
                    reqwest::multipart::Part::bytes(bytes.to_vec())
                        .file_name(policy_archive::BUNDLE)
                        .mime_str("application/octet-stream")?,
                )
                .text("metadata", metadata.to_string());
            let sent = self
                .http
                .put(&url)
                .bearer_auth(&kv.token)
                .multipart(form)
                .send()
                .await;
            match sent {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                Ok(resp) => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    let body: String = body.chars().take(200).collect();
                    // 4xx is ours to fix, not to retry.
                    if status.is_client_error() {
                        bail!("KV PUT {status}: {body}");
                    }
                    last = Some(format!("KV PUT {status}: {body}"));
                }
                Err(e) => last = Some(format!("KV PUT: {e}")),
            }
            tokio::time::sleep(Duration::from_secs(u64::from(attempt) * 2)).await;
        }
        bail!("{} (after 3 attempts)", last.unwrap_or_default())
    }

    async fn prune(&self, policy: &str, keep: &BTreeSet<ObjPath>) -> Result<usize> {
        let prefix = ObjPath::from(format!("{PREFIX}/{policy}"));
        let mut listed = self.store.list(Some(&prefix));
        let mut stale = Vec::new();
        while let Some(meta) = listed.next().await {
            let meta = meta.context("list")?;
            if !keep.contains(&meta.location) {
                stale.push(meta.location);
            }
        }
        for key in &stale {
            self.store
                .delete(key)
                .await
                .with_context(|| format!("delete {key}"))?;
        }
        Ok(stale.len())
    }
}

#[derive(Serialize)]
struct BundleMetadata {
    updated_unix: u64,
}

// ── CLI ──────────────────────────────────────────────────────────────────

/// `token-ledger publish` — publish one policy's archive by hand, from the
/// same environment the service uses. What the landing path does after
/// every pass; this is for archives it has not touched, and for looking.
#[derive(clap::Args, Debug)]
pub struct PublishArgs {
    /// Archive root (`<root>/<policy_hex>/manifest.json`).
    #[arg(long, default_value = "archive")]
    pub archive_dir: PathBuf,
    /// 56-hex policy id.
    #[arg(long)]
    pub policy: String,
}

pub fn run(args: PublishArgs) -> Result<()> {
    let policy = args.policy.to_lowercase();
    let dir = crate::archive::policy_dir(&args.archive_dir, &policy);
    let targets = Targets::from_env();
    println!("targets: {}", targets.describe());
    let Some(publisher) = Publisher::new(targets)? else {
        bail!(
            "R2 is not configured (R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_BUCKET)"
        );
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let record = runtime.block_on(publisher.publish(&dir, &policy))?;
    println!("{}", record.summary());
    println!("{}", serde_json::to_string_pretty(&record)?);
    if !record.all_done() {
        bail!("publish incomplete — see {}", dir.join(RECORD).display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The record is the whole point: a step that never ran must read as
    /// such, and only a run where every configured step succeeded is done.
    #[test]
    fn a_record_is_done_only_when_every_configured_step_is() {
        let mut r = Record {
            policy: "ab".into(),
            manifest_updated_unix: 1,
            published_unix: 2,
            secs: 0.1,
            files: Outcome::Done,
            uploaded: 1,
            skipped: 2,
            uploaded_bytes: 3,
            manifest: Outcome::Done,
            bundle: Outcome::NotConfigured,
            kv: Vec::new(),
            prune: Outcome::Done,
            pruned: 0,
        };
        assert!(r.all_done(), "KV unconfigured is published, not failed");
        r.manifest = Outcome::failed("etag mismatch");
        r.prune = Outcome::NotAttempted;
        assert!(!r.all_done());
        assert!(r.summary().contains("manifest FAILED: etag mismatch"));
        assert!(r.summary().contains("prune skipped"));
        let back: Record = serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(back.manifest, r.manifest);
    }

    /// Targets come from the environment by the names the box already has;
    /// KV without a token is "not configured", never an error.
    #[test]
    fn targets_read_the_box_environment() {
        // SAFETY (test-only): no other thread reads these variables here.
        unsafe {
            std::env::set_var("R2_ENDPOINT", "https://x.r2.cloudflarestorage.com");
            std::env::set_var("R2_ACCESS_KEY_ID", "k");
            std::env::set_var("R2_SECRET_ACCESS_KEY", "s");
            std::env::set_var("R2_BUCKET", "b");
            std::env::set_var("R2_ACCOUNT_ID", "acct");
            std::env::set_var("KV_NAMESPACE_IDS", "one two");
            std::env::remove_var("CF_KV_TOKEN");
        }
        let t = Targets::from_env();
        assert!(t.r2.is_some());
        assert!(t.kv.is_none(), "no token, no KV");
        unsafe {
            std::env::set_var("CF_KV_TOKEN", "SECRET-TOKEN-VALUE");
        }
        let t = Targets::from_env();
        assert!(
            !t.describe().contains("SECRET-TOKEN-VALUE") && !t.describe().contains("acct"),
            "never a secret or an account id in the log line: {}",
            t.describe()
        );
        let kv = t.kv.expect("configured");
        assert_eq!(kv.account_id, "acct", "falls back to the R2 account id");
        assert_eq!(kv.namespaces, vec!["one", "two"]);
    }
}
