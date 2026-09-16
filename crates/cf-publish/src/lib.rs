//! Publishing a derived artifact to Cloudflare — R2 over the S3 surface, a
//! Workers KV fan-out, and a typed record of how each step went.
//!
//! # Why this is code and not a script
//!
//! The first cut shelled out to rclone and curl. One exit code covered a
//! four-step publish, so a KV failure after a successful R2 copy read as
//! "published"; nothing recorded when, or whether, an artifact had actually
//! reached the edge; a transient API error stayed unretried until the next
//! pass happened to land; and the manifest flip could not be conditional, so
//! two writers would race silently. The user's verdict: *an incredibly brittle
//! surface*.
//!
//! # What lives here, and what does not
//!
//! Here: credentials from the box's environment, the client, conditional put,
//! listing and pruning, the KV write with its retries, and the
//! [`Outcome`] vocabulary a `published.json` is written in.
//!
//! NOT here: the ORDER a given artifact publishes in. A policy archive uploads
//! immutable Parquet, flips a manifest under `If-Match`, fans a bundle out to
//! KV and prunes its predecessors. A collection catalogue writes one
//! content-addressed blob and flips a pointer at it. Those sequences differ in
//! what is conditional on what, and flattening them into one parameterised
//! "publish" would hide the only part that is load-bearing.
//!
//! # The invariant every caller keeps
//!
//! **Read the pointer's version BEFORE writing anything, and make the flip
//! conditional on it.** A reader must never see a pointer naming an object
//! that is not there, and a second writer must fail loudly rather than win
//! quietly.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

/// The object-store vocabulary a caller needs to name keys and set headers,
/// re-exported so a publisher does not have to take its own `object_store`
/// dependency — and cannot end up resolving a SECOND, differently-versioned
/// copy of these types.
pub use object_store::path::Path as ObjPath;
pub use object_store::{Attribute, Attributes, UpdateVersion};

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
    /// Every namespace gets the same value: dev and prod read one bucket.
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
        Self::from_env_with_bucket("R2_BUCKET")
    }

    /// The same, with the bucket read from `bucket_var` first and `R2_BUCKET`
    /// as the fallback — for an artifact that wants its own bucket without
    /// needing a second set of credentials.
    pub fn from_env_with_bucket(bucket_var: &str) -> Self {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
        let r2 = match (
            var("R2_ENDPOINT"),
            var("R2_ACCESS_KEY_ID"),
            var("R2_SECRET_ACCESS_KEY"),
            var(bucket_var).or_else(|| var("R2_BUCKET")),
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
///
/// ⚠️ Four states, not a bool. "Landed" and "published" are two facts, and a
/// step that never ran is neither a success nor a failure — collapsing
/// `NotAttempted` into `Failed` makes an unconfigured KV look like an
/// incident, and collapsing it into `Done` is how a half-publish reads as
/// complete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Outcome {
    Done,
    /// No target for this step; the artifact is published without it.
    NotConfigured,
    /// An earlier step failed, or there was nothing to do, so this never ran.
    NotAttempted,
    Failed {
        error: String,
    },
}

impl Outcome {
    pub fn failed(e: impl std::fmt::Display) -> Self {
        Outcome::Failed {
            error: format!("{e:#}"),
        }
    }

    /// Did this step leave the artifact publishable? An unconfigured target
    /// is not a failure.
    pub fn is_done(&self) -> bool {
        matches!(self, Outcome::Done | Outcome::NotConfigured)
    }

    /// A record written before a step existed says nothing about it, which is
    /// `NotAttempted` — not a failure and not a success. Used as a serde
    /// `default` by records that gained a step.
    pub fn not_attempted() -> Self {
        Outcome::NotAttempted
    }

    /// `files ok` / `manifest FAILED: …`, for a one-line summary.
    pub fn describe(&self, name: &str) -> String {
        match self {
            Outcome::Done => format!("{name} ok"),
            Outcome::NotConfigured => format!("{name} n/a"),
            Outcome::NotAttempted => format!("{name} skipped"),
            Outcome::Failed { error } => format!("{name} FAILED: {error}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvOutcome {
    pub namespace: String,
    pub outcome: Outcome,
}

/// Write a record beside its artifact, atomically — a half-written
/// `published.json` is indistinguishable from a corrupt one on the next read.
pub fn store_record<T: Serialize>(path: &Path, record: &T) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(record)?)
        .with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("renaming into {}", path.display()))?;
    Ok(())
}

/// Read a record, or `None` when none has been written yet.
pub fn load_record<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Option<T>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read(path)?;
    Ok(Some(
        serde_json::from_slice(&raw).with_context(|| format!("parsing {}", path.display()))?,
    ))
}

// ── Cache posture ────────────────────────────────────────────────────────

/// A year, immutable — for an object whose key never names different bytes.
pub fn immutable_attributes() -> Attributes {
    let mut a = Attributes::new();
    a.insert(
        Attribute::CacheControl,
        "public, max-age=31536000, immutable".into(),
    );
    a
}

/// `no-cache` — for the pointer that is flipped. ⚠️ A pointer with the files'
/// cache posture pins a stale artifact for a year.
pub fn pointer_attributes(content_type: &str) -> Attributes {
    let mut a = Attributes::new();
    a.insert(Attribute::CacheControl, "no-cache".into());
    a.insert(Attribute::ContentType, content_type.to_string().into());
    a
}

// ── The client ───────────────────────────────────────────────────────────

pub struct Client {
    store: Arc<dyn ObjectStore>,
    kv: Option<KvTarget>,
    http: reqwest::Client,
}

impl Client {
    /// `None` when R2 is not configured: a KV value names R2 keys, so KV on
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
            // The pointer flip: `If-Match` on the version read before the
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

    /// The raw store, for a caller that needs an operation this surface does
    /// not name.
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    pub fn kv(&self) -> Option<&KvTarget> {
        self.kv.as_ref()
    }

    /// The version to make a later flip conditional on. `Ok(None)` means the
    /// object does not exist yet, which a caller turns into
    /// [`object_store::PutMode::Create`] — so a racing first writer still
    /// fails loudly.
    ///
    /// ⚠️ **Call this BEFORE writing anything the pointer will name.** That
    /// ordering is the whole guarantee.
    pub async fn version_of(&self, key: &ObjPath) -> Result<Option<UpdateVersion>> {
        match self.store.head(key).await {
            Ok(m) => Ok(Some(UpdateVersion {
                e_tag: m.e_tag,
                version: m.version,
            })),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(anyhow::Error::from(e).context(format!("head {key}"))),
        }
    }

    /// The size R2 already holds for a key, or `None` if it holds nothing.
    /// Immutable objects never reuse a name, so size is identity enough to
    /// skip an upload.
    pub async fn size_of(&self, key: &ObjPath) -> Result<Option<u64>> {
        match self.store.head(key).await {
            Ok(m) => Ok(Some(m.size)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(anyhow::Error::from(e).context(format!("head {key}"))),
        }
    }

    /// An unconditional put.
    pub async fn put(&self, key: &ObjPath, body: Vec<u8>, attributes: Attributes) -> Result<()> {
        self.store
            .put_opts(
                key,
                PutPayload::from(body),
                PutOptions {
                    attributes,
                    ..Default::default()
                },
            )
            .await
            .with_context(|| format!("put {key}"))?;
        Ok(())
    }

    /// The flip: conditional on `prior`, which must be the version read
    /// before this publish began. A precondition failure means another writer
    /// moved the pointer under us and is reported as such rather than retried.
    pub async fn put_conditional(
        &self,
        key: &ObjPath,
        body: Vec<u8>,
        attributes: Attributes,
        prior: Option<UpdateVersion>,
    ) -> Result<()> {
        let mode = prior.map_or(PutMode::Create, PutMode::Update);
        let put = self
            .store
            .put_opts(
                key,
                PutPayload::from(body),
                PutOptions {
                    mode,
                    attributes,
                    ..Default::default()
                },
            )
            .await;
        match put {
            Ok(_) => Ok(()),
            Err(e @ object_store::Error::Precondition { .. })
            | Err(e @ object_store::Error::AlreadyExists { .. }) => {
                bail!("{key} changed in R2 under this publish — another writer? ({e})")
            }
            Err(e) => Err(anyhow::Error::from(e).context(format!("put {key}"))),
        }
    }

    /// One KV write, three attempts. The API wants multipart: the value and a
    /// JSON metadata part.
    ///
    /// ⚠️ A 4xx is ours to fix, not to retry — three rounds of a bad token
    /// is three times the same error and a minute of nothing.
    pub async fn put_kv(
        &self,
        kv: &KvTarget,
        namespace: &str,
        key: &str,
        bytes: &[u8],
        file_name: &str,
        metadata: &str,
    ) -> Result<()> {
        let url = format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/storage/kv/namespaces/{namespace}/values/{key}",
            kv.account_id
        );
        let mut last = None;
        for attempt in 1..=3u32 {
            let form = reqwest::multipart::Form::new()
                .part(
                    "value",
                    reqwest::multipart::Part::bytes(bytes.to_vec())
                        .file_name(file_name.to_string())
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

    /// Delete everything under `prefix` that `keep` does not name.
    ///
    /// ⚠️ **Only after the flip succeeded**, so a stale prune can never remove
    /// objects a live pointer still names. And `keep` must list DERIVED
    /// artifacts too: the archive publisher once wrote its movement graph and
    /// deleted it seconds later in the same function, recording `graph: done`
    /// for a put that genuinely succeeded.
    pub async fn prune(&self, prefix: &ObjPath, keep: &BTreeSet<ObjPath>) -> Result<usize> {
        let mut listed = self.store.list(Some(prefix));
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Targets come from the environment by the names the box already has;
    /// KV without a token is "not configured", never an error. And the log
    /// line never carries a secret or an account id.
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

        // ⚠️ The bucket override is asserted HERE rather than in its own test.
        // The process environment is global and `cargo test` runs tests in
        // parallel, so a second test setting `R2_BUCKET` races this one and
        // fails whichever loses — which is a flake, not a finding.
        //
        // A second artifact can take its own bucket without a second set of
        // credentials, and falls back rather than silently publishing nowhere.
        unsafe {
            std::env::remove_var("CATALOGUE_R2_BUCKET");
        }
        let t = Targets::from_env_with_bucket("CATALOGUE_R2_BUCKET");
        assert_eq!(t.r2.expect("configured").bucket, "b", "falls back");
        unsafe {
            std::env::set_var("CATALOGUE_R2_BUCKET", "its-own");
        }
        let t = Targets::from_env_with_bucket("CATALOGUE_R2_BUCKET");
        assert_eq!(t.r2.expect("configured").bucket, "its-own");
        unsafe {
            std::env::remove_var("CATALOGUE_R2_BUCKET");
        }
    }

    /// ⚠️ The four states are the whole point. An unconfigured target is
    /// published, a step that never ran is neither, and a failure says why.
    #[test]
    fn an_outcome_distinguishes_unconfigured_from_unattempted_from_failed() {
        assert!(Outcome::Done.is_done());
        assert!(Outcome::NotConfigured.is_done(), "KV off is not a failure");
        assert!(!Outcome::NotAttempted.is_done());
        let failed = Outcome::failed("etag mismatch");
        assert!(!failed.is_done());
        assert_eq!(failed.describe("pointer"), "pointer FAILED: etag mismatch");
        assert_eq!(Outcome::NotAttempted.describe("kv"), "kv skipped");

        // And it survives the round trip a published.json makes.
        let back: Outcome = serde_json::from_str(&serde_json::to_string(&failed).unwrap()).unwrap();
        assert_eq!(back, failed);
    }

    /// A half-written record is indistinguishable from a corrupt one, so the
    /// write is atomic — and a missing record reads as `None`, not an error.
    #[test]
    fn a_record_is_written_atomically_and_reads_back() {
        let dir = std::env::temp_dir().join(format!("cf-publish-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("published.json");
        let _ = std::fs::remove_file(&path);
        assert!(load_record::<Outcome>(&path).unwrap().is_none());
        store_record(&path, &Outcome::Done).unwrap();
        assert_eq!(load_record::<Outcome>(&path).unwrap(), Some(Outcome::Done));
        assert!(
            !path.with_extension("json.tmp").exists(),
            "no tmp left behind"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The pointer must not inherit the files' year-long cache posture.
    #[test]
    fn a_pointer_is_never_cached_like_an_immutable_object() {
        let p = pointer_attributes("application/json");
        assert_eq!(
            p.get(&Attribute::CacheControl).map(|v| v.as_ref()),
            Some("no-cache")
        );
        let i = immutable_attributes();
        assert!(
            i.get(&Attribute::CacheControl)
                .map(|v| v.as_ref().contains("immutable"))
                .unwrap_or(false)
        );
    }
}
