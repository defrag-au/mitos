//! Publishing a policy's archive — the Parquet and manifest to R2, the
//! bundle to Workers KV — in process, as typed steps with a record.
//!
//! # Where the machinery lives
//!
//! The client, the credentials, the conditional put, the KV fan-out and the
//! [`Outcome`] vocabulary moved to `cf-publish` (2026-09-15) when the
//! collection catalogue became the second publisher on this box. **This file
//! is now the archive's ORDER and nothing else** — which is the part that was
//! never generic: a catalogue flips a pointer at a content-addressed blob and
//! prunes one predecessor, an archive flips a manifest naming many immutable
//! files and prunes a rollup's ancestry.
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
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use object_store::Attributes;
use object_store::path::Path as ObjPath;
use serde::{Deserialize, Serialize};

use crate::archive::MANIFEST;

/// Credentials, the client and the outcome vocabulary — shared with every
/// other publisher on the box. See `cf-publish` for why only these moved.
pub use cf_publish::{KvOutcome, Outcome, Targets};
use cf_publish::{immutable_attributes, pointer_attributes};

/// Key prefix in the bucket: `policy-archive/<policy_hex>/<relative path>`,
/// the same relative paths the manifest carries.
pub const PREFIX: &str = "policy-archive";
/// The record of the last publish, beside the manifest.
pub const RECORD: &str = "published.json";

// ── The record ───────────────────────────────────────────────────────────

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
    /// The MOVEMENT GRAPH artifact, when one has been generated.
    #[serde(default = "Outcome::not_attempted")]
    pub graph: Outcome,
    /// What it weighed on the wire — gzipped when that measurably paid.
    #[serde(default)]
    pub graph_bytes: u64,
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
        format!(
            "{} ({} up, {} same, {} B); {}; {}; {} ({} B); {} ({} pruned); {:.1}s",
            self.files.describe("files"),
            self.uploaded,
            self.skipped,
            self.uploaded_bytes,
            self.manifest.describe("manifest"),
            self.bundle.describe("bundle"),
            self.graph.describe("graph"),
            self.graph_bytes,
            self.prune.describe("prune"),
            self.pruned,
            self.secs
        )
    }
}

pub fn load_record(dir: &Path) -> Result<Option<Record>> {
    cf_publish::load_record(&dir.join(RECORD))
}

fn store_record(dir: &Path, r: &Record) -> Result<()> {
    cf_publish::store_record(&dir.join(RECORD), r)
}

// ── The publisher ────────────────────────────────────────────────────────

pub struct Publisher {
    cf: cf_publish::Client,
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
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
        Ok(cf_publish::Client::new(targets)?.map(|cf| Self { cf }))
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
            graph: Outcome::NotAttempted,
            graph_bytes: 0,
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
        let prior = match self.cf.version_of(&manifest_key).await {
            Ok(v) => v,
            Err(e) => {
                record.files = Outcome::failed(e);
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
        let body = std::fs::read(dir.join(MANIFEST))?;
        match self
            .cf
            .put_conditional(
                &manifest_key,
                body,
                pointer_attributes("application/json"),
                prior,
            )
            .await
        {
            Ok(()) => record.manifest = Outcome::Done,
            Err(e) => {
                record.manifest = Outcome::failed(e);
                return self.finish(dir, record, started);
            }
        }

        // 3. The MOVEMENT GRAPH, if one has been generated. After the flip
        // on purpose: it is a DERIVED convenience, and a failure here must
        // not stop the archive from being published.
        match self.put_graph(policy, dir).await {
            Ok(None) => record.graph = Outcome::NotAttempted,
            Ok(Some(n)) => {
                record.graph = Outcome::Done;
                record.graph_bytes = n;
            }
            Err(e) => record.graph = Outcome::failed(e),
        }

        // 4. The bundle, to every namespace.
        match self.cf.kv() {
            None => record.bundle = Outcome::NotConfigured,
            Some(kv) => {
                let bytes = std::fs::read(&bundle_path)?;
                let metadata = serde_json::to_string(&BundleMetadata {
                    updated_unix: manifest.updated_unix,
                })?;
                let mut failed = 0usize;
                for ns in &kv.namespaces {
                    let outcome = match self
                        .cf
                        .put_kv(kv, ns, policy, &bytes, policy_archive::BUNDLE, &metadata)
                        .await
                    {
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

        // 5. Prune. The flip succeeded, so nothing live names what goes.
        //
        // DERIVED artifacts are kept too, and this is the whole reason the
        // list is explicit: the prune deletes everything under the policy's
        // prefix that the manifest does not name, and the movement graph is
        // by design not named by the manifest. Without these two entries the
        // publish wrote the graph and then deleted it seconds later, and
        // recorded `graph: done` for a put that genuinely succeeded — the
        // object simply did not survive to the end of the same function.
        // Both names, because whether it is stored gzipped is decided from
        // the measured result and either one may be the live object.
        let keep: BTreeSet<ObjPath> = wanted
            .iter()
            .map(|(rel, _)| Self::key(policy, rel))
            .chain(std::iter::once(manifest_key))
            .chain([
                Self::key(policy, policy_archive::GRAPH),
                Self::key(policy, &format!("{}.gz", policy_archive::GRAPH)),
            ])
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

    /// The movement graph — a DERIVED artifact, not a file the manifest
    /// names, and not immutable: it is regenerated as the archive grows, so
    /// it gets the manifest's cache posture rather than the files'.
    ///
    /// Compressed only when that MEASURABLY pays. R2 does not compress for
    /// you — a body is stored pre-compressed with `Content-Encoding: gzip`
    /// and the browser decompresses it transparently — and whether a body
    /// compresses depends on what is in it: the token path's `txids`, a wall
    /// of 32-byte hashes, came out 6 KB LARGER gzipped. Measured on
    /// ClayNation, this one goes 3.45 MB → 2.20 MB, so it pays; the
    /// comparison is made every time regardless.
    ///
    /// `Ok(None)` when no graph has been generated for this policy.
    async fn put_graph(&self, policy: &str, dir: &Path) -> Result<Option<u64>> {
        let path = dir.join(policy_archive::GRAPH);
        if !path.exists() {
            return Ok(None);
        }
        let raw = std::fs::read(&path).with_context(|| format!("read {}", path.display()))?;
        let gz = crate::archive::gzip(&raw)?;
        let mut attrs = Attributes::new();
        // It changes; a year-long immutable cache would pin a stale graph.
        attrs.insert(
            object_store::Attribute::CacheControl,
            "public, max-age=300".into(),
        );
        attrs.insert(
            object_store::Attribute::ContentType,
            "application/octet-stream".into(),
        );
        // An ABSENT Content-Encoding is the identity encoding, so the raw
        // case sets no header — and the key's suffix already tells a reader
        // which case it is looking at.
        let (rel, body) = match gz.len() < raw.len() {
            true => {
                attrs.insert(object_store::Attribute::ContentEncoding, "gzip".into());
                (format!("{}.gz", policy_archive::GRAPH), gz)
            }
            false => (policy_archive::GRAPH.to_string(), raw),
        };
        let n = body.len() as u64;
        self.cf.put(&Self::key(policy, &rel), body, attrs).await?;
        Ok(Some(n))
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
            // Immutable objects never reuse a name, so size is identity
            // enough to skip the upload.
            if self.cf.size_of(&key).await? == Some(len) {
                out.skipped += 1;
                continue;
            }
            let body = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
            self.cf.put(&key, body, immutable_attributes()).await?;
            out.uploaded += 1;
            out.bytes += len;
        }
        Ok(out)
    }

    async fn prune(&self, policy: &str, keep: &BTreeSet<ObjPath>) -> Result<usize> {
        self.cf
            .prune(&ObjPath::from(format!("{PREFIX}/{policy}")), keep)
            .await
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
    if let Some(prev) = load_record(&dir)? {
        println!(
            "last publish: {} (manifest {}, at {})",
            prev.summary(),
            prev.manifest_updated_unix,
            prev.published_unix
        );
    }
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
            // No graph generated for this policy — a step that never ran.
            graph: Outcome::NotAttempted,
            graph_bytes: 0,
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
}
