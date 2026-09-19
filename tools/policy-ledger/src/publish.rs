//! `policy-ledger catalogue publish` — the artifact to R2 and Workers KV,
//! **if and only if its content changed**.
//!
//! # ⚠️ Hash the ARTIFACT, not the inputs
//!
//! `base.pidx` changes every single day, because chunks were appended. A day
//! with no new collections nonetheless produces a byte-identical catalogue,
//! and must publish nothing. Hashing the inputs would republish ~1.7 MiB daily
//! to say exactly what was already said.
//!
//! ⚠️ And not quite the encoded artifact either: `built_through_chunk` and
//! `built_at` move on every build BY CONSTRUCTION, so hashing the whole blob
//! would make every day look changed and defeat the rule it implements. The
//! hash covers [`Catalogue::content_bytes`] — what the catalogue SAYS, not
//! when it said it.
//!
//! # ⚠️ The hash is written AFTER the publish, not after the build
//!
//! The design doc proposed a `catalogue-hash` sidecar written by the last
//! publish. It is folded into `published.json` here, and recorded only when
//! every configured step SUCCEEDED — because a hash written at build time
//! makes the next run skip a publish that never actually happened, and a
//! failed publish would then heal only if the corpus changed again. One
//! sidecar, one fact: *this content reached the edge*.
//!
//! # The order, and why it is fixed
//!
//! 1. Read the pointer's version — BEFORE anything changes.
//! 2. The BLOB, at a content-addressed key, immutable, a year.
//! 3. `latest.json`, LAST of the data, `no-cache`, and CONDITIONAL on (1): a
//!    reader never sees a pointer naming an object that is not there, and a
//!    second writer fails loudly rather than winning quietly.
//! 4. KV, so a Worker opens the catalogue in one read. MEASURED in
//!    `POLICY_ARCHIVE_AND_SCALE.md`: KV took density from 0.9 s to 65 ms,
//!    because every R2 read from BNE costs ~300 ms flat.
//! 5. Prune what the pointer no longer names. Only after the flip.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use cf_publish::{
    Attribute, Client, KvOutcome, ObjPath, Outcome, Targets, immutable_attributes,
    pointer_attributes,
};
use collection_catalogue::{CATALOGUE_VERSION, Catalogue, Pointer};
use serde::{Deserialize, Serialize};

use crate::NETWORK;

/// Key prefix in the bucket.
pub const PREFIX: &str = collection_catalogue::KV_PREFIX;
/// The pointer, flipped last.
pub const POINTER: &str = "latest.json";
/// The record of the last publish, beside the artifact.
pub const RECORD: &str = "published.json";

/// The KV key a Worker reads. The spelling is part of the wire contract, so it
/// lives in `collection_catalogue` beside [`Pointer`]; this pins the network.
pub fn kv_key() -> String {
    collection_catalogue::kv_key(NETWORK)
}

/// What the last publish did — `published.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    /// The content that reached the edge. ⚠️ The change-detection key, and
    /// only ever written for a publish where every configured step succeeded.
    pub content_hash: String,
    pub built_through_chunk: u16,
    pub built_at: u64,
    pub published_unix: u64,
    /// When a build last CHECKED, whether or not it published. The freshness
    /// of the builder, kept on the box where the systemd unit's failure is
    /// also visible — deliberately not in the artifact, so "nothing changed"
    /// stays a publish of literally nothing.
    pub checked_unix: u64,
    pub secs: f64,
    pub blob: Outcome,
    pub blob_bytes: u64,
    pub pointer: Outcome,
    pub kv: Vec<KvOutcome>,
    pub kv_summary: Outcome,
    pub prune: Outcome,
    pub pruned: usize,
}

impl Record {
    /// Did every configured step succeed?
    pub fn all_done(&self) -> bool {
        self.blob.is_done()
            && self.pointer.is_done()
            && self.kv_summary.is_done()
            && self.prune.is_done()
    }

    pub fn summary(&self) -> String {
        format!(
            "{} ({} B); {}; {}; {} ({} pruned); {:.1}s",
            self.blob.describe("blob"),
            self.blob_bytes,
            self.pointer.describe("pointer"),
            self.kv_summary.describe("kv"),
            self.prune.describe("prune"),
            self.pruned,
            self.secs
        )
    }
}

// ── Hashing ──────────────────────────────────────────────────────────────

/// blake2b-256 over the catalogue's content bytes, hex.
pub fn content_hash(catalogue: &Catalogue) -> Result<String> {
    let bytes = catalogue
        .content_bytes()
        .context("encoding the catalogue's content for hashing")?;
    Ok(pallas_crypto::hash::Hasher::<256>::hash(&bytes).to_string())
}

/// The blob's key. The first 16 hex of the content hash is enough to name a
/// handful of daily artifacts distinctly; `Pointer::content_hash` carries the
/// authoritative full one.
fn blob_key(hash: &str) -> ObjPath {
    let short: String = hash.chars().take(16).collect();
    ObjPath::from(format!("{PREFIX}/{NETWORK}/catalogue-{short}.bin"))
}

fn pointer_key() -> ObjPath {
    ObjPath::from(format!("{PREFIX}/{NETWORK}/{POINTER}"))
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// gzip, and only when the comparison says it pays.
///
/// ⚠️ R2 does not compress for you — a body is stored pre-compressed with
/// `Content-Encoding: gzip` and the browser decompresses it transparently —
/// and whether a body compresses depends on what is in it. MEASURED here: the
/// catalogue's policy ids are 45% of the payload and are blake2b-224 hashes,
/// 743.9 KiB that gzip GROWS to 744.2 KiB. The names carry the whole win. The
/// comparison is made every time regardless.
fn maybe_gzip(raw: &[u8]) -> Result<(Vec<u8>, Option<&'static str>)> {
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;

    let mut enc = GzEncoder::new(Vec::new(), Compression::best());
    enc.write_all(raw)?;
    let gz = enc.finish()?;
    Ok(match gz.len() < raw.len() {
        true => (gz, Some("gzip")),
        false => (raw.to_vec(), None),
    })
}

// ── The CLI ──────────────────────────────────────────────────────────────

#[derive(clap::Args, Debug)]
pub struct PublishArgs {
    /// The artifact `catalogue build` wrote.
    #[arg(long, default_value = "catalogue.bin")]
    pub artifact: PathBuf,
    /// Publish even when the content is unchanged — for re-running a publish
    /// that failed after the record was written, or a first push into an
    /// empty bucket.
    #[arg(long)]
    pub force: bool,
    /// Say what would happen and touch nothing.
    #[arg(long)]
    pub dry_run: bool,
}

pub fn run(args: PublishArgs) -> Result<()> {
    let raw = std::fs::read(&args.artifact)
        .with_context(|| format!("reading {}", args.artifact.display()))?;
    let catalogue = collection_catalogue::decode(&raw)
        .map_err(|e| anyhow::anyhow!("{} is not a catalogue: {e}", args.artifact.display()))?;
    let hash = content_hash(&catalogue)?;
    let record_path = args.artifact.with_file_name(RECORD);
    let prior: Option<Record> = cf_publish::load_record(&record_path)?;

    println!(
        "catalogue: v{} through chunk {}, {} entries ({} named), {:.1} KiB raw",
        catalogue.version,
        catalogue.built_through_chunk,
        catalogue.entries.len(),
        catalogue.named(),
        raw.len() as f64 / 1024.0,
    );
    println!("content:   {hash}");
    if let Some(p) = &prior {
        println!(
            "last publish: {} (content {}, at {})",
            p.summary(),
            p.content_hash,
            p.published_unix
        );
    }

    // ⚠️ Unchanged only counts against a publish that SUCCEEDED. A record
    // whose steps failed is a reason to try again, not to skip.
    let unchanged = prior
        .as_ref()
        .is_some_and(|p| p.content_hash == hash && p.all_done());
    if unchanged && !args.force {
        // The check still happened; record that it did, so the box can tell a
        // quiet corpus from a builder that stopped running.
        if let Some(mut p) = prior {
            p.checked_unix = now_unix();
            cf_publish::store_record(&record_path, &p)?;
        }
        println!("unchanged — nothing published");
        return Ok(());
    }

    let targets = Targets::from_env_with_bucket("CATALOGUE_R2_BUCKET");
    println!("targets: {}", targets.describe());

    let (body, encoding) = maybe_gzip(&raw)?;
    println!(
        "blob: {:.1} KiB {} → {}",
        body.len() as f64 / 1024.0,
        encoding.unwrap_or("raw"),
        blob_key(&hash),
    );

    if args.dry_run {
        println!("dry run — nothing published");
        return Ok(());
    }

    let Some(client) = Client::new(targets)? else {
        bail!(
            "R2 is not configured (R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_BUCKET)"
        );
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let record = runtime.block_on(publish(
        &client,
        &catalogue,
        &hash,
        body,
        encoding,
        &record_path,
    ))?;
    println!("{}", record.summary());
    if !record.all_done() {
        bail!("publish incomplete — see {}", record_path.display());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn publish(
    client: &Client,
    catalogue: &Catalogue,
    hash: &str,
    body: Vec<u8>,
    encoding: Option<&'static str>,
    record_path: &Path,
) -> Result<Record> {
    let started = Instant::now();
    let blob = blob_key(hash);
    let pointer = pointer_key();

    let mut record = Record {
        content_hash: hash.to_string(),
        built_through_chunk: catalogue.built_through_chunk,
        built_at: catalogue.built_at,
        published_unix: 0,
        checked_unix: now_unix(),
        secs: 0.0,
        blob: Outcome::NotAttempted,
        blob_bytes: body.len() as u64,
        pointer: Outcome::NotAttempted,
        kv: Vec::new(),
        kv_summary: Outcome::NotAttempted,
        prune: Outcome::NotAttempted,
        pruned: 0,
    };

    // 1. The version the flip will be conditional on, read BEFORE anything
    //    changes. Absent means "create, and fail if someone beat us".
    let prior_version = match client.version_of(&pointer).await {
        Ok(v) => v,
        Err(e) => {
            record.blob = Outcome::failed(e);
            return finish(record_path, record, started);
        }
    };

    // 2. The blob. Content-addressed, so a key never names different bytes —
    //    which is what makes the year-long immutable cache safe, and makes a
    //    re-run after a failed pointer flip a no-op rather than a rewrite.
    let already = match client.size_of(&blob).await {
        Ok(n) => n,
        Err(e) => {
            record.blob = Outcome::failed(e);
            return finish(record_path, record, started);
        }
    };
    if already != Some(body.len() as u64) {
        let mut attrs = immutable_attributes();
        attrs.insert(Attribute::ContentType, "application/octet-stream".into());
        // An ABSENT Content-Encoding is the identity encoding, so the raw
        // case sets no header.
        if let Some(enc) = encoding {
            attrs.insert(Attribute::ContentEncoding, enc.into());
        }
        if let Err(e) = client.put(&blob, body.clone(), attrs).await {
            record.blob = Outcome::failed(e);
            return finish(record_path, record, started);
        }
    }
    record.blob = Outcome::Done;

    // 3. The pointer, conditionally.
    let doc = Pointer {
        version: CATALOGUE_VERSION,
        key: blob.to_string(),
        content_hash: hash.to_string(),
        bytes: body.len() as u64,
        content_encoding: encoding.map(str::to_string),
        entries: catalogue.entries.len() as u32,
        named: catalogue.named() as u32,
        built_through_chunk: catalogue.built_through_chunk,
        built_at: catalogue.built_at,
    };
    let doc_bytes = serde_json::to_vec_pretty(&doc)?;
    match client
        .put_conditional(
            &pointer,
            doc_bytes.clone(),
            pointer_attributes("application/json"),
            prior_version,
        )
        .await
    {
        Ok(()) => record.pointer = Outcome::Done,
        Err(e) => {
            record.pointer = Outcome::failed(e);
            return finish(record_path, record, started);
        }
    }

    // 4. KV, after the object it points at exists.
    match client.kv() {
        None => record.kv_summary = Outcome::NotConfigured,
        Some(kv) => {
            // The metadata is the pointer itself: a Worker that reads the KV
            // value needs to know its encoding and version without a second
            // round trip. KV metadata caps at 1 KiB; this is ~300 bytes.
            let metadata = serde_json::to_string(&doc)?;
            let mut failed = 0usize;
            for ns in &kv.namespaces {
                let outcome = match client
                    .put_kv(kv, ns, &kv_key(), &body, "catalogue.bin", &metadata)
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
            record.kv_summary = match failed {
                0 => Outcome::Done,
                n => Outcome::failed(format!("{n} of {} namespace(s)", kv.namespaces.len())),
            };
        }
    }

    // 5. Prune the predecessor. The flip succeeded, so nothing live names it.
    let keep: BTreeSet<ObjPath> = [blob, pointer].into_iter().collect();
    match client
        .prune(&ObjPath::from(format!("{PREFIX}/{NETWORK}")), &keep)
        .await
    {
        Ok(n) => {
            record.prune = Outcome::Done;
            record.pruned = n;
        }
        Err(e) => record.prune = Outcome::failed(e),
    }

    finish(record_path, record, started)
}

fn finish(path: &Path, mut record: Record, started: Instant) -> Result<Record> {
    record.published_unix = now_unix();
    record.secs = started.elapsed().as_secs_f64();
    cf_publish::store_record(path, &record)?;
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;
    use collection_catalogue::{Alias, Entry, EntryFlags, NameOrigin};

    fn catalogue() -> Catalogue {
        Catalogue {
            version: CATALOGUE_VERSION,
            built_through_chunk: 9153,
            built_at: 1_757_900_000,
            entries: vec![Entry {
                policy: [0x1f; 28],
                assets: 10_000,
                names: vec![Alias::new("Clay Nation", NameOrigin::Declared)],
                flags: EntryFlags::HAZARD,
                ext: Vec::new(),
            }],
            ext: Vec::new(),
        }
    }

    /// 🔑 **The whole point of the change detection.** Tomorrow's build of an
    /// unchanged corpus has a later stamp and a later chunk, and must hash the
    /// same — otherwise the daily refresh republishes ~1.7 MiB to say nothing.
    #[test]
    fn the_hash_ignores_the_freshness_stamp_and_notices_the_content() {
        let today = catalogue();
        let mut tomorrow = today.clone();
        tomorrow.built_through_chunk += 7;
        tomorrow.built_at += 86_400;
        assert_eq!(
            content_hash(&today).unwrap(),
            content_hash(&tomorrow).unwrap()
        );

        let mut changed = today.clone();
        changed.entries[0].assets += 1;
        assert_ne!(
            content_hash(&today).unwrap(),
            content_hash(&changed).unwrap()
        );
    }

    /// The key is content-addressed, so it never names different bytes — the
    /// whole basis of the immutable cache posture.
    #[test]
    fn the_blob_key_follows_the_content() {
        let a = content_hash(&catalogue()).unwrap();
        let mut other = catalogue();
        other.entries[0].names.clear();
        let b = content_hash(&other).unwrap();
        assert_ne!(blob_key(&a), blob_key(&b));
        assert_eq!(blob_key(&a), blob_key(&a));
        assert!(
            blob_key(&a)
                .to_string()
                .starts_with("collection-catalogue/mainnet/catalogue-"),
            "{}",
            blob_key(&a)
        );
    }

    /// ⚠️ A record whose steps failed is a reason to RETRY, not to skip. The
    /// hash alone matching is not enough.
    #[test]
    fn a_failed_publish_does_not_suppress_the_next_one() {
        let hash = content_hash(&catalogue()).unwrap();
        let mut r = Record {
            content_hash: hash.clone(),
            built_through_chunk: 9153,
            built_at: 1,
            published_unix: 2,
            checked_unix: 2,
            secs: 0.1,
            blob: Outcome::Done,
            blob_bytes: 10,
            pointer: Outcome::Done,
            kv: Vec::new(),
            kv_summary: Outcome::NotConfigured,
            prune: Outcome::Done,
            pruned: 0,
        };
        assert!(r.all_done(), "KV unconfigured is published, not failed");

        r.pointer = Outcome::failed("etag mismatch");
        assert!(!r.all_done());
        assert!(r.summary().contains("pointer FAILED: etag mismatch"));

        // And it survives the round trip published.json makes.
        let back: Record = serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(back.content_hash, hash);
        assert!(!back.all_done());
    }

    /// The comparison is made every time; a body that does not shrink is
    /// stored raw, with NO `Content-Encoding` — the identity encoding is the
    /// absence of the header, not `"identity"`.
    #[test]
    fn gzip_is_used_only_when_it_pays() {
        let compressible = vec![b'a'; 100_000];
        let (body, enc) = maybe_gzip(&compressible).unwrap();
        assert_eq!(enc, Some("gzip"));
        assert!(body.len() < compressible.len());

        // Eight bytes of entropy cannot be compressed below the gzip header.
        let incompressible = [0x9f, 0x3a, 0x11, 0xcd, 0x04, 0x7e, 0xb2, 0x68];
        let (body, enc) = maybe_gzip(&incompressible).unwrap();
        assert_eq!(enc, None, "storing it larger to claim a compression win");
        assert_eq!(body, incompressible);
    }

    /// The pointer is an API contract with a Worker and a frontend — a typed
    /// struct that round-trips, never a `json!`.
    #[test]
    fn the_pointer_round_trips() {
        let p = Pointer {
            version: CATALOGUE_VERSION,
            key: "collection-catalogue/mainnet/catalogue-0123456789abcdef.bin".into(),
            content_hash: "ab".repeat(32),
            bytes: 1_150_000,
            content_encoding: Some("gzip".into()),
            entries: 30_619,
            named: 27_207,
            built_through_chunk: 9153,
            built_at: 1_757_900_000,
        };
        let back: Pointer = serde_json::from_slice(&serde_json::to_vec(&p).unwrap()).unwrap();
        assert_eq!(back, p);
        // ⚠️ An absent encoding must decode as None, not as the string "null".
        let raw = serde_json::to_string(&Pointer {
            content_encoding: None,
            ..p
        })
        .unwrap();
        let back: Pointer = serde_json::from_str(&raw).unwrap();
        assert_eq!(back.content_encoding, None);
    }

    #[test]
    fn the_kv_key_names_the_network() {
        assert_eq!(kv_key(), "collection-catalogue:mainnet");
    }
}
