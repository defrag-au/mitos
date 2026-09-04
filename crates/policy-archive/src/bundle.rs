//! One entry per policy for a key-value store: the manifest and every file's
//! footer, together — everything a reader needs to OPEN the archive, none of
//! what it needs to page it.
//!
//! # Why this exists
//!
//! Opening an archive over a remote is the manifest, then each file's tail,
//! then the rest of the footer when the tail fell short: three or four round
//! trips before the first row group. Measured from a Worker in Brisbane
//! against a bucket in eastern North America, every one of those cost about
//! 300 ms whatever its size — the whole two-second page was round trips,
//! not bytes. The box that writes the archive has all of it in hand at
//! publish time, so it writes this blob beside the manifest and the push
//! puts it in KV, where a colo-cached read is milliseconds. A page is then
//! one KV read and one range request.
//!
//! # What is and is not in it
//!
//! The footer is the thrift `FileMetaData` block at the end of a Parquet
//! file: the schema, every row group's column-chunk offsets and statistics,
//! and the key-value metadata the writer stamped — the stamp and the per-day
//! side tables. Density, slot pruning and group ranges all come from it. The
//! bloom filters do NOT: they sit in the data section and a hash lookup
//! fetches them by the offsets the footer names, from the file.
//!
//! The manifest rides along as the bytes the box wrote, not as a second
//! schema, so a bundle and a manifest can never disagree about a file's
//! name.
//!
//! # On the wire
//!
//! postcard, on disk (`bundle.bin`, beside the manifest) and as the KV
//! value, byte for byte. A trap met while verifying it: wrangler 4's `kv key
//! put` and `kv key list` work against its LOCAL store unless `--remote` is
//! given, so a key can be "there" to wrangler and absent to the Worker.
//!
//! # Freshness
//!
//! Written every time the manifest is, keyed by policy, replaced whole. A
//! reader holding a bundle from before a rollup names files the rollup
//! pruned; its group fetch fails and the caller falls back. That window is
//! the store's cache TTL, and rollups are rare.

use serde::{Deserialize, Serialize};

use crate::manifest::{FileKind, Manifest, kind_of};
use crate::multi::FetchedFooter;
use crate::{Error, Result};

/// File name beside the manifest; the KV key is the policy.
pub const BUNDLE: &str = "bundle.bin";
pub const BUNDLE_FORMAT: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bundle {
    pub format: u32,
    /// `manifest.json`, byte for byte.
    pub manifest: Vec<u8>,
    /// One per file the manifest names, in the manifest's order.
    pub files: Vec<BundledFooter>,
}

/// A file's footer, with what a reader needs to place it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundledFooter {
    /// Relative path, exactly as the manifest names it.
    pub file: String,
    pub total_len: u64,
    /// Offset of the first footer byte in the file.
    pub footer_start: u64,
    /// `footer_start..total_len` — metadata, its length, and the magic.
    pub footer: Vec<u8>,
}

impl Bundle {
    pub fn manifest(&self) -> Result<Manifest> {
        Manifest::from_json(&self.manifest).map_err(|e| Error::Bundle(format!("manifest: {e}")))
    }

    /// postcard, like the pending sidecar.
    pub fn encode(&self) -> Result<Vec<u8>> {
        postcard::to_allocvec(self).map_err(|e| Error::Bundle(e.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let b: Self = postcard::from_bytes(bytes).map_err(|e| Error::Bundle(e.to_string()))?;
        if b.format > BUNDLE_FORMAT {
            return Err(Error::Bundle(format!(
                "format {} is newer than this reader ({BUNDLE_FORMAT})",
                b.format
            )));
        }
        Ok(b)
    }

    /// The footers as the reader takes them, keyed by `key_of(relative path)`
    /// — a bucket prefix joined on, typically. Kind comes from the file name,
    /// as it does for the manifest.
    pub fn footers(
        self,
        key_of: impl Fn(&str) -> String,
    ) -> Vec<(String, FileKind, FetchedFooter)> {
        self.files
            .into_iter()
            .map(|f| {
                let kind = kind_of(f.file.rsplit('/').next().unwrap_or(&f.file));
                (
                    key_of(&f.file),
                    kind,
                    FetchedFooter {
                        total_len: f.total_len,
                        start: f.footer_start,
                        bytes: f.footer.into(),
                        requests: 0,
                        fetched: 0,
                    },
                )
            })
            .collect()
    }

    /// Total footer bytes carried.
    pub fn footer_bytes(&self) -> usize {
        self.files.iter().map(|f| f.footer.len()).sum()
    }
}
