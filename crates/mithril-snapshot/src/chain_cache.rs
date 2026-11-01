//! A certificate-chain verification cache that survives the process.
//!
//! `mithril-client` verifies a certificate by walking the chain to genesis: one
//! HTTP request and one multi-signature check per epoch since Mithril started.
//! Every `fetch_with` call pays that walk, and an ingest is one `fetch_with` per
//! batch, so a backfill would spend most of its wall time re-proving a chain it
//! proved a minute earlier. The client exposes [`CertificateVerifierCache`] for
//! exactly this; what it ships is an in-memory cache, and this is the persisted
//! form of it.
//!
//! An entry is the claim the verifier's own cache contract makes: *this
//! certificate's signature was checked, and its parent is this hash*. It is only
//! written after `verify_without_cache` validated the certificate, so a hit
//! skips work that has already been done rather than work that was assumed.
//! Certificates are immutable, so a hit never needs re-checking.
//!
//! The file is one link per line, two hex hashes separated by a space. That is
//! deliberately not JSON: both values are 64 hex characters, so the encoding is
//! unambiguous without an escaping layer, and a crash mid-append costs at most
//! the last line.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::{Context, Result};
use async_trait::async_trait;
use mithril_client::MithrilResult;
use mithril_client::certificate_client::CertificateVerifierCache;

/// Where a store keeps its chain cache.
pub const FILE_NAME: &str = ".mithril-certificate-chain";

/// One validated link in a certificate chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    /// The certificate whose own signature was verified.
    pub hash: String,
    /// The parent the chain continued to.
    pub previous: String,
}

impl Link {
    fn parse(line: &str) -> Option<Self> {
        let mut parts = line.split_whitespace();
        let hash = parts.next()?;
        let previous = parts.next()?;
        if parts.next().is_some() || !is_hash(hash) || !is_hash(previous) {
            return None;
        }
        Some(Self {
            hash: hash.to_string(),
            previous: previous.to_string(),
        })
    }

    fn line(&self) -> String {
        format!("{} {}", self.hash, self.previous)
    }
}

fn is_hash(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// The links a store has already proved, backed by [`FILE_NAME`] beside it.
pub struct ChainCache {
    path: PathBuf,
    links: Mutex<HashMap<String, String>>,
}

impl ChainCache {
    /// Read the cache at `path`, which need not exist yet.
    ///
    /// A line that does not parse is skipped: the last one can be a torn append
    /// after a crash, and the cost of skipping a good line is a re-verification
    /// rather than a wrong answer.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut links = HashMap::new();
        if path.exists() {
            let file = fs::File::open(&path).with_context(|| {
                format!("opening the certificate chain cache {}", path.display())
            })?;
            for line in BufReader::new(file).lines() {
                let line = line.with_context(|| {
                    format!("reading the certificate chain cache {}", path.display())
                })?;
                if let Some(link) = Link::parse(&line) {
                    links.insert(link.hash, link.previous);
                }
            }
        }
        Ok(Self {
            path,
            links: Mutex::new(links),
        })
    }

    /// The number of links already known, for tests and reports.
    pub fn len(&self) -> usize {
        self.links
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn append(&self, link: &Link) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| {
                format!(
                    "opening the certificate chain cache {} for writing",
                    self.path.display()
                )
            })?;
        writeln!(file, "{}", link.line()).with_context(|| {
            format!(
                "appending to the certificate chain cache {}",
                self.path.display()
            )
        })
    }
}

#[async_trait]
impl CertificateVerifierCache for ChainCache {
    async fn store_validated_certificate(
        &self,
        certificate_hash: &str,
        previous_certificate_hash: &str,
    ) -> MithrilResult<()> {
        let link = Link {
            hash: certificate_hash.to_string(),
            previous: previous_certificate_hash.to_string(),
        };
        let mut links = self
            .links
            .lock()
            .map_err(|_| anyhow::anyhow!("the certificate chain cache lock was poisoned"))?;
        if links.get(&link.hash) == Some(&link.previous) {
            return Ok(());
        }
        self.append(&link)?;
        links.insert(link.hash, link.previous);
        Ok(())
    }

    async fn get_previous_hash(&self, certificate_hash: &str) -> MithrilResult<Option<String>> {
        let links = self
            .links
            .lock()
            .map_err(|_| anyhow::anyhow!("the certificate chain cache lock was poisoned"))?;
        Ok(links.get(certificate_hash).cloned())
    }

    async fn reset(&self) -> MithrilResult<()> {
        let mut links = self
            .links
            .lock()
            .map_err(|_| anyhow::anyhow!("the certificate chain cache lock was poisoned"))?;
        links.clear();
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).with_context(|| {
                format!(
                    "removing the certificate chain cache {}",
                    self.path.display()
                )
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn hash(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("mithril-chain-cache-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create the test directory");
        dir
    }

    #[test]
    fn a_link_survives_a_reopen() {
        let dir = temp_dir("reopen");
        let path = dir.join(FILE_NAME);
        let parent = hash('a');

        let cache = ChainCache::open(&path).expect("open an empty cache");
        assert!(cache.is_empty());
        tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(cache.store_validated_certificate(&hash('b'), &parent))
            .expect("store a link");

        let reopened = ChainCache::open(&path).expect("reopen the cache");
        assert_eq!(reopened.len(), 1);
        assert_eq!(
            tokio::runtime::Runtime::new()
                .expect("runtime")
                .block_on(reopened.get_previous_hash(&hash('b')))
                .expect("read a link"),
            Some(parent)
        );
        assert_eq!(
            tokio::runtime::Runtime::new()
                .expect("runtime")
                .block_on(reopened.get_previous_hash(&hash('c')))
                .expect("read an unknown link"),
            None
        );

        fs::remove_dir_all(&dir).expect("clean up");
    }

    #[test]
    fn storing_the_same_link_twice_writes_one_line() {
        let dir = temp_dir("dedupe");
        let path = dir.join(FILE_NAME);
        let cache = ChainCache::open(&path).expect("open an empty cache");
        let runtime = tokio::runtime::Runtime::new().expect("runtime");

        runtime
            .block_on(cache.store_validated_certificate(&hash('d'), &hash('e')))
            .expect("store a link");
        runtime
            .block_on(cache.store_validated_certificate(&hash('d'), &hash('e')))
            .expect("store the same link");

        let text = fs::read_to_string(&path).expect("read the cache file");
        assert_eq!(text.lines().count(), 1);
        fs::remove_dir_all(&dir).expect("clean up");
    }

    #[test]
    fn a_torn_line_is_skipped_and_reset_removes_the_file() {
        let dir = temp_dir("torn");
        let path = dir.join(FILE_NAME);
        fs::write(&path, format!("{} {}\nnot a link\n", hash('f'), hash('0')))
            .expect("write a torn cache");

        let cache = ChainCache::open(&path).expect("open the cache");
        assert_eq!(cache.len(), 1);

        tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(cache.reset())
            .expect("reset the cache");
        assert!(!path.exists());

        fs::remove_dir_all(&dir).expect("clean up");
    }

    #[test]
    fn a_link_requires_two_full_hex_hashes() {
        let value = hash('1');
        assert!(Link::parse(&format!("{value} {value}")).is_some());
        assert!(Link::parse(&value).is_none());
        assert!(Link::parse(&format!("{value} {value} {value}")).is_none());
        assert!(Link::parse(&format!("{value} nothex")).is_none());
        assert!(Link::parse(&format!("{value} {}", "1".repeat(63))).is_none());
    }

    #[test]
    fn path_is_where_the_store_expects_it() {
        let store = Path::new("/var/lib/mithril");
        assert_eq!(
            store.join(FILE_NAME),
            Path::new("/var/lib/mithril/.mithril-certificate-chain")
        );
    }
}
