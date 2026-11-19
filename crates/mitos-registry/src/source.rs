//! Reading the registry from the hosted repository, or a local copy of it.
//!
//! Two files, both `resolved/` artefacts of `theyexist/address-registry` at the
//! rev this build was tested against. A native tool fetches them once and hands
//! the bytes to [`Loaded`], which parses them and records what it read — the
//! stamps are what a caller writes beside its result, so an answer can be traced
//! to the registry version that produced it.
//!
//! A successful fetch is also written to a cache, so a run whose fetch fails
//! classifies with the last verified bytes rather than failing outright. The
//! cache is per [`REV`] and digest-checked, so a fallback is always the revision
//! this build names — never bytes from another rev, and never a file that was
//! truncated or edited under it.
//!
//! This module is behind the `fetch` feature: the crate is otherwise pure, and a
//! consumer with no HTTP client (the wasm module) uses `pinned` instead.

use crate::{Registry, RegistryError};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// The registry revision these bytes came from. Bumping it is a deliberate act:
/// the pinned artefacts under `data/` must move with it.
pub const REV: &str = "45ff9557d0f98ed17fddcf7a8f6edcbfccf2b995";

/// Where the artefacts are served.
const BASE: &str = "https://raw.githubusercontent.com/theyexist/address-registry";

pub const BEHAVIOUR_FILE: &str = "resolved/behaviour.bin";
pub const STAKES_FILE: &str = "resolved/stake-services.bin";

/// The cache's own record of what it holds: the two digests, `sha256sum` format.
/// Written last, so a cache that was interrupted mid-write has no record and is
/// refused rather than half-trusted.
const STAMP_FILE: &str = "SHA256SUMS";

pub fn behaviour_url() -> String {
    format!("{BASE}/{REV}/{BEHAVIOUR_FILE}")
}

pub fn stakes_url() -> String {
    format!("{BASE}/{REV}/{STAKES_FILE}")
}

/// What was read, and from where. Written beside a result so it can be traced to
/// the registry version that produced it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stamp {
    pub rev: &'static str,
    /// The behaviour artefact's sha256, lowercase hex.
    pub behaviour_sha256: String,
    /// The stake-service artefact's sha256, lowercase hex.
    pub stakes_sha256: String,
    /// The behaviour file's own `generated_at`, so a stamp carries both the
    /// exact bytes and the publisher's claim about when they were made.
    pub generated_at: String,
}

impl Stamp {
    /// The two digests in `sha256sum` form — the cache's record of itself.
    fn sha256sums(&self) -> String {
        format!(
            "{}  behaviour.bin\n{}  stake-services.bin\n",
            self.behaviour_sha256, self.stakes_sha256
        )
    }
}

/// How the bytes arrived. A caller says so when they are not the plain fetch: a
/// cached answer is a real one, and the operator's only signal that the registry
/// may be behind the network's copy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Origin {
    /// `ADDRESS_REGISTRY_DIR` named a directory; nothing was fetched.
    Dir,
    /// Fetched over HTTPS at [`REV`] in this run.
    Fetched,
    /// Fetched in an earlier run, written to the cache, and read back because
    /// this run's fetch failed.
    Cache,
}

/// A parsed registry plus the stamp for the bytes it came from.
#[derive(Debug)]
pub struct Loaded {
    pub registry: Registry,
    pub stamp: Stamp,
    pub origin: Origin,
    /// Why a successful fetch could not be written to the cache. The fetch
    /// already answered the run, so this never fails it — but a cache that is
    /// silently not being written is the next outage with no fallback, so it is
    /// carried for the caller to say out loud.
    pub cache_note: Option<String>,
}

#[derive(Debug)]
pub enum LoadError {
    Io {
        path: PathBuf,
        error: std::io::Error,
    },
    Http(String),
    Registry(RegistryError),
    /// The fetch failed and there is no verified cached copy of [`REV`].
    Unavailable {
        fetch: String,
        cache: Option<PathBuf>,
    },
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadError::Io { path, error } => write!(f, "{}: {error}", path.display()),
            LoadError::Http(error) => write!(f, "fetching the address registry: {error}"),
            LoadError::Registry(error) => write!(f, "the address registry: {error}"),
            LoadError::Unavailable { fetch, cache } => match cache {
                Some(dir) => write!(
                    f,
                    "{fetch} — and no verified cached copy at {}",
                    dir.display()
                ),
                None => write!(
                    f,
                    "{fetch} — and no cache directory is set (ADDRESS_REGISTRY_CACHE, \
                     XDG_CACHE_HOME or HOME)"
                ),
            },
        }
    }
}

impl std::error::Error for LoadError {}

impl From<RegistryError> for LoadError {
    fn from(error: RegistryError) -> Self {
        LoadError::Registry(error)
    }
}

impl Loaded {
    /// Parse the two artefacts and stamp them. This is the one place the digest
    /// is computed, so every caller records the same thing.
    fn parse(behaviour: &[u8], stakes: &[u8], origin: Origin) -> Result<Self, LoadError> {
        let registry = Registry::from_bytes(behaviour, stakes)?;
        let stamp = Stamp {
            rev: REV,
            behaviour_sha256: sha256(behaviour),
            stakes_sha256: sha256(stakes),
            generated_at: registry.generated_at().to_string(),
        };
        Ok(Self {
            registry,
            stamp,
            origin,
            cache_note: None,
        })
    }

    /// The artefacts from a local directory — a checkout's `resolved/`, or a
    /// cache. The files are named as the published ones are. Nothing is fetched
    /// and nothing is written.
    pub fn from_dir(dir: &Path) -> Result<Self, LoadError> {
        let behaviour = read(&dir.join("behaviour.bin"))?;
        let stakes = read(&dir.join("stake-services.bin"))?;
        Self::parse(&behaviour, &stakes, Origin::Dir)
    }

    /// The artefacts at [`REV`], over HTTPS, and nothing else. Blocking: every
    /// caller is a command that does one fetch before its work starts. [`load`]
    /// is the form a run should use — this one neither reads nor writes the
    /// cache.
    #[cfg(feature = "fetch")]
    pub fn fetch() -> Result<Self, LoadError> {
        let (behaviour, stakes) = fetch_bytes()?;
        Self::parse(&behaviour, &stakes, Origin::Fetched)
    }

    /// The cached copy for [`REV`], when one verifies: both files present, and
    /// the digests beside them describe what is on disk. `None` for no cache, a
    /// cache written for another revision, or one that does not match its
    /// record — a fallback must never serve bytes it cannot vouch for.
    pub fn cached() -> Option<Self> {
        Self::read_cache(&cache_dir()?)
    }

    fn read_cache(dir: &Path) -> Option<Self> {
        let behaviour = std::fs::read(dir.join("behaviour.bin")).ok()?;
        let stakes = std::fs::read(dir.join("stake-services.bin")).ok()?;
        let recorded = std::fs::read_to_string(dir.join(STAMP_FILE)).ok()?;
        let loaded = Self::parse(&behaviour, &stakes, Origin::Cache).ok()?;
        (recorded == loaded.stamp.sha256sums()).then_some(loaded)
    }

    /// Write these bytes where a later run can find them. Per file: write beside
    /// the destination, then rename, so a reader never sees a partial file; the
    /// digest record goes last, so a crash between the two leaves a cache that
    /// [`Self::cached`] refuses rather than trusts.
    fn write_cache(&self, dir: &Path, behaviour: &[u8], stakes: &[u8]) -> std::io::Result<()> {
        std::fs::create_dir_all(dir)?;
        write_atomic(&dir.join("behaviour.bin"), behaviour)?;
        write_atomic(&dir.join("stake-services.bin"), stakes)?;
        write_atomic(&dir.join(STAMP_FILE), self.stamp.sha256sums().as_bytes())
    }
}

/// The artefacts from `ADDRESS_REGISTRY_DIR` when it is set — a checkout's
/// `resolved/`, or a cache — and from the hosted repository otherwise. One env
/// var, so every tool in a run reads the same bytes.
///
/// A fetch that succeeds is cached; a fetch that fails falls back to the cache
/// and says so through [`Loaded::origin`]. The failure that leaves both
/// unavailable is an error with both causes in it, rather than a silently empty
/// registry.
#[cfg(feature = "fetch")]
pub fn load() -> Result<Loaded, LoadError> {
    if let Some(dir) = std::env::var_os("ADDRESS_REGISTRY_DIR") {
        return Loaded::from_dir(Path::new(&dir));
    }

    let (behaviour, stakes) = match fetch_bytes() {
        Ok(bytes) => bytes,
        Err(fetch_error) => {
            return match Loaded::cached() {
                Some(cached) => Ok(cached),
                None => Err(LoadError::Unavailable {
                    fetch: fetch_error.to_string(),
                    cache: cache_dir(),
                }),
            };
        }
    };

    let mut loaded = Loaded::parse(&behaviour, &stakes, Origin::Fetched)?;
    loaded.cache_note = match cache_dir() {
        Some(dir) => loaded
            .write_cache(&dir, &behaviour, &stakes)
            .err()
            .map(|error| error.to_string()),
        None => Some(
            "no cache directory is set (ADDRESS_REGISTRY_CACHE, XDG_CACHE_HOME or HOME)"
                .to_string(),
        ),
    };
    Ok(loaded)
}

/// Where a fetched copy is kept: `ADDRESS_REGISTRY_CACHE` when set, else
/// `$XDG_CACHE_HOME/address-registry`, else `$HOME/.cache/address-registry` —
/// each with [`REV`] as a subdirectory.
///
/// The revision is a directory level on purpose. A build pins the schema and the
/// rev together; a cache written by a different rev would answer for bytes this
/// build was never tested against, which is worse than no answer.
pub fn cache_dir() -> Option<PathBuf> {
    let root = match std::env::var_os("ADDRESS_REGISTRY_CACHE") {
        Some(dir) => PathBuf::from(dir),
        None => match std::env::var_os("XDG_CACHE_HOME") {
            Some(dir) => PathBuf::from(dir).join("address-registry"),
            None => PathBuf::from(std::env::var_os("HOME")?).join(".cache/address-registry"),
        },
    };
    Some(root.join(REV))
}

#[cfg(feature = "fetch")]
fn fetch_bytes() -> Result<(Vec<u8>, Vec<u8>), LoadError> {
    Ok((get(&behaviour_url())?, get(&stakes_url())?))
}

fn read(path: &Path) -> Result<Vec<u8>, LoadError> {
    std::fs::read(path).map_err(|error| LoadError::Io {
        path: path.to_path_buf(),
        error,
    })
}

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let temp = path.with_extension("tmp");
    std::fs::write(&temp, bytes)?;
    std::fs::rename(&temp, path)
}

#[cfg(feature = "fetch")]
fn get(url: &str) -> Result<Vec<u8>, LoadError> {
    let response = reqwest::blocking::get(url)
        .and_then(reqwest::blocking::Response::error_for_status)
        .map_err(|error| LoadError::Http(error.to_string()))?;
    response
        .bytes()
        .map(|bytes| bytes.to_vec())
        .map_err(|error| LoadError::Http(error.to_string()))
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const BEHAVIOUR: &[u8] = include_bytes!("../data/behaviour.bin");
    const STAKES: &[u8] = include_bytes!("../data/stake-services.bin");

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mitos-registry-cache-{}-{name}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir
    }

    fn fresh() -> Loaded {
        Loaded::parse(BEHAVIOUR, STAKES, Origin::Fetched).expect("the pinned artefacts decode")
    }

    #[test]
    fn a_written_cache_reads_back_verified() {
        let root = scratch("roundtrip");
        let dir = root.join(REV);
        let loaded = fresh();
        loaded
            .write_cache(&dir, BEHAVIOUR, STAKES)
            .expect("write cache");

        let back = Loaded::read_cache(&dir).expect("the cache verifies");
        assert_eq!(back.origin, Origin::Cache);
        assert_eq!(back.stamp, loaded.stamp);
        assert_eq!(back.registry.generated_at(), loaded.registry.generated_at());
        assert_eq!(
            back.registry.stakes().entries.len(),
            loaded.registry.stakes().entries.len()
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn a_tampered_cache_is_refused_rather_than_served() {
        let root = scratch("tampered");
        let dir = root.join(REV);
        fresh()
            .write_cache(&dir, BEHAVIOUR, STAKES)
            .expect("write cache");
        assert!(Loaded::read_cache(&dir).is_some());

        let mut tampered = BEHAVIOUR.to_vec();
        tampered[0] ^= 0xFF;
        std::fs::write(dir.join("behaviour.bin"), &tampered).expect("tamper");
        assert!(
            Loaded::read_cache(&dir).is_none(),
            "bytes that do not match the recorded digest are not a fallback"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn a_cache_with_no_digest_record_is_not_a_cache() {
        let root = scratch("unrecorded");
        let dir = root.join(REV);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        std::fs::write(dir.join("behaviour.bin"), BEHAVIOUR).expect("write");
        std::fs::write(dir.join("stake-services.bin"), STAKES).expect("write");
        assert!(
            Loaded::read_cache(&dir).is_none(),
            "an interrupted or hand-made cache is refused"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn the_cache_is_keyed_by_revision() {
        let dir = cache_dir().expect("HOME gives a cache path");
        assert!(
            dir.ends_with(REV),
            "a cache for another rev must never answer: {}",
            dir.display()
        );
    }
}
