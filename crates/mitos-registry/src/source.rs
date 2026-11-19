//! Reading the registry from the hosted repository, or a local copy of it.
//!
//! Two files, both `resolved/` artefacts of `theyexist/address-registry` at the
//! rev this build was tested against. A native tool fetches them once and hands
//! the bytes to [`Loaded`], which parses them and records what it read — the
//! stamps are what a caller writes beside its result, so an answer can be traced
//! to the registry version that produced it.
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

/// A parsed registry plus the stamp for the bytes it came from.
#[derive(Debug)]
pub struct Loaded {
    pub registry: Registry,
    pub stamp: Stamp,
}

#[derive(Debug)]
pub enum LoadError {
    Io {
        path: PathBuf,
        error: std::io::Error,
    },
    Http(String),
    Registry(RegistryError),
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadError::Io { path, error } => write!(f, "{}: {error}", path.display()),
            LoadError::Http(error) => write!(f, "fetching the address registry: {error}"),
            LoadError::Registry(error) => write!(f, "the address registry: {error}"),
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
    pub fn from_bytes(behaviour: &[u8], stakes: &[u8]) -> Result<Self, LoadError> {
        let registry = Registry::from_bytes(behaviour, stakes)?;
        let stamp = Stamp {
            rev: REV,
            behaviour_sha256: sha256(behaviour),
            stakes_sha256: sha256(stakes),
            generated_at: registry.generated_at().to_string(),
        };
        Ok(Self { registry, stamp })
    }

    /// The artefacts from a local directory — a checkout's `resolved/`, or a
    /// cache. The files are named as the published ones are.
    pub fn from_dir(dir: &Path) -> Result<Self, LoadError> {
        let behaviour = read(&dir.join("behaviour.bin"))?;
        let stakes = read(&dir.join("stake-services.bin"))?;
        Self::from_bytes(&behaviour, &stakes)
    }

    /// The artefacts at [`REV`], over HTTPS. Blocking: every caller is a
    /// command that does one fetch before its work starts.
    #[cfg(feature = "fetch")]
    pub fn fetch() -> Result<Self, LoadError> {
        let behaviour = get(&behaviour_url())?;
        let stakes = get(&stakes_url())?;
        Self::from_bytes(&behaviour, &stakes)
    }
}

/// The artefacts from `ADDRESS_REGISTRY_DIR` when it is set — a checkout's
/// `resolved/`, or a cache — and from the hosted repository otherwise. One env
/// var, so every tool in a run reads the same bytes.
#[cfg(feature = "fetch")]
pub fn load() -> Result<Loaded, LoadError> {
    match std::env::var_os("ADDRESS_REGISTRY_DIR") {
        Some(dir) => Loaded::from_dir(Path::new(&dir)),
        None => Loaded::fetch(),
    }
}

fn read(path: &Path) -> Result<Vec<u8>, LoadError> {
    std::fs::read(path).map_err(|error| LoadError::Io {
        path: path.to_path_buf(),
        error,
    })
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
