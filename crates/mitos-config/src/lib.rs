//! The node's description, as types and tests.
//!
//! `/etc/mitos/mitos.toml` is the source of truth once it exists. This crate
//! parses it, overlays env via a **lookup function** (never `std::env`), and
//! derives the dolos-shaped config mitos actually reads — as plain structs,
//! so nothing here takes a dolos, tokio, or I/O dependency.
//!
//! No `mitos.toml` is a host concern: that path is today's
//! `DOLOS_CONFIG` + unit env, byte for byte, and does not come through here.

pub mod catalogue;
pub mod derive;
pub mod digest;
pub mod env;
pub mod error;
pub mod file;
pub mod network;
pub mod resolve;

pub use catalogue::{Catalogue, CatalogueEntry};
pub use derive::{
    DerivedChain, DerivedDolos, DerivedGenesis, DerivedMinibf, DerivedMithril, DerivedStorage,
    DerivedSync, derive_dolos,
};
pub use digest::Sha256Hex;
pub use error::Error;
pub use file::{
    CachesSection, DEFAULT_AUTH_TOKEN_FILE, FallbackGate, FallbackProvider, FallbackSection,
    MitosFile, ModulesSection, NetworkId, Profile, TxIndexSection,
};
pub use network::{DOLOS_SOURCE_TAG, GenesisFiles, NetworkSpec, spec};
pub use resolve::{EnvLookup, Resolved, ResolvedFallback, TxIndexPaths, resolve};
