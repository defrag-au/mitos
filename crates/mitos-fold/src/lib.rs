//! The fold plane's contract: aggregates, one deterministic walk, one artifact envelope.
//!
//! `docs/design/FOLD_PLANE.md` is the spec. The short version:
//!
//! - A **flow plane** follows the tip and dispatches per-block events to wasm modules.
//! - A **fold plane** walks a range deterministically and produces an aggregate artifact.
//!   This crate is the second one's contract, not its walker.
//!
//! An [`Aggregate`] observes every block of one walk and produces one artifact at
//! `finish`. A [`View`] runs once over a substrate the walk finished — the live book — and
//! produces one artifact from that. A [`Registry`] runs a set of aggregates in registration
//! order and a [`ViewRegistry`] does the same for views, so one pass can produce several
//! artifacts. Determinism is part of the contract: the same input and the same [`Stamp`] must
//! produce byte-identical output, because the bytes are what a parity test compares and what a
//! reader trusts.
//!
//! The two shapes are not alternatives. An aggregate sees what *happened* in the window; a
//! view sees what is *live* at its end. Some artifacts are one, some are the other, and the
//! holder table is both — which is a check rather than a redundancy.
//!
//! # What this crate does not do
//!
//! No walk, no chunk or block decode, no file I/O, no publish, no wasm. Those are the
//! walker's and the artifact writer's jobs. The crate is dependency-free so the walker,
//! the publisher and a test harness can all link it without inheriting a chain stack.
//!
//! # Example
//!
//! ```
//! use mitos_fold::{Aggregate, AggregateResult, Fingerprint, Registry, SchemaId, Stamp};
//!
//! struct Count(u64);
//!
//! impl Aggregate<u64> for Count {
//!     type Artifact = u64;
//!
//!     fn schema(&self) -> SchemaId {
//!         SchemaId::new("block-count", 1)
//!     }
//!
//!     fn observe(&mut self, _block: &u64) -> AggregateResult<()> {
//!         self.0 += 1;
//!         Ok(())
//!     }
//!
//!     fn finish(self: Box<Self>, _stamp: Stamp) -> AggregateResult<u64> {
//!         Ok(self.0)
//!     }
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let mut registry = Registry::new();
//! registry.register("count", Count(0))?;
//! registry.observe(&1)?;
//! registry.observe(&2)?;
//!
//! let stamp = Stamp::new(2, None, Fingerprint::from_u128(0x1));
//! let finished = registry.finish(stamp)?;
//! assert_eq!(finished[0].artifact, 2);
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

mod artifact;
mod registry;
mod schema;
mod stamp;
mod view;

use std::fmt;

pub use artifact::{Artifact, ArtifactFile, ArtifactKind, Counts, Extension};
pub use registry::{Aggregate, Finished, Registry};
pub use schema::SchemaId;
pub use stamp::{Fingerprint, Stamp};
pub use view::{View, ViewRegistry};

/// The boxed error an aggregate returns. The registry wraps it with the aggregate's name.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The result type an [`Aggregate`] uses for its own work.
pub type AggregateResult<T> = std::result::Result<T, BoxError>;

/// The result type this crate uses for the contract itself.
pub type Result<T> = std::result::Result<T, Error>;

/// Something the contract refuses.
#[derive(Debug)]
pub enum Error {
    /// A schema id is not a valid identifier.
    InvalidSchema {
        schema: SchemaId,
        reason: &'static str,
    },
    /// A fingerprint is not 32 hex digits.
    InvalidFingerprint { value: String, reason: &'static str },
    /// An aggregate name is empty, padded or contains a control character.
    InvalidAggregateName { name: String, reason: &'static str },
    /// Two aggregates were registered under the same name.
    DuplicateName { name: String },
    /// A view name is empty, padded or contains a control character.
    InvalidViewName { name: String, reason: &'static str },
    /// Two views were registered under the same name.
    DuplicateViewName { name: String },
    /// Two aggregates or two views were registered with the same schema id.
    DuplicateSchema { schema: SchemaId },
    /// An aggregate rejected a block.
    Observe { aggregate: String, source: BoxError },
    /// A view rejected the substrate.
    View { view: String, source: BoxError },
    /// A view failed to produce its artifact.
    ViewFinish { view: String, source: BoxError },
    /// An aggregate failed to produce its artifact.
    Finish { aggregate: String, source: BoxError },
    /// An artifact lists no files.
    EmptyArtifact,
    /// A file name is empty, absolute, or escapes the artifact root.
    UnsafeFileName { name: String, reason: &'static str },
    /// The manifest could not be read: a missing field, an unknown one, a bad number.
    InvalidEnvelope { reason: String },
    /// The manifest's format version is not one this build writes.
    UnsupportedEnvelope { version: u32 },
    /// The artifact's schema is not the one the reader requires.
    SchemaMismatch { found: String, expected: SchemaId },
    /// The artifact's stamp is not the one the reader requires.
    ///
    /// The slot and the source, as text. Holding the two whole `Stamp`s here would make the
    /// variant 128 bytes and every `Result` in this crate would pay for it; the base hash
    /// follows from the slot, and the message is what a reader acts on.
    StampMismatch { found: String, expected: String },
    /// Two artifact files share a name.
    DuplicateFileName { name: String },
    /// The declared byte count does not equal the sum of the files.
    ByteCountMismatch { counted: u64, listed: u64 },
    /// The file byte counts overflow a `u64`.
    ByteCountOverflow,
    /// An extension tag is zero, which is reserved.
    ReservedExtensionTag,
    /// Two extensions share a tag.
    DuplicateExtension { tag: u32 },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::InvalidSchema { schema, reason } => {
                write!(f, "schema `{schema}` is invalid: {reason}")
            }
            Error::InvalidFingerprint { value, reason } => {
                write!(f, "fingerprint {value:?} is invalid: {reason}")
            }
            Error::InvalidAggregateName { name, reason } => {
                write!(f, "aggregate name {name:?} is invalid: {reason}")
            }
            Error::DuplicateName { name } => {
                write!(f, "an aggregate named `{name}` is already registered")
            }
            Error::InvalidViewName { name, reason } => {
                write!(f, "view name {name:?} is invalid: {reason}")
            }
            Error::DuplicateViewName { name } => {
                write!(f, "a view named `{name}` is already registered")
            }
            Error::DuplicateSchema { schema } => {
                write!(f, "schema `{schema}` is already registered")
            }
            Error::Observe { aggregate, source } => {
                write!(f, "aggregate `{aggregate}` rejected a block: {source}")
            }
            Error::View { view, source } => {
                write!(f, "view `{view}` rejected the substrate: {source}")
            }
            Error::ViewFinish { view, source } => {
                write!(f, "view `{view}` failed to finish: {source}")
            }
            Error::Finish { aggregate, source } => {
                write!(f, "aggregate `{aggregate}` failed to finish: {source}")
            }
            Error::EmptyArtifact => write!(f, "an artifact must list at least one file"),
            Error::UnsafeFileName { name, reason } => {
                write!(f, "artifact file name {name:?} is invalid: {reason}")
            }
            Error::InvalidEnvelope { reason } => {
                write!(f, "the artifact manifest is invalid: {reason}")
            }
            Error::UnsupportedEnvelope { version } => write!(
                f,
                "the artifact manifest is version {version}; this build writes {}",
                Artifact::VERSION
            ),
            Error::SchemaMismatch { found, expected } => {
                write!(
                    f,
                    "the artifact's schema is `{found}`, not the required `{expected}`"
                )
            }
            Error::StampMismatch { found, expected } => {
                write!(f, "the artifact is {found}, not the required {expected}")
            }
            Error::DuplicateFileName { name } => {
                write!(f, "artifact file `{name}` is listed twice")
            }
            Error::ByteCountMismatch { counted, listed } => write!(
                f,
                "counts.bytes is {counted}, but the file list totals {listed}"
            ),
            Error::ByteCountOverflow => write!(f, "the file byte counts overflow u64"),
            Error::ReservedExtensionTag => write!(f, "extension tag 0 is reserved"),
            Error::DuplicateExtension { tag } => write!(f, "extension tag {tag} is used twice"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Observe { source, .. }
            | Error::View { source, .. }
            | Error::ViewFinish { source, .. }
            | Error::Finish { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}
