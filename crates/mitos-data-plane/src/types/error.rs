//! Typed error surface for data plane queries.
//!
//! Different transports surface different failure classes
//! (network for IPC, sandbox-trap for wasm, deadline-exceeded
//! for gRPC). The enum covers the union; each transport maps
//! its native errors into one variant. Callers handle each
//! shape exhaustively.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DataPlaneError {
    /// The underlying state store / domain returned an error.
    /// Wraps dolos / redb errors for the local transport, IO
    /// errors for IPC transports, etc.
    #[error("storage error: {0}")]
    Storage(String),

    /// CBOR decode failed for a value the plane was attempting
    /// to project into typed form. Shouldn't happen for valid
    /// chain data; surfaces parser bugs / chain corruption.
    #[error("decode error: {0}")]
    Decode(String),

    /// Caller passed an invalid pattern, predicate, or argument
    /// (e.g. malformed policy ID, negative pagination size).
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// Pagination cursor was malformed or stale (server-side
    /// state moved past it).
    #[error("invalid pagination cursor: {0}")]
    InvalidCursor(String),

    /// Result set would exceed the server-side cap. Caller
    /// should narrow the predicate or paginate.
    #[error("result too large: would return {would_return} items, cap is {cap}")]
    ResultTooLarge { would_return: u64, cap: u32 },

    /// Caller asked for `Trait` selection or another future
    /// feature that's reserved in the API but not yet
    /// implemented.
    #[error("not yet implemented: {0}")]
    NotYetImplemented(&'static str),

    /// Transport-level failure (network down, wasm trap, etc).
    /// Specific transport types map their errors into this for
    /// uniformity.
    #[error("transport error: {0}")]
    Transport(String),
}

pub type DataPlaneResult<T> = Result<T, DataPlaneError>;
