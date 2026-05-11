//! Transport implementations of `ChainDataPlane`.
//!
//! Today: `LocalDataPlane` (in-process, wraps
//! `dolos_core::Domain`). Future: `WasmDataPlane`,
//! `IpcDataPlane`, `GrpcDataPlane` — same trait, different
//! transports.

mod local;

pub use local::{LocalDataPlane, extract_aux_cbor};
