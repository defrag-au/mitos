//! Transport implementations of `ChainDataPlane`.
//!
//! Two today, and the second is the point: `LocalDataPlane` (in-process, wraps
//! `dolos_core::Domain`) and `BookDataPlane` (in-process, wraps a published live
//! book and needs no dolos at all). Same trait, so a module's answers on one can
//! be compared with its answers on the other — which is what makes "the phonebook
//! is complete" a diff rather than an opinion.
//!
//! Future: `WasmDataPlane`, `IpcDataPlane`, `GrpcDataPlane` — same trait,
//! different transports.

mod book;
mod local;

pub use book::BookDataPlane;
pub use local::{
    LocalDataPlane, UTXOS_BY_ADDRESS_HARD_CAP, extract_aux_cbor, project_typed_output,
};
