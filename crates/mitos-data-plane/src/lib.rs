//! Typed query API for current Cardano chain state.
//!
//! See `../../docs/design/MITOS_DATA_PLANE_API.md` for the full
//! design rationale. Headlines:
//!
//! - **Snapshot queries only.** This is purely "what does the
//!   chain look like right now?" — no chain-follow, no streaming,
//!   no historical archive. The existing `Indexer` / `Interest`
//!   replication pipeline handles the chain-update path.
//! - **One trait, multiple transports.** Today: `LocalDataPlane`
//!   for in-process Rust callers (zero serialisation cost,
//!   wraps `dolos_core::Domain`). Future: wasm host fns, Unix
//!   socket IPC, gRPC utxorpc-compat — same trait, different
//!   wire layer.
//! - **Caller-blind decode.** `TypedDatum` carries hash, payload,
//!   and original CBOR; the plane resolves witness-set datums
//!   transparently. Caller never sees the inline-vs-hash
//!   distinction unless they specifically want it.
//! - **Composable predicate algebra.** `UtxoPredicate` (and / or
//!   / not over `UtxoPattern`) is the declarative expression of
//!   "what UTxOs do I want." Mirrors utxorpc's shape; replaces
//!   their bytes-typed addresses with a typed Rust discriminated
//!   union.
//! - **Tiered decode.** `DecodeLevel::{Lean, WithDatum, Full}`
//!   controls how much expensive resolution the plane performs
//!   server-side per query. Output struct's `Option<T>` fields
//!   then mean "genuinely absent on chain", not "caller didn't
//!   ask" — that distinction is in `DecodeLevel`.

pub mod impls;
pub mod types;

#[cfg(test)]
mod tests;

pub use types::{
    AddressPattern, AssetEntry, AssetPattern, ChainTip, DataPlaneError, DataPlaneResult,
    DecodeLevel, OutputRef, OutputRefPattern, Page, PageRequest, ScriptLanguage, TypedDatum,
    TypedOutput, TypedScript, UtxoPattern, UtxoPredicate,
};

pub use impls::LocalDataPlane;

use async_trait::async_trait;
use cardano_assets::PolicyId;

/// The query API for current chain state. One trait, multiple
/// transports.
///
/// Consumers should depend on this trait generically (`<DP:
/// ChainDataPlane>`) for zero-cost dispatch. Object-safety is
/// available via the `async_trait` macro if a consumer needs
/// runtime impl-swap (`Arc<dyn ChainDataPlane>`), but the common
/// path is generic.
///
/// All methods are `async` for transport uniformity. Local /
/// in-process impls return immediately; IPC / wasm / gRPC impls
/// genuinely await. Consumers don't need to special-case the
/// transport.
#[async_trait]
pub trait ChainDataPlane: Send + Sync {
    /// Fetch a single output by reference. Returns `None` if the
    /// output isn't in the current UTxO set (spent or never
    /// existed).
    async fn read_utxo(
        &self,
        oref: &OutputRef,
        decode: DecodeLevel,
    ) -> DataPlaneResult<Option<TypedOutput>>;

    /// Bulk fetch multiple outputs. Bulk shape is the natural
    /// pattern — boundary-crossing transports especially benefit
    /// from one round-trip over many. Outputs not in the current
    /// set are silently omitted; caller compares input vs output
    /// length if they need to detect missing.
    async fn read_utxos(
        &self,
        orefs: &[OutputRef],
        decode: DecodeLevel,
    ) -> DataPlaneResult<Vec<(OutputRef, TypedOutput)>>;

    /// Predicate-driven search over the current UTxO set.
    /// Cursor-paginated; caller passes the previous response's
    /// `next_token` (or `None` for the first page).
    async fn search_utxos(
        &self,
        predicate: &UtxoPredicate,
        decode: DecodeLevel,
        page: PageRequest,
    ) -> DataPlaneResult<Page<(OutputRef, TypedOutput)>>;

    /// Bootstrap helper: enumerate output refs at one address.
    /// Refs only — callers pair with `read_utxos` when they want
    /// full outputs.
    ///
    /// Convenience wrapper over `search_utxos` for the common
    /// indexer-bootstrap pattern ("give me everything currently
    /// unspent at this script address"). Calling
    /// `search_utxos(Match(at_address(...)))` would work
    /// equivalently but pays construction overhead and pagination
    /// machinery for a use case that wants the full list in one
    /// shot.
    ///
    /// `address` is bech32 (`addr1...` / `addr_test1...`).
    /// Result is hard-capped at 100K refs.
    async fn utxos_by_address(&self, address: &str) -> DataPlaneResult<Vec<OutputRef>>;

    /// Resolve a datum hash to its Plutus payload. Side-door for
    /// callers that have a hash but not the containing UTxO
    /// context (rare — most callers should use `read_utxo` with
    /// `DecodeLevel::WithDatum` and let the plane do
    /// witness-set resolution transparently).
    async fn read_datum(
        &self,
        hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<TypedDatum>>;

    /// Resolve a script hash. Side-door analog of `read_datum`.
    async fn read_script(
        &self,
        hash: &pallas_primitives::Hash<28>,
    ) -> DataPlaneResult<Option<TypedScript>>;

    /// Total quantity of an asset minted to date (sum of mints
    /// minus burns; not just current UTxO sum). Asset name is
    /// optional — when omitted, returns total minted across all
    /// asset names under the policy.
    async fn total_supply(
        &self,
        policy: &PolicyId,
        asset_name_hex: Option<&str>,
    ) -> DataPlaneResult<u64>;

    /// Number of distinct holders of any asset under a policy.
    /// Holder is defined by the address holding the asset; stake-
    /// key dedup is consumer-side concern.
    async fn holder_count(&self, policy: &PolicyId) -> DataPlaneResult<u64>;

    /// Current chain tip the plane is reading from. Snapshot
    /// queries are answered against this point; callers
    /// paginating across mutations can detect drift by comparing
    /// the `tip` field on each `Page` response.
    async fn tip(&self) -> DataPlaneResult<ChainTip>;

    /// Current protocol parameters at the chain tip.
    async fn protocol_params(&self) -> DataPlaneResult<types::ProtocolParameters>;
}
