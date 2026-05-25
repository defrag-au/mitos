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

pub mod block_events;
pub mod dispatch;
pub mod impls;
pub mod types;

#[cfg(test)]
mod tests;

pub use types::{
    AddressPattern, AssetEntry, AssetMintState, AssetPattern, ChainPoint, ChainTip, Cip25Resolution,
    ConsumedEvent, ConsumedInput, DataPlaneError, DataPlaneResult, DecodeLevel, DispatchEvent,
    InterestPredicate,
    InterestSet, MintEntry, MintedEvent, OutputRef, OutputRefPattern, Page, PageRequest,
    ProducedEvent, ReferencedEvent, ReferencedInput, Resolution, RollbackEvent, ScriptLanguage,
    StakeCred, TickEvent, TxContextEvent, TxEventBatch, TxRecord, TypedDatum, TypedOutput,
    TypedScript, UtxoEvent, UtxoPattern, UtxoPredicate, ValidityInterval,
};

pub use impls::{LocalDataPlane, extract_aux_cbor, project_typed_output};

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

    /// Return every currently-unspent output that holds at
    /// least one unit of a given policy id (28-byte hash).
    /// Symmetric primitive to `utxos_by_address` for the
    /// policy-keyed case. Dolos maintains a first-class
    /// `BY_POLICY` index (multimap in redb / prefix-keyed in
    /// fjall), so this is O(log N + M) where M is the number
    /// of UTxOs currently holding the policy — sub-second
    /// even for very active policies.
    ///
    /// Use cases: cold-start hydration of per-policy holder
    /// ledgers (`holder-distribution` module), NFT-collection
    /// scanners, vesting-contract indexers — anything that
    /// wants the current "all UTxOs of asset X" set without
    /// writing its own event-replay loop.
    ///
    /// Result is hard-capped at 100K refs.
    async fn utxos_by_policy(&self, policy: &[u8]) -> DataPlaneResult<Vec<OutputRef>>;

    /// Enumerate the current unspent set whose address shares a
    /// given payment credential (28-byte payment-key or
    /// payment-script hash), regardless of staking part.
    ///
    /// Use cases:
    /// - cold-start hydration for contract sweeps where the
    ///   payment part is the script identity but the staking
    ///   part varies per UTxO (CrowdLock vests — same contract,
    ///   different derived staking creds per lock)
    /// - resolving a wallet's stake credential from a payment
    ///   key hash discovered in a datum (find any UTxO using
    ///   the PKH; parse its address; read the stake part).
    ///   `resolve_stake_for_payment_pkh` below wraps this case.
    ///
    /// Result is hard-capped at 100K refs.
    async fn utxos_by_payment_cred(&self, cred: &[u8]) -> DataPlaneResult<Vec<OutputRef>>;

    /// Convenience over `utxos_by_payment_cred` for the common
    /// PKH→stake resolution path. Looks up any current UTxO
    /// whose address uses the given 28-byte payment credential,
    /// parses the address, returns the stake part.
    ///
    /// Returns `None` when:
    /// - the PKH has no current UTxOs
    /// - all UTxOs using the PKH are at enterprise (no-stake)
    ///   addresses (rare)
    ///
    /// Default impl walks `utxos_by_payment_cred` + reads each
    /// output until a stake part is found. Impls with faster
    /// paths (single-row index lookup, cached resolution table)
    /// can override.
    async fn resolve_stake_for_payment_pkh(
        &self,
        pkh: &[u8],
    ) -> DataPlaneResult<Option<StakeCred>> {
        let refs = self.utxos_by_payment_cred(pkh).await?;
        for r in &refs {
            let Some(out) = self.read_utxo(r, DecodeLevel::Lean).await? else {
                continue;
            };
            let Ok(addr) = pallas_addresses::Address::from_bech32(&out.address) else {
                continue;
            };
            let pallas_addresses::Address::Shelley(s) = addr else {
                continue;
            };
            use pallas_addresses::ShelleyDelegationPart;
            match s.delegation() {
                ShelleyDelegationPart::Key(h) => {
                    let mut bytes = [0u8; 28];
                    bytes.copy_from_slice(h.as_ref());
                    return Ok(Some(StakeCred::KeyHash(bytes)));
                }
                ShelleyDelegationPart::Script(h) => {
                    let mut bytes = [0u8; 28];
                    bytes.copy_from_slice(h.as_ref());
                    return Ok(Some(StakeCred::ScriptHash(bytes)));
                }
                ShelleyDelegationPart::Null | ShelleyDelegationPart::Pointer(_) => continue,
            }
        }
        Ok(None)
    }

    /// Bulk datum lookup for a list of output refs. Returns a
    /// parallel `Vec<Option<TypedDatum>>` where the i'th element
    /// is the datum on `refs[i]`, resolved caller-blind (inline
    /// or hash-via-state). `None` for outputs with no datum on
    /// chain or refs that don't exist in the current UTxO set.
    ///
    /// Default impl walks `read_utxo` per ref; impls with bulk
    /// shortcuts (e.g. batched state reads) can override.
    async fn read_output_datums(
        &self,
        refs: &[OutputRef],
    ) -> DataPlaneResult<Vec<Option<TypedDatum>>> {
        let mut out = Vec::with_capacity(refs.len());
        for r in refs {
            let resolved = self.read_utxo(r, DecodeLevel::WithDatum).await?;
            out.push(resolved.and_then(|t| t.datum));
        }
        Ok(out)
    }

    /// Return just the datum *hashes* per ref — `None` when the
    /// output has no datum on chain. Decoupled from byte
    /// resolution so consumers can still get the hash even when
    /// the plane couldn't resolve the payload (typical: hash-
    /// attached datums whose bytes only live in TX metadata,
    /// outside the witness-set index).
    async fn output_datum_hashes(
        &self,
        refs: &[OutputRef],
    ) -> DataPlaneResult<Vec<Option<pallas_primitives::Hash<32>>>> {
        let mut out = Vec::with_capacity(refs.len());
        for r in refs {
            let resolved = self.read_utxo(r, DecodeLevel::WithDatum).await?;
            out.push(resolved.and_then(|t| t.datum.map(|d| d.hash)));
        }
        Ok(out)
    }

    /// Auxiliary-data CBOR for a transaction by hash, looked up
    /// via the archive's tx index. Used by app-aware modules
    /// that decode datums (or other data) from TX metadata
    /// rather than witness sets — e.g. jpg.store's collection-
    /// offer labels-50+ convention.
    ///
    /// `None` when the tx isn't known to the archive or has no
    /// auxiliary data attached.
    async fn tx_metadata(
        &self,
        tx_hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<Vec<u8>>>;

    /// Full TX rollup: inputs (with redeemers), reference
    /// inputs, outputs, mint, required signers, validity
    /// interval, aux-data CBOR. Backed by the archive's
    /// `slot_by_tx_hash` + `get_block_by_slot` lookups; the
    /// implementation projects the matching tx out of the
    /// decoded block.
    ///
    /// Prior outputs for consumed/reference inputs are
    /// best-effort via current-state lookups — `None` for
    /// already-spent inputs (the typical case for any TX
    /// older than the one being processed). Modules that
    /// need prior outputs for spent inputs should process
    /// the relevant TX in real-time via
    /// `consumed-event.prior-output` rather than retrospectively
    /// via `read-tx`.
    ///
    /// `None` when the tx isn't known to the archive.
    ///
    /// Default impl returns `None` so `LocalDataPlane` (and
    /// any future archive-bearing plane) can override; non-
    /// archive transports return `None`.
    async fn read_tx(
        &self,
        _tx_hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<TxRecord>> {
        Ok(None)
    }

    /// Raw block CBOR for a given slot. Used by admin tooling
    /// to produce fixtures from real chain state without going
    /// through the offline `capture-block` path (which needs
    /// the writer stopped). The block bytes are the exact CBOR
    /// the consensus follower produced — `mitos-run --block`
    /// consumes them unchanged.
    ///
    /// Default impl returns `None` so only archive-bearing
    /// planes (LocalDataPlane today) need to override; transport
    /// proxies and test stubs return `None`.
    async fn read_block(&self, _slot: u64) -> DataPlaneResult<Option<Vec<u8>>> {
        Ok(None)
    }

    /// Resolve a tx hash to its containing slot via the archive's
    /// tx index. Companion to `read_block` for admin / fixture
    /// tooling that has a tx hash (from cardanoscan, Maestro,
    /// etc.) and wants the block CBOR.
    ///
    /// Default impl returns `None`; archive-bearing planes
    /// override.
    async fn slot_by_tx_hash(
        &self,
        _tx_hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<u64>> {
        Ok(None)
    }

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

    /// Per-asset mint/burn provenance from dolos's `AssetState`
    /// ledger: the mint TX (`initial_tx`), the metadata-bearing TX
    /// (`metadata_tx`), the mint count, and net supply. This is
    /// *state*, retained across the archive horizon — so `initial_tx`
    /// resolves the CIP-25 label-721 mint TX even when its block body
    /// has been pruned. `policy` is the 28-byte policy id; `asset_name`
    /// is the raw on-chain asset name (no CIP-67 prefix handling).
    /// Default returns `None` (planes with no dolos state store).
    async fn asset_state(
        &self,
        _policy: &[u8],
        _asset_name: &[u8],
    ) -> DataPlaneResult<Option<types::AssetMintState>> {
        Ok(None)
    }

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

    /// Oldest slot whose block body is still retained by the archive —
    /// the "archive horizon". Inputs/datums older than this can't be
    /// resolved from the archive: e.g. hash-only CIP-68 reference
    /// datums below the horizon resolve to nothing, yielding empty
    /// trait metadata. `None` when unknown, the archive is empty, or
    /// the plane has no dolos archive in scope (the default).
    async fn archive_horizon_slot(&self) -> DataPlaneResult<Option<u64>> {
        Ok(None)
    }
}
