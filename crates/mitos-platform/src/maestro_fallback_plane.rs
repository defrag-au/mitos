//! `ChainDataPlane` wrapper that fills `read_utxos` misses via
//! the Maestro REST API.
//!
//! Wraps the local plane so that prior-output lookups for inputs
//! older than the 7-day dolos archive horizon still resolve.
//! Without this, `mitos_data_plane::dispatch::build_event_batches`
//! falls back to `TypedOutput::unresolved()` for those inputs,
//! `InterestSet::matches_output` doesn't match (no address), and
//! `Consumed` events for archive-pruned create-TXs get silently
//! dropped. The user-visible symptom is a Cancel/Accept TX where
//! mitos sees and emits events for the recently-created offers
//! but not the old ones in the same TX (the "1/2 resolved" cart
//! state that surfaced this).
//!
//! Tier ordering, in `read_utxos`:
//!
//! 1. Inner plane's `read_utxos` — current UTxO state, then
//!    dolos archive per miss (7-day window).
//! 2. For any ref the inner plane didn't return, ask Maestro's
//!    `GET /transactions/{tx_hash}/outputs/{index}/txo` endpoint.
//!    Each gap is one HTTP call; the shared rate-limiting
//!    semaphore inside `MaestroClient` keeps total Maestro
//!    pressure in check across all modules.
//!
//! When `MaestroClient::shared()` returns `None` (no API key in
//! environment), this wrapper is a no-op pass-through. Same
//! behaviour as the existing platform when Maestro isn't
//! configured.
//!
//! Performance: at the tip, archive-horizon misses are rare
//! (only Cancel/Accept TXs against >7-day-old UTxOs), so the
//! Maestro call rate stays low. The semaphore (default 4)
//! still bounds parallelism if a recapture walks many old TXs
//! at once.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use async_trait::async_trait;
use cardano_assets::PolicyId;
use mitos_data_plane::{
    ChainDataPlane, ChainTip, DataPlaneResult, DecodeLevel, OutputRef, Page, PageRequest,
    TypedDatum, TypedOutput, TypedScript, UtxoPredicate,
    types::{ProtocolParameters, StakeCred, TxRecord},
};

use crate::fallback::FallbackProvider;
use crate::watched_ref_index::WatchedRefIndex;

/// How the watched-UTxO index scopes the Maestro prior-output
/// fallback. Set process-wide via `MITOS_FALLBACK_GATE`. See
/// `docs/design/WATCHED_UTXO_INDEX.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateMode {
    /// Index ignored — resolve every archive-horizon gap via Maestro,
    /// exactly as before the index existed. Default.
    Off,
    /// Resolve every gap (no behaviour change) but record whether the
    /// gate *would* have skipped it — the measurement + safety pass
    /// before flipping the gate on.
    Shadow,
    /// Only resolve gaps whose ref is in the watched index; skip the
    /// rest (provably non-matching by the symmetry argument).
    On,
}

impl GateMode {
    /// Parse `MITOS_FALLBACK_GATE` once. Unset / unrecognised → `Off`.
    pub fn from_env() -> Self {
        static MODE: OnceLock<GateMode> = OnceLock::new();
        *MODE.get_or_init(|| {
            match std::env::var("MITOS_FALLBACK_GATE")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str()
            {
                "shadow" => GateMode::Shadow,
                "on" => GateMode::On,
                _ => GateMode::Off,
            }
        })
    }
}

/// Process-wide gate counters. Greppable summary lines stand in for a
/// metrics exporter (none wired yet — see `infra/docs/mitos-operations.md`).
static WOULD_RESOLVE: AtomicU64 = AtomicU64::new(0); // gap ref IS watched
static WOULD_SKIP: AtomicU64 = AtomicU64::new(0); // gap ref is NOT watched

/// Record one gate decision and periodically log a rolling summary
/// (every 1000 decisions ≈ tens of lines/day at the observed rate).
fn record_gate_decision(in_scope: bool, mode: GateMode) {
    let resolve = if in_scope {
        WOULD_RESOLVE.fetch_add(1, Ordering::Relaxed) + 1
    } else {
        WOULD_RESOLVE.load(Ordering::Relaxed)
    };
    let skip = if in_scope {
        WOULD_SKIP.load(Ordering::Relaxed)
    } else {
        WOULD_SKIP.fetch_add(1, Ordering::Relaxed) + 1
    };
    let total = resolve + skip;
    if total.is_multiple_of(1000) {
        let pct_skip = skip.saturating_mul(100).checked_div(total).unwrap_or(0);
        tracing::info!(
            target: "fallback_gate",
            mode = ?mode,
            would_resolve = resolve,
            would_skip = skip,
            pct_skippable = pct_skip,
            "watched-index gate rolling summary",
        );
    }
}

/// Cache key for Maestro-resolved outputs. Different `DecodeLevel`s
/// populate different fields (e.g. datum payload only at
/// `WithDatum`+), so they're separate cache entries.
type CacheKey = (pallas_primitives::Hash<32>, u32, DecodeLevel);

/// Process-wide dedup cache for Maestro fallback lookups.
///
/// Each `ModuleHostV2::start()` builds its own `MaestroFallbackPlane`
/// wrapping the same shared inner plane. That means 13 modules
/// applying the same block trigger 13 independent `read_utxos`
/// calls for the same archive-pruned ref — without dedup we'd
/// pay Maestro 13× per miss. This cache lifts the lookup result
/// out of any individual wrapper so the second-through-Nth
/// module hit the cache instead.
///
/// UTxOs are immutable on chain, so cached entries never need
/// invalidation. Memory growth is bounded by the number of
/// distinct archive-pruned refs the process sees over its
/// lifetime — empirically tens to low-hundreds per day on the
/// mainnet bundle, so capped capacity isn't required.
static OUTPUT_CACHE: OnceLock<RwLock<HashMap<CacheKey, TypedOutput>>> = OnceLock::new();

fn output_cache() -> &'static RwLock<HashMap<CacheKey, TypedOutput>> {
    OUTPUT_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Take from cache if present (clone — `TypedOutput::Clone` is
/// shallow per `Vec`-sized fields).
fn cache_get(key: &CacheKey) -> Option<TypedOutput> {
    output_cache().read().ok()?.get(key).cloned()
}

/// Insert into the cache. Quietly drops on lock poisoning so a
/// poisoned lock doesn't take resolution down — worst case is
/// we re-fetch on the next call.
fn cache_put(key: CacheKey, value: TypedOutput) {
    if let Ok(mut guard) = output_cache().write() {
        guard.insert(key, value);
    }
}

/// Inflight tracking for single-flight semantics. Without this,
/// 13 modules all applying the same tip block trigger 13
/// concurrent Maestro fetches for the same ref before any of
/// them populates the cache — thundering herd. The `OnceCell`
/// per key lets the first caller do the actual fetch while the
/// rest await its result.
///
/// Entries are removed after the first resolution; the
/// permanent dedup story is `OUTPUT_CACHE`. This map only
/// exists to coalesce concurrent in-flight requests.
type InflightMap = HashMap<CacheKey, Arc<tokio::sync::OnceCell<Option<TypedOutput>>>>;
static INFLIGHT: OnceLock<std::sync::Mutex<InflightMap>> = OnceLock::new();

fn inflight() -> &'static std::sync::Mutex<InflightMap> {
    INFLIGHT.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Coalesce concurrent fetches for the same key into a single
/// Maestro call. Cache check happens first; if miss, claim or
/// join the inflight cell. Leader populates the cell + the
/// shared cache; followers await the leader's result.
///
/// Returns `None` when Maestro returned 404 / errored / isn't
/// configured. Errors are logged inside the leader.
async fn fetch_dedup(
    provider: &dyn FallbackProvider,
    oref: &OutputRef,
    decode: DecodeLevel,
) -> Option<TypedOutput> {
    let key: CacheKey = (oref.tx_hash, oref.index, decode);

    if let Some(cached) = cache_get(&key) {
        return Some(cached);
    }

    // Claim or join the inflight slot. The short mutex hold is
    // safe — no awaits while holding it.
    let cell: Arc<tokio::sync::OnceCell<Option<TypedOutput>>> = {
        let mut map = inflight().lock().expect("inflight mutex");
        map.entry(key)
            .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
            .clone()
    };

    // `get_or_init` ensures exactly one task runs the init body
    // per cell. Followers await the leader's completion and see
    // its result.
    let result = cell
        .get_or_init(|| async {
            match provider.fetch_output(oref, decode).await {
                Ok(Some(typed)) => {
                    tracing::debug!(
                        tx_hash = %hex::encode(oref.tx_hash),
                        index = oref.index,
                        "prior output resolved via fallback provider"
                    );
                    cache_put(key, typed.clone());
                    Some(typed)
                }
                Ok(None) => {
                    tracing::debug!(
                        tx_hash = %hex::encode(oref.tx_hash),
                        index = oref.index,
                        "fallback provider: utxo not found"
                    );
                    None
                }
                Err(e) => {
                    tracing::warn!(
                        tx_hash = %hex::encode(oref.tx_hash),
                        index = oref.index,
                        error = %e,
                        "fallback provider: utxo lookup failed"
                    );
                    None
                }
            }
        })
        .await
        .clone();

    // Best-effort cleanup so the inflight map doesn't grow
    // unboundedly. Followers may race past the leader's remove
    // — `HashMap::remove` is idempotent.
    if let Ok(mut map) = inflight().lock() {
        map.remove(&key);
    }

    result
}

/// `ChainDataPlane` impl that augments an inner plane's
/// `read_utxos` with a Maestro fallback for the gaps. Every
/// other method passes through unchanged.
pub struct MaestroFallbackPlane<P> {
    inner: Arc<P>,
    fallback: Option<Arc<dyn FallbackProvider>>,
    /// Watched-UTxO index for this module — the gate's membership
    /// test. `None` collapses the gate to fail-open (resolve all),
    /// matching `GateMode::Off`.
    watched: Option<Arc<WatchedRefIndex>>,
    gate: GateMode,
}

impl<P> MaestroFallbackPlane<P> {
    /// `fallback = None` collapses this wrapper to a no-op
    /// pass-through. We still construct + use the wrapper so the
    /// follower path is uniform regardless of fallback-provider
    /// configuration.
    ///
    /// `watched` + `gate` drive interest-scoping of the prior-output
    /// fallback (`docs/design/WATCHED_UTXO_INDEX.md`). Pass `gate =
    /// GateMode::Off` (and/or `watched = None`) for the pre-index
    /// behaviour.
    pub fn new(
        inner: Arc<P>,
        fallback: Option<Arc<dyn FallbackProvider>>,
        watched: Option<Arc<WatchedRefIndex>>,
        gate: GateMode,
    ) -> Self {
        Self {
            inner,
            fallback,
            watched,
            gate,
        }
    }

    /// Gate decision for one archive-horizon gap: returns `true` if
    /// Maestro should be asked for this ref. In `Shadow` it always
    /// returns `true` (resolve everything) but records what `On`
    /// *would* have done; in `On` it returns index membership.
    fn gate_allows(&self, oref: &OutputRef) -> bool {
        gate_decision(self.gate, self.watched.as_deref(), oref)
    }
}

/// Pure gate decision — extracted so the policy is unit-testable
/// without a full `ChainDataPlane`. `Off` short-circuits (no
/// counting); `Shadow` always resolves but records what `On` would
/// do; `On` resolves only in-scope refs. A missing index under an
/// active gate fails open — never silently drop resolution on a
/// wiring gap.
fn gate_decision(gate: GateMode, watched: Option<&WatchedRefIndex>, oref: &OutputRef) -> bool {
    if gate == GateMode::Off {
        return true;
    }
    let in_scope = watched.is_none_or(|w| w.contains(oref));
    record_gate_decision(in_scope, gate);
    match gate {
        GateMode::Off | GateMode::Shadow => true,
        GateMode::On => in_scope,
    }
}

#[async_trait]
impl<P: ChainDataPlane + Send + Sync + 'static> ChainDataPlane for MaestroFallbackPlane<P> {
    async fn read_utxo(
        &self,
        oref: &OutputRef,
        decode: DecodeLevel,
    ) -> DataPlaneResult<Option<TypedOutput>> {
        if let Some(out) = self.inner.read_utxo(oref, decode).await? {
            return Ok(Some(out));
        }
        let Some(provider) = &self.fallback else {
            return Ok(None);
        };
        if !self.gate_allows(oref) {
            return Ok(None);
        }
        Ok(fetch_dedup(provider.as_ref(), oref, decode).await)
    }

    async fn read_utxos(
        &self,
        orefs: &[OutputRef],
        decode: DecodeLevel,
    ) -> DataPlaneResult<Vec<(OutputRef, TypedOutput)>> {
        let mut out = self.inner.read_utxos(orefs, decode).await?;
        let Some(provider) = &self.fallback else {
            return Ok(out);
        };

        // Build the "already resolved" set so we only burn
        // Maestro budget on actual gaps.
        let found: std::collections::HashSet<(pallas_primitives::Hash<32>, u32)> =
            out.iter().map(|(o, _)| (o.tx_hash, o.index)).collect();

        for oref in orefs {
            if found.contains(&(oref.tx_hash, oref.index)) {
                continue;
            }
            if !self.gate_allows(oref) {
                continue;
            }
            if let Some(typed) = fetch_dedup(provider.as_ref(), oref, decode).await {
                out.push((*oref, typed));
            }
        }
        Ok(out)
    }

    async fn search_utxos(
        &self,
        predicate: &UtxoPredicate,
        decode: DecodeLevel,
        page: PageRequest,
    ) -> DataPlaneResult<Page<(OutputRef, TypedOutput)>> {
        self.inner.search_utxos(predicate, decode, page).await
    }

    async fn utxos_by_address(&self, address: &str) -> DataPlaneResult<Vec<OutputRef>> {
        self.inner.utxos_by_address(address).await
    }

    async fn utxos_by_policy(&self, policy: &[u8]) -> DataPlaneResult<Vec<OutputRef>> {
        self.inner.utxos_by_policy(policy).await
    }

    async fn utxos_by_payment_cred(&self, cred: &[u8]) -> DataPlaneResult<Vec<OutputRef>> {
        self.inner.utxos_by_payment_cred(cred).await
    }

    async fn resolve_stake_for_payment_pkh(
        &self,
        pkh: &[u8],
    ) -> DataPlaneResult<Option<StakeCred>> {
        self.inner.resolve_stake_for_payment_pkh(pkh).await
    }

    async fn read_output_datums(
        &self,
        refs: &[OutputRef],
    ) -> DataPlaneResult<Vec<Option<TypedDatum>>> {
        self.inner.read_output_datums(refs).await
    }

    async fn output_datum_hashes(
        &self,
        refs: &[OutputRef],
    ) -> DataPlaneResult<Vec<Option<pallas_primitives::Hash<32>>>> {
        self.inner.output_datum_hashes(refs).await
    }

    async fn tx_metadata(
        &self,
        tx_hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<Vec<u8>>> {
        self.inner.tx_metadata(tx_hash).await
    }

    async fn read_tx(
        &self,
        tx_hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<TxRecord>> {
        self.inner.read_tx(tx_hash).await
    }

    async fn read_block(&self, slot: u64) -> DataPlaneResult<Option<Vec<u8>>> {
        self.inner.read_block(slot).await
    }

    async fn slot_by_tx_hash(
        &self,
        tx_hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<u64>> {
        self.inner.slot_by_tx_hash(tx_hash).await
    }

    async fn read_datum(
        &self,
        hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<TypedDatum>> {
        self.inner.read_datum(hash).await
    }

    async fn read_script(
        &self,
        hash: &pallas_primitives::Hash<28>,
    ) -> DataPlaneResult<Option<TypedScript>> {
        self.inner.read_script(hash).await
    }

    async fn total_supply(
        &self,
        policy: &PolicyId,
        asset_name_hex: Option<&str>,
    ) -> DataPlaneResult<u64> {
        self.inner.total_supply(policy, asset_name_hex).await
    }

    async fn asset_state(
        &self,
        policy: &[u8],
        asset_name: &[u8],
    ) -> DataPlaneResult<Option<mitos_data_plane::AssetMintState>> {
        self.inner.asset_state(policy, asset_name).await
    }

    async fn holder_count(&self, policy: &PolicyId) -> DataPlaneResult<u64> {
        self.inner.holder_count(policy).await
    }

    async fn tip(&self) -> DataPlaneResult<ChainTip> {
        self.inner.tip().await
    }

    async fn protocol_params(&self) -> DataPlaneResult<ProtocolParameters> {
        self.inner.protocol_params().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::watched_ref_index::WatchedRefIndex;
    use pallas_primitives::Hash;

    fn oref(byte: u8) -> OutputRef {
        OutputRef::new(Hash::new([byte; 32]), 0)
    }

    #[test]
    fn gate_on_resolves_only_indexed_refs() {
        let idx = WatchedRefIndex::in_memory();
        let watched = oref(0x11);
        let other = oref(0x22);
        idx.seed([watched]);

        // On: indexed → resolve; un-indexed → skip.
        assert!(gate_decision(GateMode::On, Some(&idx), &watched));
        assert!(!gate_decision(GateMode::On, Some(&idx), &other));

        // Shadow + Off: always resolve regardless of membership.
        assert!(gate_decision(GateMode::Shadow, Some(&idx), &other));
        assert!(gate_decision(GateMode::Off, Some(&idx), &other));

        // Active gate but no index wired → fail open (resolve).
        assert!(gate_decision(GateMode::On, None, &other));
    }
}
