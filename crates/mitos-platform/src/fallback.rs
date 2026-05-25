//! Pluggable chain-data fallback provider.
//!
//! Generalises the concrete Maestro client into a trait so a
//! deployment can choose its fallback source — Maestro, Koios, or
//! none — for the two resolution tiers that reach past the dolos
//! archive horizon:
//! - aux-data (CIP-25 tx metadata) + hash-only datums, via
//!   [`crate::host_fns::CachingDataPlane`];
//! - archive-pruned prior outputs, via
//!   [`crate::maestro_fallback_plane::MaestroFallbackPlane`].
//!
//! Selection is by `MITOS_FALLBACK_PROVIDER` (`maestro` default,
//! `koios`, or `none`); see [`shared`]. The batch methods are the
//! enabler for the CIP-25 cold-start prefetch — a provider with a
//! native batch endpoint (Koios `/tx_metadata`) overrides them with
//! one HTTP call; the default impl fans out over the single methods
//! with bounded concurrency, so even Maestro gets parallelism.
//!
//! Design: `docs/design/FALLBACK_PROVIDER_AND_BATCH_PREFETCH.md`.

use std::collections::HashMap;
use std::sync::Arc;

use futures_util::stream::{self, StreamExt};
use mitos_data_plane::{DecodeLevel, OutputRef, TypedOutput};

/// Bounded concurrency for the default batch fan-out (providers
/// without a native batch endpoint). Caps in-flight requests
/// regardless of the provider's own limiter; the provider's
/// semaphore (if any) still applies on top.
const DEFAULT_BATCH_CONCURRENCY: usize = 8;

/// Provider-agnostic fallback error. Call sites only surface this
/// via `Display` (logged best-effort), so a flat message suffices;
/// each concrete provider keeps its own typed errors internally.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct FallbackError(pub String);

/// A pluggable source for chain data that dolos can't serve past its
/// archive horizon.
#[async_trait::async_trait]
pub trait FallbackProvider: Send + Sync {
    /// Auxiliary-data (tx metadata) CBOR for a tx by hex hash.
    /// `Ok(None)` = the provider has the tx but it carries no
    /// aux-data, or doesn't have the tx.
    async fn fetch_aux_data(&self, tx_hash_hex: &str) -> Result<Option<Vec<u8>>, FallbackError>;

    /// A single prior output by reference, at the given decode level.
    async fn fetch_output(
        &self,
        oref: &OutputRef,
        level: DecodeLevel,
    ) -> Result<Option<TypedOutput>, FallbackError>;

    /// A datum's raw CBOR by its hex hash.
    async fn fetch_datum(&self, datum_hash_hex: &str) -> Result<Option<Vec<u8>>, FallbackError>;

    /// Batch aux-data resolution. Returns only the txs that resolved
    /// (key = tx hex). **Best-effort**: per-tx errors and misses are
    /// dropped, since the caller (a cache prefetch) re-resolves any
    /// gap individually on demand. Default impl fans out over
    /// [`Self::fetch_aux_data`] with bounded concurrency; a provider
    /// with a native batch endpoint should override.
    async fn fetch_aux_data_batch(&self, tx_hash_hexes: &[String]) -> HashMap<String, Vec<u8>> {
        stream::iter(tx_hash_hexes.iter().cloned())
            .map(|tx| async move {
                match self.fetch_aux_data(&tx).await {
                    Ok(Some(bytes)) => Some((tx, bytes)),
                    _ => None,
                }
            })
            .buffer_unordered(DEFAULT_BATCH_CONCURRENCY)
            .filter_map(|r| async move { r })
            .collect()
            .await
    }

    /// Batch prior-output resolution. Key = `(tx_hash, index)`.
    /// Best-effort, same contract as [`Self::fetch_aux_data_batch`].
    async fn fetch_outputs_batch(
        &self,
        orefs: &[OutputRef],
        level: DecodeLevel,
    ) -> HashMap<(pallas_primitives::Hash<32>, u32), TypedOutput> {
        stream::iter(orefs.iter().cloned())
            .map(|oref| async move {
                match self.fetch_output(&oref, level).await {
                    Ok(Some(out)) => Some(((oref.tx_hash, oref.index), out)),
                    _ => None,
                }
            })
            .buffer_unordered(DEFAULT_BATCH_CONCURRENCY)
            .filter_map(|r| async move { r })
            .collect()
            .await
    }

    /// Batch datum resolution. Key = datum hex. Best-effort.
    async fn fetch_datums_batch(&self, hashes: &[String]) -> HashMap<String, Vec<u8>> {
        stream::iter(hashes.iter().cloned())
            .map(|h| async move {
                match self.fetch_datum(&h).await {
                    Ok(Some(bytes)) => Some((h, bytes)),
                    _ => None,
                }
            })
            .buffer_unordered(DEFAULT_BATCH_CONCURRENCY)
            .filter_map(|r| async move { r })
            .collect()
            .await
    }
}

/// Return the configured fallback provider, or `None` when fallback
/// is disabled / unconfigured. Process-wide — each provider lazy-
/// inits its own `shared()` singleton, so the connection pool and
/// rate-limit semaphore stay global.
///
/// `MITOS_FALLBACK_PROVIDER`:
/// - unset / `maestro` (default) → Maestro (`MAESTRO_API_KEY`);
///   `None` if no key, exactly as before this abstraction.
/// - `none` → fallback disabled (planes pass through).
/// - `koios` → Koios ([`crate::koios::KoiosProvider`]). Builds a
///   working client even without `KOIOS_API_KEY` (free public tier),
///   so this is `Some` unless the HTTP client fails to build.
pub fn shared() -> Option<Arc<dyn FallbackProvider>> {
    let selected =
        std::env::var("MITOS_FALLBACK_PROVIDER").unwrap_or_else(|_| "maestro".to_owned());
    match selected.as_str() {
        "none" => {
            tracing::info!("fallback provider disabled (MITOS_FALLBACK_PROVIDER=none)");
            None
        }
        "koios" => {
            // Koios builds a working client even without an API key
            // (free public tier), so this is `Some` unless the HTTP
            // client itself fails to build.
            crate::koios::KoiosProvider::shared().map(|c| c as Arc<dyn FallbackProvider>)
        }
        other => {
            if other != "maestro" {
                tracing::warn!(provider = %other, "unknown MITOS_FALLBACK_PROVIDER; using Maestro");
            }
            maestro_provider()
        }
    }
}

fn maestro_provider() -> Option<Arc<dyn FallbackProvider>> {
    crate::maestro::MaestroClient::shared().map(|c| c as Arc<dyn FallbackProvider>)
}
