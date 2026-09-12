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
//! Selection is by `MITOS_FALLBACK_PROVIDER` (`koios` default,
//! `maestro`, or `none`); see [`shared`]. The batch methods are the
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

/// Which fallback source a deployment has selected.
///
/// A named decision rather than a bare string compared at the use
/// site: the parse happens once, every arm is explicit, and the one
/// that matters most — what *absence* of configuration means — has a
/// name and a place to hang the reasoning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provider {
    Koios,
    /// Rollback path only. The Maestro Developer API shuts down
    /// 2026-09-18; after that this arm cannot resolve anything.
    Maestro,
    /// Fallback disabled — the planes pass through.
    None,
}

impl Provider {
    /// What an unset or unrecognised `MITOS_FALLBACK_PROVIDER` means.
    ///
    /// **Koios, deliberately.** Maestro shuts down 2026-09-18, so any
    /// path that silently lands on Maestro is a path that silently
    /// stops resolving — and an absent env var fails at *runtime*, on
    /// a cache miss, not at startup where it would be noticed. A
    /// rebuilt box, a dropped env line or a typo must degrade to
    /// "works", not to "quietly resolves nothing". Koios also builds a
    /// working keyless client, so this default holds with no secrets
    /// present at all.
    ///
    /// **Consequence worth knowing:** that last property cuts both
    /// ways. Before this was the default, an environment with no
    /// `MAESTRO_API_KEY` — a test run, CI, a bare `cargo run` — got
    /// `None` and made no network calls at all. That was accidental
    /// (prod always had a key), but it was load-bearing by habit. An
    /// unconfigured environment now gets a *live* Koios client, so any
    /// context that must not talk to the network has to say
    /// [`Provider::None`] explicitly rather than rely on silence.
    pub const DEFAULT: Provider = Provider::Koios;

    /// Parse a raw `MITOS_FALLBACK_PROVIDER` value; `None` = unset.
    ///
    /// Anything unrecognised resolves to [`Self::DEFAULT`] — not an
    /// error, and deliberately not Maestro. A misspelled provider must
    /// not be able to select a dead API, and must not disable
    /// resolution either; only an explicit `none` does that.
    pub fn from_env_value(raw: Option<&str>) -> Provider {
        let Some(raw) = raw else {
            return Provider::DEFAULT;
        };
        match raw.trim().to_ascii_lowercase().as_str() {
            "" => Provider::DEFAULT,
            "koios" => Provider::Koios,
            "maestro" => Provider::Maestro,
            "none" => Provider::None,
            other => {
                tracing::warn!(
                    provider = %other,
                    default = ?Provider::DEFAULT,
                    "unknown MITOS_FALLBACK_PROVIDER; using the default",
                );
                Provider::DEFAULT
            }
        }
    }
}

/// Return the configured fallback provider, or `None` when fallback
/// is disabled / unconfigured. Process-wide — each provider lazy-
/// inits its own `shared()` singleton, so the connection pool and
/// rate-limit semaphore stay global.
///
/// `MITOS_FALLBACK_PROVIDER`, parsed by [`Provider::from_env_value`]:
/// - unset / unrecognised → [`Provider::DEFAULT`] (Koios).
/// - `koios` → Koios ([`crate::koios::KoiosProvider`]). Builds a
///   working client even without `KOIOS_API_KEY` (free public tier),
///   so this is `Some` unless the HTTP client fails to build.
/// - `maestro` → Maestro (`MAESTRO_API_KEY`); `None` if no key.
///   Rollback path only — see [`Provider::Maestro`].
/// - `none` → fallback disabled (planes pass through).
pub fn shared() -> Option<Arc<dyn FallbackProvider>> {
    let raw = std::env::var("MITOS_FALLBACK_PROVIDER").ok();
    match Provider::from_env_value(raw.as_deref()) {
        Provider::Koios => {
            crate::koios::KoiosProvider::shared().map(|c| c as Arc<dyn FallbackProvider>)
        }
        Provider::Maestro => {
            tracing::warn!(
                "MITOS_FALLBACK_PROVIDER=maestro — the Maestro Developer API shuts \
                 down 2026-09-18; this is a rollback path only",
            );
            maestro_provider()
        }
        Provider::None => {
            tracing::info!("fallback provider disabled (MITOS_FALLBACK_PROVIDER=none)");
            None
        }
    }
}

fn maestro_provider() -> Option<Arc<dyn FallbackProvider>> {
    crate::maestro::MaestroClient::shared().map(|c| c as Arc<dyn FallbackProvider>)
}

#[cfg(test)]
mod tests {
    use super::Provider;

    /// The whole point of the 2026-09-18 change: absence of config must
    /// not select the API that is going away.
    #[test]
    fn unset_selects_koios_not_maestro() {
        assert_eq!(Provider::from_env_value(None), Provider::Koios);
        assert_eq!(Provider::from_env_value(Some("")), Provider::Koios);
    }

    /// A typo must not be able to select a dead provider, and must not
    /// silently disable resolution either.
    #[test]
    fn unknown_values_fall_back_to_the_default() {
        for raw in ["maestroo", "MITOS_FALLBACK_PROVIDR", "kois", "yes", "0"] {
            assert_eq!(
                Provider::from_env_value(Some(raw)),
                Provider::DEFAULT,
                "{raw} should resolve to the default",
            );
        }
        assert_eq!(Provider::DEFAULT, Provider::Koios);
    }

    #[test]
    fn explicit_values_are_honoured() {
        assert_eq!(Provider::from_env_value(Some("koios")), Provider::Koios);
        assert_eq!(Provider::from_env_value(Some("maestro")), Provider::Maestro);
        assert_eq!(Provider::from_env_value(Some("none")), Provider::None);
    }

    /// Env files get hand-edited; stray case and whitespace are the
    /// most common way a correct intent reads as a typo.
    #[test]
    fn parsing_tolerates_case_and_whitespace() {
        assert_eq!(Provider::from_env_value(Some("  koios ")), Provider::Koios);
        assert_eq!(Provider::from_env_value(Some("KOIOS")), Provider::Koios);
        assert_eq!(Provider::from_env_value(Some("None")), Provider::None);
        assert_eq!(
            Provider::from_env_value(Some(" Maestro")),
            Provider::Maestro
        );
    }
}
