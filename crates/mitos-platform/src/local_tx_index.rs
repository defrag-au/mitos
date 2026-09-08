//! Local tx-index tier — auxiliary data served by mmap from the Mithril
//! chunk store instead of a remote provider.
//!
//! ## Why this tier exists
//!
//! jpg.store commits listing and offer datums by HASH and publishes the
//! preimage in the transaction's own metadata. Decoding the residual jpg ask
//! book therefore means one aux-data lookup per listing. On the live path that
//! is free — the metadata rides in the block being processed — but a BOOTSTRAP
//! re-walk is resolving transactions that are years old, so every one of them
//! went out to the configured fallback provider. Measured on mainnet: ~4
//! lookups/second, about 14 hours for the ~219k-listing book, and a restart
//! part-way through started it over.
//!
//! `tx-index` already maps tx hash → (chunk, offset) over the same immutable
//! chunk store, at a p50 of well under a millisecond. Teaching it to index
//! auxiliary data alongside tx bodies turns those hours into a local `pread`.
//!
//! ## Why it is a tier and not a `FallbackProvider`
//!
//! [`crate::fallback::FallbackProvider::fetch_aux_data`] returns
//! `Option<Vec<u8>>`, and its contract deliberately collapses two different
//! answers: "this tx has no metadata" and "I don't have this tx". A remote
//! provider can afford that — it is the last tier, so both mean give up. A
//! LOCAL tier cannot: it has to say which, because "no metadata" is a final
//! answer while "not in a completed chunk" must fall through to the remote.
//! Get that wrong and every metadata-less transaction — the majority of them —
//! goes out to the network anyway, which is the entire cost being removed.
//!
//! [`tx_index::AuxLookup`] keeps the distinction, so this sits between the
//! dolos archive and the remote fallback inside
//! [`crate::host_fns::CachingDataPlane`] rather than behind the trait.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use tx_index::{AuxLookup, IndexHandle};

/// Index directory (`base.idx` + `segments/`).
pub const INDEX_DIR_ENV: &str = "MITOS_TX_INDEX_DIR";
/// The immutable chunk store the located spans are read from.
pub const IMMUTABLE_ENV: &str = "MITOS_TX_INDEX_IMMUTABLE";

/// How often a lookup may re-check the index directory for a compaction or a
/// freshly landed segment. The refresh job runs weekly, so this is about
/// noticing eventually, not promptly — and it is deliberately checked on the
/// lookup path so there is no background task to own or leak.
const RELOAD_INTERVAL_SECS: u64 = 60;

pub struct LocalTxIndex {
    handle: Arc<IndexHandle>,
    /// Unix seconds of the last `reload_if_changed` attempt.
    last_reload_check: AtomicU64,
}

impl LocalTxIndex {
    pub fn open(index_dir: &Path, immutable: &Path) -> Result<LocalTxIndex> {
        let handle = IndexHandle::open(index_dir, immutable).with_context(|| {
            format!(
                "opening tx-index at {} over {}",
                index_dir.display(),
                immutable.display()
            )
        })?;
        Ok(LocalTxIndex {
            handle: Arc::new(handle),
            last_reload_check: AtomicU64::new(now_secs()),
        })
    }

    /// Auxiliary data for `tx_hash`.
    ///
    /// An I/O or decode failure answers [`AuxLookup::UnknownTx`], not an error:
    /// this is an optimisation tier, and the honest meaning of a failure here
    /// is "this tier cannot say", which is exactly what makes the caller fall
    /// through to the remote provider. The failure is logged so a broken index
    /// shows up as noise rather than as silently slow lookups.
    pub async fn tx_metadata(&self, tx_hash: &[u8; 32]) -> AuxLookup {
        self.maybe_reload();
        let index = self.handle.get();
        let hash = *tx_hash;
        let result = tokio::task::spawn_blocking(move || index.tx_metadata(&hash)).await;
        match result {
            Ok(Ok(lookup)) => lookup,
            Ok(Err(e)) => {
                tracing::warn!(
                    tx = %hex::encode(tx_hash),
                    error = %format!("{e:#}"),
                    "local tx-index aux lookup failed; falling through",
                );
                AuxLookup::UnknownTx
            }
            Err(e) => {
                tracing::warn!(
                    tx = %hex::encode(tx_hash),
                    error = %e,
                    "local tx-index aux lookup task failed; falling through",
                );
                AuxLookup::UnknownTx
            }
        }
    }

    /// Re-map if the index moved on disk and enough time has passed since the
    /// last check. Cheap enough to sit on the lookup path: at most one
    /// directory stat a minute, and none at all in between.
    fn maybe_reload(&self) {
        let now = now_secs();
        let last = self.last_reload_check.load(Ordering::Relaxed);
        if now.saturating_sub(last) < RELOAD_INTERVAL_SECS {
            return;
        }
        // Whoever wins the swap does the check; the rest skip it. A lost race
        // just means the check happens a minute later.
        if self
            .last_reload_check
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        match self.handle.reload_if_changed() {
            Ok(true) => tracing::info!("local tx-index re-mapped after on-disk change"),
            Ok(false) => {}
            Err(e) => tracing::warn!(error = %format!("{e:#}"), "local tx-index reload failed"),
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

static SHARED: OnceLock<Option<Arc<LocalTxIndex>>> = OnceLock::new();

/// The process-wide local index, or `None` when it is not configured or could
/// not be opened.
///
/// Absence is never fatal — it only means aux-data lookups take the remote
/// path, which is what they did before this tier existed. Both env vars are
/// required together: an index directory without a chunk store to read located
/// spans from cannot answer anything, and silently half-configuring it would
/// look like the tier is on when it is not.
pub fn shared() -> Option<Arc<LocalTxIndex>> {
    SHARED
        .get_or_init(|| {
            let index_dir = std::env::var(INDEX_DIR_ENV).ok().filter(|s| !s.is_empty());
            let immutable = std::env::var(IMMUTABLE_ENV).ok().filter(|s| !s.is_empty());
            match (index_dir, immutable) {
                (Some(index_dir), Some(immutable)) => {
                    let (index_dir, immutable) = (PathBuf::from(index_dir), PathBuf::from(immutable));
                    match LocalTxIndex::open(&index_dir, &immutable) {
                        Ok(idx) => {
                            let cov = idx.handle.get().coverage();
                            tracing::info!(
                                index_dir = %index_dir.display(),
                                ?cov,
                                "local tx-index tier enabled for aux-data lookups",
                            );
                            Some(Arc::new(idx))
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %format!("{e:#}"),
                                "local tx-index configured but unusable; aux-data \
                                 lookups will use the remote fallback",
                            );
                            None
                        }
                    }
                }
                (None, None) => None,
                _ => {
                    tracing::warn!(
                        "local tx-index needs BOTH {INDEX_DIR_ENV} and {IMMUTABLE_ENV}; \
                         tier disabled",
                    );
                    None
                }
            }
        })
        .clone()
}
