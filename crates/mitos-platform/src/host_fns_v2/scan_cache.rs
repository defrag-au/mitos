//! Frozen, paged UTxO-scan cache for the v2 `chain-data`
//! host-fns. Phase 2 of `docs/design/WASM_BUDGET_CHUNKING.md`.
//!
//! dolos v1.0.3's `bypolicy` (and by-address / by-payment-cred)
//! indexes are dump-all only — there is no keyed range-scan. So
//! pagination happens at the **host-fn ↔ wasm boundary**, which
//! is mitos's to define: on the first call of a scan the host
//! materialises dolos's dump-all into native host memory, sorts
//! it into a stable order, freezes it under an opaque scan
//! token, and slices it by offset on each later call.
//!
//! Because the cache is *frozen* at scan-start, an offset cursor
//! is stable here — the "keyed, not offset" rule applies only to
//! scanning a live mutating index, which this deliberately is
//! not. The whole scan is consistent as-of one tip
//! (`anchor_slot`, captured once at scan-start).
//!
//! The wasm module only ever receives one page; it never sees,
//! never has to allocate, the full ref list. That is what
//! eliminates the input-memory `cabi_realloc` OOM.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use mitos_data_plane::OutputRef;

/// How long an idle materialised scan survives before eviction.
/// A recapture scans one predicate at a time and loops promptly,
/// so 5 minutes is generous headroom; the TTL only guards a scan
/// abandoned mid-stream (module crash, host shutdown).
const SCAN_TTL: Duration = Duration::from_secs(300);

/// Hard cap on a materialised scan's ref count. Preserves the
/// "Cap: 100K refs" contract the unbounded host-fns carried; the
/// native-memory cost at the cap is single-digit MB (~36 bytes ×
/// count), in the host heap, not wasm linear memory.
pub const MAX_SCAN_REFS: usize = 100_000;

/// One page of a paged scan — host-internal shape, converted to
/// the WIT `utxo-page` at the host-fn boundary.
pub struct ScanPage {
    pub refs: Vec<OutputRef>,
    /// Tip slot the scan was frozen as-of. Stable across pages.
    pub anchor_slot: u64,
    /// Opaque continuation token, or `None` on the last page.
    pub next: Option<Vec<u8>>,
}

/// A scan's frozen ref-set plus its as-of anchor.
struct MaterializedScan {
    /// Full ref-set, sorted into a stable order at scan-start.
    refs: Vec<OutputRef>,
    anchor_slot: u64,
    last_touched: Instant,
}

impl MaterializedScan {
    /// Cut the page `[offset, offset + clamp)`, attaching a
    /// continuation token when refs remain past it.
    fn page_at(&self, offset: usize, clamp: usize, scan_id: u64) -> ScanPage {
        let start = offset.min(self.refs.len());
        let end = start.saturating_add(clamp).min(self.refs.len());
        let next = if end < self.refs.len() {
            Some(encode_token(scan_id, end as u64))
        } else {
            None
        };
        ScanPage {
            refs: self.refs[start..end].to_vec(),
            anchor_slot: self.anchor_slot,
            next,
        }
    }
}

/// Why a continuation `resume` failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanError {
    /// The token didn't decode — wrong length. A module bug.
    BadToken,
    /// The scan the token refers to is gone (TTL-evicted, or the
    /// host restarted). The caller must restart with `after =
    /// None`.
    Expired,
}

impl std::fmt::Display for ScanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScanError::BadToken => f.write_str("malformed continuation token"),
            ScanError::Expired => {
                f.write_str("scan expired (TTL or host restart); restart with after=none")
            }
        }
    }
}

impl std::error::Error for ScanError {}

/// Per-instance cache of in-flight frozen scans. Lives on
/// `HostStateV2`. A recapture scans one predicate at a time, so
/// at most a handful of entries are ever live at once.
#[derive(Default)]
pub struct ScanCache {
    scans: HashMap<u64, MaterializedScan>,
    /// Monotonic scan-id allocator. Wraparound needs 2^64 scans —
    /// not a practical collision risk.
    next_id: u64,
}

impl ScanCache {
    /// Begin a fresh scan over `refs` (the dolos dump-all). The
    /// list is sorted into a stable order, capped at
    /// `MAX_SCAN_REFS`, and frozen; the first page is returned.
    /// The scan is retained for continuation **only if** more
    /// pages remain — a single-page scan leaves no cache entry.
    pub fn begin(&mut self, mut refs: Vec<OutputRef>, anchor_slot: u64, clamp: usize) -> ScanPage {
        self.evict_stale();

        // Stable order — (tx_hash, index). Sorting here is what
        // makes an offset cursor valid against the frozen set.
        refs.sort_unstable_by(|a, b| {
            a.tx_hash
                .as_ref()
                .cmp(b.tx_hash.as_ref())
                .then(a.index.cmp(&b.index))
        });
        refs.truncate(MAX_SCAN_REFS);

        let scan_id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        let scan = MaterializedScan {
            refs,
            anchor_slot,
            last_touched: Instant::now(),
        };
        let page = scan.page_at(0, clamp.max(1), scan_id);
        if page.next.is_some() {
            self.scans.insert(scan_id, scan);
        }
        page
    }

    /// Resume a scan from a prior page's continuation token. The
    /// scan is evicted once its last page is handed out.
    pub fn resume(&mut self, token: &[u8], clamp: usize) -> Result<ScanPage, ScanError> {
        let (scan_id, offset) = decode_token(token)?;
        let scan = self.scans.get_mut(&scan_id).ok_or(ScanError::Expired)?;
        scan.last_touched = Instant::now();
        let page = scan.page_at(offset as usize, clamp.max(1), scan_id);
        if page.next.is_none() {
            self.scans.remove(&scan_id);
        }
        Ok(page)
    }

    /// Number of in-flight scans — telemetry / tests.
    pub fn len(&self) -> usize {
        self.scans.len()
    }

    pub fn is_empty(&self) -> bool {
        self.scans.is_empty()
    }

    fn evict_stale(&mut self) {
        let now = Instant::now();
        self.scans
            .retain(|_, s| now.duration_since(s.last_touched) < SCAN_TTL);
    }
}

/// Token layout: `scan_id` (u64 BE) ++ `offset` (u64 BE). 16
/// bytes, opaque to the module.
fn encode_token(scan_id: u64, offset: u64) -> Vec<u8> {
    let mut t = Vec::with_capacity(16);
    t.extend_from_slice(&scan_id.to_be_bytes());
    t.extend_from_slice(&offset.to_be_bytes());
    t
}

fn decode_token(token: &[u8]) -> Result<(u64, u64), ScanError> {
    let bytes: [u8; 16] = token.try_into().map_err(|_| ScanError::BadToken)?;
    let scan_id = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let offset = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
    Ok((scan_id, offset))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pallas_primitives::Hash;

    fn refs(n: u32) -> Vec<OutputRef> {
        (0..n)
            .map(|i| {
                let mut h = [0u8; 32];
                h[28..32].copy_from_slice(&i.to_be_bytes());
                OutputRef::new(Hash::new(h), 0)
            })
            .collect()
    }

    #[test]
    fn single_page_leaves_no_cache_entry() {
        let mut cache = ScanCache::default();
        let page = cache.begin(refs(10), 42, 100);
        assert_eq!(page.refs.len(), 10);
        assert_eq!(page.anchor_slot, 42);
        assert!(page.next.is_none());
        assert!(cache.is_empty(), "fully-drained scan must not be retained");
    }

    #[test]
    fn multi_page_walk_covers_every_ref_once() {
        let mut cache = ScanCache::default();
        let total = 250usize;
        let mut seen = 0usize;
        let mut page = cache.begin(refs(total as u32), 7, 100);
        loop {
            seen += page.refs.len();
            assert_eq!(page.anchor_slot, 7, "anchor stable across pages");
            match page.next.clone() {
                Some(token) => page = cache.resume(&token, 100).unwrap(),
                None => break,
            }
        }
        assert_eq!(seen, total);
        assert!(cache.is_empty(), "scan evicted on last page");
    }

    #[test]
    fn bad_token_is_rejected() {
        let mut cache = ScanCache::default();
        assert!(matches!(
            cache.resume(&[0u8; 4], 100),
            Err(ScanError::BadToken)
        ));
    }

    #[test]
    fn unknown_scan_is_expired() {
        let mut cache = ScanCache::default();
        let token = encode_token(999, 0);
        assert!(matches!(
            cache.resume(&token, 100),
            Err(ScanError::Expired)
        ));
    }

    #[test]
    fn empty_scan_yields_one_empty_page() {
        let mut cache = ScanCache::default();
        let page = cache.begin(Vec::new(), 99, 100);
        assert!(page.refs.is_empty());
        assert!(page.next.is_none());
        assert_eq!(page.anchor_slot, 99);
    }
}
