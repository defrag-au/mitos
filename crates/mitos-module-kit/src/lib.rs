//! `mitos-module-kit` — author-facing helpers for mitos
//! community modules.
//!
//! Phase 5 of `docs/design/WASM_BUDGET_CHUNKING.md`: a
//! **re-entrant scan affordance** so module authors writing a
//! `rebootstrap` (or any cold-start scan) don't re-derive the
//! predicate-cursor bookkeeping by hand.
//!
//! # The re-entrant scan problem
//!
//! A wasm module call runs under a fuel budget; a cold-start
//! scan over a busy policy does not fit one call. The fix is to
//! make `rebootstrap` re-entrant: one call does **one bounded
//! page** of work and reports `done` + `ingested`; the host
//! loops, refuelling each call, until a step is `done`.
//!
//! That requires bookkeeping the module shouldn't re-invent:
//!
//! - a **predicate list** (the policies / addresses / creds the
//!   module is re-scanning), in a stable order;
//! - a **`predicate_idx`** — which predicate is in progress —
//!   that must be *durable* (a host restart resumes mid-round);
//! - a **page cursor** (`after` token) for the current
//!   predicate, *volatile* — a trap or restart restarts the
//!   current predicate from page 0, which is safe because each
//!   predicate emits a full authoritative snapshot;
//! - an optional **per-predicate accumulator** (a holder ledger,
//!   a lock set) that is resident across the predicate's pages
//!   and reset when it completes.
//!
//! [`ReentrantRound`] owns exactly that bookkeeping — pure logic,
//! no host-fn or WIT coupling. The module keeps the parts that
//! genuinely vary: the predicate type, the per-page chain-data
//! fetch, the fold, the snapshot emit, and the ~3 lines of
//! `state-kv` IO for the durable cursor.
//!
//! # Cursor model
//!
//! The durable cursor (in `state-kv`) is the **`predicate_idx`
//! only**. The page cursor and accumulator are volatile — held
//! in the [`ReentrantRound`] inside a module thread-local,
//! resident across the host's re-entrant loop on one wasm
//! instance, discarded on a trap or host restart. Recovery
//! restarts the current predicate from page 0. Storing the page
//! cursor durably would be pointless: it indexes the host's
//! in-memory scan cache, which a restart drops.
//!
//! **The predicate list MUST be in a deterministic order** —
//! sort it before handing it to [`ReentrantRound::resume`] — or
//! `predicate_idx` would point at a different predicate after a
//! restart.
//!
//! # Worked example
//!
//! ```ignore
//! thread_local! {
//!     static ROUND: RefCell<Option<ReentrantRound<[u8; 28], Ledger>>> =
//!         RefCell::new(None);
//! }
//!
//! fn rebootstrap() -> Result<RebootstrapStep, String> {
//!     ROUND.with(|cell| {
//!         let mut slot = cell.borrow_mut();
//!         if slot.is_none() {
//!             let mut policies = tracked_policies();
//!             policies.sort_unstable();             // deterministic!
//!             *slot = Some(ReentrantRound::resume(policies, load_cursor()));
//!         }
//!         let round = slot.as_mut().unwrap();
//!
//!         let Some(&policy) = round.current() else {
//!             clear_cursor();
//!             *slot = None;
//!             return Ok(RebootstrapStep { done: true, ingested: 0 });
//!         };
//!
//!         let page = utxos_by_policy(&policy, round.after(), PAGE_HINT);
//!         let ingested = page.refs.len() as u64;
//!         fold(round.acc_mut(), &policy, &page.refs);
//!
//!         match page.next {
//!             Some(token) => {
//!                 round.page_more(ingested, token);
//!                 Ok(RebootstrapStep { done: false, ingested })
//!             }
//!             None => {
//!                 round.page_last(ingested);
//!                 emit_snapshot(&policy, round.acc(), round.items());
//!                 let adv = round.finish_predicate();
//!                 if adv.round_done { clear_cursor(); *slot = None; }
//!                 else { save_cursor(adv.predicate_idx); }
//!                 Ok(RebootstrapStep { done: adv.round_done, ingested })
//!             }
//!         }
//!     })
//! }
//! ```

#![forbid(unsafe_code)]

/// Outcome of [`ReentrantRound::finish_predicate`] — what the
/// module must persist + whether to stop looping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Advance {
    /// The new predicate index. The module persists this as its
    /// durable `state-kv` cursor (unless `round_done`, in which
    /// case it clears the cursor instead).
    pub predicate_idx: usize,
    /// `true` when every predicate has been scanned — the host
    /// loop stops.
    pub round_done: bool,
}

/// An in-flight re-entrant scan over a list of `predicates`,
/// each with a per-predicate accumulator `A`.
///
/// Lives in a module thread-local across the host's re-entrant
/// `rebootstrap` loop. Pure logic — no IO; the module owns the
/// `state-kv` cursor read/write and the chain-data fetch.
///
/// `P` is the predicate type (e.g. a 28-byte policy id, a bech32
/// address `String`, an enum). `A` is the per-predicate
/// accumulator and must be `Default` — `finish_predicate` resets
/// it to `A::default()` between predicates. Use `A = ()` for a
/// scan that emits per-page and accumulates nothing.
pub struct ReentrantRound<P, A> {
    predicates: Vec<P>,
    predicate_idx: usize,
    after: Option<Vec<u8>>,
    acc: A,
    items: u64,
}

impl<P, A: Default> ReentrantRound<P, A> {
    /// Begin — or resume — a round.
    ///
    /// `predicates` MUST already be in a deterministic order
    /// (sort it first); `start_idx` is the durable cursor read
    /// from `state-kv` (`0` for a fresh round). `start_idx` is
    /// clamped to `predicates.len()` so a stale cursor can't
    /// index out of bounds.
    pub fn resume(predicates: Vec<P>, start_idx: usize) -> Self {
        let predicate_idx = start_idx.min(predicates.len());
        Self {
            predicates,
            predicate_idx,
            after: None,
            acc: A::default(),
            items: 0,
        }
    }

    /// The predicate currently being scanned, or `None` when the
    /// round is complete (every predicate done, or an empty
    /// predicate list).
    pub fn current(&self) -> Option<&P> {
        self.predicates.get(self.predicate_idx)
    }

    /// Index of the current predicate.
    pub fn predicate_idx(&self) -> usize {
        self.predicate_idx
    }

    /// Total number of predicates in the round.
    pub fn predicate_count(&self) -> usize {
        self.predicates.len()
    }

    /// The page cursor for the current predicate's *next* page —
    /// pass it straight to a paged `utxos-by-*` host-fn as
    /// `after`. `None` ⇒ start at page 0.
    pub fn after(&self) -> Option<&[u8]> {
        self.after.as_deref()
    }

    /// The current predicate's accumulator.
    pub fn acc(&self) -> &A {
        &self.acc
    }

    /// The current predicate's accumulator, mutably — fold each
    /// page's resolved outputs into this.
    pub fn acc_mut(&mut self) -> &mut A {
        &mut self.acc
    }

    /// Items (UTxOs) processed for the current predicate so far,
    /// across all its pages. Reset to `0` by `finish_predicate`.
    pub fn items(&self) -> u64 {
        self.items
    }

    /// Record a page that is **not** the last for this predicate:
    /// count its `item_count` items and store the continuation
    /// `next` token for the following call.
    pub fn page_more(&mut self, item_count: u64, next: Vec<u8>) {
        self.items += item_count;
        self.after = Some(next);
    }

    /// Record the **last** page of the current predicate: count
    /// its `item_count` items. The module then emits the
    /// predicate's snapshot (reading `acc()` / `items()`) and
    /// calls [`finish_predicate`](Self::finish_predicate).
    pub fn page_last(&mut self, item_count: u64) {
        self.items += item_count;
    }

    /// Advance past the just-finished predicate: step
    /// `predicate_idx`, reset the page cursor, accumulator, and
    /// item count for the next predicate.
    ///
    /// Returns the [`Advance`] the module persists — the new
    /// `predicate_idx` to write as the durable cursor, and
    /// whether the whole round is now `done`.
    pub fn finish_predicate(&mut self) -> Advance {
        self.predicate_idx += 1;
        self.after = None;
        self.acc = A::default();
        self.items = 0;
        Advance {
            predicate_idx: self.predicate_idx,
            round_done: self.predicate_idx >= self.predicates.len(),
        }
    }

    /// Whether the round has no more predicates to scan —
    /// equivalent to `current().is_none()`.
    pub fn is_complete(&self) -> bool {
        self.predicate_idx >= self.predicates.len()
    }
}

// ============================================================
// ChunkedBootstrap — re-entrant scan + sharded persist + chunked
// emit, for modules whose per-asset payload is large enough that a
// single whole-ledger serialize traps (the deferred "pathological
// accumulator" case in `WASM_BUDGET_CHUNKING.md`).
// ============================================================

// ## Why a second driver
//
// `ReentrantRound` pages the *scan* but leaves the module to
// accumulate a resident ledger and persist/emit it. For modules
// whose per-entry payload is small (a holding ≈ 80 B) that's fine.
// For a metadata module (a CIP-68 `metadata_json` per asset ≈ KB),
// the resident ledger reaches multiple MB and the last-page
// `persist_ledger` + clone + emit-open traps `out-of-fuel` — and,
// because the durable cursor only advanced at emit-close, the
// re-instantiate-on-trap loop re-emits `SnapshotBegin` forever.
//
// `ChunkedBootstrap` removes the resident ledger entirely:
//
// - Scan pages the policy and writes one kv key per asset
//   (sharded) as it goes — no accumulator, no big serialize.
// - Emit lists the shards (`state-kv list-values`, the single
//   source of truth — no separate key index to drift) and drains
//   one bounded `SnapshotChunk` per call, the emit offset durable
//   in the cursor.
// - Every progress field (predicate index, phase, emit offset) is
//   durable in `state-kv`, so a re-instantiation mid-emit resumes
//   at the offset instead of restarting the policy. No call ever
//   touches the whole ledger, so the per-call cost shrinks with the
//   page and can't trap on size.
//
// The kit stays pure: all host IO (chain scan, kv get/set/list,
// typed event emit) is supplied by the module through `BootstrapIo`,
// exactly as `ReentrantRound` leaves IO to the module.

/// Phase of a [`ChunkedBootstrap`] for one predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Paging `utxos-by-*` and sharding each entry to `state-kv`.
    Scan,
    /// Draining `SnapshotChunk`s from the sharded entries.
    Emit,
}

/// Durable progress for a [`ChunkedBootstrap`]. Encoded (no serde —
/// the kit is dependency-free) and stored by the module in
/// `state-kv` so a trap / host restart resumes exactly.
/// [`BootstrapCursor::encode`] / [`BootstrapCursor::decode`] are the
/// wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapCursor {
    /// Index into the sorted predicate list.
    pub predicate_idx: u64,
    /// Phase within the current predicate.
    pub phase: Phase,
    /// `anchor_slot` frozen for the predicate being scanned/emitted.
    pub anchor_slot: u64,
    /// The last `entry_key` emitted (a **key cursor**, not an offset)
    /// — only meaningful in [`Phase::Emit`]. The emit pages forward
    /// with `shard_scan(after = emit_after)`. A key cursor (vs an
    /// offset) is robust to concurrent live-tail shard mutations
    /// during a long emit. `None` ⇒ emit not started / from the start.
    pub emit_after: Option<Vec<u8>>,
}

impl BootstrapCursor {
    const VERSION: u8 = 2;

    fn fresh() -> Self {
        Self {
            predicate_idx: 0,
            phase: Phase::Scan,
            anchor_slot: 0,
            emit_after: None,
        }
    }

    /// Encode to the durable form for `state-kv`:
    /// `version(1) | phase(1) | predicate_idx(8 BE) | anchor_slot(8 BE)
    /// | emit_after_len(4 BE) | emit_after`.
    pub fn encode(&self) -> Vec<u8> {
        let after = self.emit_after.as_deref().unwrap_or(&[]);
        let mut b = Vec::with_capacity(1 + 1 + 8 + 8 + 4 + after.len());
        b.push(Self::VERSION);
        b.push(match self.phase {
            Phase::Scan => 0,
            Phase::Emit => 1,
        });
        b.extend_from_slice(&self.predicate_idx.to_be_bytes());
        b.extend_from_slice(&self.anchor_slot.to_be_bytes());
        // `None` and empty are encoded identically; only meaningful in
        // Emit, where `None` means "from the start".
        b.extend_from_slice(&(after.len() as u32).to_be_bytes());
        b.extend_from_slice(after);
        b
    }

    /// Decode from `state-kv`. `None` on a malformed / wrong-version
    /// blob (treated as "no cursor" → fresh round).
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 22 || bytes[0] != Self::VERSION {
            return None;
        }
        let phase = match bytes[1] {
            0 => Phase::Scan,
            1 => Phase::Emit,
            _ => return None,
        };
        let predicate_idx = u64::from_be_bytes(bytes[2..10].try_into().ok()?);
        let anchor_slot = u64::from_be_bytes(bytes[10..18].try_into().ok()?);
        let after_len = u32::from_be_bytes(bytes[18..22].try_into().ok()?) as usize;
        let after = bytes.get(22..22 + after_len)?;
        let emit_after = if after.is_empty() {
            None
        } else {
            Some(after.to_vec())
        };
        Some(Self {
            predicate_idx,
            phase,
            anchor_slot,
            emit_after,
        })
    }
}

/// One scanned page handed back from [`BootstrapIo::scan_page`].
pub struct ScannedPage<E> {
    /// `(entry_key, entry)` pairs decoded from this page. `entry_key`
    /// is the opaque per-asset shard key (the module owns its
    /// format); the kit only routes it back through
    /// [`BootstrapIo::shard_get`] / [`BootstrapIo::shard_put`].
    pub entries: Vec<(Vec<u8>, E)>,
    /// Continuation token for the next page; `None` ⇒ last page.
    pub next: Option<Vec<u8>>,
    /// Anchor slot for this scan (frozen into the cursor).
    pub anchor_slot: u64,
    /// UTxOs seen this page (telemetry only).
    pub ingested: u64,
}

/// All host IO a [`ChunkedBootstrap`] needs, supplied by the module
/// using its own WIT host bindings. The kit never calls host-fns;
/// this trait is the seam.
pub trait BootstrapIo {
    /// The module's per-asset entry, ready for snapshot emission.
    /// The kit stores it via [`encode_entry`](Self::encode_entry) /
    /// [`decode_entry`](Self::decode_entry) and never inspects it.
    type Entry;

    // ---- chain scan (one page) -------------------------------------
    /// Scan one page of `predicate`. `after == None` ⇒ page 0.
    fn scan_page(&mut self, predicate: &[u8], after: Option<&[u8]>) -> ScannedPage<Self::Entry>;

    // ---- sharded per-asset store (batched / paginated) -------------
    /// Batched read of many shards' bytes, parallel to `keys`
    /// (`None` ⇒ absent). One read txn (backed by `state-kv get-many`).
    /// Only called on the SUM path (`accumulates`), once per scan page.
    fn shard_get_many(&self, predicate: &[u8], keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>>;
    /// Batched write of a page's shards in one txn (backed by
    /// `state-kv set-many`) — one fsync per page, not per entry.
    fn shard_put_many(&mut self, predicate: &[u8], entries: &[(Vec<u8>, Vec<u8>)]);
    /// Paginated `(entry_key, entry_bytes)` scan, sorted, `after`
    /// EXCLUSIVE (`None` ⇒ from the start), up to `limit`. Backed by
    /// `state-kv kv-scan` — pages the emit through shards O(n) total,
    /// nothing resident, values inline (no follow-up gets). The kv
    /// keys are the single source of truth for which assets exist.
    fn shard_scan(
        &self,
        predicate: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)>;
    /// Delete every shard for a predicate in one txn (before a fresh
    /// scan; backed by `state-kv delete-prefix`).
    fn shard_clear(&mut self, predicate: &[u8]);

    // ---- entry (de)serialization (module owns the codec) ----------
    fn encode_entry(&self, entry: &Self::Entry) -> Vec<u8>;
    fn decode_entry(&self, bytes: &[u8]) -> Option<Self::Entry>;

    /// `true` if the same `entry_key` legitimately recurs across
    /// UTxOs/pages and must be combined (e.g. holder balances SUM).
    /// `false` (default) ⇒ last-write-wins (CIP-68 datum rotation,
    /// one-NFT-per-name) and the driver skips the read-before-write.
    fn accumulates(&self) -> bool {
        false
    }
    /// Combine a freshly scanned entry with whatever is stored.
    /// Only called when [`accumulates`](Self::accumulates) is `true`;
    /// the default (replace) returns `incoming`.
    fn merge(&self, _prior: Option<Self::Entry>, incoming: Self::Entry) -> Self::Entry {
        incoming
    }

    // ---- durable driver cursor ------------------------------------
    fn load_cursor(&self) -> Option<Vec<u8>>;
    fn save_cursor(&mut self, bytes: &[u8]);
    fn clear_cursor(&mut self);

    // ---- typed emission -------------------------------------------
    fn emit_begin(&mut self, predicate: &[u8], anchor_slot: u64);
    fn emit_chunk(&mut self, predicate: &[u8], entries: Vec<Self::Entry>);
    fn emit_end(&mut self, predicate: &[u8], total: u64);

    /// Entries per `SnapshotChunk`. Module-owned (metadata ≈ 100,
    /// holders/holder-distribution ≈ 1000).
    fn chunk_size(&self) -> usize;
}

/// Result of one [`ChunkedBootstrap::step`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepOutcome {
    /// `true` when every predicate has been scanned + emitted.
    pub done: bool,
    /// UTxOs ingested this step (telemetry).
    pub ingested: u64,
}

/// A re-entrant scan→shard→emit driver. Held in a module
/// thread-local across the host's re-entrant loop on one wasm
/// instance; on a trap the host drops it and the module rebuilds it
/// from the durable cursor via [`ChunkedBootstrap::resume`].
pub struct ChunkedBootstrap {
    predicates: Vec<Vec<u8>>,
    cursor: BootstrapCursor,
    /// Volatile page token for the predicate currently scanning.
    after: Option<Vec<u8>>,
    /// Whether the current predicate's stale shards were cleared
    /// this run (once, before its first scan page). Volatile — a
    /// re-instantiation re-clears, which is correct (the re-scan
    /// starts clean).
    cleared_current: bool,
    /// Best-effort running count of entries emitted for the current
    /// predicate, reported in `SnapshotEnd`. Resident (not durable):
    /// a re-instantiation mid-emit resets it, so the reported count
    /// can undercount after a trap. Informational only — the consumer
    /// wipes on `SnapshotBegin` and doesn't depend on the count.
    emit_count: u64,
}

impl ChunkedBootstrap {
    /// Begin — or resume — a round. `predicates` MUST be sorted
    /// (deterministic), same rule as [`ReentrantRound`]. `cursor` is
    /// the decoded durable cursor (`None` ⇒ fresh).
    pub fn resume(predicates: Vec<Vec<u8>>, cursor: Option<BootstrapCursor>) -> Self {
        let mut cursor = cursor.unwrap_or_else(BootstrapCursor::fresh);
        // Clamp a stale cursor past the end.
        if cursor.predicate_idx as usize > predicates.len() {
            cursor.predicate_idx = predicates.len() as u64;
        }
        Self {
            predicates,
            cursor,
            after: None,
            cleared_current: false,
            emit_count: 0,
        }
    }

    /// The current durable cursor (mirror of what's in `state-kv`).
    pub fn cursor(&self) -> BootstrapCursor {
        self.cursor.clone()
    }

    /// Do one bounded unit of work: one scan page, or one emit
    /// chunk, or one phase/predicate transition. The host loops this
    /// (refuelling each call) until `done`.
    pub fn step<IO: BootstrapIo>(&mut self, io: &mut IO) -> StepOutcome {
        let Some(predicate) = self.predicates.get(self.cursor.predicate_idx as usize).cloned()
        else {
            io.clear_cursor();
            return StepOutcome {
                done: true,
                ingested: 0,
            };
        };

        match self.cursor.phase {
            Phase::Scan => self.step_scan(io, &predicate),
            Phase::Emit => self.step_emit(io, &predicate),
        }
    }

    fn step_scan<IO: BootstrapIo>(&mut self, io: &mut IO, predicate: &[u8]) -> StepOutcome {
        // Clear stale shards once before this predicate's first page
        // so a re-scan after a trap can't double-count accumulating
        // entries (and replace entries start clean too).
        if self.after.is_none() && !self.cleared_current {
            io.shard_clear(predicate);
            self.cleared_current = true;
        }

        let page = io.scan_page(predicate, self.after.as_deref());
        self.cursor.anchor_slot = page.anchor_slot;

        // Build the page's writes: one batched read (SUM only) + one
        // batched write per page — no per-entry fsync, no per-entry
        // get. For SUM, the read-modify-write makes cross-page sums
        // durable (page N sees page N-1's committed writes).
        let writes: Vec<(Vec<u8>, Vec<u8>)> = if io.accumulates() {
            let keys: Vec<Vec<u8>> = page.entries.iter().map(|(k, _)| k.clone()).collect();
            let priors = io.shard_get_many(predicate, &keys);
            page.entries
                .into_iter()
                .zip(priors)
                .map(|((key, entry), prior_bytes)| {
                    let prior = prior_bytes.and_then(|b| io.decode_entry(&b));
                    let merged = io.merge(prior, entry);
                    (key, io.encode_entry(&merged))
                })
                .collect()
        } else {
            page.entries
                .into_iter()
                .map(|(key, entry)| (key, io.encode_entry(&entry)))
                .collect()
        };
        if !writes.is_empty() {
            io.shard_put_many(predicate, &writes);
        }

        match page.next {
            Some(token) => {
                self.after = Some(token);
                io.save_cursor(&self.cursor.encode());
                StepOutcome {
                    done: false,
                    ingested: page.ingested,
                }
            }
            None => {
                // Last page → open the emit. Light: just SnapshotBegin
                // + cursor flip. No whole-ledger op (values already
                // sharded), so this transition can't trap on size.
                self.cursor.phase = Phase::Emit;
                self.cursor.emit_after = None;
                self.emit_count = 0;
                self.after = None;
                io.emit_begin(predicate, self.cursor.anchor_slot);
                io.save_cursor(&self.cursor.encode());
                StepOutcome {
                    done: false,
                    ingested: page.ingested,
                }
            }
        }
    }

    fn step_emit<IO: BootstrapIo>(&mut self, io: &mut IO, predicate: &[u8]) -> StepOutcome {
        // Page forward by key cursor — `shard_scan` returns sorted
        // (key, value) pairs after `emit_after`, values inline.
        let page = io.shard_scan(predicate, self.cursor.emit_after.as_deref(), io.chunk_size());

        if page.is_empty() {
            io.emit_end(predicate, self.emit_count);
            // Advance to the next predicate.
            self.cursor.predicate_idx += 1;
            self.cursor.phase = Phase::Scan;
            self.cursor.emit_after = None;
            self.cleared_current = false;
            self.emit_count = 0;
            let done = self.cursor.predicate_idx as usize >= self.predicates.len();
            if done {
                io.clear_cursor();
            } else {
                io.save_cursor(&self.cursor.encode());
            }
            return StepOutcome { done, ingested: 0 };
        }

        let last_key = page.last().map(|(k, _)| k.clone());
        self.emit_count += page.len() as u64;
        let entries: Vec<IO::Entry> = page
            .into_iter()
            .filter_map(|(_, v)| io.decode_entry(&v))
            .collect();
        io.emit_chunk(predicate, entries);
        self.cursor.emit_after = last_key; // durable key cursor
        io.save_cursor(&self.cursor.encode());
        StepOutcome {
            done: false,
            ingested: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive a whole round of `n` predicates, `pages_each` pages
    /// per predicate, and assert the bookkeeping.
    #[test]
    fn full_round_walks_every_predicate_and_page() {
        let predicates: Vec<u32> = (0..4).collect();
        let mut round: ReentrantRound<u32, u64> = ReentrantRound::resume(predicates, 0);

        let pages_each = 3u64;
        let items_per_page = 10u64;
        let mut finished = Vec::new();

        while let Some(&predicate) = round.current() {
            // Simulate one page.
            let page_idx = round.items() / items_per_page;
            *round.acc_mut() += items_per_page;
            if page_idx + 1 < pages_each {
                round.page_more(items_per_page, vec![page_idx as u8]);
            } else {
                round.page_last(items_per_page);
                // Snapshot would emit here — accumulator + items
                // are visible before the advance resets them.
                assert_eq!(round.items(), pages_each * items_per_page);
                assert_eq!(*round.acc(), pages_each * items_per_page);
                finished.push(predicate);
                let adv = round.finish_predicate();
                assert_eq!(adv.round_done, finished.len() == 4);
                // Accumulator + item count reset for the next.
                assert_eq!(*round.acc(), 0);
                assert_eq!(round.items(), 0);
            }
        }
        assert_eq!(finished, vec![0, 1, 2, 3]);
        assert!(round.is_complete());
    }

    #[test]
    fn empty_predicate_list_is_immediately_complete() {
        let round: ReentrantRound<u32, ()> = ReentrantRound::resume(Vec::new(), 0);
        assert!(round.current().is_none());
        assert!(round.is_complete());
    }

    #[test]
    fn resume_from_a_mid_round_cursor() {
        // A host restart resumed the round at predicate 2 of 4.
        let predicates: Vec<u32> = vec![10, 11, 12, 13];
        let round: ReentrantRound<u32, ()> = ReentrantRound::resume(predicates, 2);
        assert_eq!(round.predicate_idx(), 2);
        assert_eq!(round.current(), Some(&12));
        // The page cursor + accumulator start fresh — the
        // current predicate restarts from page 0.
        assert!(round.after().is_none());
    }

    #[test]
    fn stale_cursor_past_the_end_is_clamped() {
        // A cursor left over from a longer prior round.
        let predicates: Vec<u32> = vec![1, 2];
        let round: ReentrantRound<u32, ()> = ReentrantRound::resume(predicates, 99);
        assert!(round.is_complete());
        assert!(round.current().is_none());
    }

    #[test]
    fn page_cursor_advances_then_clears_on_finish() {
        let mut round: ReentrantRound<u32, ()> = ReentrantRound::resume(vec![7], 0);
        assert!(round.after().is_none());
        round.page_more(5, vec![1, 2, 3]);
        assert_eq!(round.after(), Some(&[1u8, 2, 3][..]));
        assert_eq!(round.items(), 5);
        round.page_last(2);
        assert_eq!(round.items(), 7);
        let adv = round.finish_predicate();
        assert!(adv.round_done);
        // Page cursor cleared for the (non-existent) next predicate.
        assert!(round.after().is_none());
    }

    // ====================================================
    // ChunkedBootstrap
    // ====================================================

    use std::collections::BTreeMap;

    /// Test entry: carries its own key so emitted chunks are
    /// inspectable.
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TE {
        key: Vec<u8>,
        val: u64,
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Ev {
        Begin(Vec<u8>),
        Chunk(Vec<TE>),
        End(Vec<u8>, u64),
    }

    struct MockIo {
        /// Scripted scan pages per predicate (immutable, so a re-scan
        /// after a simulated re-instantiation replays identically).
        script: BTreeMap<Vec<u8>, Vec<Vec<(Vec<u8>, u64)>>>,
        /// (predicate, entry_key) -> encoded entry.
        shards: BTreeMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
        cursor: Option<Vec<u8>>,
        events: Vec<Ev>,
        accumulates: bool,
        chunk: usize,
    }

    impl MockIo {
        fn new(accumulates: bool, chunk: usize) -> Self {
            Self {
                script: BTreeMap::new(),
                shards: BTreeMap::new(),
                cursor: None,
                events: Vec::new(),
                accumulates,
                chunk,
            }
        }
    }

    impl BootstrapIo for MockIo {
        type Entry = TE;

        fn scan_page(&mut self, predicate: &[u8], after: Option<&[u8]>) -> ScannedPage<TE> {
            let pages = self.script.get(predicate).cloned().unwrap_or_default();
            let idx = after.map(|b| b[0] as usize).unwrap_or(0);
            let entries: Vec<(Vec<u8>, TE)> = pages
                .get(idx)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| (k.clone(), TE { key: k, val: v }))
                .collect();
            let ingested = entries.len() as u64;
            let next = if idx + 1 < pages.len() {
                Some(vec![(idx + 1) as u8])
            } else {
                None
            };
            ScannedPage {
                entries,
                next,
                anchor_slot: 100,
                ingested,
            }
        }

        fn shard_get_many(&self, predicate: &[u8], keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
            keys.iter()
                .map(|k| self.shards.get(&(predicate.to_vec(), k.clone())).cloned())
                .collect()
        }
        fn shard_put_many(&mut self, predicate: &[u8], entries: &[(Vec<u8>, Vec<u8>)]) {
            for (k, v) in entries {
                self.shards
                    .insert((predicate.to_vec(), k.clone()), v.clone());
            }
        }
        fn shard_scan(
            &self,
            predicate: &[u8],
            after: Option<&[u8]>,
            limit: usize,
        ) -> Vec<(Vec<u8>, Vec<u8>)> {
            // BTreeMap iterates sorted → filter predicate + `> after`,
            // take limit (matches host kv-scan semantics).
            self.shards
                .iter()
                .filter(|((p, _), _)| p == predicate)
                .filter(|((_, k), _)| after.map(|a| k.as_slice() > a).unwrap_or(true))
                .map(|((_, k), v)| (k.clone(), v.clone()))
                .take(limit)
                .collect()
        }
        fn shard_clear(&mut self, predicate: &[u8]) {
            self.shards.retain(|(p, _), _| p != predicate);
        }

        fn encode_entry(&self, e: &TE) -> Vec<u8> {
            let mut b = vec![e.key.len() as u8];
            b.extend_from_slice(&e.key);
            b.extend_from_slice(&e.val.to_be_bytes());
            b
        }
        fn decode_entry(&self, bytes: &[u8]) -> Option<TE> {
            let klen = *bytes.first()? as usize;
            let key = bytes.get(1..1 + klen)?.to_vec();
            let val = u64::from_be_bytes(bytes.get(1 + klen..1 + klen + 8)?.try_into().ok()?);
            Some(TE { key, val })
        }

        fn accumulates(&self) -> bool {
            self.accumulates
        }
        fn merge(&self, prior: Option<TE>, incoming: TE) -> TE {
            match prior {
                Some(p) => TE {
                    key: incoming.key,
                    val: p.val + incoming.val,
                },
                None => incoming,
            }
        }

        fn load_cursor(&self) -> Option<Vec<u8>> {
            self.cursor.clone()
        }
        fn save_cursor(&mut self, bytes: &[u8]) {
            self.cursor = Some(bytes.to_vec());
        }
        fn clear_cursor(&mut self) {
            self.cursor = None;
        }

        fn emit_begin(&mut self, predicate: &[u8], _anchor: u64) {
            self.events.push(Ev::Begin(predicate.to_vec()));
        }
        fn emit_chunk(&mut self, _predicate: &[u8], entries: Vec<TE>) {
            self.events.push(Ev::Chunk(entries));
        }
        fn emit_end(&mut self, predicate: &[u8], total: u64) {
            self.events.push(Ev::End(predicate.to_vec(), total));
        }
        fn chunk_size(&self) -> usize {
            self.chunk
        }
    }

    /// Drive a fresh driver to completion (no re-instantiation).
    fn run_to_done(io: &mut MockIo, predicates: Vec<Vec<u8>>) {
        let cursor = io.load_cursor().and_then(|b| BootstrapCursor::decode(&b));
        let mut drv = ChunkedBootstrap::resume(predicates, cursor);
        for _ in 0..10_000 {
            if drv.step(io).done {
                return;
            }
        }
        panic!("did not finish within step budget");
    }

    fn begins(io: &MockIo) -> usize {
        io.events
            .iter()
            .filter(|e| matches!(e, Ev::Begin(_)))
            .count()
    }
    fn ends(io: &MockIo) -> usize {
        io.events
            .iter()
            .filter(|e| matches!(e, Ev::End(_, _)))
            .count()
    }
    fn emitted_entries(io: &MockIo) -> Vec<TE> {
        io.events
            .iter()
            .flat_map(|e| match e {
                Ev::Chunk(es) => es.clone(),
                _ => Vec::new(),
            })
            .collect()
    }

    #[test]
    fn replace_round_emits_each_asset_once_sorted() {
        let mut io = MockIo::new(false, 2);
        io.script.insert(
            vec![0xAA],
            vec![
                vec![(vec![3], 30), (vec![1], 10)],
                vec![(vec![2], 20)],
            ],
        );
        run_to_done(&mut io, vec![vec![0xAA]]);

        assert_eq!(begins(&io), 1);
        assert_eq!(ends(&io), 1);
        let got = emitted_entries(&io);
        // Sorted by key, each once.
        assert_eq!(
            got,
            vec![
                TE { key: vec![1], val: 10 },
                TE { key: vec![2], val: 20 },
                TE { key: vec![3], val: 30 },
            ]
        );
        assert!(matches!(io.events.last(), Some(Ev::End(_, 3))));
    }

    #[test]
    fn sum_merge_combines_same_key_across_pages() {
        let mut io = MockIo::new(true, 100);
        io.script.insert(
            vec![0xAA],
            vec![
                vec![(vec![1], 10), (vec![2], 5)],
                vec![(vec![1], 7)], // same key 1 again → sums to 17
            ],
        );
        run_to_done(&mut io, vec![vec![0xAA]]);

        let got = emitted_entries(&io);
        assert_eq!(
            got,
            vec![
                TE { key: vec![1], val: 17 },
                TE { key: vec![2], val: 5 },
            ]
        );
    }

    #[test]
    fn reinstantiate_mid_emit_resumes_without_rebegin() {
        // Build shards via a full scan, but stop partway through the
        // emit, drop the driver (simulating an out-of-fuel trap), and
        // rebuild from the durable cursor.
        let mut io = MockIo::new(false, 1); // chunk=1 → many emit steps
        io.script.insert(
            vec![0xAA],
            vec![vec![(vec![1], 10), (vec![2], 20), (vec![3], 30)]],
        );

        // Phase 1: drive until we've emitted at least one chunk.
        {
            let cursor = io.load_cursor().and_then(|b| BootstrapCursor::decode(&b));
            let mut drv = ChunkedBootstrap::resume(vec![vec![0xAA]], cursor);
            // scan page (1 step) + begin transition is folded into the
            // last scan step; then emit chunks one per step.
            drv.step(&mut io); // scan last page → emit_begin
            drv.step(&mut io); // emit chunk 0 (key 1)
            // Driver dropped here — thread-local lost (trap).
            assert_eq!(begins(&io), 1);
            assert_eq!(emitted_entries(&io).len(), 1);
        }

        // Phase 2: rebuild from the durable cursor + persisted shards
        // and finish. Must NOT re-emit Begin, must emit the remaining
        // entries exactly once, then End.
        {
            let cursor = io.load_cursor().and_then(|b| BootstrapCursor::decode(&b));
            assert!(matches!(cursor, Some(ref c) if c.phase == Phase::Emit));
            let mut drv = ChunkedBootstrap::resume(vec![vec![0xAA]], cursor);
            for _ in 0..100 {
                if drv.step(&mut io).done {
                    break;
                }
            }
        }

        assert_eq!(begins(&io), 1, "SnapshotBegin must be emitted exactly once");
        assert_eq!(ends(&io), 1);
        let got = emitted_entries(&io);
        assert_eq!(
            got,
            vec![
                TE { key: vec![1], val: 10 },
                TE { key: vec![2], val: 20 },
                TE { key: vec![3], val: 30 },
            ],
            "every asset emitted exactly once across the re-instantiation"
        );
    }

    #[test]
    fn cursor_round_trips() {
        // With a key cursor set.
        let c = BootstrapCursor {
            predicate_idx: 7,
            phase: Phase::Emit,
            anchor_slot: 123456,
            emit_after: Some(vec![0xde, 0xad, 0xbe, 0xef]),
        };
        assert_eq!(BootstrapCursor::decode(&c.encode()), Some(c));
        // With no key cursor (Scan phase / emit not started).
        let c2 = BootstrapCursor {
            predicate_idx: 0,
            phase: Phase::Scan,
            anchor_slot: 0,
            emit_after: None,
        };
        assert_eq!(BootstrapCursor::decode(&c2.encode()), Some(c2));
        assert_eq!(BootstrapCursor::decode(&[]), None);
        assert_eq!(BootstrapCursor::decode(&[9, 9, 9]), None);
    }
}
