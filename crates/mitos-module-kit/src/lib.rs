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
}
