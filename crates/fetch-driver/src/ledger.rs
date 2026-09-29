//! What each purpose cost — the ledger a cost table is built from.
//!
//! # Why not phases
//!
//! A phased reader can time a phase by reading the clock at its start and at its end,
//! because a phase has the wire to itself. A dispatch queue's purposes are **concurrent** —
//! a datum catalogue lands while the index's fences are still in the air — so an interval
//! covering both says "each of these took 400 ms" for work that overlapped, and a table
//! built on intervals reports more wall clock than the run had. That is not a coarser
//! number; it is a wrong one, and it is the same class of lie the counter machinery exists
//! to prevent, one level up.
//!
//! So a row here is **summed over the requests themselves**:
//!
//! | column | what it is | what it answers |
//! |---|---|---|
//! | `requests` | how many went out for this purpose | is this purpose many small round trips or few large ones |
//! | `bytes` | what they asked for, excluding multipart framing | is this purpose bytes-bound |
//! | `millis` | Σ of each request's dispatch → settle | how much of the wire this purpose used |
//! | `worst_ms` | the slowest single request of the purpose | the straggler a queue cannot hide |
//! | `span_ms` | first dispatch → last settle | how long the purpose was in play at all |
//!
//! ⚠️ **`millis` may exceed `span_ms`, and that is the point.** Two requests of one purpose
//! overlapping spend twice the wire time in half the wall clock; the gap between the two
//! columns *is* the concurrency, and a purpose where `millis ≈ span_ms` is one that ran
//! alone — the phase defect, visible as a number.
//!
//! # Nothing is inferred
//!
//! The ledger is told every request twice: once when the transport is handed it
//! ([`Ledger::dispatched`]) and once when it comes back ([`Ledger::settled`] or
//! [`Ledger::record`]). The difference is [`Ledger::dispatched`] minus the rows' own
//! total, and it must be zero — see the `unattributed` row the callers put on screen. A
//! settle that was never recorded, and a request still in the air when the run stopped,
//! both land there rather than quietly leaving the table short.

use std::collections::BTreeMap;

use crate::Settled;

/// What one purpose cost, summed over its requests.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Tally {
    /// How many requests went out.
    pub requests: u32,
    /// Bytes asked for, excluding multipart framing.
    pub bytes: u64,
    /// Σ of each request's dispatch → settle.
    pub millis: f64,
    /// The slowest single request of this purpose.
    pub worst_ms: f64,
    /// First dispatch and last settle, as the caller's clock read them.
    first_at: Option<f64>,
    last_at: Option<f64>,
}

impl Tally {
    /// First dispatch → last settle: how long this was in play. Zero before the first
    /// request, and **not** the sum of the requests' durations — see the module header.
    pub fn span_ms(&self) -> f64 {
        match (self.first_at, self.last_at) {
            (Some(first), Some(last)) => (last - first).max(0.0),
            _ => 0.0,
        }
    }

    /// Add one request to this tally.
    fn add(&mut self, bytes: u64, millis: f64, issued_at: f64, settled_at: f64) {
        self.requests += 1;
        self.bytes += bytes;
        self.millis += millis;
        self.worst_ms = self.worst_ms.max(millis);
        self.first_at = Some(self.first_at.map_or(issued_at, |at| at.min(issued_at)));
        self.last_at = Some(self.last_at.map_or(settled_at, |at| at.max(settled_at)));
    }

    /// Add another tally's totals in, recomputing the span from the two ends rather than
    /// adding it — a caller rolling rows up into one arm wants a span, not a sum.
    fn merge(&mut self, other: &Tally) {
        self.requests += other.requests;
        self.bytes += other.bytes;
        self.millis += other.millis;
        self.worst_ms = self.worst_ms.max(other.worst_ms);
        if let Some(at) = other.first_at {
            self.first_at = Some(self.first_at.map_or(at, |first| first.min(at)));
        }
        if let Some(at) = other.last_at {
            self.last_at = Some(self.last_at.map_or(at, |last| last.max(at)));
        }
    }
}

/// Every request the transport made, by purpose.
///
/// Not `Clone`, and that is deliberate: a copy would be a second count of one run, kept in
/// step by hand. The callers hold one and borrow it.
pub struct Ledger<P> {
    rows: BTreeMap<P, Tally>,
    all: Tally,
    dispatched: u32,
}

impl<P> Default for Ledger<P> {
    fn default() -> Self {
        Ledger {
            rows: BTreeMap::new(),
            all: Tally::default(),
            dispatched: 0,
        }
    }
}

impl<P: Ord + Clone> Ledger<P> {
    pub fn new() -> Ledger<P> {
        Ledger::default()
    }

    /// One request handed to the transport.
    ///
    /// ⚠️ Counted here as well as at settle, because the two are counted in different
    /// places: a request the transport was given and never recorded — a settle path that
    /// returned early, a read still in the air when the run gave up — is invisible to a
    /// ledger that only ever hears about answers. This is the count that makes it visible.
    pub fn dispatch(&mut self) {
        self.dispatched += 1;
    }

    /// One request that came back, straight from the driver's own [`Settled`].
    pub fn settled<K>(&mut self, purpose: P, settled: &Settled<K>) {
        self.record(purpose, settled.bytes, settled.millis, settled.issued_at);
    }

    /// One request that came back, from a transport that is not the queue's — a
    /// whole-object read whose length is not known before it arrives has no
    /// [`crate::Geometry`] and so cannot be planned, but it is still a request the run
    /// paid for.
    ///
    /// `millis` is dispatch → settle and `issued_at` is from the caller's clock, exactly
    /// as [`Settled`] carries them.
    pub fn record(&mut self, purpose: P, bytes: u64, millis: f64, issued_at: f64) {
        let settled_at = issued_at + millis;
        self.all.add(bytes, millis, issued_at, settled_at);
        self.rows
            .entry(purpose)
            .or_default()
            .add(bytes, millis, issued_at, settled_at);
    }

    /// Every row, in the purposes' own order — which is the order the enum declares, so a
    /// table is stable across runs.
    pub fn rows(&self) -> impl Iterator<Item = (&P, &Tally)> {
        self.rows.iter()
    }

    /// Every request that came back, whatever it was for.
    pub fn total(&self) -> &Tally {
        &self.all
    }

    /// Requests handed to the transport. ⚠️ Against this, **not against
    /// [`Ledger::total`]**, is where a caller's `unattributed` row is measured: a request
    /// dispatched and never recorded is in neither a row nor the total, and it is the one
    /// defect a table built from settles alone cannot show.
    pub fn dispatched(&self) -> u32 {
        self.dispatched
    }

    /// Whether anything went out at all.
    pub fn is_empty(&self) -> bool {
        self.dispatched == 0 && self.rows.is_empty()
    }

    /// The rows a filter keeps, added up — how a caller rolls purposes into an arm.
    ///
    /// ⚠️ **`span_ms` is recomputed from the two ends, never summed.** Two purposes that
    /// overlapped were in play at the same time, so adding their spans would report more
    /// wall clock than the run had: the interval table's defect, rebuilt by hand in the
    /// caller.
    pub fn total_of(&self, keep: impl Fn(&P) -> bool) -> Tally {
        let mut out = Tally::default();
        for (purpose, row) in &self.rows {
            if keep(purpose) {
                out.merge(row);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Geometry, Queue};
    use live_book::plan::{Shape, Want};

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum P {
        Index,
        Set,
    }

    /// `Ledger::record` without a `Settled`, for the rows a test writes by hand.
    fn row(ledger: &mut Ledger<P>, purpose: P, bytes: u64, millis: f64, at: f64) {
        ledger.dispatch();
        ledger.record(purpose, bytes, millis, at);
    }

    /// The sum is the sum, the straggler is the worst, and the span is the two ends —
    /// three different numbers from three overlapping requests of one purpose.
    #[test]
    fn a_row_sums_its_requests_and_keeps_the_straggler() {
        let mut ledger = Ledger::new();
        // Dispatched at 0, 100 and 200; settling at 400, 150 and 250.
        row(&mut ledger, P::Index, 10, 400.0, 0.0);
        row(&mut ledger, P::Index, 20, 50.0, 100.0);
        row(&mut ledger, P::Index, 30, 50.0, 200.0);

        let tally = ledger.rows().next().expect("one row").1;
        assert_eq!(tally.requests, 3);
        assert_eq!(tally.bytes, 60);
        assert_eq!(
            tally.millis, 500.0,
            "the sum over requests, not an interval"
        );
        assert_eq!(tally.worst_ms, 400.0, "the straggler, not the sum");
        assert_eq!(tally.span_ms(), 400.0, "first dispatch to last settle");
        assert!(
            tally.millis > tally.span_ms(),
            "three overlapping requests spend more wire than wall clock — that gap is \
             the concurrency, and summing spans would have hidden it"
        );
    }

    /// ⚠️ The defect the ledger exists to replace, stated as a test: a purpose's span is
    /// **not** the sum of its requests' durations.
    #[test]
    fn a_span_is_not_the_sum_of_its_requests() {
        let mut ledger = Ledger::new();
        // Four requests, all in flight together, all taking 100 ms.
        for i in 0..4 {
            row(&mut ledger, P::Set, 1, 100.0, i as f64 * 10.0);
        }
        let tally = ledger.rows().next().expect("one row").1;
        assert_eq!(tally.millis, 400.0);
        assert_eq!(
            tally.span_ms(),
            130.0,
            "0 → 30 dispatched, the last settling at 130"
        );
    }

    #[test]
    fn a_purpose_that_never_went_out_has_no_row() {
        let mut ledger = Ledger::new();
        row(&mut ledger, P::Index, 1, 1.0, 0.0);
        let rows: Vec<P> = ledger.rows().map(|(purpose, _)| *purpose).collect();
        assert_eq!(rows, vec![P::Index]);
        assert_eq!(ledger.total_of(|p| *p == P::Set).requests, 0);
    }

    /// A roll-up keeps the sum of the parts and the span of the whole.
    #[test]
    fn a_roll_up_takes_the_span_of_everything_it_keeps() {
        let mut ledger = Ledger::new();
        row(&mut ledger, P::Index, 1, 40.0, 0.0);
        row(&mut ledger, P::Index, 2, 10.0, 100.0);
        row(&mut ledger, P::Set, 4, 30.0, 50.0);

        let index = ledger.total_of(|p| *p == P::Index);
        assert_eq!(index.requests, 2);
        assert_eq!(index.bytes, 3);
        assert_eq!(index.millis, 50.0);
        assert_eq!(
            index.span_ms(),
            110.0,
            "0 → 40 dispatched, the second settling at 110"
        );

        let set = ledger.total_of(|p| *p == P::Set);
        assert_eq!(set.requests, 1);
        assert_eq!(set.span_ms(), 30.0);

        let all = ledger.total_of(|_| true);
        assert_eq!(all.requests, 3);
        assert_eq!(all.bytes, 7, "1 + 2 + 4");
        assert_eq!(
            all.span_ms(),
            110.0,
            "the arms' spans do not add up to the whole — 140 ms of spans over a 110 ms run"
        );
        assert_eq!(ledger.total().requests, all.requests);
    }

    /// ⚠️ The row that must read zero, and the reason it can read something else: a
    /// settle nobody recorded is invisible to every other count here.
    #[test]
    fn a_dispatched_request_that_no_row_accounts_for_is_visible() {
        let mut ledger = Ledger::new();
        ledger.dispatch();
        ledger.dispatch();
        ledger.record(P::Index, 10, 5.0, 0.0);

        assert_eq!(ledger.dispatched(), 2);
        assert_eq!(ledger.total().requests, 1);
        let attributed: u32 = ledger.rows().map(|(_, row)| row.requests).sum();
        assert_eq!(
            ledger.dispatched() - attributed,
            1,
            "one request went out and came back unrecorded"
        );
    }

    /// ⚠️ **The reading a run's rows turn on.** Thirteen requests issued together, each about
    /// half a second: the span is the slowest of them and the Σ is all thirteen. A row whose
    /// two numbers are equal ran alone; a row whose Σ dwarfs its span is that purpose's own
    /// requests overlapping — which is the whole point of dispatching them as one group.
    #[test]
    fn requests_issued_together_have_a_span_of_the_slowest_not_the_sum() {
        let mut ledger = Ledger::new();
        for i in 0..13 {
            ledger.dispatch();
            ledger.record(P::Index, 3_760, 490.0 + i as f64, 1_000.0);
        }
        let row = ledger.rows().next().expect("one row").1;
        assert_eq!(row.requests, 13);
        assert_eq!(
            row.millis, 6_448.0,
            "490 + 491 + … + 502: thirteen requests of about half a second"
        );
        assert_eq!(row.worst_ms, 502.0);
        assert_eq!(
            row.span_ms(),
            502.0,
            "issued together: the span is the slowest, not the sum"
        );
    }

    /// ⚠️ **A roll-up's span is a superset of the rows it keeps**, so an arm can never read
    /// smaller than one of its own rows. When a screen shows a purpose whose span exceeds the
    /// leg built from it, the row is where to look — the merge is min-of-issues to
    /// max-of-settles and cannot lose an interval.
    #[test]
    fn a_merged_span_contains_every_row_span_it_keeps() {
        let mut ledger = Ledger::new();
        row(&mut ledger, P::Index, 1, 22.0, 100.0);
        row(&mut ledger, P::Set, 1, 500.0, 1_000.0);
        row(&mut ledger, P::Index, 1, 30.0, 1_600.0);

        let index = ledger.total_of(|p| *p == P::Index);
        assert_eq!(index.span_ms(), 1_530.0, "100 → 1630");
        let all = ledger.total_of(|_| true);
        assert_eq!(all.span_ms(), 1_530.0, "100 → 1630");
        for (purpose, row) in ledger.rows() {
            assert!(
                all.span_ms() >= row.span_ms(),
                "the whole contains {purpose:?}: {} vs {}",
                all.span_ms(),
                row.span_ms()
            );
        }
    }

    /// A queue driven by hand, with every request accounted for — the reading that must
    /// hold in a run, so that a non-zero difference in one means something.
    #[test]
    fn a_driven_queue_leaves_nothing_unattributed() {
        let mut queue: Queue<P> = Queue::new(4, Shape::default(), || 0.0);
        queue.declare(P::Index, Geometry::whole(4_096)).unwrap();
        let ticket = queue
            .push(
                P::Index,
                Want {
                    offset: 0,
                    len: 100,
                },
            )
            .unwrap();

        let mut ledger = Ledger::new();
        for request in queue.take() {
            ledger.dispatch();
            let settled = queue.settle(request).expect("its own request");
            ledger.settled(settled.key, &settled);
            assert_eq!(settled.shares[0].ticket, ticket);
        }

        assert!(queue.is_idle());
        assert_eq!(ledger.total().requests, 1);
        assert_eq!(ledger.total().bytes, 100);
        assert_eq!(ledger.total().worst_ms, 0.0, "a clock that never moves");
        let attributed: u32 = ledger.rows().map(|(_, row)| row.requests).sum();
        assert_eq!(ledger.dispatched() - attributed, 0);
    }

    /// A clock that moves, so the ledger sees the driver's own timing rather than a
    /// constant — the seam `issued_at` on `Settled` exists for.
    #[test]
    fn the_ledger_places_a_request_on_the_runs_own_timeline() {
        use std::cell::Cell;
        use std::rc::Rc;

        // 5 ms per clock read: the driver reads it once at dispatch and once at settle.
        let now = Rc::new(Cell::new(1_000.0_f64));
        let clock = {
            let hand = now.clone();
            move || {
                let at = hand.get();
                hand.set(at + 5.0);
                at
            }
        };

        let mut queue: Queue<P> = Queue::new(1, Shape::default(), clock);
        queue.declare(P::Set, Geometry::whole(1_000)).unwrap();
        queue.push(P::Set, Want { offset: 0, len: 10 }).unwrap();

        let mut ledger = Ledger::new();
        for request in queue.take() {
            ledger.dispatch();
            let settled = queue.settle(request).unwrap();
            ledger.settled(settled.key, &settled);
            assert_eq!(settled.issued_at, 1_000.0);
            assert_eq!(settled.millis, 5.0);
        }

        let row = ledger.rows().next().expect("one row").1;
        assert_eq!(row.millis, 5.0);
        assert_eq!(row.worst_ms, 5.0);
        assert_eq!(row.span_ms(), 5.0, "issued at 1000, settled at 1005");
    }
}
