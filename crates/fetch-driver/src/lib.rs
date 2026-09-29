//! Readiness-driven dispatch for ranged object reads.
//!
//! A reader of a published artifact does not know its whole question up front. The
//! rows worth reading are discovered from the entries that answer the first read; a
//! window worth widening is discovered from a row that would not parse; a payload's
//! offset is discovered from the window that indexes it. The natural expression of
//! that is **phases** — plan everything for this stage, await it all, plan the next —
//! and a phase waits for its slowest item, so every boundary costs one round trip for
//! work that was computable long before the boundary arrived.
//!
//! This crate is the other shape. Work enters as **wants** (a byte range of a named
//! object) and leaves as **requests** (one object, ascending non-overlapping ranges,
//! coalesced, at most `Shape::spans`) the moment a slot is free:
//!
//! ```text
//!   push(key, want) ─▶ ready ─▶ take() ─▶ transport ─▶ settle() ─▶ continuations
//!                        ▲                                              │
//!                        └──────────────── push(more) ◀─────────────────┘
//! ```
//!
//! # The two rules that matter
//!
//! - **Never idle.** If a slot is free and anything is ready, [`Queue::take`] returns
//!   a request — however small the group. Waiting for a group to grow while the wire
//!   sits empty is the phase defect in miniature.
//! - **Never fragment.** While the wire is full, ready wants accumulate, so the request
//!   that goes out when a slot frees carries everything that became ready meanwhile.
//!   Batching is a **dispatch-time** decision, never a creation-time one.
//!
//! That is why there is no timer here, and must not be: a flush-on-timeout would need
//! one, and a timer buys nothing that dispatch-on-a-free-slot does not already give.
//!
//! # The budget is in-flight **requests**, and it is exact
//!
//! `budget` bounds requests taken and not yet settled, never wants: a group that needs
//! more requests than the slots free is **cut at the budget**, and the wants the cut
//! carried are re-queued with their tickets still attached. So a cut want comes back
//! across the requests it was cut into — as does one that crosses a part boundary — and
//! a caller assembles a ticket by collecting its pieces and ordering them by `part`.
//!
//! # The clock is injected, because no source works everywhere
//!
//! `Instant::now` does not exist on `wasm32-unknown-unknown`; a Worker and a test have
//! their own answers; and a wall clock read inside a queue is a test that cannot be
//! written deterministically. So the caller passes a [`Clock`], and the only thing it
//! is used for is the dispatch→settle time carried on [`Settled`] — which is what the
//! per-purpose cost table is built from.
//!
//! # What this crate does not do
//!
//! It does not fetch, and it knows nothing about HTTP: the caller owns the transport,
//! the object naming, and the continuations (what a returned range *means*). It does
//! not plan either — [`live_book::plan`] does, and is driven here rather than
//! reimplemented, so the 299-range cap exists in one place.
//!
//! # Geometry
//!
//! Wants are in **file** coordinates. A storage handle may be one object or many parts
//! of one logical file (`rows.bin/00007.bin`), so an object is declared with its
//! [`Geometry`] first and each [`Request`] names the `part` its key addresses. A caller
//! with no part scheme passes `part_size = file_len` and every request is part 0.
//!
//! ```
//! use fetch_driver::{Geometry, Queue};
//! use live_book::plan::{Shape, Want};
//!
//! # fn main() -> Result<(), fetch_driver::Error> {
//! let len = 4_096;
//! // The clock is the caller's: this one never moves, which is all a doctest needs.
//! let mut queue = Queue::new(8, Shape::default(), || 0.0);
//! queue.declare("rows.bin", Geometry::whole(len))?;
//!
//! let ticket = queue.push("rows.bin", Want { offset: 1_024, len: 166 })?;
//!
//! for request in queue.take() {
//!     // `request.key` and `request.batch.part` name the object; this is what to send.
//!     let _range = request.range_header();
//!     // …fetch it, then give the slot back and read where the bytes landed…
//!     for share in queue.settle(request)?.shares {
//!         assert_eq!(share.ticket, ticket);
//!         let _ = share.pieces;
//!     }
//! }
//! assert!(queue.is_idle());
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;
use std::fmt;

use live_book::plan::{Batch, PartSize, Shape, Want, plan};

/// Identifies one want, so a returned range can be placed back in the caller's own
/// state without this crate knowing anything about that state.
///
/// Tickets are unique within a queue and never reused, and they travel with the want:
/// a group is planned, split and dispatched, and the caller still knows which of *its*
/// questions each range answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ticket(u64);

/// Where an object's bytes live inside its file.
///
/// Two numbers that are adjacent and must not be swapped, so they arrive as a struct
/// rather than as two `u64` parameters — the same argument `plan`'s [`PartSize`] makes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Geometry {
    /// The size of one storage handle (`rows.bin/00007.bin`), **not** the file's.
    pub part_size: PartSize,
    /// The whole file, so the last part is clamped rather than addressed past its end.
    pub file_len: u64,
}

impl Geometry {
    /// One object holding the whole file — the caller with no part scheme.
    pub fn whole(len: u64) -> Geometry {
        Geometry {
            part_size: PartSize(len.max(1)),
            file_len: len,
        }
    }
}

/// Where one ticket's bytes are, in a request that has come back.
///
/// `part` and `span` are indices into the storage handle and its range list; `within`
/// and `len` slice that part's bytes. A want cut at a part boundary yields **one piece
/// in each part**, so a caller assembling a ticket collects its pieces and orders them
/// by `part` — which is why `part` is here rather than read off the request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Piece {
    /// Which part of the caller's file — 0 for a caller with no part scheme.
    pub part: u64,
    /// Index into the request's `batch.spans`, and so into the response's parts **in
    /// the order they were asked for**.
    pub span: usize,
    /// How far into that span's bytes this piece starts.
    pub within: u64,
    pub len: u64,
}

/// One ticket's share of a request that went out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Share {
    pub ticket: Ticket,
    /// In file order — concatenating them in this order reconstructs the want.
    pub pieces: Vec<Piece>,
}

/// One request to make: one key, one part, the ranges to ask for.
///
/// The caller supplies the address; this crate does not know how a key and a part
/// become a URL, which is what keeps it usable by a reader of the book, a reader of
/// the chunk mirror, and a test.
///
/// ⚠️ **Deliberately not `Clone`.** [`Queue::settle`] consumes a request, and one that
/// could be copied would be two slots returned for one take — so the compiler is what
/// stops a request being settled twice, and the in-flight count cannot drift below
/// what was handed out.
#[derive(Debug, PartialEq)]
pub struct Request<K> {
    pub key: K,
    /// What to ask for. `batch.part` is the storage handle, `batch.spans` the ranges.
    pub batch: Batch,
    /// One entry per ticket this request carries, in the order the wants were pushed.
    /// A ticket appears in more than one request when its want crossed a part boundary,
    /// or when the budget cut the group it was in.
    pub shares: Vec<Share>,
    /// When it went out, from the injected clock.
    issued_at: f64,
}

impl<K> Request<K> {
    /// The `Range` header this request wants — ascending and non-overlapping by
    /// construction, which the protocol requires: out of order degrades to the whole
    /// object.
    pub fn range_header(&self) -> String {
        self.batch.range_header()
    }

    /// Bytes this request will return, excluding multipart framing.
    pub fn bytes(&self) -> u64 {
        self.batch.bytes()
    }
}

/// What one settled request cost, and what it carried.
///
/// ⚠️ **This is the record a cost table is built from**: one per request, naming the
/// object it went to, the bytes it asked for, and how long the wire held the slot. The
/// caller adds its own purpose (it knows why it wanted those bytes) and the transport's
/// own outcome (status, framing, received bytes).
#[derive(Clone, Debug, PartialEq)]
pub struct Settled<K> {
    pub key: K,
    /// Which storage handle — with `key`, the object that was addressed.
    pub part: u64,
    /// Bytes asked for, excluding multipart framing.
    pub bytes: u64,
    /// Dispatch → settle, from the injected [`Clock`].
    pub millis: f64,
    /// Where each ticket's bytes are.
    pub shares: Vec<Share>,
}

/// Where the queue's clock comes from.
///
/// Injected because no one source works everywhere: `Instant` does not exist on
/// `wasm32-unknown-unknown`, a browser's answer is `performance.now()`, a Worker's is
/// its own, and a test's is a number the test moves. Any `Fn() -> f64` is a clock, so
/// the common cases are a closure: `|| api::now_ms()`.
///
/// Units are milliseconds and the origin is the caller's business; the queue only ever
/// subtracts two readings.
pub trait Clock {
    fn now_ms(&self) -> f64;
}

impl<F: Fn() -> f64> Clock for F {
    fn now_ms(&self) -> f64 {
        self()
    }
}

/// Why a want could not be queued.
///
/// Every variant is a caller bug rather than a runtime condition: a want for an object
/// whose geometry was never declared, a want with no bytes in it, a want that begins
/// past the end of the file, or a second declaration of one key that disagrees with the
/// first. They are values rather than panics because the callers are `async` and a panic
/// in a wasm task is not a way to report a book that will not parse.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// No geometry for this key. A want cannot be placed without one, and guessing a
    /// part size is how a reader fetches plausible bytes from the wrong offsets.
    UnknownObject,
    /// A want of zero bytes can never be satisfied, so a ticket for it is a caller
    /// waiting forever for a range no server will send.
    Empty,
    /// The want begins at or past the file's end. A want that merely *runs past* the
    /// end is not this: it is clamped, which is what a speculative window at the last
    /// row of an arena needs.
    PastEnd,
    /// The key was declared again with a different geometry. Two readers disagreeing
    /// about where a part ends is a wrong-bytes bug, not a reconfiguration.
    Redeclared,
    /// A request was settled into a queue that never issued it. ⚠️ The one hole the type
    /// system cannot close: tying `Request` to its queue by lifetime makes the natural
    /// loop — take several, await them, settle them one by one — uncomputable, because
    /// the requests hold the borrow that `settle` needs. See [`Queue::settle`].
    ForeignRequest,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnknownObject => write!(f, "no geometry declared for this object"),
            Error::Empty => write!(f, "a want of zero bytes can never be satisfied"),
            Error::PastEnd => write!(f, "the want begins past the end of the file"),
            Error::Redeclared => {
                write!(f, "the object was declared again with a different geometry")
            }
            Error::ForeignRequest => write!(
                f,
                "the request was settled into a queue that had nothing in flight"
            ),
        }
    }
}

impl std::error::Error for Error {}

/// A queue of wants, and the budget of requests the caller allows in flight.
///
/// ⚠️ **Deliberately not `Clone`.** A copy would hand out the same tickets twice, from
/// two in-flight counts, which is the whole guarantee this type exists to keep. `Debug`
/// is written out rather than derived so a log line is the state of the run and not
/// every want in it.
pub struct Queue<K> {
    objects: BTreeMap<K, Geometry>,
    /// Wants waiting to be dispatched, per key, **in push order** — the order the
    /// caller's tickets are indexed by, and so the order [`Request::shares`] is in.
    ready: BTreeMap<K, Vec<(Ticket, Want)>>,
    shape: Shape,
    budget: usize,
    in_flight: usize,
    next_ticket: u64,
    clock: Box<dyn Clock>,
}

impl<K: Ord + Clone> Queue<K> {
    /// `budget` is how many **requests** may be in flight at once — the caller's
    /// fan-out, which is a property of the transport (a browser multiplexes differently
    /// from a Worker) rather than of this crate. It is a bound, and it is respected
    /// exactly: see [`Queue::take`].
    ///
    /// `shape` is the batching policy handed to [`live_book::plan`]: the coalescing gap
    /// and the ranges-per-request cap. [`Shape::default`] fills to the protocol's 299
    /// because that was measured fastest on this substrate; it stays a knob because that
    /// ranking belongs to one origin.
    ///
    /// `clock` is the caller's — see [`Clock`]. The queue only reads it to time a
    /// request, and never to schedule.
    pub fn new(budget: usize, shape: Shape, clock: impl Clock + 'static) -> Queue<K> {
        Queue {
            objects: BTreeMap::new(),
            ready: BTreeMap::new(),
            shape,
            budget: budget.max(1),
            in_flight: 0,
            next_ticket: 0,
            clock: Box::new(clock),
        }
    }

    /// How the queue batches. Swapping it re-plans what is **not yet dispatched**;
    /// ranges already out are unaffected.
    pub fn set_shape(&mut self, shape: Shape) {
        self.shape = shape;
    }

    /// Declare an object before pushing wants for it. Declaring the same geometry twice
    /// is a no-op, because two code paths may both need to know an object exists;
    /// declaring a different one is [`Error::Redeclared`].
    pub fn declare(&mut self, key: K, geometry: Geometry) -> Result<(), Error> {
        match self.objects.get(&key) {
            Some(known) if *known != geometry => Err(Error::Redeclared),
            Some(_) => Ok(()),
            None => {
                self.objects.insert(key, geometry);
                Ok(())
            }
        }
    }

    /// Queue one want, in file coordinates, and take its ticket.
    ///
    /// The ticket is the caller's handle on the answer: it comes back in
    /// [`Share::ticket`] when the bytes do, whatever route they took through the
    /// planner.
    pub fn push(&mut self, key: K, want: Want) -> Result<Ticket, Error> {
        let Some(geometry) = self.objects.get(&key).copied() else {
            return Err(Error::UnknownObject);
        };
        if want.len == 0 {
            return Err(Error::Empty);
        }
        if want.offset >= geometry.file_len {
            return Err(Error::PastEnd);
        }
        let ticket = Ticket(self.next_ticket);
        self.next_ticket += 1;
        self.ready.entry(key).or_default().push((ticket, want));
        Ok(ticket)
    }

    /// Every request the caller may start now, for every slot it has free — and never
    /// more than it has free, because the budget is in-flight **requests**.
    ///
    /// Groups are dispatched **largest first** — by the bytes still wanted for the key
    /// — because with a bounded number of slots the choice is which group is worth the
    /// wire; a tie is broken by key so a run is reproducible.
    ///
    /// ⚠️ **A group that needs more requests than there are free slots is cut at the
    /// budget, and the wants the cut carried go back on the queue with their tickets
    /// still attached.** They keep their place in the run: a ticket the cut touched comes
    /// back across the requests it was cut into, exactly as one that crossed a part
    /// boundary does, and the caller assembles it by ordering its pieces by `part`. The
    /// alternative — issuing the group whole — is what makes a budget stop being one.
    pub fn take(&mut self) -> Vec<Request<K>> {
        let mut out = Vec::new();
        while self.in_flight < self.budget {
            let Some(key) = self.largest_ready() else {
                break;
            };
            let geometry = self.objects[&key];
            let waiting = self.ready.remove(&key).expect("the key came from `ready`");
            let wants: Vec<Want> = waiting.iter().map(|(_, want)| *want).collect();

            let planned = plan(&wants, geometry.part_size, geometry.file_len, self.shape);
            // How many of this group's requests the budget has room for. At least one:
            // the loop only gets here with a slot free, and an empty `take` while work is
            // ready and a slot is free is the one state this queue exists to avoid.
            let free = self.budget - self.in_flight;
            let cut = free.min(planned.batches.len());

            // Whatever the cut left behind goes back, trimmed to the ranges that were
            // not dispatched — the issued half of a straddling want is already on its way
            // to the caller, so re-queuing the whole want would fetch it twice.
            let put_back = &planned.located;
            self.in_flight += cut;
            for (i, (ticket, _)) in waiting.iter().enumerate() {
                for segment in put_back[i].segments.iter().filter(|seg| seg.batch >= cut) {
                    let batch = &planned.batches[segment.batch];
                    let span = batch.spans[segment.span];
                    self.ready.entry(key.clone()).or_default().push((
                        *ticket,
                        Want {
                            offset: batch.part * geometry.part_size.get()
                                + span.start
                                + segment.within,
                            len: segment.len,
                        },
                    ));
                }
            }

            for (at, batch) in planned.batches.iter().enumerate().take(cut) {
                let shares = waiting
                    .iter()
                    .enumerate()
                    .filter_map(|(i, (ticket, _))| {
                        let pieces: Vec<Piece> = planned.located[i]
                            .segments
                            .iter()
                            .filter(|segment| segment.batch == at)
                            .map(|segment| Piece {
                                part: batch.part,
                                span: segment.span,
                                within: segment.within,
                                len: segment.len,
                            })
                            .collect();
                        (!pieces.is_empty()).then_some(Share {
                            ticket: *ticket,
                            pieces,
                        })
                    })
                    .collect();
                out.push(Request {
                    key: key.clone(),
                    batch: batch.clone(),
                    shares,
                    issued_at: self.clock.now_ms(),
                });
            }
        }
        out
    }

    /// Give a slot back and take the request apart, with what it cost.
    ///
    /// One call for both, because a slot that came back without its shares is a request
    /// whose bytes nobody looked at. A read that **failed** settles too — the slot is the
    /// caller's either way — and the caller reports the tickets it could not fill from
    /// [`Share::ticket`] rather than leaving them unaccounted for.
    ///
    /// The wall clock comes from the injected [`Clock`] and is measured here rather than
    /// by the caller so a caller cannot forget it, and so the same number is the one the
    /// cost table shows.
    ///
    /// # Errors
    ///
    /// [`Error::ForeignRequest`] when nothing is in flight, which is a request that came
    /// from somewhere else. ⚠️ **Settling a request into a *busy* queue it did not come
    /// from is not caught** — the count is decremented one too many and the budget is off
    /// by one for the rest of the run. That residue is the price of the loop shape this
    /// crate exists for; the alternative, a queue-session with an invariant brand, cannot
    /// be held across an `.await`.
    pub fn settle(&mut self, request: Request<K>) -> Result<Settled<K>, Error> {
        if self.in_flight == 0 {
            return Err(Error::ForeignRequest);
        }
        let millis = self.clock.now_ms() - request.issued_at;
        // No underflow and no guard: `Request` is not `Clone`, so this queue handed out
        // each of its slots at most once and `in_flight` counts exactly what is out.
        self.in_flight -= 1;
        Ok(Settled {
            key: request.key,
            part: request.batch.part,
            bytes: request.batch.bytes(),
            millis,
            shares: request.shares,
        })
    }

    /// Wants queued and not yet dispatched — including the ones a cut put back.
    pub fn waiting(&self) -> usize {
        self.ready.values().map(Vec::len).sum()
    }

    /// Requests taken and not yet settled.
    pub fn in_flight(&self) -> usize {
        self.in_flight
    }

    /// The bound `in_flight` is held to.
    pub fn budget(&self) -> usize {
        self.budget
    }

    /// Nothing ready and nothing out: the run is over, so a driver's loop can stop.
    pub fn is_idle(&self) -> bool {
        self.in_flight == 0 && self.ready.is_empty()
    }

    /// The key with the most bytes still wanted — the group worth the wire when slots
    /// are scarce. Ties go to the smallest key so a run is reproducible.
    fn largest_ready(&self) -> Option<K> {
        self.ready
            .iter()
            .map(|(key, wants)| {
                let bytes: u64 = wants.iter().map(|(_, want)| want.len).sum();
                (bytes, key)
            })
            .max_by(|(a_bytes, a_key), (b_bytes, b_key)| {
                a_bytes.cmp(b_bytes).then_with(|| b_key.cmp(a_key))
            })
            .map(|(_, key)| key.clone())
    }
}

impl<K: Ord + Clone> fmt::Debug for Queue<K> {
    /// The state of the run, not every want in it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Queue")
            .field("objects", &self.objects.len())
            .field("waiting", &self.waiting())
            .field("in_flight", &self.in_flight)
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use live_book::plan::MAX_SPANS;
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::rc::Rc;

    /// A clock the test drives, so a timing assertion is a number the test wrote rather
    /// than one the runner happened to produce.
    #[derive(Clone, Default)]
    struct Script(Rc<Cell<f64>>);

    impl Script {
        fn advance(&self, ms: f64) {
            self.0.set(self.0.get() + ms);
        }

        /// The `impl Clock` a queue takes: a closure over the same cell.
        fn clock(&self) -> impl Clock + 'static {
            let hand = self.0.clone();
            move || hand.get()
        }
    }

    /// A file whose byte at `i` is `i % 251`, so a range of bytes says where it came
    /// from without a fixture anyone has to trust.
    fn file(len: u64) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn want(offset: u64, len: u64) -> Want {
        Want { offset, len }
    }

    /// One request the driver issued, as the wire saw it: which object, which part, and
    /// the ranges asked for.
    struct Issued {
        key: &'static str,
        part: u64,
        spans: Vec<(u64, u64)>,
    }

    /// A driver over a queue, standing in for the transport: it reads each request out
    /// of the declared files, checks every invariant a real server would punish, and
    /// reassembles each ticket's bytes.
    ///
    /// This is the test surface the crate exists to make possible — the loop a browser
    /// or a Worker would run, with the wire replaced by a `Vec`.
    struct Harness {
        queue: Queue<&'static str>,
        files: BTreeMap<&'static str, Vec<u8>>,
        part_sizes: BTreeMap<&'static str, u64>,
        /// Per ticket, one chunk per piece, tagged with the piece's part so a straddling
        /// want concatenates in part order however the settles were interleaved.
        answers: BTreeMap<Ticket, Vec<(u64, Vec<u8>)>>,
        /// One entry per request the driver issued, in dispatch order.
        requests: Vec<Issued>,
    }

    impl Harness {
        fn new(budget: usize, shape: Shape) -> Harness {
            Harness {
                queue: Queue::new(budget, shape, Script::default().clock()),
                files: BTreeMap::new(),
                part_sizes: BTreeMap::new(),
                answers: BTreeMap::new(),
                requests: Vec::new(),
            }
        }

        /// Declare an object of `len` bytes in parts of `part_size` (0 = one part).
        fn object(&mut self, key: &'static str, len: u64, part_size: u64) {
            let size = if part_size == 0 {
                len.max(1)
            } else {
                part_size
            };
            self.files.insert(key, file(len));
            self.part_sizes.insert(key, size);
            self.queue
                .declare(
                    key,
                    Geometry {
                        part_size: PartSize(size),
                        file_len: len,
                    },
                )
                .expect("a fresh declaration");
        }

        fn push(&mut self, key: &'static str, want: Want) -> Ticket {
            self.queue.push(key, want).expect("a declared object")
        }

        /// One turn of a caller's loop: take what is free, fetch it, settle it, keep the
        /// bytes. Returns how many requests went out, so a caller can assert on the turn.
        fn step(&mut self) -> usize {
            let requests = self.queue.take();
            assert!(
                self.queue.in_flight() <= self.queue.budget(),
                "{} requests in flight against a budget of {}",
                self.queue.in_flight(),
                self.queue.budget()
            );
            assert!(
                !requests.is_empty(),
                "nothing was taken while work was ready and slots were free, so the \
                 loop would spin: waiting={} in_flight={}",
                self.queue.waiting(),
                self.queue.in_flight()
            );
            let started = requests.len();
            self.deliver(requests);
            started
        }

        /// Fetch these requests and settle them: the transport, with the wire replaced by
        /// a `Vec`, plus the checks a real server would punish.
        fn deliver(&mut self, requests: Vec<Request<&'static str>>) {
            for request in requests {
                self.check(&request);
                let bytes = self.files[request.key].clone();
                let part_size = self.part_sizes[request.key];
                // What the wire would return, in span order: the bytes of each range.
                let returned: Vec<Vec<u8>> = request
                    .batch
                    .spans
                    .iter()
                    .map(|span| {
                        let start = (request.batch.part * part_size + span.start) as usize;
                        let end = (request.batch.part * part_size + span.end) as usize;
                        bytes[start..=end].to_vec()
                    })
                    .collect();
                self.requests.push(Issued {
                    key: request.key,
                    part: request.batch.part,
                    spans: request
                        .batch
                        .spans
                        .iter()
                        .map(|span| (span.start, span.end))
                        .collect(),
                });
                for share in self
                    .queue
                    .settle(request)
                    .expect("a settle from this queue")
                    .shares
                {
                    for piece in &share.pieces {
                        let slice = &returned[piece.span];
                        let from = piece.within as usize;
                        let to = from + piece.len as usize;
                        assert!(to <= slice.len(), "a piece past the range it came from");
                        self.answers
                            .entry(share.ticket)
                            .or_default()
                            .push((piece.part, slice[from..to].to_vec()));
                    }
                }
            }
        }

        /// Run to idle exactly as a caller's loop would.
        fn run(&mut self) {
            let mut guard = 0;
            while !self.queue.is_idle() {
                guard += 1;
                assert!(guard < 10_000, "the driver never drained");
                let _ = self.step();
            }
        }

        /// Every invariant the protocol punishes, checked on every request issued.
        fn check(&self, request: &Request<&'static str>) {
            let spans = &request.batch.spans;
            assert!(!spans.is_empty(), "a request with no ranges");
            assert!(
                spans.len() <= MAX_SPANS,
                "{} ranges in one request: 300 returns the whole object",
                spans.len()
            );
            assert!(
                spans.len() <= self.queue.shape.spans(),
                "{} ranges, past the shape's cap of {}",
                spans.len(),
                self.queue.shape.spans()
            );
            for pair in spans.windows(2) {
                assert!(
                    pair[0].end < pair[1].start,
                    "ranges {}..{} and {}..{} are not ascending and disjoint",
                    pair[0].start,
                    pair[0].end,
                    pair[1].start,
                    pair[1].end
                );
            }
            let part_size = self.part_sizes[request.key];
            for span in spans {
                assert!(
                    span.end < part_size,
                    "range {}..{} runs past the {part_size}-byte part it is addressed in",
                    span.start,
                    span.end,
                );
            }
        }

        /// The bytes a ticket asked for, in part order.
        fn bytes(&self, ticket: Ticket) -> Vec<u8> {
            let mut chunks = self
                .answers
                .get(&ticket)
                .unwrap_or_else(|| panic!("ticket {ticket:?} was never answered"))
                .clone();
            chunks.sort_by_key(|(part, _)| *part);
            chunks.into_iter().flat_map(|(_, bytes)| bytes).collect()
        }
    }

    #[test]
    fn a_want_comes_back_as_the_bytes_it_asked_for() {
        let mut h = Harness::new(4, Shape::default());
        h.object("rows.bin", 4_096, 0);
        let a = h.push("rows.bin", want(1_024, 166));
        let b = h.push("rows.bin", want(2_000, 64));
        h.run();
        assert_eq!(h.bytes(a), file(4_096)[1_024..1_024 + 166].to_vec());
        assert_eq!(h.bytes(b), file(4_096)[2_000..2_000 + 64].to_vec());
    }

    #[test]
    fn wants_closer_than_the_gap_ride_one_request() {
        let mut h = Harness::new(8, Shape::default());
        h.object("rows.bin", 4_096, 0);
        let a = h.push("rows.bin", want(100, 40));
        let b = h.push("rows.bin", want(160, 40));
        let c = h.push("rows.bin", want(230, 40));
        h.run();
        assert_eq!(h.requests.len(), 1, "one group, one request");
        assert_eq!(
            h.requests[0].spans.len(),
            1,
            "the gap merged all three ranges into one"
        );
        assert_eq!(h.bytes(a), file(4_096)[100..140].to_vec());
        assert_eq!(h.bytes(b), file(4_096)[160..200].to_vec());
        assert_eq!(h.bytes(c), file(4_096)[230..270].to_vec());
    }

    #[test]
    fn a_want_that_crosses_a_part_boundary_comes_back_from_both_parts() {
        let mut h = Harness::new(4, Shape::default());
        h.object("rows.bin", 4_096, 1_000);
        let straddler = h.push("rows.bin", want(950, 100));
        h.run();
        let parts: Vec<u64> = h.requests.iter().map(|issued| issued.part).collect();
        assert_eq!(parts, vec![0, 1], "one request in each part");
        assert_eq!(
            h.bytes(straddler),
            file(4_096)[950..1_050].to_vec(),
            "and the pieces concatenate in part order"
        );
    }

    #[test]
    fn a_group_wider_than_the_cap_is_split_without_losing_a_want() {
        let shape = Shape {
            gap: 0,
            max_spans: 16,
        };
        let mut h = Harness::new(64, shape);
        h.object("rows.bin", 1_000_000, 0);
        let tickets: Vec<Ticket> = (0..100)
            .map(|i| h.push("rows.bin", want(i * 1_000, 10)))
            .collect();
        h.run();
        assert!(
            h.requests.iter().all(|issued| issued.spans.len() <= 16),
            "no request may exceed the cap"
        );
        assert!(
            h.requests.len() >= 7,
            "100 ranges at 16 to a request is at least 7 requests, got {}",
            h.requests.len()
        );
        for (i, ticket) in tickets.iter().enumerate() {
            let start = i * 1_000;
            assert_eq!(
                h.bytes(*ticket),
                file(1_000_000)[start..start + 10].to_vec(),
                "ticket {i} lost its bytes in the split"
            );
        }
    }

    #[test]
    fn the_largest_group_goes_first_when_slots_are_scarce() {
        let shape = Shape {
            gap: 0,
            max_spans: MAX_SPANS,
        };
        let mut h = Harness::new(1, shape);
        h.object("small.bin", 1_000_000, 0);
        h.object("large.bin", 1_000_000, 0);
        // One small want, and three large ones far enough apart to stay separate.
        h.push("small.bin", want(0, 100));
        for i in 0..3u64 {
            h.push("large.bin", want(i * 100_000, 1_000));
        }
        h.run();
        assert_eq!(
            h.requests[0].key, "large.bin",
            "the group worth the most bytes is dispatched first"
        );
        assert_eq!(
            h.requests[0].spans.len(),
            3,
            "and it carries all three of its ranges"
        );
    }

    #[test]
    fn the_wire_is_never_idle_while_a_slot_is_free() {
        let mut h = Harness::new(8, Shape::default());
        h.object("a.bin", 1_000, 0);
        h.push("a.bin", want(0, 10));
        let taken = h.queue.take();
        assert_eq!(taken.len(), 1, "one want, one slot free, one request");
        assert_eq!(h.queue.in_flight(), 1);
        // Nothing left to take — and that is `waiting()` being zero, not the budget.
        assert!(h.queue.take().is_empty());
        assert_eq!(h.queue.waiting(), 0);
        for request in taken {
            h.queue.settle(request).expect("this queue issued it");
        }
        assert!(h.queue.is_idle());
    }

    #[test]
    fn work_that_arrives_while_the_wire_is_full_rides_the_next_request() {
        let mut h = Harness::new(1, Shape::default());
        h.object("rows.bin", 1_000_000, 0);
        h.push("rows.bin", want(0, 10));
        let first = h.queue.take();
        assert_eq!(first.len(), 1);
        assert_eq!(h.queue.in_flight(), 1, "the budget is spent");

        // The wire is full. Three more wants arrive, close enough to coalesce into one
        // range — they must ride the request that goes out when the slot frees, not
        // fragment into one request each.
        h.push("rows.bin", want(20, 10));
        h.push("rows.bin", want(120, 10));
        h.push("rows.bin", want(220, 10));
        assert!(h.queue.take().is_empty(), "no slot, no request");

        for request in first {
            h.queue.settle(request).expect("this queue issued it");
        }
        let second = h.queue.take();
        assert_eq!(second.len(), 1, "one request for the three of them");
        assert_eq!(
            second[0].shares.len(),
            3,
            "carrying every ticket that waited"
        );
        assert_eq!(
            second[0].batch.spans.len(),
            1,
            "and they coalesced while the wire was busy"
        );
    }

    #[test]
    fn the_declared_shape_is_what_the_wire_sees() {
        let shape = Shape {
            gap: 0,
            max_spans: 2,
        };
        let mut h = Harness::new(4, shape);
        h.object("rows.bin", 10_000, 0);
        h.push("rows.bin", want(0, 10));
        h.push("rows.bin", want(500, 10));
        h.push("rows.bin", want(1_000, 10));
        let taken = h.queue.take();
        assert_eq!(taken.len(), 2, "three ranges at two to a request");
        assert_eq!(taken[0].batch.spans.len(), 2);
        assert_eq!(taken[1].batch.spans.len(), 1);
    }

    #[test]
    fn a_want_that_cannot_be_fetched_is_refused_rather_than_queued() {
        let mut h = Harness::new(1, Shape::default());
        h.object("rows.bin", 1_000, 0);
        assert_eq!(
            h.queue.push("nowhere.bin", want(0, 10)),
            Err(Error::UnknownObject)
        );
        assert_eq!(h.queue.push("rows.bin", want(0, 0)), Err(Error::Empty));
        assert_eq!(
            h.queue.push("rows.bin", want(1_000, 10)),
            Err(Error::PastEnd)
        );
        // A want that merely RUNS past the end is clamped, not refused: a speculative
        // window at the last row of an arena does exactly this.
        let last = h.push("rows.bin", want(990, 64));
        h.run();
        assert_eq!(h.bytes(last), file(1_000)[990..1_000].to_vec());
    }

    #[test]
    fn the_budget_bounds_requests_in_flight_not_wants() {
        // One range per request, so the group is five requests wide against a budget of
        // two — the case a whole-group dispatch would blow through.
        let shape = Shape {
            gap: 0,
            max_spans: 1,
        };
        let mut h = Harness::new(2, shape);
        h.object("rows.bin", 1_000_000, 0);
        let tickets: Vec<Ticket> = (0..5)
            .map(|i| h.push("rows.bin", want(i * 10_000, 16)))
            .collect();

        let first = h.queue.take();
        assert_eq!(first.len(), 2, "only the slots that were free");
        assert_eq!(h.queue.in_flight(), 2);
        assert_eq!(
            h.queue.waiting(),
            3,
            "and the rest are still queued, not dropped"
        );
        h.deliver(first);
        h.run();
        for (i, ticket) in tickets.iter().enumerate() {
            let start = i * 10_000;
            assert_eq!(
                h.bytes(*ticket),
                file(1_000_000)[start..start + 16].to_vec()
            );
        }
    }

    #[test]
    fn a_want_the_budget_cut_comes_back_across_the_requests_it_was_cut_into() {
        // The same straddling want as above, but with one slot: the budget cuts the
        // group between its parts, and the ticket must still assemble — which is what
        // re-queuing the untouched remainder is for.
        let mut h = Harness::new(1, Shape::default());
        h.object("rows.bin", 4_096, 1_000);
        let straddler = h.push("rows.bin", want(950, 100));
        h.run();
        let parts: Vec<u64> = h.requests.iter().map(|issued| issued.part).collect();
        assert_eq!(parts, vec![0, 1]);
        assert_eq!(
            h.bytes(straddler),
            file(4_096)[950..1_050].to_vec(),
            "a cut is not a lost piece"
        );
    }

    #[test]
    fn a_request_reports_the_wall_clock_between_dispatch_and_settle() {
        let script = Script::default();
        let mut queue: Queue<&str> = Queue::new(1, Shape::default(), script.clock());
        queue.declare("rows.bin", Geometry::whole(1_000)).unwrap();
        queue.push("rows.bin", want(0, 10)).unwrap();

        let request = queue.take().pop().expect("one want, one slot, one request");
        script.advance(37.0);
        let settled = queue.settle(request).expect("this queue issued it");

        assert_eq!(settled.key, "rows.bin");
        assert_eq!(settled.part, 0);
        assert_eq!(settled.bytes, 10);
        assert_eq!(settled.millis, 37.0, "the injected clock, not a real one");
        assert_eq!(settled.shares.len(), 1);
    }

    #[test]
    fn declaring_the_same_object_twice_is_a_no_op_but_moving_it_is_not() {
        let mut queue: Queue<&str> = Queue::new(1, Shape::default(), || 0.0);
        queue.declare("rows.bin", Geometry::whole(1_000)).unwrap();
        queue
            .declare("rows.bin", Geometry::whole(1_000))
            .expect("the same geometry twice");
        assert_eq!(
            queue.declare("rows.bin", Geometry::whole(2_000)),
            Err(Error::Redeclared)
        );
    }
}
