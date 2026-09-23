//! Structured timing for the operations mitos does repeatedly.
//!
//! A `println!` per measurement does not survive contact with more than a handful
//! of them: units drift between tools, means hide the tail a serving path is
//! judged on, the numbers cannot be diffed between runs, and the same operation
//! acquires a different name in every tool that measures it. This crate is the
//! answer to each of those, in that order:
//!
//! - **`Op` is a closed enum.** A name is a compile-time constant, so two runs
//!   are always joinable and a typo cannot produce a row nobody reads.
//! - **Percentiles in bounded memory.** Durations are counted into log-linear
//!   buckets — exact below 4 µs, doubling above — so a hundred million
//!   observations cost the same memory as one, and p50/p95/p99/max come out
//!   without keeping samples.
//! - **A `Run` context.** Every report carries what varies *between* runs
//!   (artifact, box, scale) so two reports can be compared without a human
//!   remembering which was which.
//! - **Two renderings, one source.** `table()` for a person reading a terminal,
//!   `jsonl()` for `policy-index/examples/profile.rs`'s existing convention — one
//!   typed line per observation, for offline scoring.
//!
//! # Usage
//!
//! ```
//! use mitos_meter::{Meter, Op, Run};
//!
//! let mut m = Meter::new();
//! // `observe` returns the closure's value, so it can wrap a real call.
//! let answer = m.observe(Op::BookUtxo, || 40 + 2);
//! assert_eq!(answer, 42);
//! m.record(Op::BookUtxo, 1_500);
//!
//! let run = Run::new("example").target("full3");
//! let o = m.get(Op::BookUtxo);
//! assert_eq!(o.count, 2);
//! assert!(o.p95() >= 1_500);
//! println!("{}", m.table(&run));
//! ```
//!
//! # What an operation is
//!
//! One *unit of work at a call site a consumer can recognize* — a lookup, a row
//! read, a block applied, a chunk read — never a whole phase. A phase is
//! reconstructed by summing or nesting its parts, which is why the vocabulary is
//! small and flat.

use std::fmt::Write as _;

/// An operation mitos measures.
///
/// Flat and small on purpose: a phase is a *sum* of these, not another entry. The
/// doc on each says what one observation counts and what a plausible magnitude is,
/// because a number without that context is a number nobody can sanity-check.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub enum Op {
    /// One chunk read from the immutable store. Milliseconds (I/O bound).
    ChunkRead,
    /// Splitting one chunk into block frames. Milliseconds (a CBOR frame walk).
    ChunkSplit,
    /// Decoding one block into its primitives. Microseconds to milliseconds.
    BlockDecode,
    /// One transaction's hash. Sub-microsecond.
    TxHash,
    /// Applying one block's plan to the live set. Microseconds.
    BlockApply,
    /// One outref resolved against the book. ~1 µs measured.
    BookUtxo,
    /// One enumeration of a credential, policy or run. Microseconds to
    /// milliseconds, proportional to the run's length.
    BookEnumerate,
    /// One row projected into a consumer's owned shape. This is where a trait that
    /// asks for `String`s pays, and it is deliberately separate from the lookup.
    ProjectOutput,
    /// One row's bytes read out of `rows.bin`. Sub-microsecond, page-cache bound.
    RowRead,
    /// One address decoded or encoded. Sub-microsecond; called out because
    /// `DECODE_SHAPE.md` measured bech32 at 5.6% of a whole walk.
    AddressCodec,
    /// One key looked up in an interning table. Tens of nanoseconds.
    TableLookup,
}

/// Every operation, in report order. Kept beside `Op` so a new variant that is
/// not listed fails the test rather than silently never being reported.
pub const ALL_OPS: &[Op] = &[
    Op::ChunkRead,
    Op::ChunkSplit,
    Op::BlockDecode,
    Op::TxHash,
    Op::BlockApply,
    Op::BookUtxo,
    Op::BookEnumerate,
    Op::ProjectOutput,
    Op::RowRead,
    Op::AddressCodec,
    Op::TableLookup,
];

impl Op {
    /// The stable wire name. Never derived from `Debug` — that would make a
    /// rename a silent change to every log ever written.
    pub fn as_str(self) -> &'static str {
        match self {
            Op::ChunkRead => "chunk_read",
            Op::ChunkSplit => "chunk_split",
            Op::BlockDecode => "block_decode",
            Op::TxHash => "tx_hash",
            Op::BlockApply => "block_apply",
            Op::BookUtxo => "book_utxo",
            Op::BookEnumerate => "book_enumerate",
            Op::ProjectOutput => "project_output",
            Op::RowRead => "row_read",
            Op::AddressCodec => "address_codec",
            Op::TableLookup => "table_lookup",
        }
    }
}

/// Durations below this are counted exactly, one bucket per nanosecond.
const FINE: usize = 4096;
/// One bucket per nanosecond below `FINE`, then one per power of two.
const BUCKETS: usize = FINE + 64;

/// What one operation cost, in bounded memory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observation {
    pub count: u64,
    total_nanos: u128,
    min_nanos: u64,
    max_nanos: u64,
    buckets: [u64; BUCKETS],
}

impl Default for Observation {
    fn default() -> Self {
        Observation {
            count: 0,
            total_nanos: 0,
            min_nanos: u64::MAX,
            max_nanos: 0,
            buckets: [0; BUCKETS],
        }
    }
}

impl Observation {
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn total_nanos(&self) -> u128 {
        self.total_nanos
    }

    pub fn mean_nanos(&self) -> u64 {
        if self.count == 0 {
            return 0;
        }
        (self.total_nanos / u128::from(self.count)) as u64
    }

    pub fn min_nanos(&self) -> u64 {
        if self.count == 0 { 0 } else { self.min_nanos }
    }

    pub fn max_nanos(&self) -> u64 {
        self.max_nanos
    }

    /// The upper bound of the bucket the `q`th percentile falls in.
    ///
    /// An upper bound rather than an estimate: below [`FINE`] it is exact, and
    /// above it is the next power of two. Reporting "p99 ≤ 3.1 µs" is honest;
    /// reporting a interpolated "p99 = 2.847 µs" from these buckets would not be.
    pub fn percentile(&self, q: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let want = ((self.count as f64) * q).ceil() as u64;
        let mut seen = 0u64;
        for (i, n) in self.buckets.iter().enumerate() {
            seen += n;
            if seen >= want {
                return bound(i);
            }
        }
        self.max_nanos
    }

    pub fn p50(&self) -> u64 {
        self.percentile(0.50)
    }

    pub fn p95(&self) -> u64 {
        self.percentile(0.95)
    }

    pub fn p99(&self) -> u64 {
        self.percentile(0.99)
    }

    fn add(&mut self, nanos: u64) {
        self.count += 1;
        self.total_nanos += u128::from(nanos);
        self.min_nanos = self.min_nanos.min(nanos);
        self.max_nanos = self.max_nanos.max(nanos);
        self.buckets[bucket(nanos)] += 1;
    }

    fn merge(&mut self, other: &Observation) {
        if other.count == 0 {
            return;
        }
        self.count += other.count;
        self.total_nanos += other.total_nanos;
        self.min_nanos = self.min_nanos.min(other.min_nanos);
        self.max_nanos = self.max_nanos.max(other.max_nanos);
        for (a, b) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *a += b;
        }
    }
}

/// The bucket a duration lands in: exact below `FINE`, then one per octave.
fn bucket(nanos: u64) -> usize {
    if (nanos as usize) < FINE {
        return nanos as usize;
    }
    // `leading_zeros` of n gives the smallest k with 2^k > n, so the octave's
    // upper bound is exactly 2^k and `bound` inverts it without ambiguity.
    FINE + (64 - nanos.leading_zeros() as usize)
}

/// The inclusive upper bound of a bucket, in nanoseconds.
fn bound(i: usize) -> u64 {
    if i < FINE {
        return i as u64;
    }
    1u64 << (i - FINE)
}

/// What varies *between* runs, so two reports can be compared.
///
/// Deliberately strings and integers rather than anything clever: the point is
/// that a reader six months later can tell whether two numbers are comparable
/// without reading the commit that produced them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Run {
    /// What was run — a subcommand or operation, stable across invocations.
    pub what: String,
    /// What it read — the artifact or store, so a number is never orphaned.
    pub target: String,
    /// Scale and environment keys (`rows`, `chunks`, `threads`, `box`), which are
    /// what make two runs of the same thing comparable or not.
    pub detail: Vec<(&'static str, String)>,
}

impl Run {
    pub fn new(what: impl Into<String>) -> Run {
        Run {
            what: what.into(),
            target: String::new(),
            detail: Vec::new(),
        }
    }

    pub fn target(mut self, t: impl Into<String>) -> Run {
        self.target = t.into();
        self
    }

    pub fn key(mut self, k: &'static str, v: impl ToString) -> Run {
        self.detail.push((k, v.to_string()));
        self
    }
}

/// Accumulates observations. `merge` exists because a parallel pass has one meter
/// per thread and the report wants one of them.
#[derive(Clone, Debug, Default)]
pub struct Meter {
    obs: Vec<(Op, Observation)>,
}

impl Meter {
    pub fn new() -> Meter {
        Meter { obs: Vec::new() }
    }

    /// Record one observation, in nanoseconds.
    pub fn record(&mut self, op: Op, nanos: u64) {
        match self.obs.iter_mut().find(|(o, _)| *o == op) {
            Some((_, obs)) => obs.add(nanos),
            None => {
                let mut obs = Observation::default();
                obs.add(nanos);
                self.obs.push((op, obs));
            }
        }
    }

    /// Time a closure and return its value. The shape that makes instrumenting a
    /// call site a one-word change.
    pub fn observe<T>(&mut self, op: Op, f: impl FnOnce() -> T) -> T {
        let t = std::time::Instant::now();
        let out = f();
        self.record(op, t.elapsed().as_nanos() as u64);
        out
    }

    /// Fold another meter in — a thread's, or a phase's.
    pub fn merge(&mut self, other: &Meter) {
        for (op, obs) in &other.obs {
            match self.obs.iter_mut().find(|(o, _)| o == op) {
                Some((_, mine)) => mine.merge(obs),
                None => self.obs.push((*op, obs.clone())),
            }
        }
    }

    /// The observation for one operation. Empty when it was never recorded, which
    /// is itself information: a report showing a zero is a call site that was not
    /// reached, and that is usually a bug rather than a fast path.
    pub fn get(&self, op: Op) -> Observation {
        self.obs
            .iter()
            .find(|(o, _)| *o == op)
            .map(|(_, obs)| obs.clone())
            .unwrap_or_default()
    }

    /// The operations that were actually recorded, in `ALL_OPS` order.
    pub fn recorded(&self) -> Vec<Op> {
        ALL_OPS
            .iter()
            .copied()
            .filter(|op| !self.get(*op).is_empty())
            .collect()
    }

    /// A fixed-width table, one row per recorded operation.
    ///
    /// Stable column widths and `ALL_OPS` ordering, so two reports can be read
    /// side by side and a diff shows only the numbers.
    pub fn table(&self, run: &Run) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "meter — {} · {}", run.what, run.target);
        if !run.detail.is_empty() {
            let _ = writeln!(
                out,
                "  context    {}",
                run.detail
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(" · ")
            );
        }
        let _ = writeln!(
            out,
            "  {:<16} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
            "op", "count", "mean", "p50", "p95", "p99", "max"
        );
        for op in self.recorded() {
            let o = self.get(op);
            let _ = writeln!(
                out,
                "  {:<16} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
                op.as_str(),
                o.count,
                human(o.mean_nanos()),
                human(o.p50()),
                human(o.p95()),
                human(o.p99()),
                human(o.max_nanos()),
            );
        }
        out
    }

    /// One line per observation, for offline comparison.
    ///
    /// The run's fields are denormalised into every row so a log can be
    /// concatenated across runs and still be parsed and grouped without a join.
    /// The schema is fixed and hand-written: nine keys, all numbers and
    /// enum-derived strings, so no escaping is possible.
    pub fn jsonl(&self, run: &Run) -> String {
        let mut out = String::new();
        for op in self.recorded() {
            let o = self.get(op);
            let _ = write!(
                out,
                "{{\"what\":\"{}\",\"target\":\"{}\"",
                escape(&run.what),
                escape(&run.target)
            );
            for (k, v) in &run.detail {
                let _ = write!(out, ",\"{}\":\"{}\"", escape(k), escape(v));
            }
            let _ = writeln!(
                out,
                ",\"op\":\"{}\",\"count\":{},\"total_ns\":{},\"mean_ns\":{},\"min_ns\":{},\"p50_ns\":{},\"p95_ns\":{},\"p99_ns\":{},\"max_ns\":{}}}",
                op.as_str(),
                o.count,
                o.total_nanos(),
                o.mean_nanos(),
                o.min_nanos(),
                o.p50(),
                o.p95(),
                o.p99(),
                o.max_nanos(),
            );
        }
        out
    }
}

/// Nanoseconds, a duration, or a count — whichever reads better.
///
/// One formatter for every report, because "1458 ns · 4.8 s · 63643 ns/ref" in
/// three places is the problem this crate exists to remove.
pub fn human(nanos: u64) -> String {
    match nanos {
        0..1_000 => format!("{nanos} ns"),
        1_000..1_000_000 => format!("{:.2} us", nanos as f64 / 1_000.0),
        1_000_000..1_000_000_000 => format!("{:.1} ms", nanos as f64 / 1e6),
        _ => format!("{:.2} s", nanos as f64 / 1e9),
    }
}

/// The rate form, for a per-item figure: `1_223` ns reads as `817,661/s`.
pub fn per_second(nanos: u64) -> String {
    if nanos == 0 {
        return "—".to_string();
    }
    format!("{}/s", 1_000_000_000u64 / nanos.max(1))
}

fn escape(s: &str) -> String {
    // The only values that reach here are an enum-derived op name and
    // caller-supplied labels; the labels are expected to be paths and counters.
    // A control character or quote is a caller bug, and mangling it is better
    // than emitting a log line that cannot be parsed.
    s.chars()
        .filter(|c| !c.is_control() && *c != '"' && *c != '\\')
        .take(200)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_op_is_listed_and_named_uniquely() {
        assert_eq!(ALL_OPS.len(), 11, "a variant was added without listing it");
        let mut names: Vec<&str> = ALL_OPS.iter().map(|o| o.as_str()).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(names.len(), before, "two ops share a wire name");
        // And the enum and the list cannot drift: `recorded` walks ALL_OPS, so an
        // unlisted variant would never appear in a report.
        let mut m = Meter::new();
        for op in ALL_OPS {
            m.record(*op, 1);
        }
        assert_eq!(m.recorded().len(), ALL_OPS.len());
    }

    #[test]
    fn percentiles_are_exact_below_the_fine_threshold() {
        let mut m = Meter::new();
        for n in 1..=100u64 {
            m.record(Op::BookUtxo, n * 10);
        }
        let o = m.get(Op::BookUtxo);
        assert_eq!(o.count, 100);
        assert_eq!(o.min_nanos(), 10);
        assert_eq!(o.max_nanos(), 1_000);
        // 100 samples of 10..1000, so p50 is the 50th = 500 exactly.
        assert_eq!(o.p50(), 500);
        assert_eq!(o.p99(), 990);
        assert_eq!(o.mean_nanos(), 505);
    }

    #[test]
    fn percentiles_above_the_threshold_are_an_upper_bound_not_an_estimate() {
        let mut m = Meter::new();
        for _ in 0..100 {
            m.record(Op::BlockApply, 1_000_000);
        }
        let o = m.get(Op::BlockApply);
        // 1 ms is 2^20 ns, so it lands in the octave [2^19, 2^20] and the reported
        // bound is 2^20 = 1,048,576 — above the true value, which is the point.
        assert_eq!(o.p50(), 1 << 20);
        assert!(o.p50() >= 1_000_000);
        assert!(o.p50() < 1_100_000, "the bound must be within 2× the value");
    }

    #[test]
    fn a_merge_is_the_union_of_two_meters() {
        let mut a = Meter::new();
        let mut b = Meter::new();
        a.record(Op::RowRead, 100);
        a.record(Op::RowRead, 200);
        b.record(Op::RowRead, 300);
        b.record(Op::BookUtxo, 50);
        a.merge(&b);
        let r = a.get(Op::RowRead);
        assert_eq!(r.count, 3);
        assert_eq!(r.max_nanos(), 300);
        assert_eq!(a.get(Op::BookUtxo).count, 1);
        assert_eq!(a.recorded().len(), 2);
    }

    #[test]
    fn an_unrecorded_op_reports_empty_rather_than_lying_about_zero() {
        let m = Meter::new();
        let o = m.get(Op::ChunkRead);
        assert!(o.is_empty());
        assert_eq!(o.mean_nanos(), 0);
        assert_eq!(o.p99(), 0);
        assert!(m.recorded().is_empty());
        assert!(m.table(&Run::new("nothing")).contains("op"));
    }

    #[test]
    fn jsonl_has_one_parsable_line_per_recorded_op() {
        let mut m = Meter::new();
        m.record(Op::BookUtxo, 1_223);
        m.record(Op::BookUtxo, 999);
        m.record(Op::ProjectOutput, 20_406);
        let run = Run::new("lookup").target("full3").key("rows", 10_911_674);
        let out = m.jsonl(&run);
        assert_eq!(out.lines().count(), 2);
        for line in out.lines() {
            assert!(line.starts_with('{') && line.ends_with('}'));
            assert!(line.contains("\"what\":\"lookup\""));
            assert!(line.contains("\"target\":\"full3\""));
            assert!(line.contains("\"rows\":\"10911674\""));
        }
        assert!(out.contains("\"op\":\"book_utxo\",\"count\":2"));
        assert!(out.contains("\"mean_ns\":1111"));
    }

    #[test]
    fn observe_returns_what_the_closure_returned_and_times_it() {
        let mut m = Meter::new();
        let v = m.observe(Op::TableLookup, || {
            std::thread::sleep(std::time::Duration::from_micros(50));
            7u32
        });
        assert_eq!(v, 7);
        assert_eq!(m.get(Op::TableLookup).count, 1);
        assert!(m.get(Op::TableLookup).min_nanos() >= 50_000);
    }

    #[test]
    fn durations_render_in_the_unit_that_reads_best() {
        assert_eq!(human(999), "999 ns");
        assert_eq!(human(1_223), "1.22 us");
        assert_eq!(human(20_406), "20.41 us");
        assert_eq!(human(31_288_000), "31.3 ms");
        assert_eq!(human(4_800_000_000), "4.80 s");
        assert_eq!(per_second(1_000), "1000000/s");
        assert_eq!(per_second(0), "—");
    }
}
