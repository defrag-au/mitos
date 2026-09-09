//! Progress events — the passes report through a callback so the same
//! machinery serves both the CLI (eprintln) and the hosted service (job
//! state a client polls / streams over SSE).

#[derive(Clone, Copy, Debug)]
pub enum Progress<'a> {
    /// A byte-sieve pass ticking through chunks.
    Scan {
        pass: &'a str,
        done: u64,
        total: u64,
        gb_per_s: f64,
    },
    /// The resolve pass naming senders through the tx index, one point
    /// lookup per wanted hash.
    Lookup { done: usize, total: usize },
    /// The resolve SWEEP ticking through bands, newest first — the fallback
    /// for chunks the index has not covered yet.
    Resolve {
        done: usize,
        total: usize,
        wanted_left: usize,
    },
    /// A phase boundary worth surfacing verbatim.
    Phase { label: &'a str, detail: &'a str },
}

/// The callback shape every pass takes.
pub type Prog<'x> = &'x (dyn Fn(Progress<'_>) + Sync);
