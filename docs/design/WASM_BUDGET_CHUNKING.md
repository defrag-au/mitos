# Wasm budget chunking — re-entrant operations for unbounded work

**Status: design draft** (2026-05-19). Prompted by a production
incident: `holder-distribution`'s `cold_start` trapped in
`cabi_realloc` during a `recapture`-driven rebootstrap of a large
policy (NIKEPIG) — wasm linear-memory exhaustion. Earlier the same
session a multi-policy rebootstrap trapped on fuel. Both are one
root cause: **a wasm module call is bounded; several module
operations are not.**

The storage assumptions below are validated against the **dolos
v1.0.3** source (`~/code/github/dolos`); file:line references are
inline.

Cross-references:
- `RECAPTURE.md` — recapture drives `rebootstrap`, the first
  consumer of the re-entrant pattern.
- `MITOS_COMPANION_PATTERN.md` — where the re-entrant step pattern
  should land as a documented module-author affordance.
- `MITOS_DATA_PLANE_API.md` — the `chain-data` host-fn surface
  this doc proposes paginating.
- `HOLDER_DISTRIBUTION_MODULE.md` — `cold_start` is the worked
  example throughout.

## The problem

A wasm module invocation runs under three host-imposed ceilings:

- **Fuel** — an instruction-count budget per call (`set_fuel`).
  Exhaustion → a distinct `OutOfFuel` trap.
- **Linear memory** — the module's memory has a maximum.
  `memory.grow` past it returns `-1`; the component-ABI allocator
  `cabi_realloc` → the Rust allocator → on a failed grow Rust
  aborts → an **`unreachable` trap**, *indistinguishable from a
  genuine `panic!` or logic bug*.
- **Epoch deadline** — wall-clock-ish; exhaustion → an interrupt
  trap.

Several module operations are **intrinsically unbounded** — their
cost scales with chain data, not a fixed constant. The canonical
case is a **cold-start scan**: `holder-distribution::cold_start`
enumerates every UTxO of a policy, resolves each, folds a holder
ledger, and emits a snapshot. For a small policy (Aliens: 134
UTxOs) it fits one call. For a large one (NIKEPIG) it traps.

Doing unbounded work in one bounded call is the bug. The fix is
structural and uniform: **every unbounded module operation is a
re-entrant step function** — bounded work per call, progress
checkpointed, the host loops with a fresh budget each call.

## The re-entrant step pattern

The contract between host and module:

> The module does **one bounded page of work per call**,
> checkpoints its progress to `state-kv`, and reports
> *more* / *done*. The host **loops**, applying a fresh fuel
> budget (and observing the memory ceiling) on each call, until
> the module reports *done*.

`recapture`'s existing host loop already has this shape. It keeps
its loop *shape* — it now terminates on the step's `done` flag and
sums the per-call `ingested` count rather than testing a bare
non-zero return — and the granularity of "one unit" goes from a
predicate to a page.

A bounded step returns **two** things, and they must be separate
fields: an explicit *done* flag and a *count of work done*. A bare
`u64` cannot carry both — if the number means "items processed,"
then `0` is ambiguous (round complete vs. a legitimately empty
page mid-scan). So the step is a record:

```wit
// world mitos-module-v2
record rebootstrap-step {
    /// the round is complete — the host stops looping.
    done: bool,
    /// items (UTxOs) this page processed. Meaningful telemetry,
    /// not a flag — the host sums it into the recapture's
    /// `events_emitted` ("re-scanned N UTxOs across M pages").
    ingested: u64,
}
rebootstrap: func() -> result<rebootstrap-step, string>;
```

```rust
// `rebootstrap` — re-entrant. One bounded page per call.
fn rebootstrap() -> Result<RebootstrapStep, String> {
    let mut cur = Cursor::load_from_state_kv()      // durable — see "Cursors"
        .unwrap_or_else(Cursor::start);

    let policy = tracked[cur.predicate_idx];
    // ONE page. `limit` is generous; the host clamps the returned
    // page to the current budget (see "Adaptive page sizing").
    let page = chain_data::utxos_by_policy(policy, cur.after.clone(), PAGE_HINT);
    let ingested = page.refs.len() as u64;
    let outs = chain_data::read_utxos(&page.refs);  // transient — dropped at call end
    for o in outs { fold_into_ledger(policy, o) }   // ledger: resident across calls

    let done = match page.next {
        Some(next) => {                             // same policy, next page
            cur.after = Some(next);
            false
        }
        None => {
            emit_snapshot(policy);                  // chunked — see "Output"
            cur.advance_predicate();
            cur.done()
        }
    };
    if done { Cursor::clear(); } else { cur.save_to_state_kv(); }
    Ok(RebootstrapStep { done, ingested })
}
```

The host loop: `loop { let s = call_rebootstrap()?; total += s.ingested;
if s.done { break } }`. `total` is the honest `events_emitted` —
a real count of UTxOs re-scanned, not a page tally.

The re-entrant calls hit the **same wasm instance** (the host
loops one driver), so a resident accumulator survives between
calls in a thread-local. That gives the load-bearing split:

- **Resident across calls** — the accumulating ledger. Bounded by
  *holder count*, not UTxO count (small for almost any token; see
  "Open questions" for the pathological case).
- **Transient per call** — one page of refs + one page of
  `read_utxos` results. Allocated, folded, dropped; the allocator
  reclaims it before the next call.

No single call holds more than `resident accumulator + one page`.
That is what eliminates the `cabi_realloc` OOM.

## The three budget axes

Chunking must address all three, or the trap just moves:

| Axis | Failure | Chunking |
|---|---|---|
| **Fuel** | `OutOfFuel` trap | Host refuels per call — already done by the recapture loop. One page's processing must fit one call's fuel; the host sizes the page so it does. |
| **Memory — input** | `cabi_realloc`/`unreachable` | The bulk host-fns (`utxos_by_*`) must **page** — the module never receives more than one page's worth of refs. This is the NIKEPIG trap. |
| **Memory — output** | `cabi_realloc` on emit | A full-snapshot emit builds the whole CBOR buffer in wasm memory. Large holder sets need a **chunked emission protocol** (see "Output"). |

Fuel is the easy one. Input-memory is the urgent one. Output-memory
is the completeness one (most real tokens' snapshots still fit one
emit; a million-holder token would not).

## Adaptive page sizing — host-owned

The module must not hardcode a page size — the right size depends
on the per-call budget and on how heavy each item is (a UTxO with
50 assets costs far more to lift + fold than a bare one).

**The host owns page sizing; the module is naive.** The module
asks for a generous page (`PAGE_HINT`); the host's `utxos_by_*`
implementation **clamps the returned page** to what the current
budget affords, attaches a continuation cursor, and the module
processes exactly what it got and returns. The host then refuels
and loops.

The host adapts the clamp from per-call telemetry:

- **Fuel** — `Store` fuel delta per call: `start_fuel - end_fuel`.
- **Memory** — a wasmtime **`ResourceLimiter`** on the Store; its
  `memory_growing(current, desired, maximum)` hook fires on every
  `memory.grow`, so the host sees peak memory and proximity to the
  ceiling.

Control loop (AIMD-style): a call that used `< ~50%` of fuel and
left memory headroom → grow the next page; a call that used
`> ~80%` → shrink; a call that **trapped** (fuel or an OOM-deny
from the limiter) → halve and **retry the same page** (the cursor
was not advanced — see below). The module carries zero adaptive
logic; all of it lives in the host-fn + the recapture loop.

## Budget observability

**Fuel exhaustion is already a meaningful, distinct error**
(`OutOfFuel`). **An OOM is not** — it surfaces as an opaque
`unreachable` trap, the same shape as a module logic bug. The only
incidental tell is `cabi_realloc` in the innermost backtrace
frame.

The fix is the `ResourceLimiter` above. With it the host:

1. **Detects memory pressure first-class** — it sees grows
   approaching the ceiling *before* the trap, and can classify a
   trap that follows a denied grow as a definite OOM rather than
   guessing from the backtrace.
2. Can **deny a grow deliberately** (return `false` →
   `memory.grow` returns `-1`) to force a clean, classified OOM at
   a known point rather than an unbounded one.
3. Feeds the adaptive sizer.

Recapture (and any heavy-op driver) should surface a classified
outcome — `Completed`, `OutOfFuel(retrying)`, `OutOfMemory`,
`ModuleError` — instead of today's opaque "trapped." Trap
classification: `OutOfFuel` from the trap code; OOM from a
limiter-observed denied grow; anything else is a genuine module
fault.

## Cursors

Two distinct cursors, both must be deliberate.

### Continuation cursor — durability

The re-entrant progress cursor (`predicate_idx`, `after`) **must
live in `state-kv`**, not only a thread-local. Reason: **trap
recovery.** A page that traps was not checkpointed (the trap
preceded `save`); the host must re-attempt it with a smaller
clamp. A trap may poison the wasm instance, so recovery is
re-instantiate → `init` → resume — which only works if the cursor
survived the trap. A thread-local does not; `state-kv` does. So
state-kv is *required*, not merely nice. It also makes a heavy op
survive a host restart mid-run.

### Pagination cursor — a frozen materialised scan

dolos v1.0.3's `bypolicy` index is a redb **multimap** —
`policy_id → {(tx_hash, idx)}` — with a dump-all API only
(`FilterIndexes::get_by_policy` returns the whole `HashSet<TxoRef>`
— `crates/redb3/src/indexes/mod.rs:86,176`). There is **no keyed
range-scan** — no "entries for policy P after key K, limit N." So
a cursor cannot page the live index directly, and v1.0.3 won't be
forked to add one.

It doesn't need to. Pagination happens at the **host-fn ↔ wasm
boundary**, and that boundary is mitos's to define. On the first
`utxos_by_policy` call of a scan the host materialises dolos's
dump-all into **native host memory** (one short redb read txn),
sorts it into a stable order, and caches it under an opaque scan
token. Each later call slices that **frozen** cache by offset; the
cache is evicted on the last page (or a TTL). The wasm module only
ever receives one page — it never sees, never has to allocate, the
full list.

An **offset cursor is stable here** — not because the underlying
index is stable (it isn't), but because the *cache is frozen* at
scan-start. The "keyed, not offset" rule applies to scanning a
*live* mutating index; this design deliberately doesn't. dolos's
own minibf does exactly the same — materialise-all, then
offset-page in memory (`crates/minibf/src/routes/addresses.rs:125`).

Native-memory cost is one policy's ref-set — ~36 bytes × UTxO
count (single-digit MB even at the 100K cap), in the host heap,
not wasm linear memory. Recapture scans one predicate at a time,
so one cache entry is live at once.

### Consistency model

The materialised ref-set is **frozen at scan-start**, so the whole
scan is consistent as-of one point — the tip when the first page
was taken. That tip is cheap: dolos stores it as a single
cursor-table row, `StateStore::read_cursor()`, an O(1) lookup
(`crates/redb3/src/state/mod.rs:244`; minibf's tip accessor at
`crates/minibf/src/lib.rs:85`). The host captures it once at
scan-start and stamps every `utxo-page` with `anchor-slot`; the
module carries it into the emitted snapshot's `cursor_slot` —
fixing today's `cursor_slot: 0` wart so live deltas resume from
the right point.

dolos has **no as-of-slot historical read** — state is
current-only — so the scan can't be pinned to a past point; it's
anchored at whatever the tip was at scan-start. Two residual
drifts, both self-correcting via the live `HolderDelta` stream
(which `holder-distribution` already depends on):

- a UTxO frozen in the ref-set but spent before its page's
  `read_utxos` resolves not-found — correctly dropped (no longer a
  holder);
- a re-org past `anchor-slot` mid-scan bases the snapshot on an
  orphaned chain.

A slot-pinned read — holding one redb read txn across the whole
scan — is **rejected**: redb expects short-lived read txns; a
multi-minute open txn pins old versions and stalls compaction.

## Host-fn surface — paged-only, one function

The bulk `chain-data` host-fns become **paged-only**. There is no
`utxos_by_policy` *and* `utxos_by_policy_page` — a dual surface is
a maintenance cost and a footgun (someone calls the unbounded one
and OOMs). One function, paged by construction; "get everything"
is "loop the cursor."

This is **entirely a mitos-side change** — dolos v1.0.3 is used
as-is (its dump-all `get_by_policy` + O(1) `read_cursor`); no
dolos fork or new index. The host-fn implementation does the
materialise-cache-and-page described under "Cursors."

```wit
record utxo-page {
    refs: list<output-ref>,
    /// tip slot the materialised scan is frozen as-of; the module
    /// stamps it into the emitted snapshot's `cursor_slot`.
    anchor-slot: u64,
    /// opaque continuation — the host holds the frozen
    /// materialised ref-set keyed by this token. Absent ⇒ last
    /// page. The module treats it as bytes; only the host reads it.
    next: option<list<u8>>,
}

utxos-by-policy:       func(policy: list<u8>,  after: option<list<u8>>, limit: u32) -> utxo-page;
utxos-by-address:      func(address: string,   after: option<list<u8>>, limit: u32) -> utxo-page;
utxos-by-payment-cred: func(cred: list<u8>,    after: option<list<u8>>, limit: u32) -> utxo-page;
```

`limit` is the module's *hint*; the host returns
`min(limit, adaptive_budget)` refs (see "Adaptive page sizing").
`read-utxos` / `read-output-datums` already take a caller-supplied
`list` and so are already caller-bounded — they stay, but callers
must pass bounded chunks.

## Output — chunked snapshot emission

A heavy operation's *output* is also unbounded. `emit(Snapshot {
holders: <all> })` builds the entire snapshot CBOR in wasm memory
before emitting. For a large holder set that buffer is itself an
OOM site.

The emission protocol becomes a sequence rather than a single
event — e.g. for `holder-distribution`:

```
HolderEvent::SnapshotBegin { policy, anchor_slot }
HolderEvent::SnapshotChunk { policy, holders: [..bounded..] }   × N
HolderEvent::SnapshotEnd   { policy }
```

Consumer semantics: `SnapshotBegin` → wipe the policy's projected
rows; `SnapshotChunk` → insert; `SnapshotEnd` → commit / mark
authoritative. This also lets a re-entrant scan emit each page's
contribution as it goes, so the module need never hold the whole
holder set — though the *accumulator* (the dedup-by-holder ledger)
must still be resident until the scan completes, because a holder
spans pages (the index is UTxO-ordered, not holder-ordered).

This is a `mitos-community-events` wire change + a consumer-side
change in every snapshot consumer.

## Affected operations

Every intrinsically-unbounded module operation adopts the pattern:

- `holder-distribution::cold_start` — the worked example.
- `burn-address::cold_start_address` — paged `utxos_by_address`.
- `vesting-tracker` cold-start over addresses + payment creds —
  paged `utxos_by_address` / `utxos_by_payment_cred`.
- `rebootstrap` (all three self-bootstrapping modules) — already
  re-entrant at the *predicate* grain; extends to the *page* grain.
- Any future heavy module op (bulk mint backfills, contract
  sweeps).

Event-driven modules are unaffected — their refill is the
host-side `run_bootstrap` over manifest `[interest]`, which
already chunks via per-batch `handle-events` calls.

## Migration / phasing

1. **`ResourceLimiter` + trap classification** (host only). Lands
   OOM observability + the telemetry the rest depends on. No
   module changes.
2. **Paged `chain-data` host-fns.** WIT change; the opaque scan
   token; host-side materialise-and-cache + adaptive clamp.
   Existing modules' `cold_start` callsites migrate to the loop
   form. mitos-side only — dolos v1.0.3 is used as-is.
3. **Re-entrant `cold_start`** in the three self-bootstrapping
   modules — page loop, resident accumulator, `state-kv`
   continuation cursor, trap-retry recovery in the host loop.
4. **Chunked snapshot protocol** — `mitos-community-events` +
   consumers. The completeness item; can trail (1)–(3) since most
   real tokens' snapshots still fit one emit.
5. **SDK affordance** — a documented re-entrant-step helper in the
   module SDK so module authors don't re-derive the cursor dance.

Phases 1–3 unblock NIKEPIG-class policies. Phase 4 makes it scale
without an upper bound.

## Open questions

1. **Pathological accumulator size.** The resident ledger is
   holder-count-bounded. A token with millions of holders would
   need the *accumulator* paged to `state-kv` too (load/merge/
   persist per page) — a real I/O cost. Defer until a token
   demands it; note the design extends cleanly (the cursor already
   exists; the ledger just moves behind it).
2. **Snapshot anchor slot** — *resolved (dolos v1.0.3).* The tip
   is an O(1) cursor-table read (`StateStore::read_cursor`); the
   host captures it once at scan-start and stamps every
   `utxo-page` with `anchor-slot`. No `chain_data::tip()` export
   is needed — the anchor rides the page.
3. **Adaptive clamp cold-start.** The first page of a fresh scan
   has no telemetry — pick a conservative default `limit` and let
   the loop converge. What default? Suggest sizing from the
   manifest fuel budget assuming a mid-weight UTxO.
4. **Cross-call instance lifetime.** The resident accumulator
   relies on the host looping *one* instance. Confirm the
   recapture loop never re-instantiates between pages except on a
   trap (where re-instantiate + state-kv resume is the intended
   recovery).
