# Cold-start trap isolation — a trapped backfill must never poison the live module instance

**Status: Phases 1–3 implemented** (2026-05-25) — the two-plane split
(D), the shared re-instantiate-capable pump (A), proactive recycling
(B), and the progress-aware retry budget (C) have landed in
`backfill_v2.rs` + `host_v2.rs` + `follower_v2.rs`. Phase 4 (live
follower supervisor, E) is not yet implemented. Prompted by a
production incident:
onboarding a ~420K-asset collection (Hosky Cash Grab,
`a5bb0e5b…`) into dev `collection-ownership` trapped
`collection-holders`' onboard cold-start, **poisoned the shared
wasm component instance**, and left the live tip-dispatch path
armed to die on the next event for *any* collection. A second
collection (Toolheads) added afterwards got zero holders because
its cold-start tried to enter the already-dead instance.

This is a direct follow-on to `WASM_BUDGET_CHUNKING.md`. That work
made the **recapture** rebootstrap path re-entrant + re-instantiate
on trap. This doc covers the two failures that work did not reach:

1. The **onboard** (subscribe-time) cold-start never got the same
   treatment — it runs on the *live* instance and cannot
   re-instantiate, so a trap poisons live dispatch.
2. The re-instantiation budget is sized for *occasional* traps, not
   *periodic* OOM across a large scan — so even recapture gives a
   partial refill for a big collection.

The governing principle this doc argues for: **mitos must be
self-protecting. A consumer handing the platform an oversized or
pathological policy is a normal input, not an error to be screened
out upstream.** A worker-side size/age guard is at best a UX/cost
optimisation in one consumer; it is explicitly **not** a safety
boundary, because any current or future consumer can submit any
policy. The protection lives in the platform.

Storage assumptions validated against **dolos v1.0.3**; file:line
references inline.

Cross-references:
- `WASM_BUDGET_CHUNKING.md` — the re-entrant step pattern +
  recapture's re-instantiate-on-trap this generalises.
- `RECAPTURE.md` — recapture drives `rebootstrap`; the operator
  safety net referenced throughout.
- `SUBSCRIPTION_MECHANICS.md` / `UNIFIED_SUBSCRIBE.md` — the
  subscribe-time onboard path (`apply_interest_mutation`).
- `MITOS_ISOLATION_ROADMAP.md` — per-module/instance isolation
  posture this advances.

---

## The incident

Dev `collection-ownership` runs six community modules
(`collection-holders`, `collection-metadata`, `asset-transfer`,
`cip-25-mint`, `cip-68-mint`, `asset-metadata-update`), one wasm
instance each, shared across all subscribed companions
(collections). Timeline (host journal, UTC):

| Time | Event |
|---|---|
| 05:30:34 | Hosky (`a5bb0e5b…`) companion registered on all six modules; `update-interest` Add fires the onboard cold-start. |
| 05:30:35 | `collection-holders` onboard `rebootstrap` **traps** at `utxos=1728 steps=26`. The onboard pump logs a warning and continues — **but the wasm instance is now poisoned.** |
| 05:36:06 | Toolheads (`285c0b8e…`) added. |
| 05:36:15 | Toolheads' `collection-holders` cold-start → `error=wasm trap: cannot enter component instance`, **0 utxos ingested**. Its `update-interest` Add on the same module fails identically. |

Toolheads' *other* five modules onboarded fine (separate
instances). But `collection-holders` — the module that builds the
holder map — was dead, so Toolheads has no holders.

The follower did not die *immediately* only because `apply_block`
filters host-side and skips entering the wasm when a block carries
no watched-policy event (`driver_v2.rs:214`, `if
batches.is_empty()`). The next live event for any watched
collection (IslaNOVA, SpaceBudz, Nikeverse, Black Flag, …) would
call `handle_events` on the poisoned instance → `apply_block`
returns `Err` → the follower task exits with no supervisor
(`follower_v2.rs:184`, "Supervisor wiring is a v2.x follow-up") →
live holder tracking dark for **every** collection on
`collection-holders` until a restart or recapture.

Blast radius was contained to `collection-holders` (prod ownership
uses legacy sync, 0 companions; other modules are separate
instances) — but the *mechanism* is general: any module's onboard
trap bricks that module for all its consumers.

---

## Root cause

### The trap itself: OOM, not fuel

The trap backtrace:

```
0:  Vec<T,A>::reserve
1:  String::from_iter
2:  hex::encode
3:  collection_holders_module::shard_kv_key
4:  rebootstrap::{{closure}}
5:  rebootstrap
```

A denied `memory.grow` → the Rust allocator's error path →
`unreachable` → trap (the exact mechanism `WASM_BUDGET_CHUNKING.md`
documents: "indistinguishable from a genuine `panic!`"). It
surfaced in `shard_kv_key`'s `hex::encode` only because that is
where the next allocation happened to land.

Why it OOM'd at only 1,728 utxos:

- wasm linear memory **only grows** — it never shrinks back to the
  OS. Across cold-start pages, the allocator high-water mark climbs
  from fragmentation plus the per-page churn of marshalling state-kv
  keys/values across the component-ABI boundary (`shard_kv_key` →
  `hex::encode` allocates a fresh `String` per entry, per page).
- The host imposes **no ceiling by default** — `budget.rs` ships
  `BudgetLimiter::new(max_memory_bytes: None)`, "leaving the
  module's own declared maximum as the only ceiling." So growth runs
  unchecked until the module's declared wasm max.
- The adaptive sizer (`budget.rs` `MIN_PAGE=64`, `INITIAL_PAGE=256`)
  had already shrunk pages to the 64-entry floor (the `~66/page × 26
  steps = 1728`). **Page-shrink bounds per-call peak; it cannot
  reclaim lifetime high-water.** Only re-instantiation resets linear
  memory.

So the cold-start grew memory monotonically across pages and hit
the wall on page ~27. (Whether the per-page high-water climb is pure
allocator fragmentation or a retained allocation worth fixing in the
module is an open optimisation — see Follow-ups — but the robust fix
below does not depend on the answer.)

### Defect 1 — onboard runs on the live instance and cannot re-instantiate

`pump_onboard_rebootstrap` (`follower_v2.rs:393`) is handed the
**same `DriverV2` the live follower loop uses** — one wasm instance
per module, shared by subscribe-time onboard cold-start *and* live
tip dispatch. On a trap it logs and `break`s without rebuilding
(`follower_v2.rs:381`: "this does NOT re-instantiate… the follower
lacks the registry context"). The poisoned instance stays wired
into live dispatch. This is the landmine.

Contrast recapture (`host_v2.rs:627`), which re-instantiates from
the durable cursor on a retryable trap and hands a *clean* instance
to the follower. Onboard never got that path.

### Defect 2 — the re-instantiation budget is not progress-aware

Recapture's loop caps cumulative re-instantiations at
`REBOOTSTRAP_MAX_REINSTANTIATIONS = 6` (`host_v2.rs:624`) and
**never resets the counter on forward progress** (`host_v2.rs:671`,
`reinstantiations += 1`). That budget assumes traps are *rare* (a
few pathological pages). A collection that OOMs *periodically* — Hosky
hits the wall roughly every ~1,728 utxos — exhausts the budget after
6 cycles (~10K utxos) and aborts with "still trapping at the minimum
page… refill may be partial." So **even recapture cannot fully
refill a large collection** under this memory pattern; it caps at
~6× the per-instance memory capacity.

### Defect 3 — memory management is reactive only; no proactive recycling, no ceiling

`max_memory_bytes = None` means OOM is the module's hard declared
max — an *uncontrolled* abort rather than a *managed* checkpoint.
There is no proactive recycle: the only memory reclaim is a trap →
shrink → re-instantiate cycle, which is wasteful (it discards the
in-flight page) and, combined with Defect 2, bounded to 6 uses.

### Defect 4 — no supervisor on the live dispatch path

A trap during live `handle_events` returns `Err` and the follower
task exits permanently (`follower_v2.rs:184`). Even unrelated to
backfill, a single bad block can take a module dark with no
recovery short of operator action.

---

## Design

Two planes, each given the resilience appropriate to it:

> **Backfill is unbounded and disposable. Live dispatch is bounded
> and durable. They must not share a wasm instance.**

All module state is durable in state-kv and all output flows through
the emit sink; the *in-memory* wasm instance carries no state the
other plane needs. So the two can run on separate instances that
communicate only through state-kv — which is exactly what makes
isolation cheap.

### Fix D (structural, primary) — run backfill on a disposable instance

Cold-start (onboard) and recapture `rebootstrap` always run on a
**dedicated, throwaway** instance, never the live tip-dispatch
instance. The backfill instance scans → writes shards to state-kv →
emits; when it finishes (or traps unrecoverably) it is dropped. The
live instance reads the resulting state-kv on demand and is never
entered for backfill.

Consequence: **a backfill trap is contained by construction.** It
can poison only the disposable instance, which is discarded anyway.
The live follower for every collection on the module is untouched.
This is the fix that makes "one oversized collection cannot take
down the module for other collections" a structural guarantee rather
than a budget we hope holds.

### Fix A — one shared, re-instantiate-capable rebootstrap pump

Extract recapture's re-instantiate loop (`host_v2.rs:627-728`) into
a single reusable pump that **both** recapture and onboard call,
operating on a backfill instance it owns and can rebuild. The
follower no longer needs "the registry context" inline — it asks the
host (via a `DriverFactory` handle: registry + config + caching
plane + sink) to run the backfill pump and signal completion. On a
retryable trap the pump rebuilds from the durable cursor and
continues. Onboard thereby gains recapture's completion semantics,
and — because of Fix D — never touches the live instance.

### Fix B — proactive instance recycling + an explicit ceiling

Set an explicit `max_memory_bytes` on the backfill instance's
limiter so OOM is a *classified, controlled* event, and recycle the
instance **proactively** when `peak_memory_bytes`
(`budget.rs:66`, already tracked) crosses a high-water fraction of
the ceiling (or every N pages) — *before* the wall. The fresh
instance resumes from the durable cursor with zeroed memory. This
keeps a large scan flowing without ever tripping the OOM trap,
turning trap-driven re-instantiation back into the rare safety net
it was designed to be. (The live instance keeps the conservative
default; this ceiling is a backfill-plane concern.)

### Fix C — bound reactive trap-rebuilds with an absolute cap

Cap *reactive* (trap-driven) re-instantiations at a fixed ceiling
(`MAX_TRAP_REBUILDS`, 64), **never reset**; proactive memory recycles
(Fix B) are not counted. This guarantees the pump terminates: a
pathological policy whose page is pinned at the `MIN_PAGE` floor
(`budget.rs`) and still traps gives up the round with a partial
refill — exactly the "if even `MIN_PAGE` traps, the module is
pathological and the host gives up" intent the adaptive sizer already
documents. Operator `recapture` resumes from the durable cursor.

> **Revised after the post-deploy validation (2026-05-25).** The first
> implementation made this cap *progress-aware* — reset on any page
> that ingested > 0. That is unsound: in a real scan a light
> ("sparse") page succeeds (resetting the cap) immediately before a
> heavy ("dense") page traps, so the cap resets every cycle and a
> sustained floor-trap **never terminates**. Hosky's recapture churned
> at ~1.1 trap-rebuilds/sec indefinitely (`out-of-fuel` at
> `retry_page=64`, `peak_memory=2.5 MB`) — the live instance stayed
> healthy (Fix D held), but the backfill never completed and blocked
> the follower's tip processing. The absolute, non-resetting cap is
> the fix. The genuine large-collection case (periodic OOM in a
> healthy scan) is served by Fix B's proactive recycle, which doesn't
> count against this cap.

### Fix E — supervisor restart on live follower exit

Wrap the live follower task so an exit (a dispatch trap on a normal
block) rebuilds the instance from the durable cursor and respawns,
with bounded backoff, instead of dying permanently. This is the
flagged "v2.x follow-up" (`follower_v2.rs:182`) and is independent
of backfill — defence in depth for the live plane.

### Explicitly out of scope: consumer-side guards

A `collection-ownership` worker check that rejects/flags policies
over the 100K-UTxO cap or whose mints predate the archive horizon
(Hosky fails both) is reasonable as a **cost/UX** optimisation in
that consumer — it avoids paying for a backfill that will be capped
anyway. It is **not** part of mitos's safety story and must not be
relied on as one: mitos accepts policies from any consumer and must
remain correct and isolated regardless of what it is handed.

---

## Phasing

1. **Phase 1 — stop the bleeding (correctness + no poison).**
   Fix A + Fix C. Route onboard through the shared re-instantiate
   pump and make the budget progress-aware. Removes the landmine and
   lets onboard complete for moderately-large collections. Smallest
   change with the biggest safety payoff.
2. **Phase 2 — large-collection completion.** Fix B. Proactive
   recycling + explicit ceiling so 100K-cap-scale backfills complete
   smoothly without trap churn.
3. **Phase 3 — structural isolation.** Fix D. Backfill on a
   disposable instance, separate from live dispatch. Makes the
   blast-radius guarantee structural; subsumes the poison-prevention
   of Phase 1 for the onboard path.
4. **Phase 4 — live-plane safety net.** Fix E. Supervisor restart on
   follower exit.

Phases 1–2 are the urgent path (they fix the demonstrated outage and
make large onboards work). Phases 3–4 harden the architecture so the
class of failure cannot recur even if a future regression redresses
1–2.

---

## Testing

- **Regression (the incident):** onboard a synthetic policy whose
  cold-start exceeds one instance's memory ceiling (force a low
  `max_memory_bytes` in a test budget). Assert: cold-start completes
  to the cap; the live instance remains enterable; a subsequent
  onboard of a *second* policy on the same module succeeds (the
  Toolheads case). This is the golden that would have caught it.
- **Periodic-OOM completion:** a policy sized to require > 6
  re-instantiation cycles. Assert full ingestion (Fix C) and that
  proactive recycling (Fix B) keeps trap count at ~0.
- **Live-dispatch supervisor:** inject a `handle_events` trap on a
  normal block; assert the follower respawns from cursor and resumes
  (Fix E).
- **Isolation:** assert the backfill instance and live instance are
  distinct `Store`s and that a forced trap on the backfill instance
  leaves the live instance enterable (Fix D).
- Capture the trapped page as a replay fixture
  (`mitos-run --fixture`) — the onboard path currently writes no
  fixture (only `init`/dispatch/recapture do); Phase 1 should make
  the shared pump write one so onboard traps are locally replayable.

---

## Follow-ups / open questions

- **Dense-UTxO throughput (the real Hosky bottleneck).** Hosky's
  recapture traps `out-of-fuel` at the `MIN_PAGE=64` floor with
  `peak_memory` ~2.5 MB — i.e. it's **fuel**-bound, not memory-bound:
  a 64-ref page of asset-dense UTxOs (a treasury wallet holding many
  NFTs per UTxO) overruns the per-call fuel budget, and the sizer
  can't shrink below 64. The absolute cap (Fix C) now bounds this to a
  partial refill, but to actually *ingest* such a collection we need
  one of: (a) lower `MIN_PAGE` so the sizer can shrink to a fuel-safe
  page (even 1 UTxO/call); (b) a higher rebootstrap `fuel_per_call`;
  or (c) intra-UTxO chunking for pathologically dense outputs. Needs
  offline `mitos-run --fixture` characterisation first. (The cursor
  *does* advance across re-instantiation — the capped Hosky run
  ingested `utxos=110400` over 1724 pages before the rebuild ceiling,
  so this is genuine-but-slow progress bottlenecked on dense pages, not
  a stuck cursor.) Note also that emit (`SnapshotChunk`) only fires
  when a predicate's scan *completes*, so a partial-giveup builds the
  shards in state-kv but emits nothing to the companion — a capped
  large collection ends up with shards-but-no-snapshot until a
  successful full pass.
- **The synthetic-bootstrap onboard path still runs on the live
  instance.** Only the *chunked* cold-start (collection-holders/
  metadata) was moved to the backfill plane. Event-driven modules'
  onboard uses `bootstrap_one_predicate` on the live driver
  (`follower_v2::apply_interest_update`, the non-chunked branch). It is
  bounded today (the dolos `search_utxos` path caps at 1000 refs), so a
  poison is unlikely — but for full isolation it should run on the
  backfill plane too. Low priority; bounded.
- **Module memory hygiene.** `shard_kv_key` re-hex-encodes the
  policy and allocates a fresh `String` per entry per page. Reusing a
  key buffer / using raw bytes keys would raise per-instance capacity
  (fewer recycles). Worth profiling whether the per-page high-water
  climb is pure allocator fragmentation or a retained allocation.
- **Recapture re-instantiation budget.** Once Fix C lands, revisit
  whether `REBOOTSTRAP_MAX_REINSTANTIATIONS = 6` should be a
  *consecutive-no-progress* cap rather than cumulative everywhere it
  appears.
- **Onboard latency.** The onboard pump blocks the follower's tip
  processing for the pump's duration (`follower_v2.rs:388`). With
  Fix D the backfill runs off the live instance, which also opens the
  door to running it off the follower's critical path entirely —
  interleaving onboard with live tip events instead of blocking.
- **Hosky specifically.** Even with all fixes, Hosky's holders cap
  at the 100K `utxos_by_policy` `HARD_CAP` (`local.rs`), and its
  CIP-25 traits are unreachable (minted 2021-12-03, ~4 years before
  the archive horizon). It exercises the resilience path but is not
  itself fully ingestible — a good stress fixture, not a target.
