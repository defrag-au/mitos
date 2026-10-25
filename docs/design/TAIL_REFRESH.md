# Tail refresh — making the volatile tail cheap, fresh, and written only when it moved

Status: proposed, 2026-10-25. Follows `CF_REPLICATION.md` (the publish path) and
`POLICY_ARCHIVE_AND_SCALE.md` in `cnft.dev-workers` (the archive format).

The goal is three properties at once: **the tail is as fresh as the chain
allows**, **a pass writes only what changed**, and **a pass scans only what it
must**. They are not in tension once the tick stops re-deriving every policy
whole.

## What a tick does today, measured

On cardano-infra, 2026-10-25 07:14 UTC, 32 archived policies:

| what | value |
|---|---|
| spool blocks in the window | 162,385 |
| blocks matching any watched policy | 519 |
| **read + filter the whole window** | **0.1 s** |
| **policies that moved** | **16 of 32** |
| time on those 16 | 3,614 ms |
| **time on the 16 that wrote nothing** | **1,870 ms** |
| whole tick | 5.6 s |

And inside the 16 that moved:

| phase | ms | share |
|---|---|---|
| **scan** (`scan_blocks`, incl. the per-policy deep clone) | **1,564** | **43%** |
| **`warm()`** (the `/live` cache re-fold) | **2,014** | **56%** |
| parquet write + compaction | 26 | 0.7% |
| manifest + bundle | 8 | 0.2% |
| landing lock | 0 | 0% |

**Both of the obvious suspects were wrong.** Parquet and the manifest are 34 ms
combined — the archive-writing side is already cheap. The tick is scanning and
warming, and **half the policies get a pass, a manifest, a bundle and a publish
for zero rows**.

So the levers, in measured order:

1. **One scan instead of 32.** Every policy re-scans all 519 candidate blocks
   from its own deep copy (`blocks.to_vec()`). This is also the *only* way to
   discover a policy is quiet before paying for it — you cannot skip a scan you
   have not run.
2. **`warm()` on demand.** 56% of the moving cost is re-folding an in-process
   cache for `/live`. `/live` already falls back to folding on the miss, so a
   policy nobody is reading is warmed for nobody. `manifest.updated_unix` moves
   on every write, so the cache is invalidated and re-folded every tick even
   when the rows are identical.
3. **Real `Wrote::Nothing`.** Removes 16 publishes per tick and the bundle
   rewrite — not the millisecond win (16 × ~17 ms) but half the publish load.

## The three wastes

### 1. Every policy is rewritten, including the ones with nothing in the window

`write_tail` returns `Wrote::Tail` for **every archive that exists**
(`tip.rs:525`) — the `Wrote::Nothing` arm at `tip.rs:326` only fires when the
policy has no archive at all. `DEFAULT_REFRESH_SECS`'s own comment says a tick
is "a Parquet file, an R2 put and a KV put PER POLICY whose tail moved"; in
fact it is per policy, moved or not.

**Half** the policies (16 of 32) produced no rows in the measured tick. Each
still costs a scan, a `warm`, a manifest write, a `bundle.bin` rewrite and a
publish — 1,870 ms and one publish each.

**Why it cannot simply skip.** The volatile pass is also how *coverage*
advances: the pass entry carries `floor`/`ceiling`. Skipping the write leaves
`walk_to` stale. That is an under-claim, not a lie — `completeness()` ignores
volatile passes entirely (`manifest.rs:333`) — so skipping is safe, but the
coverage bookkeeping has to move somewhere. See *Decouple coverage* below.

### 2. Every policy re-scans the whole candidate set, from a deep copy

`refresh` loads the window once, then hands **the same block vector to each of
the 32 policies**:

```rust
write_tail(hub, policy, bytes, &blocks, floor, ceiling, index)   // tip.rs:300
    ↳ reverse::scan_blocks(blocks.to_vec(), …)                    // tip.rs:414
```

`scan_blocks` takes `Vec<(u64, Vec<u8>)>` by value, so `blocks.to_vec()` is a
**deep clone of every candidate block, 32 times per tick**. Then each policy
memmems the same 515 blocks for its own bytes.

Both are avoidable with one scan that fans rows out to per-policy writers.
**This is the measured top cost at 1,564 ms, 43% of the moving time** — and it
is also the only way to know a policy is quiet without scanning for it.

### 3. Freshness is the tick, and the tick is a batch

Freshness is bounded by `--tip-refresh-secs 300`, and `SAFETY_DEPTH_SLOTS` is
300 anyway (~5 min). A reader sees a trade up to ~10 minutes after it lands,
and the flow explorer serves it from KV. Debouncing per policy separates *how
often we look* from *how much we write*.

## The algorithm

One thread, four moving parts.

### A cursor, so the spool read is the delta

Keep `last_ceiling` in the tail process. Each tick reads
`(last_ceiling, tip − SAFETY_DEPTH]` rather than the whole window. Today that
is 0.1 s, so this is *not* where the win is — it becomes one as the snapshot
ages, because the window is the whole span from the last complete Mithril chunk
to the tip, and it was **97,009 slots (~27 h)** when measured. It only grows
until the next snapshot lands.

### One scan, fanned out

Replace `write_tail(policy) × 32` with a single pass over the delta that
matches all policies at once and routes rows into per-policy
`SegmentWriter`/observations. This removes the 32× deep clone and the 32×
memmem over the same bytes. `reverse::scan_blocks` keeps its single-policy shape
for the walk path; the tail gets a multi-policy entry point beside it.

### Per-policy flush, debounced with a ceiling

A policy is flushed when it has rows and either

- `debounce` (default 60 s) has elapsed since its last row — quiet wins, and a
  burst of trades becomes one write; **or**
- `max_wait` (default 300 s) has elapsed since its last write — a policy that
  trades continuously would otherwise never fire.

`tokio_util::time::DelayQueue` + `HashMap<policy, delay_queue::Key>` expresses
all three triggers, because `reset_at` takes an absolute deadline:

| trigger | deadline |
|---|---|
| an event | `last_event + debounce` |
| a reader asks (`/live`, `/reach`) | `now` |
| the ceiling | `min(last_event + debounce, last_write + max_wait)` |

### Decouple coverage from the pass

The volatile pass carries `floor`/`ceiling`; that is why "nothing landed" still
writes. Add the coverage cursor to the manifest itself (or accept a stale
`walk_to` for a quiet policy, which the readers already tolerate). Then a
policy with no rows writes **nothing** — no parquet, no bundle, no publish —
and the next write that does have rows fills the gap, because it is still
derived from `floor`.

### Append, and rebuild only when the floor moves

The tail is re-derived whole every tick for one reason, stated at
`tip.rs:76`: a retracted block must correct itself. But `ceiling` is already
`tip − 300`, so **everything inside the window is 300 slots deep before it is
written** — the same assumption the code already relies on. What must be
rebuilt is not the tail, it is the part below the floor: when the snapshot
advances, the window's lower bound moves up and rows below it are now in the
immutable archive. Keeping them in the volatile pass double-counts them.

So:

- **floor unchanged** (every tick) → append the delta to the volatile pass;
- **floor moved** (a Mithril chunk landed, ~6 h) → rebuild the pass from the
  new floor;
- **rollup** when the pass has more than N segment files, so appending does not
  grow the file count the publisher and the Worker pay for.

This is the load-bearing change and the one to test hardest: the sum rule, the
"one volatile entry, replaced whole" manifest rule (`tip.rs:459`), and the
`Pending`-is-fresh rule at `reverse.rs:1826` all assume replacement.

## Invariants that must survive

1. **`SAFETY_DEPTH_SLOTS` is the contract.** Nothing inside the window may be
   written before it is that deep, and an append is only safe under the same
   assumption a rebuild makes.
2. **The floor only moves up**, and rows below it belong to the immutable tier.
3. **The landing lock** (`hub.landing_lock`) guards every manifest
   read-modify-write, because a walk may land a range at any moment.
4. **A floor too high truncates an archive and lets it call itself complete.**
   Appending must never raise a pass's `floor`.
5. **The tail has no state between writes** — a fresh `Pending` every time, so
   an unresolvable row is an honest gap rather than something carried forward.

## Order of work

| # | change | expected | risk |
|---|---|---|---|
| 1 | single scan, fan out to per-policy writers | −1,564 ms and 32× clone; tells us which policies are quiet for free | medium |
| 2 | `warm()` on demand (recent reader) rather than on every write | −2,014 ms | medium |
| 3 | real `Wrote::Nothing` for a zero-row policy (+ coverage decoupling) | −16 publishes/tick, −~270 ms | low |
| 4 | cursor + append/rebuild-on-floor-move + rollup | O(delta) per tick, not O(window) | **high** |
| 5 | `DelayQueue` debounce with `max_wait` | freshness 300 s → 60 s | medium |

1–3 take the tick from 5.6 s to well under 1 s with no change to what is
written, and 3 alone halves the publish load. 4 is the one that needs a
correctness argument, and after this measurement it is the *least* urgent — the
window scan is 0.1 s of 5.6 s.

Open question for 4: whether appending is acceptable at all, or whether the
"replaced whole" defence is deliberate insurance against a rollback deeper than
300 slots. A rebuild triggered by the snapshot's floor gives the cheap path a
correctness answer for everything except a >300-slot reorg.
