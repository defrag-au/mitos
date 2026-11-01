# Artifact sharding — immutable shards, idempotent re-ingest

Status: **proposal, nothing built** (2026-09-24). A format design for the
artifact family, driven by a requirement the current layout cannot meet:
**re-ingesting a newer Mithril snapshot must reproduce the earlier files byte
for byte and write new ones only towards the tip.**

Every figure below marked *measured* comes from `LIVE_BOOK.md` §Sizes /
§"Where we are today" or `DELTA_CHAIN.md` §5; *derived* ones state their
assumption. Nothing here has been built or benchmarked.

Owner: TBD

Related:
- `LIVE_BOOK.md` §B1/B2/B5 — the format this changes, and the `rename`-swap
  rules it replaces. §B7 "one walk, many artifacts" is the pass this rides
- `DELTA_CHAIN.md` §5 — the measured per-chunk cost. **§B4's `~1 MB` is 3× low;
  the real slope is 3.3 MiB/chunk**
- `tools/tx-index/README.md` — the `segments/NNNNN.seg` model, written once and
  never rewritten. This doc is that model generalised to a *set*
- `MITOS_AT_THE_EDGE.md` §4.1, §4.6, §4.7 — the consumer. Static assets cap a
  file at 25 MiB and make a Worker version the atomic unit
- `cnft.dev-workers/docs/design/POLICY_ARCHIVE_AND_SCALE.md` — time-aligned row
  groups "split on a transaction boundary" is the precedent for §6's split rule
- `crates/cf-publish/src/lib.rs` — content-addressed blob + pointer flip

---

## 1. The requirement

A publisher re-runs ingest against a fresh Mithril snapshot. The snapshot is a
**superset** of the previous one: the same sealed chunks, byte-identical, plus
new ones. The artifact must behave the same way.

| | must hold |
|---|---|
| **immutable** | a published shard is never edited or truncated |
| **idempotent** | re-deriving a shard from the same chunks yields the same bytes |
| **incremental** | a newer snapshot produces new shards only near the tip |
| **asset-friendly** | ≤ 25 MiB per file, ≤ 100,000 files, and the file *is* the read unit |

## 2. Why the current format cannot do any of it

The book is a **set**, and `LIVE_BOOK.md` says so in its summary: *"the first two
are logs (a row is added and never changes), and this one is a set (a row can
die)."* Three mechanisms follow, and each one alone is fatal:

1. **`rows.bin` is a compacted arena of live rows in creation order.** An output
   that dies is not tombstoned — it is absent. Every row after the first death
   moves, so every byte after it differs.
2. **Entries address rows by byte offset** (`{u64 prefix, u32 row}`, where `row`
   is an offset into the arena). Offsets shift ⇒ all 10.9 M entries of all three
   orderings change.
3. **`dir.idx` buckets are entry indices.** Entries shift ⇒ all 2^21 buckets
   change.

So one spend anywhere invalidates the entire 2.23 GB. A re-ingest today is a
**total rewrite**, which is exactly what `rename`-swap (`B5`) was designed to
make safe — the format is internally consistent, it just answers a different
question than the one above.

⚠️ **And there is a fourth, quieter one.** `B3` records that policy ids are
*"assigned in creation order"*. An id is therefore a function of the whole scan,
not of the chunk the row came from — so even a perfectly chunk-local row shard
would carry ids that shift when an earlier chunk is re-read. §5 treats this as
the trap it is.

## 3. What "idempotent" can actually mean

A live set at a later height genuinely *is* different data. Demanding that its
bytes be unchanged is demanding that the chain stop. So the property has to be
stated against a **range**, not against time:

> **A shard is named by the chunk range it was derived from. Re-deriving the same
> range yields the same bytes. Growth adds files; it never edits one.**

That is achievable, and it is the property `tx-index` already has — its
`segments/NNNNN.seg` are one per completed chunk, *"written once (tmp + rename),
never rewritten"*, and the newest chunk is deliberately never indexed because
Mithril keeps appending to it until the next one seals.

The move this doc makes is to get a **set** to behave that way, and the answer is
the one every log-structured store reaches: **store the log, derive the set, and
keep liveness out of the payload.**

## 4. The layout

```
log/                                  # immutable · idempotent · append-only
  created/<chunk>[.<part>].bin        # outputs created in chunk C, in tx order
  created/<chunk>.pol                 # C's own 28-byte policy table (§5)
  spent/<chunk>.bin                   # spends recorded in chunk C
  chunk.manifest                      # chunk -> content hash + counts

base/<epoch>/                         # derived cache · immutable by name
  live/<chunk>.bits                   # 1 bit per output created in C
  rows/<part>.bin                     # compacted live rows, 1 MiB parts
  by_{outref,addr,policy}/<part>.idx
  {dir,addr.dir,policy.dir}.idx       # 8.39 MB each
  policy.tbl · policy.runs.idx
  epoch.manifest                      # content hash of every file above
```

**Two tiers, and the distinction is the whole design.**

⚠️ **`log/spent/` is not new, and `log/created/` contradicts a recorded
decision.** `LIVE_BOOK.md` §Decided already keeps the spend log — *"~4.1 GB at
12 B × 344.6 M … because it is what makes the join builder parallel and the book
incrementally maintainable"* — so that half is adopting an existing artifact, at
its existing width. But the same section rejects a published liveness bitmap:
*"The liveness bitmap is builder-side. As an artifact it would drag a ~20 GB
created array behind it."* **This doc proposes exactly that**, and the honest
reason is that the requirement changed rather than the arithmetic: §1's
incremental re-ingest was not a goal when that line was written, and it cannot be
met without the created log existing as an artifact. The ~20 GB there and the
~65 GB in §6 are the same object at two different width assumptions (§10.2). If
the incremental requirement is dropped, §Decided's position is the better one.

`log/` is the source of truth. A sealed Mithril chunk never changes, so a file
derived purely from it never changes. This tier is **cold** — at 3.1% density
(*measured*) some 97% of it is outputs nobody will ever read again — and it lives
in R2, where 65 GB costs **$0.98/month** (*derived*, §6).

`base/<epoch>/` is a **cache over the log**, not a second source of truth. It is
immutable *by name* — epoch E is written once — but it is not idempotent across
epochs, and it does not need to be, because it is re-derivable. Its correctness
is checkable against `log/` rather than asserted.

### 4.1 Row addressing changes

A row is addressed `(chunk: u16, ordinal: u32)` — the output's position in its
chunk's creation order — not by a byte offset into a compacted arena. This is a
`layout_version` break and the central cost of the proposal.

It buys three things beyond idempotence:

- the 4 GiB arena cap `B2` flags as *"a `layout_version`-visible property, not an
  accident"* goes away
- the ordinal is already **dense per chunk**, which is precisely what
  `DELTA_CHAIN.md` §2 needed a `by_outref` entry index to synthesise — so the
  liveness bitmap shards by chunk for free
- a shard is self-locating: chunk C's rows can be rebuilt without reading
  chunks 0..C−1

### 4.2 Liveness is a bitmap, never a deletion

`live/<chunk>.bits` is one bit per output created in chunk C. A spend flips a bit
in the *epoch's* bitmap; it never touches `log/created/<chunk>.bin`.

⚠️ **The bitmap is a reader-side structure, not a published one.** What gets
published is `log/spent/<chunk>.bin` — a spend *list*, immutable per chunk — and
the reader ORs the lists it needs into a bitmap at open. A published bitmap is
cumulative, so it is a full rewrite every chunk *and* it cannot be rolled back
(§5.4). `MITOS_AT_THE_EDGE.md` §7.1 carries the correction against
`DELTA_CHAIN.md` §B4, which specifies the bitmap as the artifact.

*Measured*: 10,911,674 live bits = 1.36 MB. *Derived* over all 355,490,215
outputs ever created: **44 MB per epoch**, against re-writing a ~1.8 GB arena.
This is the substitution that makes the whole thing work — **a death costs a bit,
not a rewrite.**

## 5. The determinism requirements

Idempotence is a property of the *builder*, and these are the four ways to lose
it silently.

1. ⚠️ **Policy interning must become per-chunk.** Global creation-order ids
   (§2) make every shard depend on every earlier chunk. Each `created/<chunk>.bin`
   carries its own `<chunk>.pol` table of the 28-byte policies that chunk saw;
   global interning happens only in `base/`, where it is derived and disposable.
   *Derived* cost: a chunk sees a few thousand distinct policies ⇒ ~100 KB, against
   the 231,737 × 28 B = 6.49 MB global table (*measured*).
2. **No hash-map iteration reaches the output.** Every ordering sorts, and the
   builder asserts canonical order before writing — `write_ordering` already
   sorts, so this is an assertion to add rather than a change.
3. **The content hash must cover the builder identity.** A `layout_version` bump
   or a decode fix changes the bytes; if the hash does not name the producer, a
   code change silently invalidates every shard while every manifest still
   claims a match. `policy-archive`'s stamp already carries `format` — same idea,
   one level down.
4. **Only sealed chunks enter `log/`.** tx-index's rule, for tx-index's reason:
   Mithril appends to the newest chunk until the next one seals. The unsealed tip
   is the live tier's problem, and that boundary is what makes rollback a
   non-issue *here* — see `MITOS_AT_THE_EDGE.md` §7.3, where it becomes very much
   an issue, and forces the live delta to be a per-block **spend list** rather
   than a merged bitmap. A cumulative bitmap cannot roll back, because it does not
   record which block set each bit.

## 6. Asset-friendliness

*Derived* per-chunk sizes, over 9,181 chunks (*measured*, whole-store build):

| | per chunk | total |
|---|---|---|
| outputs created | 38,720 avg (355,490,215 / 9,181) | 355 M |
| `created/*.bin` at 182 B/row | **~7.0 MB** | **~65 GB** |
| spends | 37,533 avg | 345 M |
| `spent/*.bin` at 12 B/record | **~450 KB** | **~4.1 GB** |
| `live/*.bits` | ~4.8 KB | 44 MB/epoch |

At the **tip** the rate is lower and *measured*: 13,731 creations and 14,412
inputs per chunk, a delta slope of **3.3 MiB/chunk**, one chunk sealing about
every 6 hours.

⚠️ **The averages are not the constraint; the variance is.** Whole-store chunk
bytes average ~24 MB while `DELTA_CHAIN`'s tip window measured 6.2 MB in — so
chunk weight varies by several times and a busy chunk can exceed 25 MiB. **Split
rule: `created/<chunk>.<part>.bin`, split on a transaction boundary**, which is
`policy-archive`'s rule for busy days, for the same reason — a part that splits
mid-record is a part no reader can open alone.

**File counts**, against 100,000 assets per Worker version (paid):

| | files |
|---|---|
| `log/created` (+ splits) | ~9,500 |
| `log/spent` | 9,181 |
| `base/` at 1 MiB parts | ~2,300 |
| **total** | **~21,000** |

Comfortable on count — **9.5% of the file budget**.

⚠️ **The log does not go in static assets, and the reason is not a limit.** The
two documented limits multiply to 100,000 × 25 MiB = **2,441 GiB**, no aggregate
cap is published, and content-hash dedupe means `log/**` would upload once and
never again (§7) — so 65 GB would *technically* fit at 2.7% of the inferred
budget. **Declined deliberately.** 200 MB of assets is a medium website; 65 GB is
400× that, and fitting inside a limit is not the same as being the workload a
product is for. Recorded as considered-and-declined so it is not rediscovered.

**The layout maps onto `MITOS_AT_THE_EDGE.md` §5's three tiers**, each on the
product built for it:

| tier | from this layout | where |
|---|---|---|
| hot | `base/<epoch>/` navigation — 3 directories, `policy.tbl`, `policy.runs.idx` (~33 MB) plus published projections | static assets |
| warm | `base/<epoch>/` rows, entry arrays, clustered arena (~2.3 GB) | regional R2 + CDN, **or a lean node's local disk** |
| cold | `log/**` (~65 GB) | one R2 bucket — never on the read path |

**Part size is set by the read path, not by the cap.** A part is fetched whole,
never range-read, so:

⚠️ **Part size is a property of the SUBSTRATE, not of this format.** Measured
2026-09-25 (`EDGE_PROBE_PLAN.md`), the same fixtures on two providers disagree
completely about whether depth costs anything:

| depth cost at | Cloudflare + CDN | Tigris |
|---|---|---|
| 16 MiB | 69 ms | 7 ms |
| 64 MiB | 438 ms | −5 ms |
| **256 MiB** | **1774 ms** | **16 ms** |

On Tigris there is **no latency reason to shard at all** — a 1.69 GiB arena reads
as fast as a 1 MiB part. On Cloudflare depth is free only below ~16 MiB. So the
table below is **Cloudflare's** answer, and the format must carry part size as a
*publish-time parameter* rather than a constant: a builder targeting Tigris that
split arenas at 16 MiB would pay 1,700 extra objects for nothing, and one
targeting Cloudflare that did not would pay up to 1.77 s a read.

⚠️ **And Tigris supports exactly one range** (`416 InvalidRange: Only support one
range`), against Cloudflare's 2–299. That does not change part size but it does
make §4.8's clustering mandatory rather than preferred there — see
`MITOS_AT_THE_EDGE.md` §4.8.

**Measured 2026-09-24** (`cnft.dev-workers/docs/design/EDGE_PROBE_PLAN.md` §8a),
which replaced the 1 MiB figure an earlier draft used for both tiers:

| tier | part | why |
|---|---|---|
| hot — static assets | **1 MiB** | fetched **whole**, so the bound is a 128 MB isolate |
| warm — R2 by range | **1 MiB** | cold depth is **~35 ms/MiB** over a ~650 ms floor, and concurrency is free, so smaller wins on every path |
| cold (`log/`) | **up to 25 MiB** | never on the read path — the rebuild and verification tier (§8) |

⚠️ **The warm row said ~16 MiB until 2026-09-25, on figures that were
origin-warm.** Measured against never-touched objects (`EDGE_PROBE_PLAN.md`
P7/P8), 16 MiB costs ~1210 ms cold against ~685 ms at 1 MiB, and the multi-range
batching that was meant to justify larger parts never amortises because
**4 concurrent cold reads cost about the same as one**. Below ~1 MiB the floor
dominates (650 of 685 ms) so there is nothing further to win while object count
doubles — which is why the knee is at 1 MiB rather than lower.

**Parts are uniform**, so a reader resolves a logical offset by arithmetic —
`offset / PART` and `offset % PART` — rather than through a table on the hot path.
Only the last part of a file is short. ⚠️ A run can therefore **straddle a
boundary** — a 332 KB clustered read does so about a third of the time at 1 MiB —
so a reader must handle two parts rather than assume one. Concurrency is free, so
that costs an extra operation and not extra latency.

⚠️ **The two are not the same number, and the reasoning that said so is
retired.** Cloudflare does **not** cache range-accessed objects in 1 MiB chunks —
measured, one cold 1 KiB read caches the **whole object**. Cold cost tracks how
*deep* a read goes, and depth is free below ~16 MiB (33 ms at 1 MiB, 69 ms at
16 MiB, then 438 ms at 64 MiB). So on the warm tier 1 MiB buys nothing 16 MiB
does not, while costing sixteen times the objects — and **multi-range works only
within one object**, so more parts means more separate GETs.

⚠️ **And the warm tier has a second constraint if it sits behind an R2 custom
domain**: the maximum CDN-cacheable file is **512 MB** (Free/Pro/Business; 5 GB
Enterprise), and `r2.dev` does not cache at all. 1 MiB parts clear it by three
orders of magnitude, but a future "just store the arena whole" shortcut would
not.

**A lean node changes which of this matters.** `MITOS_AT_THE_EDGE.md` §6.1: a
node holding the warm tier on local disk runs `crates/live-book` unmodified over
`mmap`, so part size, alignment and cacheability stop being constraints
altogether. They are properties the *remote* readers need, and the format has to
carry them because the same bytes serve both.

## 7. What a re-ingest actually writes

This is the requirement, answered directly. Two ingests a week apart, ~28 new
chunks (*derived* at one chunk per ~6 h):

| | files written | bytes |
|---|---|---|
| `log/created`, `log/spent` for new chunks | ~56 | ~200 MB |
| `log/*` for existing chunks | **0** | **0** |
| `base/<new epoch>/` | ~2,300 | ~2.3 GB |
| `base/<old epoch>/` | **0** | **0** |

So the log grows by what the chain grew by, and nothing already published is
touched. The base is re-derived — and **that is a policy choice, not a
requirement**: a consumer can run off `base/<old epoch>` plus the log's newer
chunks indefinitely, paying `DELTA_CHAIN`'s measured 3.3 MiB per chunk of lag.
The epoch exists to bound that, not because the old base expires.

### 7.1 Epoch cadence

`DELTA_CHAIN.md` §1 measured the pressure: at lag 101 chunks, **57.38%** of all
inputs resolve in the gap rather than the base, against **15.82%** at lag ~0. At
one chunk per ~6 h, 101 chunks is about 25 days.

⇒ **weekly**, ~28 chunks of lag, which also lands on the existing weekly Mithril
refresh in the ops cycle. *Derived*, not tuned — §10 wants it measured.

## 8. Re-ingest becomes a self-check

The property worth having for its own sake: because every shard is
content-addressed and its input is a certified, immutable chunk, **running the
builder again is a verification pass**.

Re-derive chunks 1..N from a fresh snapshot; every hash must match
`chunk.manifest`. A mismatch is exactly one of three things, and all three are
worth being loud about: the builder changed (§5.3), the snapshot is corrupt, or
the decode is non-deterministic.

That is stronger than what exists today, where `verify` audits an artifact
against *itself* — 32.7 M entry confirms with 0 failures (*measured*) proves
internal consistency, not reproducibility from the chain.

⚠️ It also replaces `MITOS_AT_THE_EDGE.md` §4.2's attestation with something
better: a reader is not trusting a publisher's verdict, it is trusting a hash it
could recompute. The attestation stays useful for `base/` (derived, not
reproducible from chunks alone) and becomes unnecessary for `log/`.

## 9. What this costs

| | |
|---|---|
| `layout_version` break | row addressing (§4.1), per-chunk interning (§5.1) |
| storage | ~65 GB log + ~2.3 GB per live epoch ⇒ **~$1.02/month** in R2 (*derived*) |
| blowup vs today | **~30×** the 2.23 GB book, to hold 355 M rows instead of 10.9 M |
| build | unchanged in shape — §B7's one pass, with per-chunk sinks instead of one global one |

The blowup is the honest price of idempotence and it buys back more than it
costs: no total rewrite, a verifiable re-ingest, and a base that is disposable
rather than precious.

### 9.1 Why not an embedded engine

An LSM or B-tree store — fjall, RocksDB, redb — solves the multi-ordering problem
with **column families** (one keyspace per ordering) and **covering indexes**
(duplicate the payload into the index that needs it, rather than clustering the
heap twice). That is, method for method, what `B1`'s permutations and §4.8's
clustered arena already are. The design converged on the textbook answer.

`LIVE_BOOK.md` §Decided rejects adopting one, for four structural reasons, and
**three of them get stronger under this doc, not weaker**:

| reason | under this design |
|---|---|
| the input is immutable and per-file signed, so **re-derivation replaces the journal** | §8 makes re-derivation a *verification* pass. An engine's WAL is solving a problem we do not have |
| the write unit is a 6-hour file, so **there is nothing for a compaction scheduler to schedule** | §4's shards are written once per sealed chunk. The one workload an LSM is worst at is bulk-loading already-sorted data |
| a user-space cache is **anonymous and unevictable** where the page cache is not (`FOOTPRINT` §4.1, measured) | unchanged, and it is what a cgroup OOMs on |
| a B-tree lookup costs several page reads against a sorted array's **one directory page plus 1–3 entry pages** | unchanged — measured at 1223 ns |

⚠️ **And compaction would break §3 outright.** An LSM's SSTables are individually
immutable, but *which* SSTables exist is a function of the compaction schedule,
not of the input. Re-ingesting the same chunks would yield a logically identical
store with physically different files — so content-hash dedupe (§7) and the
self-check (§8) both fail. The property this doc exists to get is the one an
engine cannot give without being fought.

**Where an engine is right: the tip.** `DELTA_CHAIN.md` §5 — *"the reason a
database looks attractive is that nothing in the delta is checkpointed"* — and
the delta is small, mutable and write-heavy, which is the LSM sweet spot.
`redb` is already in the stack (module kv, `indexer_data.redb`), so that is an
incumbent rather than a new dependency.

**What to take without taking the engine:**

- **Key delta-encoding within a block.** Sorted `u64` prefixes stored as deltas
  from the previous key is standard LSM block encoding and is **unexploited
  here** — 393 MB of entry arrays is the target. Not costed.
- **A bloom per part** for cheap negatives (`policy-archive` already does this
  per row group).
- **A multi-level sparse index** if cardinality ever moves an order of magnitude;
  `dir.idx` is single-level today, and `B3` already says the width must be
  re-derived rather than inherited.

## 10. Open questions

1. ~~**Is the builder deterministic today?**~~ **Already answered, and in this
   design's favour.** `LIVE_BOOK.md` §Decided: canonical order is `(prefix,
   tail)`, the 96-bit build key *"derivable from the outref alone — so two
   builders can be compared byte for byte and the artifact does not depend on how
   the builder used its memory"*, and it is **checked, not assumed**: `rows.bin`,
   `by_outref.idx`, `dir.idx` and `policy.tbl` are **byte-identical across runs
   at 0, 1 and 7 compactions**. So the compaction worry is already measured away
   and §5's remaining requirements are about keeping a property that exists, not
   establishing one. ⚠️ Still open: that check covered four files, not
   `by_addr.idx` / `by_policy.idx` / `policy.dir.idx` / `addr.dir.idx`, and it
   was not run at the whole-store build's **67** compactions.
2. **Is 182 B/row right for historical rows?** §6 extrapolates from the *live*
   set's measured 166 B and the delta row's 182 B. The live set is young
   (`LIVE_BOOK.md` says so explicitly), so historical rows are probably lighter
   and 65 GB is probably high — but it is an extrapolation across 355 M rows and
   deserves a real sample.
3. **Does a 25 MiB cap survive the busiest chunk?** §6's split rule assumes it
   does not. Measure the heaviest chunk's created-output payload before choosing
   the part size.
4. **How big can the hot tier honestly get?** §6 declines the 2.4 TiB reading on
   judgement, not measurement, which leaves the actual line undefined. ~200 MB is
   plainly fine and 65 GB plainly is not; 2 GB is the question nobody has asked
   Cloudflare. Worth a support conversation before the hot tier is load-bearing,
   because the answer decides whether the warm tier can ever be promoted.
   Two things a small ramp would show for free either way: whether a cold colo
   pulls an asset on first request or already has it, and what a large first
   deploy actually costs in wall time.
5. **Should `base/` shard by creation chunk too?** It would make most parts
   identical between epochs and dedupe on content hash — but a spend anywhere
   dirties that chunk's part, and ~105,000 spends a day scattered over 9,181
   chunks would dirty most of them. *Derived*, and worth checking rather than
   assuming: the live set is young, so the spends may concentrate in recent
   chunks far more than uniformly.
6. **Does `tx-index` fold into this?** It is already chunk-sharded and already
   has the property. `B7` argues for one pass and many artifacts; this layout is
   the shape tx-index's segments already have, so the two may be one thing.

## 11. Decided

- **Store the log, derive the set.** §3. A set cannot be made idempotent; a log
  already is.
- **Liveness is a bitmap, never a deletion.** §4.2. A death costs a bit.
- **The chunk is the shard boundary**, because it is the boundary the chain
  itself seals — not a size we picked.
- **`base/` is a cache, not a source of truth.** It may be deleted and rebuilt.
- **Only sealed chunks enter `log/`.** tx-index's rule, and it is what makes
  rollback someone else's problem.
- **The part is the read unit**, not a range within a file.
- **The log stays out of static assets.** §6 — a judgement about what the product
  is for, not about what its limits permit.
