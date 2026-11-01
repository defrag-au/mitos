# Mitos at the edge — the artifact family, distributed

Status: **analysis, nothing built** (2026-09-24). Written from a read of
`crates/live-book`, `community-modules/`, `workers/collection-ownership`,
`crates/policy-archive` and `crates/cf-publish`; the workload figures in §3 are
**live, pulled from `ownership.cnft.dev/api/collections` on 2026-09-24**;
platform limits and prices were looked up the same day.

**No reader, no publisher and no node exist.** Figures marked *derived* are
arithmetic over a measured one with the assumption stated.

Owner: TBD

Related:
- `ARTIFACT_SHARDING.md` — **the format this doc assumes**: chunk-sharded
  immutable shards, an idempotent re-ingest, and §9.1 on why not an engine
- `LIVE_BOOK.md` — the artifact, its format rules (§B5), its reader (§B9)
- `DELTA_CHAIN.md` — the measured cost of a follower. §7 below **corrects its
  published shape**: per-block spend lists, not a cumulative bitmap
- `PHONEBOOK_COVERAGE.md` §7 — the consumer inventory from shipped call sites;
  §11 — `BookDataPlane`, and the `tip` gap §7 below closes
- `FOOTPRINT_AND_MINIMUM_SPEC.md` §7 — §6's lean node is that doc's lean profile
  with a geography
- `cnft.dev-workers/docs/design/OWNERSHIP_WITHOUT_SYNC.md` — the consumer-side
  consequence: an app that looks it up instead of syncing it
- `cnft.dev-workers/docs/design/EDGE_PROBE_PLAN.md` — **the experiments that
  replace this doc's *derived* figures with measured ones**. Targets §13's open
  questions 1, 2 and 9
- `cnft.dev-workers/docs/design/POLICY_ARCHIVE_AND_SCALE.md` §"The bundle" — the
  one time this was already done, and the latency table that is the whole lesson

---

## 1. The question

The original thesis was **put heavy queries close to fast data, and distribute
the nodes**. Cloudflare is one answer and not the only one, so:

1. Which of the things we actually ask mitos for can be answered away from the
   box, and what does each cost? (§3)
2. What holds the bytes, and where does it run? (§5, §6)
3. How fresh can it be? (§7)

## 2. What ports, and what does not

| capability | away from the box | why |
|---|---|---|
| point lookups (`read_utxos`, `utxo`) | **yes** | §3.3 |
| `utxos_by_address` / `by_payment_cred` | **yes** | §3.3 |
| `utxos_by_policy` / cold starts | **yes, with §4.8 or §4.10** | §3.1 — 9,100 round trips as shaped today |
| policy archives / density / feeds | **already there** | `flow-explorer/src/archive_r2.rs`, shipped |
| asset → owner | **as a projection, not from the book** | §3.2 |
| the tip | **yes, and it is cheap** | §7 |
| Dolos + chain-sync | **no** | 145 GB + a 217 GB chunk store |
| the fold that builds the base | **no** | 1.26 h wall, 2.42 GiB peak anon, 214.86 GiB read |
| recognition — the 19 decode modules | **no** | §11 |
| community modules as built | **isolate: no · VM: yes** | `wasm32-wasip2` **components**; Workers run core wasm |
| `read_tx`, `tx_metadata`, `asset_state`, CIP-25 | **no** | `PHONEBOOK_COVERAGE.md` §4 — they need chunks |

---

## 3. The workloads, priced

Five things we ask mitos for, costed in **round trips**, because §8 shows that is
the only unit that matters.

**The live shape of the problem** (measured 2026-09-24, `/api/collections`):

| | value |
|---|---|
| tracked policies | **163**, all `last_sync_status: ok` |
| of which added on demand | **136** — a person asked and waited |
| assets tracked | **599,798** |
| holder rows | **93,688** |
| collection size | median **2,600** · p90 **9,976** · max **18,594** |
| holders per collection | median **269** · p90 **1,419** · max **4,050** |
| onboarded per month | **20–35** (from `created_at`, Apr–Sep 2026) |

Collection-ownership subscribes **six modules per policy** — ~978 companion
registrations today, ~6,000 at the 1K target.

### 3.1 Cold-start a collection — the primary workload, and it does not port as shaped

`collection_holders.rs::scan_page` is two host calls:

```rust
let page = chain_data::utxos_by_policy(&policy, after, COLD_START_PAGE_HINT); // 10_000
for (_oref, out) in chain_data::read_utxos(refs) { apply_output_to_ledger(...) }
```

`utxos_by_policy` returns **refs**; `read_utxos` resolves them. Locally the
second traversal is free — `enumerate` already read each row to confirm the
candidate, so it is in the page cache. **Over a network it pays for the same row
twice.**

*Derived*, median collection, ~1.3 assets of the policy per output ⇒ ~2,000
outputs:

| step | round trips |
|---|---|
| `by_policy` directory bucket | 1 |
| the run (2,000 × 12 B = 24 KB — one page) | 1 |
| confirm each candidate against its row | 2,000 |
| `read_utxos`: per ref, directory + bucket + row ×1.55 | ~7,100 |
| **total** | **≈ 9,100** |

At six simultaneous connections that is ~1,517 serialised batches. Class B cost
of those reads: **$0.0033**. **The money is irrelevant and the latency is
fatal** — the opposite of how this doc's first draft framed it.

Two fixes, and they are complementary rather than competing:

| | requests | RT at 6 concurrent |
|---|---|---|
| as shaped | ~9,100 | ~1,517 |
| **§4.10 multi-range**, 299 ranges each | 7 | **2** |
| **§4.8 clustered arena**, one contiguous read | 1 | **1** |

**Two findings, and the second is the more important:**

1. Scattered reads are affordable *only* with one of §4.8 or §4.10. Neither is an
   optimisation; without them this workload is not expressible off-box.
2. ⚠️ **Refs-then-read is an `mmap` artefact.** It is free only because the
   confirm already paged the row in. Any transport with a real per-read cost pays
   double — including `MITOS_DATA_PLANE_API.md`'s planned `IpcDataPlane` and
   `GrpcDataPlane`. **A `scan_policy → rows` primitive is owed to the trait**,
   not to this doc.

**And it touches a known problem.** Per-policy onboard is O(N²) today because
recapture rescans the whole module interest. A cold start sourced from a
clustered artifact is *inherently* scoped — a policy's rows are one contiguous
range.

### 3.2 Asset → owner — the book is the wrong artifact

`LIVE_BOOK.md` §B9: *"the book is a set of outputs, not the entity graph."* It is
keyed by outref, address and policy — never by asset name — and an NFT's current
owner is a fold over the set, not a row in it.

**Publish the projection, not the set.** *Derived* at ~280 B/asset (5,000 assets
≈ 1.4 MB measured), all 163 bundles are **~170 MB**.

⚠️ But see `OWNERSHIP_WITHOUT_SYNC.md`: the *owner* column specifically **is**
derivable from `utxos_by_policy` plus an address parse, in one read. It is the
*traits and names* that are a genuine projection, because they need the mint
ledger. The split is volatility: **ownership is volatile and derivable; metadata
is stable and expensive.**

### 3.3 A wallet's UTxOs — the best fit, with an outside payoff

*Derived* for a 50-UTxO wallet: 1 directory + 1 bucket + 50 rows = **52 round
trips**, nine batches at six concurrent — or **one** multi-range request (§4.10).

The payoff is outside mitos. After cancel came down to **81 ms**, the remaining
bottleneck is CIP-30 `getUtxos` — a wallet call with no cache and no control over
it. Reading `by_payment_cred` ourselves replaces it with reads we own.

⚠️ **The run is not bounded by "a wallet".** `$burnsnek`'s sink returns **19,768
refs** through `by_payment_cred` (measured); an exchange or script credential
looks the same. The directory read gives `end − start` *before* the fetch, so
branch on it rather than discover it.

⚠️ **And this is the worst case for §4.7's amplification — on the origin leg
only.** A wallet's 50 UTxOs are scattered across `rows.bin`, so *derived* they
touch ~49 distinct 1 MiB chunks: **~49 MiB fetched edge ← R2 to serve ~13 KB
client ← edge.**

What that does and does not cost:

| | |
|---|---|
| client bandwidth | **unaffected** — 50 rows plus multipart headers |
| Class B ops | ~49, i.e. **$0.000018**. Noise |
| egress | zero |
| latency | ⚠️ **unmeasured** — one origin round trip plus however fast Cloudflare pulls 49 MiB internally. Do not assume it is bad; do not assume it is free |
| **cache occupancy** | **the real cost.** 49 MiB of shared LRU colo cache filled with chunks nobody reads again, because the next user asks about a different wallet — so scattered reads **evict the hot tier** |

Unlike §3.1 there is no clustering to reach for: `by_addr` runs are short, so a
stake-clustered arena would cost 1.69 GiB to fix a query returning tens of rows.
**This is the one genuinely unresolved trade in the doc**, and §13.9 states it
rather than pretending otherwise.

### 3.4 Holder distribution — cheap only if the rows come back

`utxos_by_policy` **twice** plus `resolve_stake_for_payment_pkh` per holder. As
shaped: two of §3.1's scans plus *derived* ~807 round trips for a median
collection's stake resolutions, ~12,000 for the largest.

Clustered, the rows come back with their address bytes and the stake part is
*inside the address* — **zero extra reads**. `resolve_stake_for_payment_pkh`
exists because dolos could not hand back the row; an artifact can.

### 3.5 Steady state — and it may not need to exist

Ownership tracking after cold start is deltas **pushed** from the box to the DO —
DO **rows written at $1.00/M** plus a request per delivery, times the six-module
fan-out. That is where money is spent today.

⚠️ §7 and `OWNERSHIP_WITHOUT_SYNC.md` together suggest **deleting this rather
than batching it**: if a consumer can derive ownership from one read plus a live
delta, the per-transaction projection has no reason to exist.

### 3.6 What the five say together

| workload | frequency | today | as shaped | fixed |
|---|---|---|---|---|
| cold-start a collection | 20–35/month, interactive | box | ~9,100 RT | **1–2** |
| asset → owner | every page view | DO SQL | *wrong artifact* | derive, or publish |
| a wallet's UTxOs | every tx build | CIP-30 | 52 RT | **1** |
| holder distribution | per sync | box | ~9,900 RT | **~1** |
| transfer deltas | continuous | push | n/a | possibly unnecessary |

**Three of four read workloads want a projection or a fold, not the set itself.**
That is the finding §5's tiering is built on, and none of the five is limited by
money.

---

## 4. The translation table — local filesystem to byte ranges

What the local reader does, and what each technique becomes when the bytes are
remote. §5 decides *which* of these you still need: a tier held locally needs
none of them, and §6.1's lean node needs none at all.

### 4.1 `mmap` → a range GET, and there is no page

`reader.rs`: *"a lookup touches one directory page plus one or two entry pages
and reads the row in place, so the resident set is the working set."* The page is
doing the work locally, and the obvious move is to pick an equivalent on the
remote side and align every fetch to it.

**Measured 2026-09-24: don't bother. There is no page.** One cold 1 KiB range
read against a 256 MiB object left *every* chunk out to 255 MiB reporting `HIT`,
immediately. **The cache unit is the object**, not a page within it, so aligning
reads to any grid buys nothing — there is no second cache key to reuse, because
there was only ever one.

Two earlier drafts of this section were wrong in the same way, and the shape of
the error is worth keeping: the first picked 64 KiB, the second corrected it to
1 MiB on the strength of Cloudflare's *"caches range-accessed objects in 1 MiB
chunks"*. That sentence describes origin **fetch** alignment; it does not
describe cache **granularity**, and both drafts derived page counts from it that
describe nothing.

**What the reader actually has to choose is object size**, because size caps how
deep a read can go and depth is what costs (§4.7). That is §5.1's decision, and
it is a format decision rather than a reader one.

### 4.2 `Base::open`'s validation → a publish-time attestation

⚠️ `Directory::validate` runs at open *on purpose* — *"every lookup indexes
through this array."* Two passes over 2^21 buckets plus a random probe into the
entry array per non-empty bucket, for **three** directories. Local: **73 ms**
(measured). Remote, *derived*: ~2.1 M random reads. Not slow — impossible.

**The form that works:** the box validates; the publisher writes an
**attestation** (fields, lengths, content hashes, verdict, build id) as one
postcard object; the reader's `open` is one small read. **This move was already
made once** — the policy archive's KV bundle took density from 0.88–1.3 s to
**60–70 ms**.

⚠️ `ARTIFACT_SHARDING.md` §8 partly supersedes this: for content-addressed log
shards a reader trusts a hash it could recompute, not a verdict. The attestation
stays necessary for `base/`, which is derived.

### 4.3 The directory probe → an 8-byte read, or resident

`range_for` reads two adjacent `u32`s at `8 + 4·b`. A directory is **8.39 MB =
9 cache chunks of 1 MiB**; all three resident is 25.2 MB of a 128 MB isolate. §5 puts
these in the hot tier precisely so this hop disappears.

### 4.4 Binary search in a bucket → fetch the bucket

Remotely each probe is a **dependent** round trip. The directory read already
returned `end − start`, so branch on it: **≤ 87,381 entries (1 MiB)** → one GET
and search in memory; larger → binary search over aligned cached chunks.

At §4.7's 1 MiB unit the branch is close to vestigial: the measured average bucket
is **5.2 entries**, and even `$burnsnek`'s pathological **19,768-entry** run is
237 KB — comfortably one chunk. The fallback exists for a cardinality that has not
happened rather than one that has.

`DIR_BITS = 21` was chosen for an 8 MB directory on an 8 GB box (`B3`); it *also*
puts the average bucket two orders of magnitude inside one page. That is luck,
and `B3` warns the width must be re-derived if cardinality moves.

### 4.5 A variable-length row → a speculative over-read

`Row::parse` needs the 56-byte header to know the row's length ⇒ **two dependent
round trips per row**. Read `56 + K` speculatively: median row **166 B**. The long
row is rarer than this section first claimed and longer: over the whole store **630
rows exceed 8 KiB**, the longest **22,656 B** (a 22,600-byte Byron address —
`LIVE_BOOK` §Sizes), and the asset-heavy end reaches the same size — HOSKY holds
343 and 352 asset records in one row. So a fixed window would be wrong in both
directions, and the implemented shape is the one that cannot be: **8 KiB
speculative, grown geometrically on `RowRead::OutOfRange`** — one read for the
median row, three for the longest, and "not a row" never confused with "needs
more bytes" (`LARGE_POLICY_VIEWS` §0). `book.meta` carries `max_addr_len`, so a
future reader can still bound the growth.

### 4.6 `rename` swap → content-addressed paths, or an atomic version

⚠️ **The local invariant inverts.** `format.rs`: *"Writing in place would
truncate under a live reader's mapping."* Object storage has no inode, so a
reader mid-lookup can take a bucket offset from the **old** directory and an
entry from the **new** array. Nothing traps; the row's confirm rejects it; the
symptom is **a live output reported as absent** — the worst failure mode, because
it looks like data.

- **R2**: build-scoped immutable paths, one pointer object flipped under
  `If-Match` (`cf-publish`'s existing invariant), resolved once per request.
- **Static assets**: a **Worker version is atomic**, so a request cannot straddle
  two builds. The invariant comes with the deployment model.

⚠️ **And the old epoch must not be pruned when the new one lands.** A client pins
an epoch (from the pointer, or from the locator's `epoch` field) and then reads
over *several* requests — a directory probe, a bucket, a run, a page of rows.
Pruning epoch N the moment N+1 publishes turns those follow-up reads into 404s
**mid-query**, and the symptom is again §4.6's worst one: rows that appear to have
vanished.

So a published epoch needs a **retention window**, and it is not a tidy-up detail:

- **at least one full epoch**, so a client that pinned N can finish even if it
  paged slowly across the boundary
- longer if any consumer caches an epoch id — a browser holding a pinned epoch
  across a session is the obvious case
- pruning is therefore *two* epochs behind, not one, and the publisher's
  `cf-publish` sequence must prune **after** the flip rather than before

`policy-archive` already prunes predecessors after a conditional flip; this is the
same ordering with a wider window, and the window is the part to write down.

### 4.7 The page cache → the whole object, and the cost is depth

**Measured 2026-09-24** against a real R2 bucket behind a custom domain
(`EDGE_PROBE_PLAN.md` §8a). Three findings, and the first two replace what two
earlier drafts of this section asserted:

**1. The cache unit is the object.** One cold 1 KiB read cached the entire
256 MiB object — chunks 8, 16, 64, 128 and 255 all `HIT` immediately afterwards,
synchronously. So there is no per-chunk cache state to reason about, no alignment
to respect (§4.1), and the chunk-count arithmetic both earlier drafts derived
describes nothing.

**2. Cold cost tracks how *deep* the read goes, not how many chunks it touches.**
Holding range count at 50 and varying only position: 50 ranges in **one** chunk
249 MiB deep took **2.82 s**, *slower* than 50 ranges across **fifty** chunks at
the same depth (2.39 s). Cloudflare streams the object sequentially from origin
and cannot complete the response until it has passed the furthest requested byte.

**3. Depth is free below ~16 MiB.** The measured ladder — tail-minus-head per
object, each pair its own control:

| object size | 64 KiB | 1 MiB | 4 MiB | 16 MiB | 64 MiB | 256 MiB |
|---|---|---|---|---|---|---|
| depth cost | −17 ms | 33 ms | −69 ms | 69 ms | **438 ms** | **1774 ms** |

Noise to 16 MiB — two values are negative — then linear at **≈ 7 ms/MiB**. The
cold floor is **~330–400 ms** (ENAM bucket, read from Brisbane) whatever the
object's size; a warm read is **11–14 ms**.

⚠️ **Amplification is on the origin leg only.** The edge returns the client
*exactly* the bytes it asked for. The fetch is **edge ← R2**, internal, zero
egress — so the costs are Class B operations (trivial), origin-fetch latency
(now measured, above), and **cache occupancy**. Never client bandwidth.

⚠️ **The real lever is caching, not layout.** ~350 ms cold against 11–14 ms warm
is a **25×** difference, an order of magnitude more than anything §4.8 or §4.10
can recover. Whatever else is decided, keeping the working set warm (§5.3) is the
optimisation that matters.

**Three constraints:**

1. **CDN caching needs an R2 custom domain** — `r2.dev` does not cache.
2. **Max cacheable file is 512 MB** (Free/Pro/Business; 5 GB Enterprise), checked
   against the complete size from `Content-Range`, so a small range does not
   bypass it. An oversized object is not cached *worse*, it is **not cached at
   all**. ⚠️ But note finding 3 makes this the *lesser* constraint: a 256 MiB
   object is cacheable and still costs 1.77 s of depth. **Latency caps part size
   long before the cache rules do.**
3. ⚠️ **R2's default is `cache-control: max-age=14400`** — four hours, not the
   "forever" immutability would justify. Untouched, every part goes cold **four
   times a day**.

   ✅ **Fixed at publish, verified 2026-09-24.** `wrangler r2 object put
   --cache-control "public, max-age=31536000, immutable"` is served back
   verbatim; an object uploaded with it returns the year-long directive while an
   untouched sibling still returns `max-age=14400`. **Set it on the object rather
   than in a zone Cache Rule** — the immutability is a property of the artifact
   (§4.6), so it should travel with it and survive being copied to another bucket
   or fronted by another zone.

Cache Reserve is unavailable for R2 public buckets anyway (§5.3).

### 4.8 Readahead and locality → a clustered replica

§3.1 and §3.4 are the argument:

| file | shape | size |
|---|---|---|
| `rows.by_policy.bin` | the rows, clustered by policy | ~1.69 GiB (*derived*) |
| `policy.runs.idx` | `policy id → (u32 offset, u32 len)` | 231,737 × 8 B ≈ **1.85 MB** |

One directory read and one contiguous range GET returns a policy's holder set
**as rows**. It rides `B7`'s *one walk, many artifacts* rule.

⚠️ **Why, restated — because the first two answers were both wrong.** This
section argued first from round-trip count, then from "1 MiB per distinct chunk
touched". §4.7's measurements retired both: the cache unit is the object, and
cost tracks **depth**, not chunk count.

**What survives is narrower and still decisive.** Within a part, clustering means
a policy's rows are contiguous, so one range request returns them all and the
read is as shallow as the part allows. Scattered over the same part, the read
reaches as deep as the furthest row — and at §5.1's part size that is the
difference between the noise floor and nothing much. **At 16 MiB parts,
clustering saves tens of milliseconds, not the 600× an earlier draft claimed.**

**So its real justification is request count, not bytes.** Clustered, a median
collection is **one** range request (§3.1). Unclustered, it is a multi-range
request that must stay under §4.10's 299 ceiling, or several — and `read_utxos`
has to run at all. That is a shape argument, and it holds regardless of what the
cache does.

**Scope.** Required for the **one** access pattern known to dominate and to be
interactive — policy cold start. *Not* needed per ordering: multi-range makes
scattered reads affordable, so `by_addr` and `by_stake` get orderings without
clustered arenas, which is the cost that would otherwise have exploded — a second
and third 1.69 GiB copy.

`policy.runs.idx` is also the family's best bytes-to-capability ratio: **1.85 MB
for a complete single-hop index**, with `holder_count` falling out of `len`.

#### 4.8a ⚠️ Measured 2026-09-25: the clustered arena is not an optimisation

Everything above was argued. `live-book-edge` is now deployed against a **windowed**
book (40 chunks, 128,567 live rows, `rows.bin` = 21 parts) and the numbers are in.
Both queries return answers identical to `fold-probe`'s over the same book, so what
follows is about cost, not correctness.

⚠️⚠️ **THE TIMINGS IN THE FIRST VERSION OF THIS SECTION WERE WRONG — read §4.8c
before quoting any of them.** They were taken with `curl --doh-url` one request per
process, so **~1.05 s of DNS-over-HTTPS + TCP + TLS was inside every number.** The
real warm wallet query is **40 ms**, not 1.16 s. The *parts* and *bytes* columns
below were never affected; only the times were, and they have been removed rather
than corrected in place so the mistake is not quotable.

| query | parts touched | bytes |
|---|---|---|
| `/v1/utxos/:cred`, 2–11 UTxOs | **4–6** | 3.5–5.1 MB |
| `/v1/policy/:policy`, 1026 holders | **24 of 24** | 22.7 MB |

The wallet query is the shape this design predicted: one directory part, one entry
part, one or two row parts, one `policy.tbl` part. It scales, because a credential
has few rows.

**The policy query touched every part of `rows.bin`.** 1026 rows scattered across
21 parts hit all 21 — and that is the windowed book *flattering* it, because 21
parts is only 21 MB. `by_policy` is an ordering over the **index**; the rows it
points at sit in `rows.bin` in creation order, i.e. everywhere.

Project it onto the real arena — 1.69 GiB, **1,730 parts** — with the coupon
collector's expectation, `1730 × (1 − e^(−k/1730))`:

| holders `k` | distinct parts | bytes moved |
|---|---|---|
| 1,026 | 773 | **~773 MB** |
| 5,000 | 1,637 | ~1.6 GiB |
| 10,000 | 1,725 | **the whole arena** |

So an unclustered policy query at full scale reads most of the artifact to return a
few megabytes of JSON. That is not a slow query, it is not a query — which is what
§4.8 says, arrived at from request *shape*. The measurement makes it a size
argument as well.

⚠️ **But only for whole-part reads.** §4.8c measured ranged reads working, and they
change this table's exponent — see §4.8d. The clustered arena is still wanted; it is
no longer the only way to stop a policy query reading the arena.

⚠️ **A windowed book cannot surface this class of problem.** Its `rows.bin` is small
enough that "touch everything" costs 21 MB, which reads as a healthy number. Any
scaling claim has to be projected onto 1,730 parts explicitly, or it will be made
from a measurement that had no room to fail.

#### 4.8b Whole-part reads are a bandwidth tax on narrow probes

A `by_addr` lookup reads **8 bytes** of an 8 MiB directory and **12 × n** bytes of a
131 MB entry array. Served as whole parts that is a megabyte each, ~2.4 MB of fixed
overhead on the smallest possible query.

Mitigating fact: the directories are only 9 parts each, 27 in total, so they converge
to warm and the megabyte is *cache-local* bandwidth rather than R2 cost. But it is
still bandwidth, it still fills the shared colo LRU (§13.9), and a narrow probe never
wants the rest of the part it landed in.

⇒ the lever is a **`Range` request against the cached part**. §4.8c measures it.

#### 4.8c ✅ Measured 2026-09-25: the Cache API DOES honour `Range`

`workers/live-book-edge/src/probe.rs` against a cached 1 MiB part: **206**, correct
`content-range`, exactly 8 bytes, **and the bytes match the same window taken from
the whole part** — checked on two windows including the last 8 bytes of the part. The
no-`Range` control returns all 1,048,576.

⚠️ The byte comparison is the point, not the status code. A 206 whose body is the
*head* of the object regardless of offset looks exactly like success, so
`store::ranged_from_cache` now treats anything but "206 with the expected length" as
an error rather than slicing it — the assumption stays checked at runtime after the
probe is deleted.

**⚠️⚠️ AND THE TIMING METHOD WAS WRONG AGAIN — a third instance of the same rule.**
Every latency figure in the first draft of §4.8a came from `curl --doh-url` invoked
once per request, so **~1.05 s of DoH + TCP + TLS handshake was inside each one**. A
404 that does no reads at all measured 1.08 s. Re-measured with the connection
pre-warmed (two URLs in one `curl`, take the second):

| scenario | **server time** | reads | bytes in |
|---|---|---|---|
| 404, no reads — the floor | **16.8 ms** | 0 | — |
| `/v1/health` | 37.8 ms | 1 | 373 KB |
| wallet, `fetch=exact` | **39.9 ms** | 5 | 389 KB |
| wallet, `fetch=whole` | 52.4 ms | 4 | 1.42 MB |
| policy 1026, `fetch=exact` | 1,084 ms | 1,038 | 8.8 MB |
| policy 1026, `fetch=whole` | **767 ms** | 24 | 22.3 MB |

So the warm wallet query is **40 ms**, not the 1.16 s first reported, and the worker's
own overhead is ~17 ms of that. **RULE, now on its third outing: a latency number is
not a measurement until the thing you are not measuring is subtracted.** Sits beside
"a virgin fixture, and a fixture is virgin once" and "a timing number without its
cache status is not a measurement".

#### 4.8d ⇒ `Fetch::Exact` changes the policy query's *exponent*

The two strategies (`store::Fetch`, selectable per request with `?fetch=`):

| | requests | bytes |
|---|---|---|
| `WholePart` | one per part touched | the whole part, each |
| `Exact` | one per row | the speculative over-read, per row |

**`Exact` reads bytes proportional to the number of rows, not to the size of the
arena.** That is the whole finding. §4.8a's projection only applies to `WholePart`:

| holders `k` | `WholePart` at full scale | `Exact` at full scale |
|---|---|---|
| 1,026 | ~773 MB | **~8.5 MB** |
| 10,000 | the whole 1.69 GiB arena | **~82 MB** |

⇒ **§4.8a's conclusion is softened, not retracted.** An unclustered policy query does
*not* have to read the arena; it has to issue one request per row. `Exact` trades the
size wall for a **request-count** wall, which is a much softer one — though it is
real: 10,000 rows is 10,000 Cache API calls against a 10,000-subrequest budget.

⚠️ **`Exact` must be concurrent, and getting this wrong cost a wrong conclusion.**
The first version read rows in a sequential loop and measured **9.03 s** against
`WholePart`'s 3.12 s — *3× slower while moving 2.5× fewer bytes* — because
`WholePart` warms its parts in one `join_all` and the sequential version had quietly
given up §4.8's phase 3. Through a bounded-concurrency stream (32 in flight) the same
query is **1,084 ms**. The lesson generalises: **any comparison between two read
strategies is invalid unless both are parallel**, because concurrency is worth more
here than bytes.

**Where each wins, measured:**
- **Wallet: `Exact`**, on both axes — 39.9 ms vs 52.4 ms and 3.7× fewer bytes. It is
  the default.
- **Wide policy run: `WholePart` on time** (767 ms vs 1,084 ms, because 24 parts is
  one concurrent batch and 1,038 reads is ~33), **`Exact` on bytes and on scaling**.
  Genuinely a trade, so it stays a parameter rather than a decision.

**Cheap improvement not yet made:** the initial window is **8 KiB** per row because
a row's length is in its header, but the median row is **166 B**. A smaller window
with the re-read the reader now does anyway — it grows geometrically on a long row,
8 KiB → 16 KiB → 32 KiB — would cut `Exact`'s bytes ~16× — 1,026 rows at 512 B is
~525 KB rather than 8.8 MB. Nothing measured; it is the obvious next lever on
`Exact`. *(Being worked separately.)*

#### 4.8f ⚠️⚠️ Two properties of a part key that are not cosmetic

Measured 2026-09-25, and both were invisible until a *browser* tried to read the
artifact.

**1. A part key needs a file extension or it is never cached.** Cloudflare's default
cache eligibility is **extension-based**. `book/v2/rows.bin/00000` returns
`cf-cache-status: DYNAMIC` forever — even after repeated whole GETs — and **a
multi-range request against it returns `400`**, because multi-range is served off the
cache path and an uncached object has nothing to slice. The control is exact:
`small.bin`, same bucket, same domain, caches (`HIT`) and serves multipart. Only the
extension differs. ⇒ `publish::PART_EXT`, and `part_key` is now the **one** place the
key is built.

⚠️ This hid for the entire investigation because every worker-side measurement used
the **Cache API**, a different mechanism that worked fine. The public-domain path —
the one §10's client-direct reads depend on — had never cached once.

**2. R2 needs a CORS policy or a browser cannot read the bucket at all.** Plain
`TypeError: Failed to fetch`. `exposeHeaders` matters as much as `allowed`: without
it JS cannot read `content-range` or `accept-ranges`. Not an exposure change — §5.4
already made the bucket public — it only stops browsers refusing already-public bytes.

**3. Part size decides whether multi-range can do anything.** It works only *within*
one object. For a 1,063-row wallet over a 1.69 GiB arena:

| part size | `rows.bin` objects | rows per object | requests |
|---|---|---|---|
| 1 MiB (`v2`) | 1,722 | ~1 | **~1,000** — nothing to coalesce |
| 64 MiB (`v3`) | **27** | ~39 | **~27**, all well under the 299 cap |

⇒ `publish::publish` now takes `part_size` as a parameter (`ARTIFACT_SHARDING.md` §6
already called for this) and a reader takes the real value from the attestation, so
no reader change is needed to republish at another size.

⚠️ **The >299 cliff is now much more expensive.** 300 ranges returns `200` with the
**whole object** — at 64 MiB that is a 64 MiB download where at 4 MiB it was 4. A
client solver must hard-cap at 299; an off-by-one is no longer a nuisance.

⚠️ **And large parts make a cold *index* read expensive.** `by_addr.idx` is 2 parts
of 64 MiB, so a cold wallet query pulled **140 MB** to read a few hundred bytes of
entries. One-off per colo and then cached, but it revives the "different part sizes
per file" question §6.3b retired: the row arena wants large parts (coalescing), the
index files want small ones (narrow probes).

#### 4.8g ✅ The browser half, finally run

`workers/live-book-edge/frontend/multirange.html`, against a **real** `v3` row part:

| ranges | result |
|---|---|
| 2 / 10 / 100 / 299 | 206 `multipart/byteranges`, all parts present, offsets correct, **bytes identical to a single-range control** |
| 300 | 200 with the whole 64 MiB object |

The page carries its own oracle — every multi-range result is compared byte-for-byte
against the same ranges fetched individually — because a multipart body that parses
into plausible-but-wrong slices passes a structure check. ⇒ **§13.2's browser arm is
answered and §10.2's client-direct design is unblocked.**

⚠️ Its *timings* are meaningless: `--virtual-time-budget` advances
`performance.now()` in discrete steps, so every arm reported exactly 10 ms. A real
comparison needs a headed run.

#### 4.8e The miss path is per-file, and it answers §13.9

⚠️ **`Fetch::Exact`'s first implementation was warm-only.** A ranged read can only be
served by a cache entry that exists, and on a miss it pulled the **whole part** from
R2. So `Exact` read bytes proportional to rows when warm and proportional to *parts
touched* when cold — and a cold wallet query on a full book touches ~1,000 of them.
**~1 GB to return ~8 MB**, which is the number §4.8d's projection quietly assumed away.

R2 serves a single range natively, so the fix is to read the row and leave the
megabyte. But applying that everywhere is also wrong, and the reason is the useful
part:

| file | parts (full book) | reuse per part | miss policy |
|---|---|---|---|
| `*.dir.idx`, `by_*.idx`, `policy.tbl` | ~34 | **every query** | fetch whole, cache it |
| `rows.bin` | **1,730** | ~none | fetch the range, cache nothing |

For the small shared files the megabyte is amortised over every query the colo ever
serves — that is the hot set, and caching it is the whole point. For `rows.bin` it
buys nothing and costs a gigabyte.

⇒ `store::OnMiss::{CacheWholePart, RangeOnly}`, chosen per call site.

**⇒ §13.9 is answered.** That question asked what absorbs the cache pollution from
scattered reads, with three candidates and no measurement. The answer is **none of
them**: scattered row reads simply do not enter the cache. The pollution was a
consequence of treating the part as the cache unit *uniformly*, and the arena never
wanted that. The "two hostnames, different cache rules" candidate is retired — it was
a config-level workaround for a policy that belongs in the reader.

⚠️ **Verified, because the failure would have been silent.** `range_from_r2` takes a
**part-local** offset (the R2 object *is* the part); a file-wide offset would return
plausible bytes from the wrong place — a row that parses into a *different output* —
rather than an error. `probe.rs` compares four windows, including the part's last 8
bytes, against the same windows taken from the whole part. All correct.

### 4.9 The interning table → resident

`by_policy`'s confirm resolves each candidate through `policy.tbl`. Fetched
lazily that is a 28-byte round trip per candidate — worse than the row beside it.
**6.49 MB** (measured); it goes in the hot tier.

### 4.10 Scattered reads → one multi-range request

`Range: bytes=a-b, c-d, …` is RFC 9110 `multipart/byteranges`, and support splits
on which path you take:

| path | multi-range |
|---|---|
| **R2 directly** (S3 API or Workers binding) | ⚠️ **no** — multiple ranges return **400 Bad Request** |
| **R2 behind a custom domain** (through the CDN) | ✅ **yes — 2 to 299 ranges** (measured), as one multipart 206 |

So it is available on exactly the path §4.7 already requires for caching. §3.1's
2,000 scattered rows become **7 requests**, ~2 round trips at six concurrent.

⚠️ **Four traps, and the first is severe — and one lower than documented:**

1. **The ceiling is 299, not 300.** Measured 2026-09-24 and bisected: 296 and
   **299** return multipart; **300 returns the complete file as `200 OK`** — not
   an error, the whole object. Cloudflare's *"between two and 300 ranges"* is
   exclusive in fact. **A client capping at 300 — as an earlier draft of this
   section said to — silently downloads the entire object.** Hard-cap at 299 in
   the client and never rely on the server refusing.
2. **Compressed content defeats it** — Cloudflare will not map ranges onto
   compressed bytes and serves the whole file. The artifacts must be published
   with no content-encoding. A publish-config decision, not a code one.
3. **Ranges must be ascending and non-overlapping** — so sort offsets first, and
   coalesce neighbours into single ranges while you are there.
4. **Binding ≠ CDN.** `env.BUCKET.get()` gets the no-multi-range path; the Worker
   must `fetch()` the custom domain instead, which changes its subrequest and
   caching story.

⚠️ **Unverified and load-bearing:** the docs describe an interaction between
range count and *cache state* — a fully-cached object without origin range
requests behaves differently. Test before depending on it.

⚠️ **Multi-range fixes round trips, not bytes.** Each range still lands in a
distinct 1 MiB cache chunk (§4.7), so a cold multi-range read pulls the same
~1 MiB per touched chunk that 300 separate GETs would. **It is a latency fix, not
an amplification fix** — which is why §4.8's clustering is strictly better where
it applies, and this is the fallback for access that is genuinely scattered.

⚠️ **And it is Cloudflare-only.** Measured 2026-09-25, **Tigris supports exactly
one range** — `416 InvalidRange: Only support one range`, not a 400 and not a
whole-object 200. So on that substrate §4.8's clustering is **mandatory**, and
§3.3's scattered wallet query becomes 50 separate GETs rather than one request.
Tigris has no depth cost to compensate with (it serves a range 256 MiB deep as
fast as one at the start), so a *clustered* read there is **one GET at ~186 ms**,
better than Cloudflare's clustered path — but the scattered path is the one
workload where losing multi-range genuinely hurts, and it is unmeasured under
concurrency.

**The shared component this implies:** a range batcher — sort, coalesce adjacent,
hard-cap at 299, parse multipart. Coalescing matters more than it looks: two rows
in the same MiB should become one range, because they were always going to be one
cache chunk. It lives in the client (§10.2), and serves the browser path, the
lean-node path and every future ordering.

**Sanity check worth keeping.** Every cloud-native format that hit this problem —
Zarr, Cloud-Optimized GeoTIFF, Parquet-on-S3 — solved it *without* multi-range,
because S3 does not support it: layout, coalescing, concurrent single-range GETs.
We have an option they did not, but their prior still holds — **layout first,
protocol second.** A contiguous read beats 300 scattered ones even when 300 are
cheap: no multipart parsing, no cliff, no cache-state ambiguity.

---

## 5. The three tiers

§3.6 said three of four read workloads want a projection or a fold, and §4 said
every remote hop is what hurts. Together: **do not get the whole artifact close
to the user. Name the hot slice and put only that at the edge.**

*Derived* from §3: 599,798 tracked assets at ~1.3 per output ⇒ ~460,000 UTxOs ⇒
**~76 MB of rows** — about **4%** of the 1.69 GiB arena. The working set is small
and *known*, because the publisher knows which policies are tracked.

| tier | what | size | where | why it is the right product |
|---|---|---|---|---|
| **hot** | projections + navigation (3 directories, `policy.tbl`, `policy.runs.idx`) | **~200 MB** | **Workers static assets** | per-request files a Worker serves |
| **warm** | `base/<epoch>/` — rows, entry arrays, clustered arena | **~2.3 GB** | **regional R2 + CDN**, or a lean node's local disk (§6) | object storage, paid, unremarkable |
| **cold** | `log/**` — the chunk-sharded archive | **~65 GB** | one R2 bucket | never on the read path |
| **live** | the last ~2160 blocks, plus the resident directories | **~30 MB** | the **locator** — a Durable Object, or a node's follower | §7.6. It holds the index and the tip and returns *ranges*, never row bytes |

### 5.1 Static assets, and the limit we are deliberately not using

| | paid | free |
|---|---|---|
| assets per Worker version | 100,000 | 20,000 |
| max individual file | 25 MiB | 25 MiB |
| requests · storage | free and unlimited · no cost | same |
| **aggregate cap** | **not documented** | **not documented** |

**Do not range-read assets — make the page *be* the file**, so `Range` support is
irrelevant on this tier. **1 MiB parts**, because an asset is fetched *whole* and
the bound is a 128 MB isolate: 25 MiB parts would blow it across a few reads.

⚠️ **That 1 MiB is not the warm tier's number, and an earlier draft claimed the
two "coincide".** They do not — they were derived from the same wrong premise
about 1 MiB cache chunks (§4.7). Measured, the two tiers have different bounds
and therefore different sizes:

| tier | fetched | bound | part size |
|---|---|---|---|
| **hot** — static assets | **whole** | isolate memory (128 MB) | **1 MiB** |
| **warm** — R2 by range | **a range within it** | cold depth cost | **1 MiB** (§6.3b) |

⚠️ **This said ~16 MiB, on a "measured knee" of 69 ms at 16 MiB against 438 ms at
64 MiB. Those figures were origin-warm and the conclusion was wrong.** Measured
cold (P7/P8): depth is **~35 ms/MiB** over a **~650 ms** floor, so 16 MiB costs
~1210 ms against ~685 ms at 1 MiB. And because **concurrency is free**, the
multi-range batching that was supposed to justify larger parts never amortises
them — §6.3b has the derivation.

**So the two tiers agree on 1 MiB, for unrelated reasons** — isolate memory on one,
cold depth on the other. Recorded as a coincidence rather than a principle, because
this section already treated that agreement as one once and had to retract it. The
practical upshot is convenient either way: **one uniform part size across hot and
warm**, so the format carries a single parameter and a reader resolves an offset by
arithmetic rather than a table.

⚠️ **Declined: the 2.4 TiB reading.** The two limits multiply to 100,000 ×
25 MiB = **2,441 GiB** with no published aggregate cap, so the 65 GB log would
*technically* fit at 2.7%, with content-hash dedupe meaning immutable shards
upload once ever. **We are not doing it.** 200 MB of assets is a medium website;
65 GB is 400× that, and fitting inside a limit is not the same as being the
workload a product is for. Recorded as **considered and declined**.

### 5.2 What the tiering costs the ambition

**The edge stops being a general chain-data plane and becomes a fast path for
known-hot questions.** Anything outside the tracked set falls back to warm (one
in-continent hop) or origin — as `archive_r2.rs` already does with
`X-Policy-Source: origin`. §3 is why that is a narrowing *toward* the workloads.

### 5.3 Getting the warm tier close

**There is no API to push into Cloudflare's cache.** Warming means someone made a
request, so the lever is not "warm 300 colos", it is *reduce how many places need
warming.*

**Smart Tiered Cache** works on an R2 custom domain and *"automatically selects
the upper-tier data center with the lowest latency to your R2 bucket"*. A cold
leaf colo then misses to a warm upper tier rather than to the origin.

⚠️ **But it is a single upper tier next to the bucket.** With one ENAM bucket a
Brisbane leaf miss still crosses the Pacific — to a cache instead of to R2, which
is marginal. **This is what makes regional buckets load-bearing rather than a
nice-to-have:**

```
leaf colo  →  regional upper tier  →  regional bucket
```

Cold misses stay in-continent, and "warm ~300 leaves" becomes "warm ~5 upper
tiers" — one request per object per region, entirely practical. Demand warms the
leaves for free.

⚠️ **Cache Reserve is not available here.** Requests to R2 public buckets linked
to a zone's domain do not use it, so there is no persistent backstop tier.

⚠️ **And a new epoch flushes every cache — for a reason a TTL cannot fix.**
Immutable paths (§4.6) mean a new base has entirely **new URLs**, so every leaf
and every upper tier is cold again, weekly under §7.1's cadence. Setting
`max-age=31536000` (§4.7) keeps an object hot once cached and does nothing about
this: the old epoch's objects stay cached perfectly and nobody reads them.

So the two are separate problems, and only one is solved:

| | |
|---|---|
| R2's 4-hour default TTL | ✅ fixed at publish, verified (§4.7) |
| new epoch ⇒ new URLs ⇒ cold everywhere | ⚠️ **open**, and inherent to content addressing |

Three responses to the second:

1. lengthen the base epoch — but §7.1's weekly choice is *derived* against delta
   lag, so this is a real trade rather than a free knob
2. run a warming pass over the upper tiers immediately after publish — ~5 requests
   per object, cheap
3. **keep the hot tier as static assets**, which distribute with the Worker
   deployment rather than through the cache, so the navigational layer has no
   cold-cache problem at all

Option 3 is a second, independent argument for the hot/warm split landing where it
did, and it is recorded as such rather than as a coincidence.

### 5.4 A public bucket is a design consequence, not an oversight

§10 wants the browser to range-read the artifact directly, and §4.10's multi-range
only exists on the CDN path. **Both require the bucket to be public**, so this has
to be stated rather than discovered.

**Confidentiality is a non-issue** — every byte is public chain data, derived from
a certified Mithril snapshot. There is nothing here that is not already on the
chain.

⚠️ **Cost is the issue, and it is asymmetric.** R2 egress is zero *to us*, and
free bandwidth is exactly what makes an open bucket attractive to someone else:
a third party can point their own application at our artifact and we pay the Class
B operations for their traffic. At $0.36/M that is cheap to absorb and unbounded
in principle.

Three postures, and the first is probably right for now:

| | |
|---|---|
| **accept it** | it is public data, the ops are cheap, and a third party hosting Cardano's live set from our artifact is arguably the point (`INSTALLABLE_MITOS.md` I5). Add Cloudflare rate limiting and watch the op count |
| **token-gate the custom domain** | preserves multi-range and CDN caching, breaks anonymous browser reads — so it forecloses §10 |
| **keep the bucket private, proxy through a Worker** | full control, but the Worker must `fetch()` the domain anyway for multi-range (§4.10), and it puts a request charge on every read |

**Decided: accept it, with monitoring.** But the decision is load-bearing enough
that it belongs beside the design rather than in an ops runbook — the moment
anonymous browser reads are foreclosed, §10's whole client story goes with them.

### 5.5 What this requires paying for

Checked 2026-09-24, because it decides where experiments can run:

| | plan |
|---|---|
| R2, and its free tier (10 GB · 1 M Class A · 10 M Class B per month) | **free** |
| R2 **custom domain** — the precondition for CDN caching and multi-range | **free**; only requires the domain to be a zone in the same account |
| CDN caching, Cache Rules | **free** |
| **Tiered Cache** (§5.3) | **all plans**, including Free |
| **Workers Paid** | **required for the locator**, since Durable Objects need it. Not required for any read-path experiment |
| Rate limiting (§5.4) | paid |
| **512 MB cacheable-file cap** | ⚠️ **not raisable** below Enterprise. The one constraint money does not solve at our scale, and the reason §4.7's split is mandatory |

**So the read plane can be proven end to end on a Free zone.** Only the locator
pulls in Workers Paid, and that is a later phase.

---

## 6. Deployment substrates

> **Isolate platforms have no filesystem and never will — storage is always a
> network hop. Micro-VM platforms have a real one but reach tens of regions, not
> hundreds. Nothing spans both**, because a 300-PoP product with local disk means
> replicating disk to 300 places.

| | isolate edge | micro-VM regional |
|---|---|---|
| examples | Workers, Fastly Compute, Deno Deploy | Fly.io, Koyeb, Cloud Run, plain VMs |
| reach | hundreds of PoPs | tens of regions |
| storage | network hop (assets / R2 / KV / DO) | real disk, real `mmap` |
| our wasip2 modules | **no** | **yes** |
| §4 translation table | **required** | **not needed at all** |

### 6.1 The lean node

`FOOTPRINT_AND_MINIMUM_SPEC.md`'s lean profile, with a geography:

| | full node | lean node |
|---|---|---|
| holds | 145 GB Dolos + 217 GB chunks | the warm tier, **~2.3 GB** |
| does | walk, fold, import | serves the phonebook |
| is | a per-region capital decision | a small VPS |

**And it has no cold-read penalty at all.** All of §4 exists because bytes are
far away; a node holding them locally runs `crates/live-book` unmodified at the
measured **1223 ns** point lookup, cold or warm, every time.

### 6.2 The candidates

**Fly.io** — the direct answer. Firecracker VMs, **30+ regions**, **anycast
included** (they own the ASN and do the BGP), volumes at **$0.15/GB-month** ⇒
~$1.50/region for the warm tier. Machines autostop/autostart. ⚠️ Volumes bill on
*provisioned* size even when stopped; inter-region traffic is $0.01/GB in,
$0.02/GB out since Feb 2026 (~$0.05 to seed a region).

**Turso embedded replicas** — a **local SQLite file** synced from a cloud
primary, free on all plans today. Fits because our artifact is **read-only**: the
awkward half of embedded replicas is the write path (80–120 ms to a cross-region
primary) and we have no writes. ⚠️ Storage scales linearly per replica (~$75/month
reported for 50 GB × 10), and it means putting the book *in* SQLite. **The
comparison is not SQLite vs `mmap`, it is SQLite vs the network** — a local
B-tree lookup at a few µs is ~1000× slower than the custom reader and ~1000×
faster than a trans-Pacific hop.

**Durable Objects** — storage colocated with execution; the closest CF-native
lean node. **10 GB SQLite per object** fits the warm tier; `locationHint` places
them (wnam, enam, weur, eeur, apac, oc, sam, afr, me). *Derived*: loading 2.3 GB
as 64 KiB blob rows ≈ 36,000 rows ⇒ **$0.04/region/rebuild**; storage
**$0.46/region/month**. ⚠️ `locationHint` is a *suggestion, not a guarantee*, and
a DO is single-threaded.

**Cloudflare Containers** — correcting an earlier dismissal: the lean node
**does** fit `standard-1`'s 20 GB disk, with real Linux and real modules. But
memory and disk bill on *provisioned* capacity — *derived* ~**$28/region/month**
always-on against Fly's single digits — and sleeping loses the disk. **Wrong for
an always-on read node; right for the build side.**

**Own-ASN anycast** — what Fly gives you free. APNIC gives members two free ASNs
(Associate membership **AUD 500/yr**), but an IPv4 /24 is **$18–58/IP** ⇒ roughly
**$5–15k**; IPv6 /48 is effectively free. Vultr is the practical BGP-capable
host; ⚠️ **Equinix Metal sunsets June 2026**.

⚠️ **The tension nobody names:** anycast's unique advantage is instant failover
and a single IP, but a mid-connection BGP reroute resets TCP — and the stateful
connections (follower, dialer, chainsync) are exactly what that breaks. **The
part anycast uniquely enables is the part it handles worst.** Resolution is what
the DNS roots do: anycast the stateless read plane, keep the stateful plane on
per-node unicast hostnames.

### 6.3a Decided: Cloudflare, and sizing parts is what settled it

**Measured 2026-09-25, and the conclusion is not the one the first Tigris numbers
suggested.** Tigris looked stronger on two axes — a ~186 ms flat floor against
~330–400 ms, and **no depth cost at all** where Cloudflare charges 1774 ms at
256 MiB. Then two further measurements closed it:

| 50 scattered rows (§3.3's wallet query) | Cloudflare | Tigris |
|---|---|---|
| warm, one multi-range request | **182 ms** | n/a — no cache tier |
| cold, 16 MiB parts | ~400 ms *(derived)* | **1292 ms** |
| multi-range at all | 2–299 ranges | ⚠️ `416`, one range only |

1. **Concurrency does not recover multi-range.** 50 concurrent single reads
   against Tigris took **1292 ms** against its own 186 ms single-read floor — a
   ~7× penalty, not the ~1× full parallelism would give. The open question was
   whether HTTP/2 would hide the loss; measured, it does not.
2. ⚠️ **Sizing parts at 16 MiB neutralises Tigris's only real advantage.** Its win
   was avoiding depth on *huge* objects, and §5.1 already rejected huge objects.
   At 16 MiB, Cloudflare's depth cost is 69 ms rather than 1774 — so the
   comparison it won was against a configuration we are not building.

**So: Cloudflare.** It wins warm by ~7×, cold by ~3× once parts are sized, and
brings static assets, Workers and Durable Objects that Tigris has no answer for.

⚠️ **What would reopen it.** Tigris's distinctive property is a **flat floor with
no cache to warm** — if real traffic turns out too diffuse to keep a working set
cached, Cloudflare's advantage is exactly the thing that evaporates and Tigris's
consistency starts to matter. That is a question about production traffic, not one
these probes can answer, and it is worth re-asking once there is any.

**And the earlier substrate reasoning still stands on its own terms**: §6.1's lean
node has no cold-read penalty at all, and §6.2's Fly option remains the answer if
running wasip2 modules off-box ever becomes the requirement. This decision is
about *where the artifact is served from*, not about whether a node is ever wanted.

### 6.3b Part size: **1 MiB, uniform**, settled 2026-09-25

**One size, not a range.** Uniform parts make `offset → (part, offset-within-part)`
pure arithmetic — `offset / PART` and `offset % PART` — where a mixed layout needs
a lookup table on the hot path. Only the final part is short.

**And 1 MiB rather than 4**, because the argument for larger does not bind:

| | 1 MiB | 4 MiB |
|---|---|---|
| cold read | **~685 ms** | ~790 ms |
| objects (3.8 GB warm tier) | ~3,800 | ~950 |
| Class A per republish | $0.017 | $0.004 |
| publish wall time @ 10/s | ~6 min | ~1.5 min |

Both costs round to nothing and neither publish time troubles a weekly epoch, so
the only real difference is **105 ms — 15% — on every cold read**. Two further
reasons in the same direction: reads are depth-bound (P8), so a clustered run at a
random position in a 4 MiB part pulls ~2 MiB against ~0.5 MiB in a 1 MiB part —
**4× less cache filled per read**, which is §13.9's pollution concern directly. And
the knee is genuinely here: 4 → 1 MiB buys 105 ms, while 1 → 0.5 MiB buys only
~18 ms and doubles the object count again, because the 650 ms floor dominates.

⚠️ **The cost of 1 MiB:** a 332 KB clustered run straddles a boundary about **a
third of the time** (against ~8% at 4 MiB), needing two requests. Concurrency is
free, so that is an extra Class B operation rather than extra latency — but a
reader must handle a straddling run rather than assume one part.

**So the format carries one part-size parameter**, 1 MiB for hot and warm alike,
and the cold `log/` tier keeps its own up to 25 MiB because it is never on the read
path.

⚠️ **This section previously described an unresolved tension and set 16 MiB. Both
were wrong, because every latency figure they rested on was origin-warm.**

P7 read eight never-touched 16 MiB objects at ~968–1482 ms each, where P4's
already-read object measured 396 ms. P8 then discriminated the cold model on three
virgin objects — **the head of a 64 MiB object costs 652 ms against a 1 MiB
object's 700 ms entire, and that object's tail costs 2878 ms.** So a cold read
fetches *to the requested byte*, and the corrected constants are:

| | earlier (origin-warm) | **measured cold** |
|---|---|---|
| floor | ~330–400 ms | **~650–700 ms** |
| depth | ~7 ms/MiB | **~35 ms/MiB** |

| part | cold read | parts over 1.69 GiB |
|---|---|---|
| **1 MiB** | **~685 ms** | 1,730 |
| **4 MiB** | **~790 ms** | 433 |
| 16 MiB | ~1210 ms | 106 |
| 64 MiB | ~2890 ms | 27 |

**The tension dissolves because concurrency is free (P7).** A scattered read costs
about *one* request, so larger parts do not amortise — they only cost more depth.
Batching never rescues them either: over 50 scattered rows, depth grows faster
than request count falls, and the ordering survives even at a Worker's
six-in-flight cap (~6.2 s at 1 MiB against ~14.5 s at 64 MiB).

**Below ~1 MiB the floor dominates** — 650 ms of a 685 ms read — so there is
nothing left to win and object count multiplies. **1–4 MiB, chosen on object count
rather than latency**; across that range latency differs by ~50 ms.

⚠️ The hot and warm tiers therefore land on the same number again, for *unrelated*
reasons — isolate memory there, depth here. A coincidence worth recording, not a
principle: §5.1 already got burned once treating it as one.

### 6.3c ⚠️ The mistake this section made three times

Part size was answered wrongly three times, and always the same way: **a figure
measured against an object something had already read, taken as a cold number.**

| draft | said | why it was wrong |
|---|---|---|
| first | 64 KiB grid | derived from CF's "1 MiB chunks" doc line, which describes origin *fetch* alignment, not cache granularity |
| second | 1 MiB both tiers | assumed the cache unit was 1 MiB; it is the **object** |
| third | ~16 MiB warm | "measured knee" of 69 ms at 16 MiB — **origin-warm**; cold it is ~560 ms |
| **now** | **1–4 MiB** | first figures taken against objects nothing had ever read |

The recurring error is not arithmetic, it is **method**: an object read once is
warm at the origin for every subsequent measurement, so a probe that reuses a
fixture is measuring its own history. P7 only caught it because it used eight
*never-touched* objects, and P8 only settled it because each object is read
exactly once, ever.

**The rule that falls out, and it belongs with the earlier one about cache
status:** a latency measurement needs a *virgin* fixture, and a fixture is virgin
once. Anything re-read is reporting the previous run.

⚠️ Also retired with the third draft: the claim that smaller parts "fragment
batching" and so trade off against depth. They do fragment it — but since
**concurrency is free** (P7), fewer-and-larger requests never amortise, so the
trade-off does not exist in the direction it was stated.

✅ **And the "different part sizes per file" idea goes with it.** That followed
from the same false trade-off: the clustered arena sized for contiguous reads, the
canonical arena for scattered batching. With concurrency free and depth linear,
**both want the same thing — small** — so one number serves both files and the
format keeps one parameter instead of two.

### 6.3 Recommendation

**Do not start with anycast.** At five to ten PoPs its latency advantage over
GeoDNS is marginal. Its real wins are failover *time*, a single IP and DDoS
absorption, none of which is a problem at 163 collections. GeoDNS costs nothing
and the routing layer is swappable later without touching code.

**Anycast solves routing, not replication — and replication is the hard part.**
The artifact work is required whichever substrate wins, which is why §12 puts it
first.

---

## 7. The tip

### 7.1 The delta is not a file kind — it is the log, read forward

`ARTIFACT_SHARDING.md` already produces `log/created/<chunk>.bin` and
`log/spent/<chunk>.bin`. **The delta for a base at boundary B is those files for
chunks > B.** No `.dlt`, no second encoding, no second builder — and the delta
inherits immutability and content-addressing for free.

The arithmetic agrees with `DELTA_CHAIN.md`'s measured slope: at tip, 13,731
creations × 182 B + 14,412 spends × 12 B ≈ **2.7 MB/chunk** against the measured
**3.3 MiB/chunk** including bookkeeping. Same object.

⚠️ **Correction to `DELTA_CHAIN.md` §B4.** It specifies **a spent bit per base
row** — 1.36 MB over 10,911,674 rows. Right in memory, wrong as a *published*
shape: it is cumulative, so it is a full rewrite every chunk. Publish **per-chunk
spend lists** instead — 14,412 × 4 B dense id ≈ **58 KB**, immutable, cacheable —
and let the reader OR them into the bitmap on open. That is already
`log/spent/<chunk>.bin`.

### 7.1a Sealed deltas are indexed by policy, like the base

⚠️ **Without this the delta dominates every query by two orders of magnitude.**
At §7.1's weekly epoch there are **28 sealed chunks** since the boundary, each
~2.7 MB, so a reader correcting a base would fetch **~75 MB to amend 332 KB** —
a median collection's entire clustered run (§3.1).

The fix is the one that already works for the base, applied one tier down: **a
sealed chunk is immutable, so a per-policy view of it is immutable too.**

| resource | immutable | typical size |
|---|---|---|
| `/delta/<chunk>/policies` — a bloom over the policies that chunk touched | ✅ | a few KB |
| `/delta/<chunk>/policy/<P>` — that chunk's rows for one policy | ✅ | usually **empty** |

**Most chunks do not touch most policies.** A collection with 2,000 live outputs
sees a handful of movements in any six-hour window, so the per-policy view is
empty or a few hundred bytes, and the bloom tells a reader which of the 28 chunks
are worth asking about at all — typically **one to three**.

So a policy query's delta cost goes from ~75 MB to **one bloom plus one to three
small cached fetches**, plus the live tail (§7.2), which is the only part that
cannot be indexed ahead of time because it is not sealed.

⚠️ This is a correction rather than an addition: an earlier draft of §7.6 split
the locator by lifetime and applied per-policy indexing **only to the base**,
which left the deltas as the dominant cost and quietly undid the split.

### 7.2 Live deltas, and they are tiny

One chunk per ~6 h and ~20 s per block ⇒ **1,080 blocks per chunk**:

| | *derived* |
|---|---|
| creations per block | **~13** |
| spends per block | **~13** |
| delta per block | **~2.5 KB** |
| six hours of blocks | ~2.6 MB |
| rollback window (k = 2160 blocks) | **~5.4 MB** |
| DO writes, ~26 rows/block × 129,600 blocks/month | ~3.4 M rows ⇒ **~$3.40/month** |

**One Durable Object holds the live delta for the whole chain**, with the full
rollback window resident. The component that looked like the expensive part is a
few megabytes and pocket change.

Freshness then goes from 6 hours to one block, and the read path is three tiers
by volatility:

| | cadence | mutable |
|---|---|---|
| base | weekly | no |
| sealed chunk deltas | ~6 h | no |
| live block deltas | ~20 s | **only the last ~2160** |

Each block's delta is immutable once the next arrives, so all but the tip few are
cacheable.

### 7.3 Three conditions

**1. ⚠️ Per-block and retractable — a cumulative bitmap cannot roll back.**
`ARTIFACT_SHARDING.md` §5.4 only admits sealed chunks precisely because the tip
is not final. Going live takes rollback on directly, and to undo one you must
know *which block* set each spent bit. A merged bitmap has thrown that away, so
the live delta is a per-block spend **list** with the bitmap derived in memory.
§7.1's correction, one granularity finer, and here it is load-bearing.

**2. ⚠️ Every delta carries its slot, and a reader can refuse.** This is
`PHONEBOOK_COVERAGE.md` §11's `tip` gap end to end. If the follower stalls, every
consumer serves stale-but-plausible answers — the failure class of the
`AwaitingHolders` stall, where a fire-and-forget wake left collections sitting
silently. With slot provenance a reader computes staleness and refuses; without
it a stall is invisible. **Make the stall loud.**

**3. The follower becomes the one stateful component, so its liveness is the
system's liveness.** Everything else is immutable files whose only failure mode
is "absent". Better than a six-subscriptions-per-policy fan-out, but it is a
*single* thing to monitor — and condition 2 is what makes monitoring possible
rather than aspirational: **the slot stamp is the health signal.** A reader
computing staleness per request means every consumer is a monitor, and an alert is
"the locator's slot stopped advancing" rather than a liveness probe that returns
200 while serving month-old answers.

### 7.4 The base/delta pairing hole, which is live today

The base-row spent bitmap is indexed by `Base::outref_entry` — the row's position
in `by_outref` — so **a delta is only meaningful against one specific base**. And
`book.meta` does not record which boundary the base is at, so **there is no way
to verify a delta belongs to a given base.** Mispair them and liveness is
silently wrong, in §4.6's direction: a live output reported absent.

Two things were needed. **Both now exist** (2026-09-24):

- ✅ **`book.meta` records `base_slot`** — optional on purpose, because a book
  built before the field is a valid book that does not know where it sits, and
  "unknown" must be a state a reader can *see*. `BookDataPlane::tip()` answers
  from it and refuses by name when it is absent, which closes
  `PHONEBOOK_COVERAGE.md` §11's structural gap. `fold-probe stamp-boundary`
  retrofits an existing artifact.
- ✅ **The base has an identity**: `Meta::fingerprint() -> BaseFingerprint`, and
  `Delta` captures it at construction. ⚠️ It is a **fingerprint for pairing, not
  a content hash** — it covers the metadata, not the 2.23 GB beneath it.
  Integrity stays a publish-time concern over the bytes
  (`ARTIFACT_SHARDING.md` §8); these are different mechanisms for different
  threats and conflating them would be worse than having neither.

**What the fingerprint deliberately ignores**, and this is the part that would
have been got wrong: `LIVE_BOOK.md` §Decided proves the artifact is
byte-identical across runs at 0, 1 and 7 compactions, so `compactions`,
`rehashes`, `spent_hits` and `spent_misses` describe *how the builder ran* and
must not change a base's identity. Nor may `Meta::DERIVED_KEYS`, which a
`reindex` repair rewrites without the rows moving. A fingerprint including either
would call identical artifacts different bases and reject valid deltas.

**Still owed, when a delta is first written down:** the reader's three-line
refusal of a base whose fingerprint differs. `Delta` borrows its `&Base` today,
so the compiler settles pairing in memory; the obligation is recorded on
`Delta::base_fingerprint`.

### 7.5 The merged cursor

`Cursor { prefix: u64, at: u64 }` is *"an entry index within one ordering"*,
deliberately opaque so a caller *"could not resume in the wrong place and
silently skip rows."* A cursor over a merged base+delta result must encode
position in **both**, and the current type cannot express it. A real API change,
and the kind that silently drops rows if fudged — exactly what the type exists to
prevent.

### 7.6 The locator — what the DO is, and what it is not

If deltas are published per chunk, **a Worker needs no DO for the tip at all** —
it reads base plus N immutable chunk files, and freshness is bounded by the seal
cadence. The DO exists for **sub-6-hour freshness** (tx building, gating, quests,
rewards) where published listings and analytics do not need it.

But once it exists it should do more, and **exactly one thing less than is
tempting: it must never touch row bytes.** It answers *where to look* and *what
has changed*:

```
{ epoch: build_id, slot, ranges: [(offset, len), …], policy_id: u32, delta: [rows] }
```

Kilobytes in, kilobytes out. Four jobs:

| job | why the DO and not the client |
|---|---|
| the three directories **resident** (25 MB) | amortised over every request it serves; a client serving one query cannot amortise 8.39 MB |
| `policy.runs.idx` → a policy's contiguous run | same, and it is 1.85 MB |
| the **live delta**, slot-stamped | the only mutable state in the system |
| **epoch provenance** | tells the client which build to read — §7.4's pairing gap closed at the point it matters |

**And one small trick worth taking: the DO resolves the interning.** A `by_policy`
confirm compares the row's `u32 policy_id` against 28 bytes via `policy.tbl`. If
the DO returns *"P's interned id is 4711"*, the client compares `u32`s and **never
downloads the 6.49 MB table**. `by_addr` needs no table at all — its confirm is
row-address against query-address, both already in hand.

⚠️ **What the DO must not become is the range batcher** — see §10.2. Routing row
bytes through a single-threaded, single-location object makes its I/O scale with
total traffic, bills its wall-clock wait, and replaces a nearest-colo hop with two
hops.

#### Most of the locator should be cached, not served

⚠️ **A single-threaded object on the path of every query is a throughput
ceiling**, and it arrives before the bill does: at a few milliseconds of CPU per
query one DO tops out around a couple of hundred queries a second. Cost is not the
argument — *derived*, ~6 M queries/month is about **$1** — the argument is that
queueing behind one isolate is a bad way to fail.

**The response above is uncacheable only because it bundles fields with wildly
different lifetimes.** Separated, almost none of it is live:

| field | changes | cacheable |
|---|---|---|
| `ranges`, `policy_id` | per **epoch** — weekly | **forever** |
| `slot`, live `delta` | per **block** — ~20 s | ~20 s |

And the two are **inverted in the way that matters**: `ranges` is per-query but
epoch-immutable, while the tip is volatile but **identical for every query** —
one answer serves every client whatever they asked about. Bundling them gives the
per-query field the volatile field's TTL, for no reason but the bundling.

✅ **Measured 2026-09-25** (`workers/locator-spike`, `EDGE_PROBE_PLAN.md` §8a):
the split works, **and a Worker's own response caches only via the Cache API.**
`cache-control` headers alone did nothing — four requests, four executions, and
`cf-cache-status` **absent entirely** rather than `MISS`. Through
`caches.default` the same routes served one execution with `HIT` from the second
request on.

⚠️ **Two consequences for how this gets built:**

- **Make the opt-in structural.** A route that forgets `cache.put()` returns
  correct data and silently costs an execution every time; nothing surfaces it
  until throughput matters. Wrap it once so a new route is cached by default
  rather than by remembering.
- **The Cache API is per colo**, so "one execution serves everyone" is per colo.
  That is still the win, honestly stated: *every query* hitting the locator
  becomes *one query per colo per TTL*. For `/tip` at 20 s across ~300 colos that
  is *derived* **~15 executions/sec globally regardless of client volume** —
  comfortably inside one DO's throughput, which is what this section exists to
  protect.

**So split by lifetime, and every request becomes cacheable:**

| resource | TTL | shared by |
|---|---|---|
| `/e/<epoch>/locate/policy/<P>` → ranges, interned id | **immutable** | everyone asking that question |
| `/e/<epoch>/…` → base parts | **immutable** | everyone |
| `/delta/<chunk>/…` → sealed deltas, per policy (§7.1a) | **immutable** | everyone |
| `/tip` → slot + live delta | **~20 s** | **everyone, all queries** |

Nothing is per-query *and* volatile, because that combination was an artefact.
The DO then serves only consumers needing fresher than the tip's TTL — tx
building, gating, quests — and not listings, catalogue or analytics.

⚠️ **Three costs of doing it this way**, and the first is not small:

- **The merge moves to the client** — §7.5's cursor problem lands in wasm rather
  than in one place we control, and §7.5 already notes a fudged cursor silently
  drops rows.
- **~20 s of staleness becomes deterministic** rather than incidental. Fine for
  gating; for tx building a UTxO spent 15 s ago still reads live. An argument for
  the API distinguishing **settled** from **tip** explicitly.
- **The live delta ships whole** — ≤2.7 MB, averaging ~1.35 MB (§7.2) — and the
  client filters. Free to us (edge→client) and unnoticeable in a browser;
  possibly not on cellular. The alternative, a per-policy *live* filter, is
  per-query and volatile, which is the combination just eliminated.

Any tip-following system can serve an answer from a block that later rolls back.
That is true today too, and it is an argument for the API distinguishing
**settled** from **tip**, not for avoiding this.

### 7.7 Two queries, traced

Written out because tracing them is what found §7.1a — the design read fine until
a real query was costed against it.

**"All assets in a policy, and who owns what."** Ownership is a fold over a
contiguous read (§3.1); names and traits are a published projection (§3.2).

| # | resource | TTL | size |
|---|---|---|---|
| 1 | `/tip` | ~20 s, shared | ~1 KB |
| 2 | `/e/<E>/locate/policy/<P>` | immutable | ~200 B |
| 3 | one range into `rows.by_policy` | immutable | **332 KB** (median) |
| 4 | `/delta/<chunk>/policies` bloom, then 1–3 per-policy deltas (§7.1a) | immutable | ~KB |
| 5 | live delta, filtered client-side | ~20 s, shared | ≤2.7 MB |
| 6 | `/e/<E>/bundle/<P>` — names, traits, rarity | immutable | ~1.4 MB |

**~6–8 requests, every one cacheable.** Warm that is ~15 ms each plus a fold over
~3,400 asset entries; cold at a fresh colo, ~350 ms each (§8).

**"Burns to `$burnsnek` for a token."** The sink accumulates and never spends —
measured at **19,724 UTxOs, none ever consumed** — so the burns are *in the live
set*, sitting at the sink address. Query **by policy** and filter on address: the
policy's rows are contiguous, so it is the trace above plus a predicate,
essentially free once the read is happening. Querying by *address* would be the
wrong way round — that run is 19,768 scattered refs.

⚠️ **But "when" is a different artifact, and the book cannot answer it.** A row
carries outref, lovelace, datum ref, datum kind, `addr_len`, `n_assets`, `era` —
**no slot**. It knows *where* it is, not *when* it arrived. Dating rows means
outref → `tx-index` → chunk → block, per row, scattered, in the cold tier.

| question | artifact |
|---|---|
| how much sits at the sink **now** | the book — one filter on a read already happening |
| **when** were they burned, at what rate | **`policy-archive`** — movements as Parquet with time-aligned row groups, already R2-served |

So "burns for a token" sounds like one question and is two, and only the first is
a live-set query. That is §3.2's rule a third time: **a question about change over
time is a projection, not a set query** — and the projection already exists.

---

## 8. The round-trip budget

**Bytes are irrelevant; round trips are everything** — measured, Brisbane against
an ENAM bucket:

| read | size | time |
|---|---|---|
| manifest | 689 B | 0.30–0.54 s |
| footer tail | 300 KB | 0.34–0.91 s |
| one row group | 72 KB | 0.31–0.62 s |

A 689-byte read and a 300 KB read cost the same. *Derived*, a cold point lookup
against one distant bucket ≈ **3 round trips cold, 0 warm**. With the hot tier
resident (§5) the first three collapse to **1**. On a lean node (§6.1) it is **0**.

**Measured 2026-09-24**, ENAM bucket read from Brisbane
(`EDGE_PROBE_PLAN.md` §8a), which puts a price on each of those round trips:

| | |
|---|---|
| cold read, any object ≤16 MiB | **~330–400 ms** |
| warm read (`HIT`) | **11–14 ms** |
| depth surcharge above ~16 MiB | ≈ **7 ms/MiB** |

⚠️ **The floor dominates everything else.** A cold read is ~25× a warm one, which
is more than §4.8 and §4.10 can recover between them. The round-trip *count* is
worth minimising; the round-trip *state* is worth far more.

⚠️ And one measurement lesson worth carrying: an earlier sweep put the floor at
34 ms because it never recorded `cf-cache-status` and was reading cache. **A
timing number without its cache status is not a measurement** — it survived two
rounds of reasoning here before being caught.

Two platform limits bound the aggressive end:

- **Subrequests.** Since 2026-02-11, paid plans default to **10,000 per
  invocation**, configurable to 10 M; R2, KV **and Cache API** calls all count.
- **Six simultaneous connections** awaiting response headers — not the subrequest
  count — is what turns §3.1's 9,100 reads into 1,517 serialised batches, and the
  strongest argument for §4.8 and §4.10.

## 9. What it costs

R2 **$0.015/GB-month**, Class A **$4.50/M**, Class B **$0.36/M**, **zero
egress**. DO SQLite: rows read **$0.001/M**, written **$1.00/M**, storage
**$0.20/GB-month**, requests **$0.15/M**, duration **$12.50/M GB-s**.

| tier | size | where | $/month |
|---|---|---|---|
| hot | ~200 MB | static assets | **0** |
| warm × 5 regions | 2.3 GB each | R2 | **0.17** |
| warm × 5 regions | — | Fly volumes | ~7.50 + machines |
| cold | ~65 GB | R2 | **0.98** |
| live | ~30 MB resident | the locator DO | **~3.40** writes + *derived* ~0.79/region duration if kept warm |

**§3 is why this table is not the interesting one.** Every workload came in at
fractions of a cent. The cost analysis matters for one decision:

> **The set goes in object storage (immutable, Class B, zero egress); only the
> tip goes in a DO (mutable, small, $1.00/M written).**

Projecting the live set into DO SQLite as a *store* costs **$10.90 per rebuild**
in row writes. That is the split `LIVE_BOOK.md` §B4 and `DELTA_CHAIN.md` already
made for unrelated reasons.

## 10. The client, and what it fetches directly

R2 serves `Range` over HTTPS with CORS and **egress is zero**, so a client
fetching artifact bytes directly costs us Class B operations and nothing else.
That makes the question *what* it should fetch, not whether it may.

### 10.1 Bulk reads go direct; navigational reads go through the locator

The distinction that organises the whole client story, and getting it backwards is
how you end up shipping a reader to every browser to do three round trips badly:

| read kind | who fetches | why |
|---|---|---|
| **bulk / sequential** — a timeline scrub, row groups, a policy archive | **the client, directly** | it wants the bytes anyway, and a `TimeSpine` playhead drag is a request per frame, so nothing server-side helps |
| **navigational** — find my UTxOs, who holds this policy | **the client asks the locator (§7.6), then fetches the ranges it names** | it wants an answer, and the index that finds it is worth holding resident somewhere that amortises |

`policy-archive` is bulk. That is why pure-over-byte-ranges in a browser bundle
was the right call there — *"what lets the same code run on cardano-infra against a
file, in a Worker against R2, and in a browser bundle, and what keeps the wasm cost
at 0.15 MB."* The book is navigational, and must not copy it wholesale.

### 10.2 The client is the range batcher, not the DO

Four reasons, and they all point the same way:

1. **The locator is one location; the CDN is 300+.** Batching in the DO routes
   every row byte R2 → DO's colo → client. Batching in the client is CDN edge →
   client, from the *nearest* colo.
2. **The DO would become a bandwidth funnel.** A cold start is 332 KB–2.3 MB;
   through a single-threaded object its I/O scales with total traffic. Client-direct
   scales with clients.
3. **The six-connection governor applies to a DO too** — it is Workers runtime. A
   browser on HTTP/2 multiplexes far more, so **the client is structurally better
   at fan-out.**
4. **DO duration bills on wall-clock while active.** Waiting on 300 ranges is
   billed time in a DO and free in a client.

So the client holds the range batcher from §4.10 — sort, coalesce adjacent,
hard-cap at 299, parse `multipart/byteranges` — plus row parse and confirm. That is
the same pure crate compiled to wasm, in `policy-archive`'s 0.15 MB neighbourhood.

**Net: 2 round trips.** One to the locator (near, small, fresh), one multi-range to
the nearest colo.

### 10.3 The 33 MB is never a client download

`Directory::range_for` reads **two adjacent `u32`s at `8 + 4·b`** — eight bytes of
an 8.39 MB file. So a stone-cold client with no locator at all is:

| | RT |
|---|---|
| directory probe (8 B) | 1 |
| `by_addr` bucket (~63 B) | 1 |
| the rows (one multi-range) | 1 |
| **total** | **3, zero download** |

The 33 MB buys the elimination of hop 1 — one hop of three. **It is a server-side
residency play, amortised across every request an instance serves, and never a
client download.** Asking a user to fetch 8.39 MB before finding their assets is
the wrong trade; asking them to spend one 8-byte round trip, or one locator call
that also carries the tip, is the right one.

### 10.4 Declined: OPFS

The Origin Private File System gives a browser real file handles and
`createSyncAccessHandle()` gives synchronous offset reads — genuine `pread`. It is
**declined**, for two reasons that compound:

- ⚠️ **It breaks the isomorphism that makes the reader worth having.** Every other
  `ChunkReader` impl is *give me bytes at an offset* — stateless, no lifecycle, no
  failure mode beyond "absent". OPFS is a **stateful local store with a lifecycle**:
  seed it, check quota, request persistence, handle eviction, decide what to drop.
  That is not a transport behind the seam; it is a cache layer with semantics
  nothing else has.
- **Its one unique capability is the case we are ruling out.** OPFS beats the Cache
  API only for arbitrary-offset `pread` across one large file — i.e. a client
  holding the 2.23 GB warm tier locally, which is not intended. For
  page-is-the-file (§5.1) the **Cache API** does the same job with no dedicated
  worker, no sync handles, and the same quota and eviction bucket.

Recorded, so it is not rediscovered: `createSyncAccessHandle()` is
dedicated-Worker-only (not the main thread, not an iframe, not a SharedWorker);
quota is ~60% of disk on Chromium and WebKit but ~5.4 GB on Firefox and as low as
~50 MB on mobile; Safari disables OPFS entirely in private browsing; and OPFS
shares one best-effort bucket with IndexedDB and the Cache API where **eviction is
all-or-nothing per origin**. Any of those alone would be manageable; together with
no unique benefit, they are not worth carrying.

⚠️ It would also have been wasm-bindgen glue, so it was never available to a
macroquad frontend anyway — `wallet-core` / `wallet-miniquad` again.

### 10.5 What the client still costs `ChunkReader`

The current reader hands out `Row<'_>` borrowed directly from the mmap — zero
copy. **Every remote impl returns copies**, R2 included, so this is not an
OPFS-specific problem and declining OPFS does not avoid it: `ChunkReader` must own
an in-memory page cache that rows borrow from, with the lifetime tied to the cache
rather than the file. A real change to the reader's shape, and it belongs before
phase 2 rather than during it.

**Prior art:** SQLite over HTTP Range (`sql.js-httpvfs`, phiresky's static-hosted
SQLite) is a custom VFS fetching B-tree pages by range from static hosting — the
same design derived independently. Validation, and a fallback if an index we have
not built is ever wanted.

## 11. What no substrate serves

- **Projections, from the book** (§3.2). Publish or derive them instead.
- **History.** `read_tx`, `tx_metadata`, `asset_state`, CIP-25 need chunks — and
  `datum_by_hash` and `tx_metadata` each have **two shipped module callers**.
- **The mint ledger** — `PHONEBOOK_COVERAGE.md` §8's only genuinely new
  extraction, behind `collection-metadata`, the heaviest consumer.
- **Recognition.** The 19 community modules are *decoders*, not queries. A
  locator says where a UTxO is; it does not say a script output is a jpg.store V3
  listing at 42 ADA. That is `mitos-marketplace-decode`, `mitos-dex-decode`,
  `mitos-vesting-decode` and years of brand knowledge — including that jpg
  **reversed its redeemer convention between V1 and V2**. No index replaces it.
- **The push plane.** Interest matching, the dialer, recapture, emission ids.
- **Producing the artifacts.** The fold is a box, and it is mitos.

**So this unbundles mitos rather than replacing it**: the read/locate plane
becomes portable, the recognise/derive plane stays. That is what
`LAYERED_RESPONSIBILITIES.md`'s three layers were already reaching for — what
changed is that the data-plane layer became something you publish instead of
something you call.

## 12. Phasing

| # | what | gate |
|---|---|---|
| 1 | ~~`book.meta` records its boundary; the base has an identity a delta can name (§7.4)~~ | ✅ **done 2026-09-24.** `base_slot` + `Meta::fingerprint()`; 20 tests in `live-book`. The reader-side refusal is owed when a delta is first written down |
| 2 | `ChunkReader` with an **owned page cache** (§10.5); file + R2 impls; the **client-side** range batcher (§4.10, §10.2) | builds for `wasm32-unknown-unknown`; existing tests pass unchanged against the file impl; **the same goldens run against every impl** — an abstraction only one transport is tested through diverges silently |
| 3 | `rows.by_policy.bin` + `policy.runs.idx` (§4.8) and a `scan_policy → rows` primitive (§3.1) | a median collection's cold start in 1–2 round trips, **measured** |
| 4 | The **locator** (§7.6): resident directories, `policy.runs.idx`, interning resolution, epoch provenance — returning ranges, never bytes | a wallet query in **2 round trips**; the locator's egress stays in kilobytes under load |
| 5 | Hot tier as static assets: navigation + projections (§5) | a projection read with **zero** remote hops |
| 6 | Per-block live deltas in the locator, retractable, slot-stamped (§7.2–7.3) | a rollback is undone correctly; a stalled follower is refused, not served |
| 7 | Regional buckets + Smart Tiered Cache + a post-publish upper-tier warming pass (§5.3) | a cold Brisbane leaf misses **in-continent**, measured across an epoch boundary |
| 8 | Bake-off: one Worker route over regional R2 against one Fly lean node | **measured** in-continent round trip — the §13.1 decision |

Phase 1 is first because it is the only item that is a correctness bug rather than
a capability. Phase 3 precedes any serving route on §3's evidence. Phase 7 is
deliberately measured *across* an epoch boundary, since §5.3's cache flush is the
one thing a steady-state measurement will not show.

⚠️ **Amended 2026-09-25 by §4.8a.** A read path is now deployed
(`cnft.dev-workers/workers/live-book-edge`) against a windowed book, ahead of phases
1 and 3, and it works — answers identical to `fold-probe`'s. That ordering was
deliberate and is fine for a windowed book, but it does **not** discharge either
phase:

- **Phase 3 is strongly wanted but is no longer the only way out.** §4.8a measures an
  unclustered *whole-part* policy query touching every part of `rows.bin` — ~773 MB at
  1,026 holders, the whole arena at 10,000. But §4.8d then measured `Fetch::Exact`
  reading bytes proportional to **rows rather than arena** (~8.5 MB and ~82 MB for the
  same two cases), so the route can be pointed at a full book behind `fetch=exact`.
  What phase 3 still buys is the **request count**: clustered, a policy is one
  contiguous read; with `Exact` it is one request per row, and 10,000 rows is 10,000
  Cache API calls against a 10,000-subrequest budget.
- **Phase 1 is untouched.** The deployed reader has no delta, so the base/delta
  pairing hole cannot bite it yet — and that is exactly why it is still owed before
  one is written.

The `/v1/utxos` route has no such dependency: §4.8a measures it at 4–6 parts, and a
credential's row count does not grow with the arena.

## 13. Open questions

1. **Is the in-continent round trip cheap?** ⚠️ **Partly answered, and the answer
   arrived from an unexpected direction.** Cloudflare ENAM from Brisbane:
   **~330–400 ms cold, 11–14 ms warm**. **Tigris, same fixtures: ~186 ms flat,
   with no depth cost at any size** — roughly 2× the floor and *none* of the
   1.77 s that a 256 MiB object costs on Cloudflare (`EDGE_PROBE_PLAN.md`).
   Still open: an explicitly `oc`-placed R2 bucket, which is the comparison this
   question originally asked for, and whether Tigris's ~186 ms is Oceania or a
   Singapore/Japan replica. ⚠️ Also note Tigris exposes no cache status, so its
   number is not a *cold* measurement the way Cloudflare's is.
2. ~~**Does a 300-range request against a cached object return multipart?**~~
   ✅ **Answered, and it moved a constant.** Multi-range **survives caching** —
   `HIT`, 14 ms, still multipart — so §4.10 and §4.8 do not conflict and
   clustering stays preferred rather than mandatory. And the ceiling is **299,
   not 300** (§4.10 trap 1). ⚠️ The browser half — does `fetch()` hand back the
   multipart body intact — is **still unrun**; it is `tools/edge-probe/web/multirange.html`.
3. **What is the tail of bucket sizes?** §4.4's branch assumes an average of ~5.2
   entries; the distribution is unmeasured and the fat-bucket path is expensive.
   ⚠️ `live-book-edge` currently reads the **whole bucket** in one range rather than
   binary searching it, trading bytes for round trips on the assumption the tail is
   short. That assumption is this question, and it is now load-bearing in deployed
   code rather than only in a design.
3a. ~~**Can a range request be served from a cached `Response`?**~~ ✅ **Answered
   2026-09-25 — YES**, and it moved a conclusion. 206 with correct bytes; see §4.8c.
   §4.8d is the consequence: `Fetch::Exact` reads bytes proportional to **rows rather
   than arena size**, which softens §4.8a's "clustered arena is a precondition" to
   "clustered arena removes the request-count wall". Still open behind it:
   **what is the right concurrency bound?** `CONCURRENT_ROWS` is 32 because it had to
   be *some* bound; the knee is unmeasured, and it is what decides how wide a policy
   run `Exact` can serve. And **how small can the speculative row over-read be?**
   The initial window is 8 KiB against a 166 B median — ~16× of `Exact`'s bytes is
   over-read.
4. **Does the delta need its own orderings, or is a scan enough?** Scan cost is
   linear in lag, and the weekly epoch is a *derived* choice, not a measured one.
5. **Does `scan_policy → rows` belong to the trait?** §3.1's second finding says
   refs-then-read costs any non-`mmap` transport double. A
   `MITOS_DATA_PLANE_API.md` decision, not this doc's.
6. **How is the hot set chosen, and by whom?** §5 says the publisher knows which
   policies are tracked — today that is one worker's table. A second consumer with
   a different hot set has no way to say so.
7. **Is the locator's residency real?** §7.6 has it holding 25 MB of directories
   resident, which is the whole reason it exists rather than the client doing hop 1
   itself. A DO is evicted without notice and rehydrating 25 MB on every cold start
   would invert the benefit. **Report hit rate and cold-start cost; do not assume
   either.**
8. **Does the weekly epoch make cache warming untenable?** §5.3's flush is
   structural, not incidental — every leaf and upper tier goes cold on publish. If
   the post-publish warming pass does not hold, the base cadence and the delta lag
   have to be traded against each other explicitly rather than chosen separately.
9. ~~**What absorbs the cache pollution from scattered reads?**~~ ✅ **Answered
    2026-09-25 by §4.8e — and the answer is that nothing has to.** Scattered row
    reads no longer enter the cache at all: `OnMiss::RangeOnly` reads the row from
    R2 as a range and leaves the part uncached, while the small shared files
    (~34 parts) are still cached whole. The pollution was a consequence of applying
    one cache unit to two access patterns. **The "two hostnames with different cache
    rules" candidate is retired** — it was a config-level workaround for a policy
    that belongs in the reader. The history below is kept because the premise moved
    twice and the wrong version is the more intuitive one.

    ⚠️ **Restated — the premise changed.** This question was built on "~49 MiB pulled per wallet
    query", which came from the per-chunk model §4.7 has now retired. Measured,
    **a read pulls the whole object into cache whatever its shape**, so pollution
    is a function of *which parts are touched*, not of how scattered the ranges
    within them are — and at §5.1's 16 MiB parts, one touched part is 16 MiB.

    That makes it a smaller question than it looked, and it collapses one of the
    three candidates: **the locator returning rows would not reduce pollution at
    all**, because the locator's own read caches the same part. Two remain:
    - **accept it** — per-colo caches are large, and at 16 MiB per touched part
      this is plausibly noise until wallet traffic is high
    - **two hostnames, different cache rules** — cached for clustered and hot
      reads, bypassed for scattered ones. Still a config change rather than an
      architecture one, and now the only candidate that actually addresses it

    What is *not* answered: whether bypassing the cache for scattered reads costs
    more (every read pays the ~350 ms floor) than the eviction it prevents. That
    needs a hit-rate measurement under realistic mixed traffic, which no probe
    here produces.
10. **How small does the client-side reader actually compile?** §10.2 assumes
   `policy-archive`'s 0.15 MB neighbourhood for parse + confirm + multipart. If the
   book's reader is much larger the bulk/navigational split shifts, because a
   client that cannot afford the parser has to be handed rows rather than ranges.

## 14. Decided

- **Hot / warm / cold / live, each on the product it is for.** §5. 200 MB of
  assets is a website; 65 GB is not, whatever the limits multiply to.
- **Set queries port; projections get published; ownership gets derived.** §3.2
  and `OWNERSHIP_WITHOUT_SYNC.md`.
- **The set in object storage, the tip in a DO.** §9's write pricing.
- **The delta is the log read forward**, per-block and retractable, never a
  cumulative published bitmap. §7.1, §7.3.
- **Layout first, protocol second.** §4.10 — multi-range is real, measured
  working even when cached, and a contiguous read still beats 299 scattered ones.
- **Object size is the latency knob; there is no page.** §4.1, §4.7 — the cache
  unit is the object and cold cost tracks depth at ~7 ms/MiB above a ~16 MiB
  knee, over a ~330–400 ms floor.
- **Two tiers, two part sizes, two different bounds.** §5.1 — 1 MiB for static
  assets (fetched whole, isolate memory), **~16 MiB** for R2 (range-read, depth
  and fan-out). They do not coincide, and an earlier draft said they did.
- **Cloudflare, decided.** §6.3a — and what settled it was **sizing parts**, not
  the raw floor: Tigris's win was avoiding depth on huge objects, which §5.1
  already rejected. Reopen only if real traffic proves too diffuse to keep a
  working set warm, which is Tigris's one distinctive property.
- **Part size belongs to the substrate, not to the format.** Measured: Cloudflare
  charges 1774 ms of depth at 256 MiB, **Tigris charges 16 ms**. So it is a
  publish-time parameter — a builder targeting Tigris that split at 16 MiB would
  pay 1,700 objects for nothing. `ARTIFACT_SHARDING.md` §6 carries this.
- ⚠️ **Part size also trades depth against multi-range batching**, and that
  tension is **unresolved** — see §6.3b. Multi-range works only *within* one
  object, so smaller parts cut depth and fragment batching at the same time.
- **Multi-range is Cloudflare-only.** §4.10 — Tigris answers `416 InvalidRange:
  Only support one range`, which makes clustering mandatory there rather than
  preferred.
- **The multi-range ceiling is 299.** §4.10 — capping at the documented 300
  returns the whole object.
- **Caching beats layout by an order of magnitude.** §8 — ~350 ms cold against
  11–14 ms warm is 25×, more than §4.8 and §4.10 can recover together. Set
  `cache-control` at publish; do not inherit R2's 4-hour default.
- **A timing number without its cache status is not a measurement.** §8 — the
  floor was wrong by 10× for two rounds because one sweep did not record it.
- **Split the locator by lifetime, not by concern.** §7.6 — `ranges` is
  per-query and epoch-immutable; the tip is volatile and identical for everyone.
  Bundled, the first inherits the second's TTL for no reason but the bundling.
  **Measured working**, via the Cache API — headers alone do nothing for a
  Worker's own response, and the opt-in must be structural or it rots.
- **Whatever is indexed by policy in the base is indexed by policy in the sealed
  deltas.** §7.1a — indexing only the base leaves ~75 MB of delta to amend
  332 KB of rows, which silently undoes the split.
- **A question about change over time is a projection, not a set query.** §7.7 —
  the book knows where a row is, never when it arrived.
- **Bulk reads go direct; navigational reads go through the locator.** §10.1. It
  is why `policy-archive` in a browser bundle was right and why the book must not
  copy it wholesale.
- **The client batches ranges, not the locator.** §10.2 — the locator is one
  location, the CDN is 300+, and a DO is subject to the same six-connection
  governor a browser is not. The locator returns ranges and never row bytes.
- **The 33 MB is server-side residency, never a client download.** §10.3 — the
  directory is range-readable, so it buys one hop of three.
- **OPFS declined.** §10.4 — it is a stateful cache with a lifecycle rather than a
  transport, and its one unique capability is the case we are not building.
- **Warming means reducing how many places need warming.** §5.3 — Smart Tiered
  Cache plus regional buckets, not 300 synthetic leaves. Cache Reserve is
  unavailable on this path.
- **Amplification is on the origin leg only, and its cost is cache occupancy.**
  §4.7 — never client bandwidth, and money is not the constraint.
- **A published epoch is retained for at least one further epoch.** §4.6 — pruning
  on the flip turns a paging client's follow-up reads into 404s mid-query.
- **The bucket is public, and that is accepted with monitoring.** §5.4 — it is the
  precondition for anonymous browser reads and for multi-range at all.
- **Immutable paths plus a conditional pointer on R2; the Worker version is the
  pointer for assets.** §4.6.
- **The part is the read unit**, not a range within a file. §5.1.
- **Anycast is not the starting point.** §6.3. GeoDNS first.
- **Not a database.** `LIVE_BOOK.md` §Decided's conclusion, reached again for a
  different reason: a mutable store at the edge is billed per row written. See
  `ARTIFACT_SHARDING.md` §9.1 for why an embedded engine does not change this.
