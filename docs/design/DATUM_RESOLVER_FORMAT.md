# The datum resolver — hash → preimage, additive and immutable on R2

Status: **the set is built and published; the reader is not** (2026-09-28). The first
full capture — 93 windows, chunks 0..=9205, **20,370,399 distinct datums, 4.55 GiB** —
is on R2 at `r2:mithril-snapshots/datum/v1/`, verified by fetching the largest pair
back and re-hashing both objects to the digests in their own keys. `datum.tbl` +
`datum.bin`, `DatumTable`, and the pushable set
(`crates/live-book/src/publish.rs::publish_datums`, written by the fold's sink into
`<datum-tbl>/publish`, pushed by `fold-probe datum-push`) all exist. No browser reads
the set yet. Everything in §2, §3 and §6.5 is *measured* on the `cardano-dev`
snapshot (9,203 chunks, 217 GB) with
`tools/mitos-chain-walk/examples/offer_datums.rs`; the figures attributed to
`DECODE_SHAPE.md` §2.10 are that document's, taken on the fold's own walks.
Derived numbers say so.

The requirement it exists to meet: **a hash-attached output carries no payload, and
the reader must be able to get the payload anyway — additively, from files on R2,
without a service in the path.**

Owner: TBD

Related:
- `crates/live-book/src/format.rs` — `write_datum_table` / `DatumTable`: the format
  this design publishes and must not change (§4)
- `crates/live-book/src/publish.rs` — `PARTED`, the manifest and the part-naming
  rule; today it does not carry `datums.*`
- `crates/cf-publish/src/lib.rs` — `immutable_attributes` vs `pointer_attributes`,
  the two cache postures a content-addressed blob and a flipped pointer need
- `ARTIFACT_SHARDING.md` §4, §7.1 — the general immutable-shard / epoch model this
  applies to one artifact rather than inventing
- `LIVE_BOOK.md` §B7 (one walk, many artifacts), §B8 (verification), §Sizes, and
  §Open questions ("who publishes, and against which boundary")
- `DECODE_SHAPE.md` §2.10 — witness-datum volumes, table sizes and the
  "answerable at creation" column
- `tools/fold-probe` (`fold --datum-tbl`, `datum-push`) — the capture and the upload;
  the capture is a sink on the deep walk, not a second pass, and the push prints the
  two `rclone` invocations rather than hiding them
- `docs/HOWTO_DATUM_CAPTURE_AND_PUSH.md` — the operational runbook for the box that
  holds the chunks
- `cnft.dev-workers/workers/live-book-edge/frontend/src/chunks.rs` — the first
  consumer (`locate_datums`, and the `Datum` enum's missing arm)
- `TX_INDEX_DATASOURCE.md` R3.2 — the staleness-against-a-boundary rule the
  catalogue's coverage has to be honest about

---

## 1. Why an artifact, and not a walk

The offer book can see thousands of live offers at Wayup's offer contract — 4,255
rows in the view's own count, 4,213 still live in this week's snapshot — and could
read 21 of them. The gap is not a decode bug and not missing data on chain: **it is a
reader that only knew two places to look.** `locate_datums` reads the output's body
span (inline datum) and the transaction's metadata (the jpg convention) — the two
sources that live on the output or beside it. Wayup's preimages are in the
transaction's *witness set*, which is a third place, and which the index does not
address.

The obvious fix — read past the end of the body — does not exist, and §2 is why.
That leaves exactly two routes to a witness set: **a span the index records**, or
**a capture taken on a walk**. This document is the second one.

## 2. The block layout that decides it (measured)

Shelley-onward blocks keep the bodies and the witness sets in **two separate
top-level arrays**:

```
[ header, transaction_bodies, transaction_witness_sets, auxiliary_data_set, invalid_transactions ]
```

so the bytes after a body are the *next body*. On the block holding a real Wayup
create — the one that made four of a holder's offers in a single transaction —
(`--trace ddb15aad…`, offsets within the block):

```
block tx #0 (1 of 6): body @863+2516 witness @5781+1273 NOT adjacent
block tx #1 (2 of 6): body @3379+198  witness @7054+208  NOT adjacent
```

`tx #0` ends at 3379 — and at 3379 begins `tx #1`'s body. Its own witness set (the
1273 bytes carrying exactly the three 353-byte datums that tx created outputs
against) sits at 5781, in the array after all six bodies.

⚠️ **A window past `body_end` cannot be sized.** `maxTxSize` (16384) bounds the
*body*; the witness set is where Plutus scripts live, and the Conway reference-script
rule alone allows ~200 KB per transaction. There is no arithmetic that reaches a
witness set from a body span, at any width. That is why this is an artifact
question rather than a reader-trick question.

## 3. What is on chain, by venue (measured)

The probe walked the Wayup offer contract's **entire life** (first creation at slot
169,006,656; window opened at 168,000,000) — 1.5 M blocks / 9.6 M txs / 13.5 GB in
143 s:

| class | created | live |
|---|---|---|
| inline | 43 | 23 |
| preimage in its own tx | 13,411 | 3,114 |
| preimage from an earlier tx | 4,322 | 1,075 |
| **no preimage anywhere in the window** | **0** | **0** |
| no datum at all | 1 | 1 |

**4,212 of 4,213 live offers readable; zero unrecoverable.** For one holder's stake:
2,455 created, 870 live, 870 readable. (A separate address-narrowed count through the
live-book worker route, `.tmp/wayup-holder.mjs`, put the same set at 875 — but it
reads a different tip than this snapshot, so the two are a cross-check on the order
of magnitude and not a reconciliation. Re-deriving that count against the same tip
is worth doing before either figure is quoted as exact.) The mechanism is stated in
our own builder: `cardano-tx/src/builder/collection_offer.rs` — *"Wayup itself
publishes the preimage in the create TX witness set (forcing a script-data hash)"* —
and 75% of creations carry it in their own transaction.

The other venue is the contrast that prices this artifact. At jpg's offer address:
6,330 created, 78 live, of which **53 live are unrecoverable** (68%), because jpg
publishes nothing at creation — its preimages arrive when an offer is spent (3,415
of 4,711 unreadable-at-creation were revealed by a spend) or through the labels-50+
metadata convention. A capture is the only thing that can hold the spend-revealed
ones.

`DECODE_SHAPE.md` §2.10 measures the same question store-wide: **95.20–98.95% of
hash-attached outputs in the tip and Babbage windows are answerable at creation**,
against 67.55% in Alonzo. Wayup is the best case; the artifact is not optional for
the rest.

## 4. What exists today

`datum.tbl` + `datum.bin`, written by the fold (`fold-probe fold --datum-tbl`):

- header: `layout_version u32 | count u64 | first_slot u64 | last_slot u64` (28 B);
- entries: `{hash: 32, offset: u32, len: u32}` (40 B) **strictly increasing by the
  full hash**, then the payloads concatenated in that same order;
- `open` refuses a pair whose entries do not tile the blob exactly — a blob from a
  different capture would otherwise answer with *wrong bytes*;
- `DatumTable::get(hash)` is a binary search; `coverage()` states the slot range, so
  a miss reads "not witnessed in this range" rather than "not on chain".

Three properties already fit this design and are why the format does not move:

1. **A table is a closed, sorted run** — it is written once, whole, and never
   appended to. Immutability is the format's natural state, not a constraint added.
2. **The key determines the value.** A datum's bytes are fixed by its hash — that
   is what the hash is for — so two tables holding the same hash hold the same bytes.
   Additive runs therefore union with no precedence rule and no conflict resolution;
   the only thing a run can get wrong is *absence*.
3. **`u32` offsets cap a table at <4 GiB**, which forces sharding — and sharding is
   what makes an append-only store natural (§6.1).

What is missing: the set is not **uploaded** anywhere (`publish::PARTED` is the book's
file list, and the datum set is laid out by `publish_datums` instead — separate on
purpose, since a book's files are fixed and a datum set accumulates), `DatumTable` is
`mmap`-only, and no browser can read either file from a bucket yet.

## 5. The requirement

- **Additive.** New information lands as new objects. Nothing published is ever
  rewritten, so R2 and its caches behave like the immutable DB they mirror.
- **Fast, and not degrading.** A lookup must not cost more as the store grows. Two
  readers must both be fast: one that *has the transaction* (the offer book) and one
  that has **only a hash** (the general resolver).
- **Honest about absence.** A miss cites the range that was covered. "Not in this
  capture" and "not on chain" are different claims and the artifact must not blur
  them.

## 6. The design

### 6.1 The unit: one closed table per window

The capture is already per-walk-window. Publish it that way: a **window object pair**
covering a fixed chunk/slot range, written once when that range is captured. New
information for a *new* range is a new pair. Nothing else changes.

⚠️ A window is never re-opened and appended to: entries are sorted by hash, so
appending would break the one invariant the reader relies on. Growth is *more
windows*, never a longer one.

### 6.2 Naming, the catalogue, and the flip

```
datum/v1/current                                  ← the only mutable object
datum/v1/w-<first_slot>-<last_slot>-<digest>.tbl  ← immutable, content-addressed
datum/v1/w-<first_slot>-<last_slot>-<digest>.bin
```

- **The digest is the file's own blake2b-256** (not the pair's), so each object
  verifies itself: a key never names different bytes, the object earns
  `immutable_attributes()` (`public, max-age=31536000, immutable`) and caches like
  the book's parts, and a reader can check what it fetched against the key it asked
  for. The part-naming rule in `publish.rs` applies as-is: an extension on every
  object.
- `current` is the flip, and takes `pointer_attributes()` (`no-cache`). A pointer
  with the files' posture pins a stale artifact for a year.
- The catalogue is **flat text, one line per window** — `book.meta`'s doctrine
  (every field required, a missing one an error, the rendering byte-stable), and no
  new dependency in `live-book`: the key is *derived* from the entry rather than
  stored, so there is one definition of the name and not two.

```
catalogue_version 1
window <first_chunk> <last_chunk> <first_slot> <last_slot> <entries> <layout_version> \
       <tbl_len> <tbl_digest> <bin_len> <bin_digest>
```

- **Chunk numbers as well as slots**, because the reader's anchor is a chunk: the
  tx-index answer for a transaction carries `entry.loc.chunk`, so the mapping
  chunk → window is a comparison, not a slot search.
- The window's slot range and entry count are read from the *table's own header* when
  the layout is written, so a name or a catalogue line cannot describe a capture that
  is not the one on disk. The chunk range is the only thing the walk itself supplies.
- A re-publish of an already-covered window **replaces its own line** and leaves
  every other window alone; the superseded objects stay in the bucket, because
  correctness depends only on which catalogue a reader was handed.
- Before anything is written, the pair is checked the way `DatumTable::open` checks
  it — entries tiling the blob exactly, and strictly increasing by hash. A pair that
  fails either would answer with **wrong bytes** rather than fail, so it must not
  reach a bucket.

### 6.3 Lookup A — the reader has the transaction (the offer book)

The live book gives the outref; the tx-index gives the creating tx's chunk; the
catalogue gives the window; the window's table gives the payload. **One object,
one binary search** — and this stays *O(1) in the size of the store*, no matter how
many windows are appended, because the selecting key is the transaction the reader
already holds. If the tip-sized tables prove too large for a whole-object fetch,
a per-table prefix directory (the `dir.idx` trick tx-index already uses) makes it
two ranged reads instead of one object.

This is the path that turns 21 readable offers into 4,212. It needs no directory
across windows, and no compaction.

### 6.4 Lookup B — the reader has only the hash (the resolver)

Time-sharding cannot prune: hashes are uniform, so every window's key range spans
the whole hash space and min/max fences buy nothing. A hash-only reader would have
to read every window.

The fix is a **second tier built from the same capture**: partition *at write time*
by hash prefix, so a window publishes N pairs, each holding one prefix's keys:

```
datum/v1/p<xx>/w-<first_slot>-<last_slot>-<digest>.tbl|.bin
```

A lookup then reads `current`, takes its prefix's run list (one entry per published
window, each tiny), and probes them newest-first, stopping at the first hit — **one
read for anything witnessed since the last capture**, and one per window in the
worst case, against a handful of tens-of-KB objects. Every object is still written
once: this is a bucketing change in the writer, not a compaction pass, and nothing
published is ever superseded.

The alternative — periodically compacting window runs into a hash-sorted table set —
is rejected for the same reason: it is a rebuild, and it is only needed because the
first tier is time-partitioned. Bucketing at write time costs one group-by on a
sorted array the writer already has.

### 6.5 Sizes

`DECODE_SHAPE.md` §2.10 measured the table against real windows: **2.7 MB payload /
0.7 MB entries at the Conway tip, 80 MB / 14 MB in Babbage, 69 MB / 24 MB in
Alonzo** — *"34 KB per chunk at the tip against ~970 in the 2021–2023 windows"*, and
a whole-history table in the single-digit GB. Store-wide capture is therefore a set
of window pairs, not one file, exactly as the format anticipated.

Derived from that: a 256-prefix bucketing makes a window's prefix tables tens of KB
at the tip and a few hundred KB in 2021–2023 — small enough to fetch whole — at the
cost of 256 × windows objects (fine for R2, and it is what buys lookup B).

## 7. Re-capture, and what a re-walk writes

A re-walk of an already-covered window that finds the same datums produces
**byte-identical** files (the writer sorts by hash and dedups), so the digest is the
same and the upload is a no-op — the store does not grow because we looked again. A
re-walk that finds *more* — a decoder fix, a missed era — publishes a new digest,
the catalogue points at it, and the superseded object stays readable for anyone who
pinned an older `current`. No deletion is required for correctness; reclaiming
superseded objects is a policy question (§11).

This is what makes the artifact safe to improve: correctness never depends on
overwriting, only on which catalogue a reader was handed.

## 8. Verification

Three checks, in order of cost:

1. **Tiling at open** — already enforced; catches a mismatched `.tbl`/`.bin` pair.
2. **Re-hash per lookup** — the reader re-hashes the payload it just fetched and
   refuses it unless it equals the key it asked for. This is what makes additive
   runs from *different walks at different times* tamper-evident: a well-formed lie
   is caught by the consumer, not by trusting the writer. It also makes the two
   tiers independently verifiable.
3. **Campaign checks** — `DECODE_SHAPE.md` §2.10 records 1,504/1,504 payloads
   rehashing to their key in a 5-chunk table, and five sampled hashes coming back
   byte-identical from Koios. Worth repeating per published window, in the same
   spirit as `LIVE_BOOK.md` §B8.

The first published set was checked this way on 2026-09-28: the **largest** window's
pair (a 29.8 MB `.tbl` and its 157 MB `.bin`) was fetched back out of the bucket and
both objects re-hashed to the digests in their own keys.

## 9. What this is not

- **Not a replacement for inline datums.** The output's own bytes are always
  first — a table is only consulted for a hash.
- **Not an aux-data artifact.** jpg's metadata convention needs the aux bytes, which
  `DECODE_SHAPE.md` §2.10 measures as the *larger* stream (101–332 MiB per 100
  chunks). If the offer book needs it, it is a sibling artifact with the same shape,
  not a field on this one.
- **Not a service.** No worker route in the path: the browser reads the catalogue
  and the tables from R2 directly, which is the property `live-book-edge` exists to
  test.

  ⚠️ **With one arm that has to earn its place.** `live-book-edge` also serves
  `POST /v1/datums`, which resolves a batch of `(hash, chunk)` needs in a colo next to
  the bucket. It is a *route*, not the path, and the reason it is allowed to exist is
  the number below: resolving 101 needs against a real capture read **12.39 MB of entry
  arrays** to find **10.1 KB of payload** — so a reader that fetches its own tables is
  pulling twelve megabytes across the wire to throw them away. The aggregator keeps
  them in the colo and returns only the preimages. Both arms are priced in the same
  table, which is what decides whether the worker stays.
- **Not the book's row layout.** Whether a datum's bytes live inline in a row or
  behind a reference is `LIVE_BOOK.md` §Open's question; this is the payload store
  either way.

## 10. What we build, in order

| | step | file | state |
|---|---|---|---|
| 1 | Publish the window pairs and the catalogue; `current` flipped per publish, window objects immutable | `crates/live-book/src/publish.rs` | **built** — `publish_datums`; the fold's sink writes the set into `<datum-tbl>/publish`, and `fold-probe datum-push` prints/runs the two `rclone` lines (`HOWTO_DATUM_CAPTURE_AND_PUSH.md`) |
| 2 | A reader over ranged reads (the local one is `mmap`-only), cache-busting the pointer and validating digests | `crates/live-book/src/format.rs` | not built |
| 3 | The table arm in the datum pass, with a new `Datum::FromTable` arm | `live-book-edge/.../chunks.rs` | **built** (2026-09-28) — `resolve_from_datum_set`: one catalogue read, one entry-array read per window it needs, local search, batched payload reads, each payload **re-hashed** before it is believed. ⚠️ Ordered inline → metadata → **table**: the first two ride reads already being made, so the table is asked third, and ordering by cost is safe because the sources cannot disagree — a datum's bytes are fixed by its hash. Awaiting a browser run (§8's checks apply) |
| 4 | The general resolver: `datum_by_hash` — hash → prefix → runs → ranged read, re-hash before believing | `crates/live-book` (reader) + `live-book-edge/frontend/src/direct.rs` (fetch) | not built |
| 5 | Prefix bucketing in the writer | `tools/fold-probe` | not built |
| 6 | The index route, if we want a reader that needs no capture at all: witness-set span in the tx-index entry (24→32 B, `FORMAT_VERSION` 3, republish) | `crates/tx-index` | not built; the entry has already been widened once for aux spans — `format.rs`'s `FORMAT_VERSION` note is the precedent |

Steps 1–3 make the offers view correct against a real capture, which is the point
of the exercise. Steps 4–5 make the resolver general. Step 6 is the only route that
answers for a transaction *nobody captured*, and it is deliberately last: it costs a
republish, and it cannot answer for a hash without a transaction.

## 11. Open questions

- **Window width.** One pair per fold run, or a fixed range (a day, 100 chunks) that
  several runs contribute to? The trade is object count and re-capture granularity
  against nothing else — lookup A does not care.
- **Prefix count.** 256 buckets is a guess derived from table sizes; it wants a
  measurement against a real Babbage-sized window.
- **A per-run filter for lookup B.** Probing a prefix's runs newest-first is W reads
  in the worst case; a few-KB Bloom filter per run would prune it to the runs that
  can hold the key. Worth measuring against the plain probe before committing to
  either.
- **Whole-object fetch vs ranges vs an in-table directory.** The tip's tables are
  small enough to fetch; the 2021–2023 ones are not. Measure before choosing.
- **A narrow capture for the offer book.** The book needs the datums of *live
  hash-attached offers* — kilobytes, not gigabytes, and it could be refreshed on
  demand. Whether that rides the general table or gets its own small artifact is
  undecided.
- **GC of superseded captures.** Nothing needs deleting for correctness. Something
  will want a retention rule.
- **Who publishes, and against which boundary.** Inherited from `LIVE_BOOK.md`
  §Open; the catalogue's coverage is where `TX_INDEX_DATASOURCE.md` R3.2's staleness
  budget lands.

## 12. Decided

- **The table format does not change.** Closed sorted runs keyed by the full hash
  are already the right shape: additive, conflict-free, verifiable, and small.
- **No walk protocol.** A body span cannot reach a witness set at any window width
  (§2, measured) — the idea was attempted and is closed.
- **Content-addressed names and a flipped pointer**, with the two cache postures
  from `cf-publish`: immutable objects, `no-cache` pointer.
- **Additivity is by closed windows**, never by appending to a table.
- **Re-hash per lookup** — the check that makes multiple captures safe.
- **Two tiers from one capture**: window tables for a reader with a transaction,
  hash-prefix runs for a reader with only a hash.
- **The first consumer is `locate_datums`**, ordered inline → table → metadata →
  `HashOnly`.
