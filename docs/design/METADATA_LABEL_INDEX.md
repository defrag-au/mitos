# The metadata label index — sizing a chain-fact index over auxiliary data

**Measured 2026-10-01; strategy proposed, decision open.** The numbers come from
`fold-probe labels`, a new subcommand in `tools/fold-probe/src/main.rs`, run over the whole
store on the ARM box. Siblings: `ROW_ADDRESSING_AND_SEARCH.md` §5.4, which shows the
searchable corpus is metadata — this is the **access** structure for it; `OFFER_INDEX_FORMAT.md`
§9, which rules domain sets out of the bucket; `TX_INDEX_DATASOURCE.md`, the index it can ride.

## 1. What it is

One flat, sorted array: **one entry per `(label, tx)` pair**, grouped by metadata label, with a
small directory naming each label's range. It answers, from the chunk mirror alone, "every
transaction carrying label L — and, if ordered by chain position, those within a chunk window",
after which a reader fetches whichever transactions' auxiliary data it asked for and interprets
it however its domain wants.

Built in **one parallel pass over the chunks: 275–289 s store-wide across three runs**, nothing
else read. The §5.4 runs put `count` at 227 s and `cip25` at 337 s over 9,194 chunks — this sits
between them, which is where it should: it decodes every block and every transaction's aux,
walks one metadata map per transaction, and re-encodes values only to measure their size.

## 2. Why this is a chain fact

The label space is decided by consensus, not by us. The ledger decodes auxiliary data by CBOR
token type — a bare map is Shelley-form metadata with labels at its top level, an array is the
Mary form, and only `tag(259)` is the post-Alonzo structure with the metadata map at key 0
(`Cardano.Ledger.Alonzo.TxAuxData.decodeTxAuxDataByTokenType`). pallas draws the same line
(`AuxiliaryData`: `Map → Shelley`, `Array → ShelleyMa`, `Tag → PostAlonzo`).

⚠️ **A first look at the histogram raises a false alarm.** Labels `0`–`10` carry millions of
hits each; they look like post-Alonzo *structure* keys bleeding in. They are not. Only the
tagged form has structure keys; an untagged map is metadata by the ledger's own rule, so a small
integer label is a label. The census reads the same line the chain does — which is what makes
the artifact nameable without a marketplace, a policy, or a decoder.

⚠️ **The `50`–`62` band has the shape of a convention, and the index must not name it.**
Near-equal counts (3–4 M hits each, and again across `57`–`62`) are what one datum spread over
consecutive labels looks like; the chunked-hex convention `mitos-marketplace-decode` reassembles
is exactly that shape — its fixture uses labels 30 and 50+. Whether every one of those hits is
that convention is a decoder question. The index records the labels and nothing else, which is
what keeps the artifact domain-neutral.

## 3. The census

Run: `fold-probe labels` over 9,218 complete chunks (newest excluded), 12 threads — 275–289 s
across three runs. Raw log `/opt/scratchpad/labels-full.log` on the box.

| | |
|---|---|
| transactions walked | 124,116,533 · 245 invalid skipped |
| carrying auxiliary data | 38,346,191 (**30.9%**) · **13.09 GiB** of aux |
| carrying a metadata map | 38,161,799 of them (**99.5%**) · 184,392 script-only |
| aux that failed to decode | **0** |
| distinct labels | **3,213** |
| `(label, tx)` hits | **137,891,485** — 3.61 a metadata-carrying tx |
| CBOR under labels | **12.78 GiB** · 100 B a hit |

The top of the histogram (`--top 30`; the log's table, verbatim):

| label | hits | share | value bytes |
|---|---:|---:|---:|
| 674 | 20,902,642 | 15.2% | 953 MiB |
| 30 | 6,264,478 | 4.5% | 12 MiB |
| 7 | 5,755,224 | 4.2% | 311 MiB |
| 1 | 5,566,444 | 4.0% | 199 MiB |
| 3 | 5,393,225 | 3.9% | 173 MiB |
| 6 | 5,140,009 | 3.7% | 323 MiB |
| 8 | 5,037,488 | 3.7% | 316 MiB |
| 9 | 5,021,322 | 3.6% | 250 MiB |
| 0 | 4,792,102 | 3.5% | 281 MiB |
| 721 | 4,690,841 | 3.4% | **6.09 GiB** |
| 50–56 | 3.80–4.19 M each | ~20% | ~230–264 MiB each |
| 57–62 | 0.91–3.13 M each | ~10% | 27–195 MiB each |
| 1000, 1002, 1003 | ~821,800 each | 1.8% | ≤46 MiB each |

⚠️ **The bytes and the hits are two different distributions.** Label 721 is 3.4% of hits and
**48% of all value bytes**. The index carries neither values nor their size — the fetch a reader
opts into after the lookup is where that 6 GiB sits.

### 3.1 Coverage: a few labels, a long tail

| | |
|---|---|
| labels covering 50% of hits | **11** |
| labels covering 90% of hits | **30** |
| labels covering 99% of hits | **119** |
| labels appearing exactly once | **2,321** of 3,213 |

That skew is the whole layout argument: a directory-addressed array handles the 11 huge ranges
and the 2,321 singleton ranges with the same code and the same isolation property.

Bands — labels at or above a hit count, and the entries they hold:

| hits at or above | labels | entries | share |
|---|---:|---:|---:|
| 1,000,000 | 26 | 120,921,518 | 87.7% |
| 100,000 | 66 | 133,386,676 | 96.7% |
| 10,000 | 179 | 137,661,377 | 99.8% |
| 1,000 | 232 | 137,846,779 | 99.97% |
| below 1,000 | 2,981 | 44,706 | 0.03% |

⚠️ **The tail is 2,981 labels and 175 KiB of entries.** Everything below 1,000 hits — most of the
label space by count — fits in less than four directories' worth of bytes. A layout that gives
it one object per label would pay request overhead for nothing (§5.1).

### 3.2 Cross-checks

- `cip25` (`ROW_ADDRESSING_AND_SEARCH.md` §5.4) saw **4,689,702** transactions declaring 721 over
  9,194 chunks; the census sees **4,690,841** hits on 721 over 9,218. The census counts the key
  whatever the value's shape, `cip25` required a map, and the store grew by 24 chunks — the gap
  is that arithmetic, not a disagreement.
- Transactions walked, 124,116,533, is consistent with `index/tx/v1`'s 124,020,199 entries at
  build `09202` (15 chunks earlier; ~6.4 k txs a chunk).
- `0` aux decode failures, and 99.5% of aux-carrying transactions carrying a metadata map — the
  index's coverage is not hiding a decode debt.

## 4. What an index costs

### 4.1 Entry shapes

137,891,485 entries. What varies is what an entry names, and therefore how many reads a query
costs and what it can be sorted by.

| entry | bytes | store | the reader gets | within-label order |
|---|---:|---:|---|---|
| `u32` ordinal into `index/tx/v1` | 4 | **526 MiB** | chunk, body span and aux span, via one contiguous 24 B read | tx-index's **hash order** |
| `u16 chunk` + `u32` ordinal | 6 | 789 MiB | the above; a chunk window is a subrange | chain order |
| `u16 chunk` + `u32` aux off + `u16` len | 8 | 1.03 GiB | the aux bytes directly; **no tx identity** | chain order |
| `u16 chunk` + body span + aux span | 14–16 | 1.80–2.06 GiB | tx identity (hash the body) and the aux, independent of tx-index | chain order |
| the 24 B `index/tx/v1` entry, copied | 24 | 3.08 GiB | the above plus the 8-byte hash prefix | chain order |

⚠️ **The coupling moves with the shape.** Ordinal entries are positions in `index/tx/v1`'s
hash-prefix-sorted array, so the two snapshots must be built and flipped together — and a label's
entries are in **hash order**, so `(label, chunk window)` is not a subrange. Span entries carry
their own addresses and flip on their own, in chain order. The natural build rides tx-index's
extraction pass either way: it already computes body and aux spans per transaction.

⚠️ **Sizes are arithmetic, not prototypes.** No object has been written; these are
`137,891,485 × n` exactly. Compression is not attempted — entries are small integers and the
existing arenas are range-read raw. §6.2 drops the two ordinal rows for publication (append
breaks ordinal stability), which leaves the span rows as the live candidates.

### 4.2 The directory

3,213 labels × 16 B ≈ **50 KiB**: `(label u64, start u32, count u32)`, sorted by label. Publish
it as its **own small object**, fetched whole and cached; data objects hold entry ranges only. A
query is then: fetch directory (once) → range-read the label's slice → fetch what each entry
names. A one-hit label costs 50 KiB of directory and 4–16 B of entries; label 674 costs 80 MiB at
the 4-byte shape (its range, and only its asker pays it); neither touches the other's bytes. The
data side is not one object — §6.1 makes it uniform 1 MiB parts, and the directory is what keeps
that invisible to a query.

## 5. The isolation strategy — one label must not slow another

The concern this section answers: with 3,213 labels of wildly different weight present in one
artifact, a query for a rare label must not pay for 674 or 721 being in the room. **The layout
makes isolation structural; the access contract keeps it that way.**

1. **Partition by label; ranges are disjoint.** Entries are grouped by label and addressed only
   through the directory, so label L's bytes occupy one contiguous range and no other label's
   bytes are ever inside it. Read cost is `O(entries for L)` — never `O(index)`.
2. **Sort within label by chain order** (span shapes). A `(label, chunk window)` query is a
   binary search into the label's range plus a subrange read — the lever that keeps a 20 M-hit
   label answerable without reading 20 M entries. The ordinal shape trades this away; that trade
   is the main reason to prefer spans despite the bytes.
3. **Ranged reads are the access pattern, and whole-object reads are a bug.** R2 serves ranged
   GETs, so a query transfers only its range — but the single way one label can slow another is a
   reader (or an intermediary) that downloads the object and filters locally. That failure is the
   same shape `OFFER_INDEX_FORMAT.md` §1 measured on the offers scan: 65 MB paid every visit
   because the client, not the store, did the filtering. The contract says so explicitly.
4. **No cross-label work anywhere.** The build is one pass with no per-label state: any label
   set costs the same ~5 minutes, a new label cannot slow an old one, and a label's growth only
   moves later offsets — which the directory is rebuilt with. Serving is directory lookup plus
   range math. There is no stage where one label's size is an input to another's cost.
5. **Split by the hot set only if a part ever needs to shrink — never reshape entries.** §6
   already partitions the index into cacheable, append-only parts. If one part still needs to be
   smaller, the census gives the cut for free: **11 labels are 50% of entries, 30 are 90%, 119
   are 99%** — a data-driven prefix of the directory, not a guess.

Snapshots flip whole (catalogue + pointer, the `datum/v1` convention), so a reader can never see
half of one build's ranges and half of another's.

### 5.1 Per-label objects, priced (asked 2026-10-01)

One file per label skips the directory: no 50 KiB fetch, no binary search. The saving is real
and negligible — the directory is one cacheable object and the search is ~12 probes — and both
layouts read `O(entries for L)`, with R2 billing per request rather than per object:

| | directory-addressed (this design) | one object per label |
|---|---|---|
| cold query, one-hit label | 50 KiB dir + 4–16 B | build pointer + 4–16 B |
| warm query | 4–16 B | 4–16 B |
| query for 674 | its 80 MiB run | the same bytes |
| multi-label query | dir + N ranges | N objects, in parallel |
| publish | parts + dir | 3,213 objects |
| atomic flip | swap the directory | a build pointer — the saved GET comes back |

What it buys is cache granularity: every label at its own stable URL, hot and cold never sharing
a cache entry — which retires the ranged-cache unknown rule 5 guards against. What it costs is
the atomic flip (or 3,213 non-atomic in-place writes) and a tail that is almost pure request
overhead.

⚠️ **The bands say the split is not 3,213 objects.** 232 labels hold 137,846,779 of the
137,891,485 entries (99.97%); the other 2,981 hold 44,706 entries — 175 KiB in total, less than
four directories. If cache isolation becomes a requirement, the shape is the one rule 5 already
names: hot labels as whole objects (≥100,000 hits: 66 objects, 96.7% of entries; ≥1,000: 232
objects, 99.97%), the tail in one small directory-addressed object. Giving the tail 2,981
objects buys nothing the tail object does not — and per-label objects change nothing about
within-label windows; those still need chain order and ranges. §6 adds the second loss: to stay
append-only they would have to be per `(label, part)`, which is more objects holding exactly the
ranges a window-major part already holds.

## 6. Publication — cacheable parts, appended not rewritten

The house rule for every published artifact is that a sync **adds** what is new and rewrites
nothing historical: the datum capture publishes closed windows behind a catalogue, `tx-index`
appends a segment per chunk and keeps a rebuildable base, the book flips a pointer. The
machinery already exists in `live-book::publish` — digest-named windows, `part_key(name, part)`,
`PART_EXT`, an attestation — and the datum push already uploads with `--ignore-existing` and a
year-long immutable `Cache-Control`. This index joins them.

`MITOS_AT_THE_EDGE.md` has measured what the layout has to satisfy, and the second limit below
is the one that binds:

| limit | value | where |
|---|---|---|
| cacheable file size | **512 MB** (Free/Pro/Business; 5 GB Enterprise), checked against the whole object via `Content-Range`, so a small range does not bypass it — over the limit is **not cached at all** | §4.7 |
| cold read cost, R2 → edge | **~650–700 ms floor + ~35 ms/MiB of depth**: 1 MiB ≈ 685 ms, 16 MiB ≈ 1210 ms, 64 MiB ≈ 2890 ms | §6.3b, measured cold |
| part size the family settled on | **1 MiB uniform**, so `offset → (part, offset-within)` is arithmetic | §6.3b |
| cache eligibility | **extension-based**: `.bin` caches; an extensionless key is `DYNAMIC` forever and multi-range against it returns `400` | §4.8f |
| multi-range | within one object, **2–299 ranges**, CDN only; 300 returns the whole object as `200` | §4.10 |
| cache TTL | R2's default is `max-age=14400`; immutable parts publish `public, max-age=31536000, immutable`, the changing catalogue a short one | §4.7 |

⚠️ **The 512 MB ceiling is the lesser constraint; depth binds.** This document nearly repeated a
recorded mistake, so the correction is cited rather than paraphrased: the “16 MiB knee” was
measured against objects something had already read and was wrong by ~4× — *“an object read once
is warm at the origin for every subsequent measurement”* (§5.1, §6.3c). Cold, a monolithic
object is 5–30 seconds deep, and it is never cached anyway: **many shallow concurrent reads beat
one deep one**, and concurrency is free.

### 6.1 The shape: time partitions, merged once at seal

Append and fan-out pull in opposite directions: new entries arrive for arbitrary labels, so the
write side wants chain-partitioned, append-only parts; a query wants one label's entries
together, so the read side wants a label-sorted, contiguous run. The shape that fits both is the
datum set's windowing with a merge at seal:

- **Partitions by chain range** (~1,024 chunks, about a year). The open partition publishes as
  append-only ~1 MiB segments, so the index follows the tip; when it seals, its segments merge
  **once** into one immutable, label-sorted run. **Nothing in a sealed partition is ever
  rewritten** — its part URLs never change, so its cache entries never go cold.
- **A query reads one range per partition the label appears in, plus any live segment.** Fan-out
  grows one per partition — one per year — not with data volume, and a window query skips the
  partitions outside it.
- **The directory** is `(label, partition, start, count)` plus the live segments, rebuilt per
  publish behind a short TTL like the datum catalogue. Measured with the census's `--partition`
  flag: today's store is **10 partitions, 5,100 (partition, label) pairs — 100 KiB at 20 B a
  pair** — and the labels one partition names run min 18 · median 428 · max 1,878, because the
  tail is concentrated in the partitions it appeared in and only the hot labels appear in all
  ten.

⚠️ **Two alternatives were priced and rejected.** *Never compact* writes least, but a hot
label's run list grows with time: 674 across today's store is ~2,100 segment reads at 16 B —
viable only for window-scoped queries. *One global base recompacted on a cadence* reads as one
run but rewrites 0.5–3 GiB per compaction and changes every part URL, flushing the whole index
cold (`MITOS_AT_THE_EDGE.md` §5.3's epoch problem, self-inflicted). Sharding that base by label
range — an earlier draft of this section — fails twice over: a window's active labels include
the spread-out tail, so a merge touches nearly every bucket, and the hot set is 99.97% of the
bytes, so compacting the hot labels *is* compacting the store.

Sizes at the 1 MiB target (today's 15.0 k entries a chunk):

| entry | entries/part | chunks/part | parts store-wide | 674, whole store |
|---|---:|---:|---:|---:|
| 4 B | 262 k | ~18 | ~530 | 80 parts |
| 8 B | 131 k | ~9 | ~1,050 | 160 parts |
| 16 B | 65.5 k | ~4.4 | ~2,100 | 320 parts |
| 24 B | 43.7 k | ~2.9 | ~3,150 | 480 parts |

⚠️ **Writes per sync are proportional to new chunks; the largest write the design ever makes is
a seal.** At ~2.8 chunks a day (9,218 over the store's ~9 years), a 16 B segment seals every
~4.4 chunks — **one 1 MiB part every day and a half** (every ~6 days at 4 B) — plus a ~100 KiB
directory refresh. A partition seal merges ~60 MB at 4 B/entry (~245 MB at 16, ~368 MB at 24)
once a year. A resync writes only what it cannot find: parts are keyed by chunk range and
digest, and `--ignore-existing` is the upload rule. **Never overwrite a part** — R2 allows one
write a second per key, and an immutable key is what lets the cache keep it for a year.

### 6.2 What this settles

- **Entries must be self-contained; ordinals are out.** An ordinal into `index/tx/v1` is a
  position in a compacted, rebuildable array, so a `tx-index` compaction would invalidate every
  historical label part. Spans are chain facts and never move — §8.1 narrows to **8 B aux-only
  vs 14–16 B body + aux**.
- **Per-label files lose a third time.** §5.1 priced them against a directory; here they would
  have to be per `(label, part)` to stay append-only, which is more objects holding the ranges
  §6.1's runs already hold.
- **Query cost becomes expressible, still unmeasured**: a rare label is one range (its partition,
  plus a live segment); a hot label is one range per partition it appears in — ~10 today, one
  more per year — read concurrently. The per-part cost is measured — **685 ms cold, 11–14 ms
  warm, plus ~35 ms/MiB of depth** — but no part of *this* artifact has been written.

## 7. What this is not

- **Not a value store.** 12.78 GiB of metadata stays in the chunk mirror; the index says where,
  never what. A reader that wants the bytes pays for exactly the bytes it asks for.
- **Not search.** "Which transactions mention X" is a scan of the bytes themselves; this index
  narrows that scan to one label's transactions, and `ROW_ADDRESSING_AND_SEARCH.md` §5 is the
  search half.
- **Not domain data.** No marketplace, policy, asset or decoder names anything in the layout —
  that is `OFFER_INDEX_FORMAT.md` §9's rule, applied. Labels are in the ledger; how a domain
  reads a label's value is the domain's business and never the artifact's.
- **Not a replacement for `index/tx/v1`.** Alone it finds transactions by label, not by hash.
- **Not a per-label decision.** No label is special-cased, capped or omitted; 2,321 one-hit
  labels cost 4–16 B each plus their directory line, and are left alone.

## 8. Open

1. **Entry shape.** §6.2 rules ordinals out under append; what remains is **8 B aux-only** (no
   tx identity, one read) vs **14–16 B body + aux span** (a reader hashes the body for the tx id,
   and needs no `tx-index`) — 1.03 vs 2.06 GiB store-wide.
2. **What rides the build.** If entries ride `tx-index`'s extraction pass, the invalid-transaction
   rule (the census skipped 245; `tx-index` may include them) must be the pass's rule, stated.
3. **Partition size and seal cadence** — 1,024 chunks (~a year) is the priced default: 10
   partitions, a 100 KiB directory, a ~60–368 MB merge per seal. Smaller seals more often and
   shrinks the merge; larger grows the merge and every whole-store query's fan-out.
4. **Live-segment fan-out in the open partition** — the open partition's segments are extra
   ranges per label until the seal; merging inside a partition more often is a knob, not a rule.
5. **Query cost, unmeasured** — a rare label's one-range read and a hot label's per-partition
   fan-out, against §6.1's measured per-part floor. No part of this artifact has been written.
6. **Flip and retention** — the `DATUM_RESOLVER_FORMAT.md` §11 question, inherited.
7. **Should the census's per-value byte column ship?** No: it is measurement, not access. Its one
   job was to show where the 12.78 GiB sits, and it did.

## 9. Provenance

```sh
# on the ARM box, from /opt/mitos/src (built natively — musl anti-scales, see fold-probe README)
/opt/mitos/src/target/release/fold-probe labels \
    --immutable /srv/data/opt/market-ledger/snapshot-full/db/immutable \
    --threads 12
```

- Box: Netcup ARM, 12 cores, 23 GiB RAM; store 215 GiB over 9,218 complete chunks
  (`09218.chunk` excluded as still growing — `list_chunks`' rule, shared with every walker).
- Wall: **275–289 s** across three full runs; progress and report in
  `/opt/scratchpad/labels-full.log`. The windowing report takes `--partition` (default 1,024
  chunks).
- The command is tested and clippy-clean in the workspace (`cargo test -p fold-probe`:
  32 passed; `clippy --all-targets -- -D warnings`). The coverage and band arithmetic have unit
  tests (`a_label_coverage_count_is_a_cumulative_prefix`,
  `a_band_counts_labels_and_hits_at_or_above_its_floor`).
