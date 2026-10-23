# The delta chain — what a follower holds, and what it costs

Status: **measured** (2026-09-23). `tools/fold-probe follow` builds the delta
`LIVE_BOOK.md` §B4 specifies, over a published book's boundary, and prices it.
Every figure below is from the ARM box against mainnet chunks, and §4 is a
cross-check against an independently-written instrument on the same range.

It answers the question `MITHRIL_PHONEBOOK.md` left open and
`PHONEBOOK_COVERAGE.md` §11 names as the one that decides the next version of
mitos: **can a module ask about a transaction that just landed, and get an answer
about outputs that are no longer in the live set?**

Owner: TBD
Related:
- `docs/design/LIVE_BOOK.md` §B4 — the spec this measures, and §B7 — one walk,
  many artifacts. §B4's `~1 MB per chunk` is corrected in §5 below
- `docs/design/PHONEBOOK_COVERAGE.md` §11 — the gap this closes, and the three
  that remain
- `docs/design/MITHRIL_PHONEBOOK.md` §3–§4 — the fold that is inherent, and the
  structures that are not
- `docs/design/TX_INDEX_DATASOURCE.md` — the sibling artifact, and §12 below is
  the answer to "can tx-index be rolled into the walk"
- `docs/design/FOOTPRINT_AND_MINIMUM_SPEC.md` §4.1 (anonymous memory is what a
  cgroup kills you on) and §8 (the walker constraints)
- `docs/design/DECODE_SHAPE.md` — the walk the delta rides, and what it costs
  without a sink
- `tools/fold-probe/src/delta.rs`, `tools/fold-probe/src/main.rs` — `follow`

## 1. The question, stated exactly

A published book is the live set at **one** chunk boundary. A module answering a
question about a transaction that landed *after* that boundary is asked about
outputs that are spent — and the outputs it has to *resolve* were, at the moment
they were created, live.

`resolve-inputs` classified every input of chunks 1000..1006 against a
from-genesis base and found four buckets, of which one is the whole design:

| bucket | where the answer is |
|---|---|
| **hit** | the book's base — one lookup, no I/O |
| **same-chunk** | the block being processed, already in hand |
| **gap** | **created after the boundary and before this chunk** |
| **never-created** | nowhere: Byron's genesis distribution, redeem-style inputs |

Measured: **43.48% / 40.70% / 15.82% / 0**, and at lag 101 the gap becomes 57.38%
of all inputs. So the gap is not a corner: at a 100-chunk lag **more than half** of
the resolution work is outputs the base cannot see.

`never-created` is zero on a from-genesis base, and that is a property of the chain
rather than of the instrument: **the log does answer every input that was ever
created.** The only structure missing was one holding what the log created after
the boundary the book was taken at.

## 2. What the delta is

One structure with three parts, and each part exists because a different question
needs it:

| holds | why |
|---|---|
| the **rows** created since the boundary, in `row`'s encoding | an input is answered with the output it spent, not with a yes |
| a **spent bit per creation** | a follower's live set is `base ∪ delta − spent`; a module enumerating an address must not be handed a spent row |
| a **spent bit per base row** | the book is a snapshot, so it does not know what the followed chunks spent. One bit per row: **1.36 MB** over mainnet's 10,911,674 |

The last one is why the reader needed a new accessor. A base row is addressed in
`rows.bin` by a **byte offset** into a variable-length arena, and two rows at
neighbouring offsets are not neighbours in any index sense — a bitmap needs a
*dense* id, which is the row's entry in `by_outref`
(`live-book`'s `Base::outref_entry`).

**The base and the delta are disjoint by construction** — the delta holds only
creations, and an output is created once — so the order the two are consulted in
cannot change an answer. The delta is consulted first anyway, because it is a hash
probe over a working set of tens of thousands of entries against the base's
directory page plus a binary search over 10.9 M.

**Spending does not remove a row.** The consumer being served is analysing a
transaction that just landed, and one of its inputs may have been spent by that very
transaction. Spending sets a bit; it does not delete.

### How it runs, and what makes "not found" mean something

Three lifetimes, and the difference between them is the design:

| | what it is | how it is made | how it is lost |
|---|---|---|---|
| the **log** | the immutable chunk store | the Mithril refresh | pruning — a `lean` profile holds fewer chunks |
| the **base** | a compaction of the log at a boundary | the fold (49.2 min for the whole of mainnet, `LIVE_BOOK.md`'s status line), or shipped as an artifact (`INSTALLABLE_MITOS` I5) | replaced by `rename`, never mutated |
| the **delta** | the log's tail since that boundary | whatever process is following the tip | **rebuilt by re-walking the chunks it missed** |

**The delta is a cache of a window of the log, not a source of truth, and it does not
need to be durable.** That is what makes its failure modes cheap: a follower down for
six hours comes back, walks the chunks it missed, and holds the same delta it would
have held had it never stopped. Only the *log's* retention bounds how far back that
works — the same bound the base is built under, which is why a `lean` profile and a
long outage are the same constraint wearing different clothes.

The property this buys is the one the plane actually needs. With a complete delta over
a boundary it knows, **"not found" means "never created"** — and not "spent", not
"created after my snapshot", not "not in a chunk I happened to walk". Those four
collapse into one `None` the moment either of two things is missing:

- **`base_chunk` in `book.meta`** (§7). Without it the plane cannot state which
  window the delta covers, so it cannot interpret its own answer.
- **The delta's coverage as a contiguous range**, recorded the way
  `POLICY_WALK_SCHEDULER.md` §1 records a walk's — as ranges, not a cursor. A cursor
  cannot express a hole, and a delta with a gap silently turns "never created" into
  "not found": the one wrong answer that looks exactly like a right one.

That asymmetry is also why the base is not a cache of the delta, whatever the
lifetimes suggest. A chain of deltas with a missing link cannot answer confidently;
a base plus a known-contiguous tail can, and can say so.

## 3. The measurement

Two runs, because they answer different questions. Same box, same binary, release
build, `CARGO_TARGET_DIR=/srv/data/cargo-target`; chunks from
`/opt/market-ledger/snapshot-full/db/immutable`.

**Run A — semantics, from genesis.** `base1000` (a whole-chain fold to the chunk-999
boundary, 1,147,326 live rows) plus chunks 1000..1006.

**Run B — scale, at the tip.** A *window* base over chunks 9155..9160 (22,654 live
rows — the residue of six tip chunks, not mainnet's live set) plus chunks
9161..9181. A window base makes `uncreated` meaningless, which is why
`--window-base` records it in the run context and the report says so; the delta's
own size and cost do not depend on the base's completeness, and that is what this
run is for.

| | Run A | Run B |
|---|---|---|
| base | from genesis, chunk 999 | window 9155..9160 |
| base rows | 1,147,326 | 22,654 |
| chunks followed | 1000..1006 (7) | 9161..9181 (21) |
| inputs | 88,745 | 302,658 |
| **base / delta / uncreated** | **43.48% / 56.52% / 0** | 2.96% / 71.68% / 76,774 ⚠ window |
| blocks · txs | 7,588 · 47,952 | 22,195 · 117,628 |
| creations | 106,009 | 288,342 |
| — still live after the run | 55,846 (53%) | 71,407 (25%) |
| base rows spent | 38,582 | 8,949 |
| delta bytes / creation | **163** | **248** |
| delta total | 17 MiB | 68 MiB |
| wall | 0.3 s (115 MiB/s) | 1.6–1.9 s (82–94 MiB/s) |

Per-operation, repeated runs of the shipped binary on the same data, so the spread
is the machine rather than the code:

| | Run A | Run B | note |
|---|---|---|---|
| delta probe, per input | 170–284 ns | **200–296 ns** | against `book_utxo`'s **496 ns** |
| delta build, per creation | 326–403 ns | **646–1016 ns** | intern, encode, append, probe |
| a full scan of the delta, per row | 157–266 ns | 287–316 ns | the floor for an enumeration with no ordering |
| clock granularity | **40 ns** | | `arch_sys_counter`; every figure above carries at most that much quantisation |

The classification is bit-identical across runs (43.48 / 56.52 / 0, every time),
which is the number the rest of the system depends on. The per-operation figures
are not stable to better than ~20% on this box, which is why they are quoted as
ranges and why §5 uses an A/B rather than a per-operation figure for the cost that
matters.

The build figures are the *instrumented* ones: each timed observation takes two
`Instant::now()` calls, and the box's clock reports in 40 ns steps. Two traps are
worth recording because both produced a wrong number before a right one:
written plainly, `Instant::now().elapsed()` **constant-folds to zero** — it has no
modelled side effects — and read through `black_box` the *minimum* is still zero,
because two reads inside one 40 ns tick are equal. Only the smallest non-zero step
is real.

## 4. The cross-check

Run A's classification was produced by a second, independently-written instrument
on the same range against the same base: `resolve-inputs` keeps the gap in a
`HashSet<[u8; 36]>` built by a separate pass over the same chunks, and classifies
against it. `follow` keeps the gap in the delta, built as the walk goes.

| bucket | `resolve-inputs` | `follow` |
|---|---|---|
| the base | 43.48% | 43.48% |
| created in the same chunk | 40.70% | — |
| created after the boundary, before this chunk | 15.82% | — |
| *those two together* | **56.52%** | **56.52%** |
| never created | 0 | 0 |

They agree exactly, and they should: in `follow` an output created inside the chunk
being processed is in the delta by the time a later block spends it, so
`same-chunk + gap` **is** the delta. Two implementations, two data structures, one
partition.

The delta also carries two invariants the report asserts rather than hopes for —
it must have seen every spend the planner counted, and hold every output it counted
— and a read-back sample resolves rows through the same accessor a module uses and
resolves every asset id through the base's `policy.tbl` plus the delta's own tail.
Both runs: **0 refs that did not resolve, 0 asset ids unreadable.**

## 5. The economics

### What it costs the walk

A per-creation figure says little on its own, so `follow --bare` runs the same
chunks through the same walk with **no sink** — the control, the way `fold`'s
`--primitives` and `--chain-decode` are a control for each other. Three runs each,
same box, same binary, same bytes:

| | no sink | with the delta |
|---|---|---|
| `block_apply` | 6.43–7.80 µs | 23.30–26.44 µs |
| `block_decode` | 37.46–42.03 µs | 37.43–40.29 µs |
| wall, 21 chunks | **1.3 s** | **1.6–1.7 s** |
| inputs classified | 0 | 302,658 |

So the sink is **+18.5 ms a chunk, +31% on the walk**, and every millisecond of it
lands in the apply phase — decode is untouched, because the delta rides a walk that
was already happening. Per chunk that is 13,731 creations at ~1.3 µs each of *net*
cost, against ~1.1 µs of *instrumented* cost; the ~250 ns between them is the part
of the sink its own timers do not cover — the enum dispatch, and `spend`'s base
lookup and tier bookkeeping behind the probe it does time.

**+31% is the number `LIVE_BOOK.md` §B7 asks for.** That section's open tension is
how much depth one pass should pay for on behalf of every consumer, and the honest
comparison is not policy-index's +1.8%: that rides a decode that already computes
hashes and spans, while a row wants the output's address, value and datum decoded
and a row written. The depth is what costs, and it is what buys every artifact of
the family.

### What it costs to hold

At the tip, per chunk — Run B divided by 21:

| | |
|---|---|
| creations | 13,731 |
| inputs | 14,412 |
| **delta growth** | **3.3 MiB** (measured as the slope; §B4's `~1 MB` estimate is ~3× low, and the row is what dominates — 182 B of the 248) |
| chunk bytes in | 6.2 MB |

A follower that is *not* re-walking — one handed the delta as an artifact — pays
3.3 MiB of I/O a chunk against a base rewrite's ~1.8 GB. That is §B4's trade, and
the reason a database looks attractive is that nothing in the delta is checkpointed.

**The memory is the headline.** A follower's working set over a published book is:

```
base bitmap     10,911,674 bits                    =    1.36 MB
delta           ~3.3 MiB per chunk of lag
```

At the snapshot cadence (one immutable chunk file per ~6 h) a follower that rebases
on each published snapshot holds **one chunk of delta**: ~4.7 MB of anonymous
memory, total, to answer about the tip. At the lag Run B measured — 21 chunks, five
days — it is 68 MiB. The rows are mmap-able and the delta does not have to be
anonymous memory at all: written out, it is the `delta/NNNNN.dlt` of §B4.

This is the number `FOOTPRINT_AND_MINIMUM_SPEC.md` §4.1 was asking for. It is four
orders of magnitude from the 145 GB the phonebook note started from, and it fits an
8 GB box with the base entirely in page cache.

### One optimisation tried

Per-row cost looked like it should be dominated by allocation — the first version
allocated a 64-byte `Vec` for the asset records of every created output, 288,342 of
them here — so it was moved into a reused scratch buffer on the delta. Measured,
before against after, on the shipped binary:

| | before | after |
|---|---|---|
| build, per creation (Run A / Run B) | 399–511 / 679–955 ns | 326–403 / 646–1016 ns |
| full scan, per row (Run A) | 170–263 ns | 157–266 ns |

Both moved in the right direction, and the ranges still overlap — so the honest
reading is *probably a small win, certainly not a regression*. The residual cost is
not the allocator: it is hashing 36 bytes an outref, the probe and its occasional
rehash, the policy intern per asset record, and the row encode. The scratch stayed
because it removes allocator traffic for free — which is what `FOOTPRINT` §4.1 cares
about, since user-space caches are what a cgroup kills — and not because it bought
much.

## 6. What the delta is *not* yet

**It has no orderings.** Point resolution is built; enumeration is not. A module
asking `utxos_by_address` at the tip needs `base ∪ delta − spent`, and the delta as
built can only answer that by reading every row: **97.5 ms for a 288,342-row
delta** (Run B, full scan). That is not a request path.

The fix is the shape the book already uses — one row store with a permutation per
ordering, 12 B an entry — so the delta carries `by_outref` (built), `by_addr` and
`by_policy`, each a permutation of its own rows, and a query unions two runs and
masks by two bitmaps. Cost, derived from the book's own measured 12 B and **not yet
measured here**: +12 B/entry per ordering, ~0.5 MiB per chunk for the two that are
missing. It is the next thing to build, and it is the last thing between this and a
`ChainDataPlane` that answers at the tip.

**It is not published.** `follow` holds it in memory. §B4's `delta/NNNNN.dlt` — one
file per chunk, written by the same walk, swapped by `rename` — is the format work
this measurement says is worth doing.

**The merge is not built.** Rebasing a delta into a new base is the builder it
already has, over a smaller input, and `reindex` measured the re-derivation of an
ordering from a row store at 4.8 s for the whole of mainnet (§B1). A rebase is a
different job (it must fold the delta's spends into the base's live set), and it is
the one piece with no measurement behind it.

## 7. What this does to the phonebook

`PHONEBOOK_COVERAGE.md` §11 lists three things needed to close the loop — a reader,
two comparable implementations, and one module run both ways. The first two are
built, and this document does not change them. What it does is answer the *first*
question that execution test was going to raise — **can a module be told about an
output the current set no longer holds** — and reorder the per-method gaps
`BookDataPlane` reports.

`crates/mitos-data-plane/src/impls/book.rs` answers **six** trait methods from the
artifact — `read_utxo` / `read_utxos`, `utxos_by_address`, `utxos_by_policy`,
`utxos_by_payment_cred`, `resolve_stake_for_payment_pkh`, `holder_count` — and
refuses the rest by name. This document moves one of them and reorders the others:

- **`read_utxo` is the one that is wrong rather than incomplete.** At the tip, an
  outref created after the boundary and an outref that never existed are the *same
  answer*: `None`. That is the failure a module analysing a landed transaction
  cannot see — it has no way to tell "spent", "never existed" and "created since my
  snapshot" apart, and every downstream judgement follows from it. The delta makes
  the second of those distinguishable, and it is the whole of what §1 asked for.
- **The four enumerations are incomplete rather than wrong**, and they are the
  delta's own remaining gap: `utxos_by_address`, `utxos_by_policy`, `holder_count`
  and the base half of `read_utxos` all read *runs* of a permutation, and the delta
  has no permutation yet (§6). Until it does, a query at the tip misses post-boundary
  outputs and can hand back a spent base row.
- **`tip`** is closed (2026-09-23), and the delta was never going to close it. A book
  is a snapshot *at* a chunk boundary, a delta is defined *relative to* the boundary
  it hangs from, so a delta cannot supply what the artifact failed to record. The
  field was the fix: `book.meta` carries an optional `base_slot`, a build writes it
  from the walk, `fold-probe stamp-boundary` fills it in for an artifact built before
  it existed, and `tip()` answers `ChainPoint::Slot(slot)`. Absent means unrecorded
  and still refuses — never `ChainTip::origin()`, because `Origin` means genesis.

  How it was found is the part worth keeping: an execution test of the shipped
  `collection-holders` module against a book **trapped on its first page scan**
  because the host freezes a scan as of `tip().slot()`, and after the field existed
  the same test passed with the module's ledger matching the book exactly. Reading
  found the gap; running proved it and then proved the fix
  (`PHONEBOOK_COVERAGE.md` §12 has both logs).

- **`search_utxos` is the one that was hiding behind `tip`.** Its refusal used to
  cite the missing chain position; with `base_slot` in place the real gap is visible:
  the book has no predicate algebra and no `Page` cursor. The host's watched-UTxO
  seeding calls it on every dynamic-interest Add, so it is a live path rather than a
  hypothetical one — it logs and carries on today.

Untouched by any of this, and for their own reasons: `read_datum` (needs `datum.tbl`,
whoever's job that is), `read_script` (the row has no `script_ref` column),
`tx_metadata` (history, tx-index's), `total_supply` and `protocol_params` (not UTxO
questions at all).

## 8. One walk, many artifacts

`LIVE_BOOK.md` §B7 asks for the book to be a consumer of a shared pass rather than a
walk of its own, and names the **created-output log** as the artifact that makes it
concrete. This is that log's tip-side segment, and the shape of the seam is now
worked rather than asserted:

```rust
/// Where one walk's spends and creations go.
enum Sink<'a, 'b> {
    Book(&'a mut Book),      // the published live set, to disk
    Delta(&'a mut Delta<'b>), // the same rows, in memory, since a boundary
}
```

One walk, one destination per run — and the destination is a parameter, not a
second walk. The proof that this is enough is that **`follow` is the first
consumer**: it feeds the primitives walk's existing sink and gets a complete,
self-consistent second artifact out of a decode that was already happening. The
walk did not grow a stage; it grew a second place to put what it already produced.

The same seam is where the other artifacts of §B7 hang — tx-index's segments,
policy-index's, the spend log, the aux tables — because they are all projections of
the same decode and the sink is the point at which the decode is in hand. What the
seam still has to decide is §B7's open tension: tx-index's extractor deliberately
never builds a `MultiEraTx`, and the book needs outputs decoded. `resolve-inputs`
and `follow` both run the *primitives* walk (the deeper one), so a walk with both
sinks is priced — and pricing it against a tx-index-only pass is the +1.8% question
§B7 says belongs in the extractor's docs.

## 9. tx-index: invert, don't join

`TX_INDEX_DATASOURCE.md` and `WATCHED_UTXO_INDEX.md` both reach for a local index to
answer the prior-output question. The right local index is not tx-index, and the
reason is a **shape mismatch** — a different key, and a different lifetime — rather
than a faster implementation of the same thing.

### First, which question

"Resolve an input" is two questions that look like one, and they have different
answers:

| the question | where the answer is | what cannot answer it |
|---|---|---|
| a transaction **just landed** — what were its inputs? | book + delta | — it is answered, 100%, §4 |
| a re-walk of a **2022** transaction — what were its inputs? | the full created-output log, or a back-pointer into the log | **book + delta**: those outputs are long spent, so they are not in a snapshot of the live set, and no delta reaches back past its boundary |
| where is tx X, and what was its metadata? | tx-index | book + delta: not keyed by transaction, and not history |

This document closes the first row. It does not touch the second, which is what
`TX_INDEX_DATASOURCE.md` R7.2 calls the archive-horizon case and is **the reason
that document's R2 is still worth doing**. The two are siblings with different
keys, not substitutes.

### What a tx-index lookup actually costs

`tx-index` is `tx hash prefix → Location { chunk, offset, len }`, and
`Index::resolve(hash, index)` (`crates/tx-index/src/reader.rs`) does five things per
call:

| | per `resolve` | per delta probe |
|---|---|---|
| find the entry | mmap'd probe on an **8-byte prefix** | in-memory probe on the **full 36-byte outref** |
| read the bytes | `File::open` + `read_exact_at` of the tx body's span | — the row is already held |
| confirm the hit | **blake2b-256 over the whole body** — a prefix can collide | a 36-byte compare against the row |
| decode | **every output of that transaction**, then `swap_remove(index)` | — the row is already decoded |
| allocate | a `Vec<u8>` body and a `Vec<ResolvedOutput>` | nothing |

Two of those are properties of the *key*. An 8-byte prefix has to be confirmed by
re-hashing the bytes it points at, and a key that names a *transaction* has to be
decoded to answer about a single *output* — a transaction with 20 outputs costs 20
decodes to answer about 1.

The third is a property of *when* it runs, and it is the whole argument. The outputs
a tip-follower cannot resolve are exactly the ones created since the book's boundary
— and the walk that produced the book had each one **in hand, decoded**, before it
was needed. A back-pointer throws that away and re-derives it, per query, from disk.

⚠️ **The right-hand column is measured (~240 ns, §3). The left is not.** Building a
like-for-like tx-index benchmark needs a 123.6 M-entry index, and `/opt/tx-index` on
the ARM box is a mountpoint with nothing in it — the index `TX_INDEX_DATASOURCE.md`
measured on 2026-09-13 is not there now. Deriving from the parts that *are* measured
(`DECODE_SHAPE.md`'s 160–260 MB/s decode, and a `File::open` a call): a pread of a
tx span, a blake2b over the body, and a decode of every output lands somewhere in
the **tens of microseconds**, so the ratio is large — but *large by derivation* is
not *measured*, and the number this section is missing is the one a reader would
want first. It is a small benchmark to write, against a tx-index built over the same
21 chunks: the per-call cost does not depend on the index's size.

### Why it is not the same index with a better hash function

What makes the delta small is **not** the structure — it is that it only keeps the
part of the created-output log that has not been compacted yet. The book *is* that
compaction: every output created before its boundary and still live is a row in it,
so the only creations a reader can still need, and cannot find there, are the ones
since. The full log is 355,490,215 outputs ever created (§B4's bitmap is its size)
and at these row widths it would be **60–90 GB**; the tail is **3.3 MiB**.

That is the same observation `LIVE_BOOK.md` §B7 already records from another
workstream: `cnft.dev-workers`' project-ledger importer measured that indexing
in-range outputs as you walk turns **94% of the resolution work into a lookup**
(`--index-all-outputs`). One walk, both artifacts, no join — and the index that
makes resolution a lookup is keyed on what a spend names, not on where a
transaction sits.

So **tx-index keeps its job** — aux data, transaction locations, and every question
that is about history — and the tip half of the prior-output path is closed by the
walk emitting a creation log. What is measured on this side of the ledger is
**~240 ns an input and 3.3 MiB a chunk**; what is not yet measured is the other
side, and until it is, "invert, don't join" is an argument from structure with one
measured end.

## 10. Reproducing

```sh
# the base: a window over the six chunks before the range
fold-probe fold --immutable <store> --start 9155 --chunks 6 --primitives \
    --book /srv/data/fold-book/base9160

# and the follower over it
fold-probe follow --book /srv/data/fold-book/base9160 --base-chunk 9160 \
    --immutable <store> --from 9161 --chunks 21 --window-base \
    --sample 1000000 --jsonl follow-base9160.jsonl
```

Run A is the same command with `--book base1000 --base-chunk 999 --from 1000
--chunks 7` and no `--window-base`. Run B needs the window base; Run A's `base1000`
was folded from genesis, which is what makes its `uncreated` zero. Add `--bare` for
the control, and `--window-base` only when the base really is a window.

`--sample N` reads N delta rows back through a consumer's accessor; larger than the
delta means every row, and that is also how the full-scan cost above was measured.
`--jsonl` writes the meter's records for offline comparison. `--bare` drops the sink
entirely: the same chunks and the same walk, with nothing written, which is the
control in §5.

## 11. Open

- **The two missing orderings**, and whether a delta's permutation is built
  incrementally (maintaining sorted runs) or derived at publish time the way
  `reindex` derives the book's. The second is proved and cheap; the first is the
  one that serves a live follower.
- **Rebase policy.** A delta's spends have to fold into the base's live set. Is
  that the same builder over a smaller input, or a merge of two artifacts? The
  former re-derives everything; the latter needs a delete-capable row store.
- **What happens to a delta the base has moved past.** A reader holding a snapshot
  pointer is at most one block stale (§B4); a reader holding one across a rebase is
  reading a merge that no longer exists.
- **Whether the delta should be *the* artifact and the base a checkpoint of it** —
  i.e. the change feed of §B9 as the primary representation. That inverts §B4's
  framing, and the delta's measured 3.3 MiB a chunk says it is affordable; what it
  costs is that every reader then folds a chain.
- **`datum.tbl` and the aux index**, which are `PHONEBOOK_COVERAGE` §10 items 3 and
  4 and are captures rather than views — they belong to the same walk and the same
  sink, and nothing here changes their cost.
