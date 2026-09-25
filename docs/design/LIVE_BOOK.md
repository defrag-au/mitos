# The live book — an on-disk format for the live UTxO set

Status: **phases 1, 3a and the read side of 4 shipped** (2026-09-23).
`tools/fold-probe` builds the artifact and reads it back; a whole-store build passes
end to end (49.2 min, 9,181 chunks) with all three artifact identities agreeing,
`by_addr` — the last of `B1`'s three orderings — was added to that same artifact by
`reindex` in **4.8 s**, all three orderings then verified entry by entry, and
**`crates/live-book` now serves it**: mapped, confirmed, paged, and checked against
the artifact by query rather than by entry (§B9).
`layout_version` is **3** — the Sizes section records the things real data
corrected, and every figure in it is now measured rather than derived. Anything
still labelled ⚠️ remains the most likely thing to move.

Owner: TBD
Related:
- `crates/live-book/` — the format and its reader, extracted from the probe the day
the second consumer appeared
- `docs/design/PHONEBOOK_COVERAGE.md` — the module surface mapped method by
  method onto this artifact: what it serves, and the gaps that remain
- `docs/design/MITHRIL_PHONEBOOK.md` §5b — the figures this format has to
  satisfy, and the argument for the artifact existing at all
- `docs/design/TX_INDEX_DATASOURCE.md` — the sibling artifact (name → location)
  and the requirements style this doc follows
- `docs/design/FOOTPRINT_AND_MINIMUM_SPEC.md` §7 (what each profile holds
  locally) and §8 (the constraints any walker must meet)
- `docs/design/WATCHED_UTXO_INDEX.md` — the live interest set and the measured
  reason the fallback provider is reached at all
- `docs/design/INSTALLABLE_MITOS.md` §4 / I5 — the published-artifact path this
  makes tractable
- `cnft.dev-workers/docs/design/PROJECT_LEDGER_IMPORTER.md` — the measured
  `--index-all-outputs` finding the join builder generalises
- `cnft.dev-workers/docs/design/POLICY_WALK_SCHEDULER.md` — coverage as ranges;
  the model for the delta chain

## Summary

The book is the **live UTxO set** — **10,911,674 outputs** as of 2026-09-22,
measured by a whole-chain fold — materialised as an immutable, mmap-able
artifact, with a small mutable delta at the tip.

It is the third artifact of the family `TX_INDEX_DATASOURCE.md` started:

| artifact | answers | key | cardinality |
|---|---|---|---|
| tx-index | where is this transaction | tx hash prefix | 123.6 M, append-only |
| policy-index | which mint created this asset | (policy, name) prefix | 19.0 M, append-only |
| **the book** | **is this output live, and what is it** | outref prefix | **10.9 M, and it shrinks** |

The difference that shapes everything: the first two are **logs** (a row is
added and never changes), and this one is a **set** (a row can die). So it needs
deletion and, at the tip, mutation — which is the entire reason a database looks
tempting here. §"Decided" records why it isn't one.

## Where we are today (measured)

From `MITHRIL_PHONEBOOK.md` §5b, all on the ARM box 2026-09-22:

| | value |
|---|---|
| live rows | **10,911,674** (fold). `created − spent` says 10,897,576 — low by 14,098, see below |
| ever created / spent | 355,490,215 / 344,592,639 |
| density (live ÷ created) | **3.1%** |
| whole-store pass | 5.3 min at 8 threads · 214.86 GiB at 725 MB/s |
| whole-chain fold | **1.26 h wall**, one thread · decode 120 MB/s · peak anon 2.17 GiB |
| decode, one core | 84–120 MB/s (window 84, whole chain 120) |
| spends naming another chunk's output | **48.1% chain-wide** · 40.6% (Conway) · 44% (Byron) |
| a fold's `hit%` once it has history | 100.0% — 344,578,541 of 344,592,639 |

⚠️ **`created − spent` is not a live-set count, and the error is not noise.**
14,098 spends name an output that **no transaction in the log ever created** —
Byron's genesis distribution and redeem-style inputs. They are invisible to
arithmetic and visible only to a fold, which is why the fold's figure is the
one to quote. `tools/fold-probe` reports the count as `misresolved` and asserts
`entries = created − spent + misresolved`, which held to the unit over the
whole chain.

And the payload shape, from a 10-chunk recent window (148,511 outputs created,
39,072 left live) — **the live set is young, so a recent window is the right
sample here even though it is the wrong sample for a chain-wide rate**:

| | value |
|---|---|
| assets per row | 1.795 (70,142 over 39,072) |
| payload per row — policy + name + datum bytes | **88 B** |
| of which the policy component alone | 1.795 × 28 B = **50 B** |
| rows carrying assets / a datum | 42% / 24% |

The 50 B is the leverage: a policy id is 28 bytes and there are only ~231,515
distinct ones chain-wide (`crates/policy-index/README.md`). Interning it to a
`u32` nets **27 B/row** — because the row then owes 1.795 × (4 + 1 + 8) = 23 B
for the interned id, a length byte and the quantity the probe did not count.
Stated as arithmetic rather than as a claim, since that is the whole saving.

**Why a recent window is the right sample for the book** (and the wrong one for
a chain-wide rate): the layer of the live set created in the last 2.5 days is
39,072 rows. Uniform over 2,290 days that would be 10,911,674 × (2.5 / 2290) ≈
**11,900**. The live set is therefore ~3.3× over-weighted toward the newest
chain — spends remove the old, so what survives is young. Derived from two
measured numbers, and it is the reason the payload shape above is
representative of the book even though it is not representative of the chain.

**Finished 2026-09-22** (`fold --chunks 0`, whole chain, one thread): **1.26 h
wall**, peak anon **2.17 GiB** at a peak set of **11,271,535 rows** — 193 B/row,
inside the range the window predicted — and **11,271,535 peaked → 10,911,674
final**. Three things follow.

First, a cross-check: the fold's `created`/`spent` totals are **identical to the
count pass's** (355,490,215 / 344,592,639), so two independent builders agree on
the arithmetic and differ only by the 14,098 the count cannot see.

Second, and more useful for sizing: **the book is bounded.** Cumulative net
creation was 7.89 M at chunk 3,000, **11.13 M at chunk 6,040**, and 10.91 M at
the tip. A book is a bounded artifact in a way a log never is — 10.9 M live
rows against 355 M ever created is 3.1% density, and that ratio, not the
*shape* of the trend, is what makes delta-plus-compaction necessary.

⚠️ The peak-then-decline is an **observation about Cardano usage, not a
property to design against.** It may or may not hold again; nothing in this
format should depend on the sign of the trend, and anything that does would be
sizing a data structure on last year's activity.

## B1 — One row store, several orderings over it

`crates/policy-index/README.md` §"Two orderings, one copy of the records" is the
pattern, and it generalises: **the array holds the records, and every other view
is a narrow permutation over the same rows.** "The two views cannot disagree
about what a record says, because there is only one record."

The book needs three orderings:

| ordering | answers | entry |
|---|---|---|
| `by_outref` | is this output live, and what is it | `{u64 prefix, u32 row}` |
| `by_addr` | what does this credential hold | `{u64 prefix, u32 row}` |
| creation order | publishing, bitmap/delta bookkeeping, "recently created" | the rows themselves |

Creation order is the row array's own order. The other two are permutations of
`u32` row ids — 12 B each rather than the row. **All three exist as files**: the
first two, plus the `by_policy` that `B1` did not know it needed, are written by
one function (`write_derived_orderings`) that both the builder and the reindex
path call, so a view added by a repair is the same bytes as a view written by a
walk.

## B2 — The row

`rows.bin` is a byte-addressed arena of variable-length rows. Rows are addressed
by `u32` offset, so the arena is capped at 4 GiB (10.9 M rows × ~150 B = 1.6 GB —
comfortable, but the cap is a `layout_version`-visible property, not an
accident).

**Vocabulary: the key is an output reference.** `OutRef` is `live-book`'s alias
`([u8; 32], u32)` — a transaction hash and an output index — and the same pair
is spelled differently at each boundary: `OutputRef` on the data-plane, protocol
and WIT (`output-ref`) surfaces, `TxoRef` at the dolos boundary and in pallas,
`TransactionInput` when it is the input side of a transaction. Nothing about the
pair changes when the output is spent; only the name does — which is why the
orderings are named for the dimension they are keyed by (`by_outref`,
`by_policy`, `by_addr`), and why the unqualified `dir.idx` is `by_outref`'s
directory while the other two qualify themselves.

```
offset  size  field
     0    36  outref        — tx hash (32) ‖ output index (u32 LE)
    36     8  lovelace
    44     4  datum_ref     — reserved; unused in phase 1
    48     1  datum_kind    — 0 none · 1 inline · 2 hash; an unknown kind reads as none
    49     2  addr_len      — u16 since `layout_version` 3; 29..57 B typical, 22,600 B longest
    51     2  n_assets
    53     1  era
    54     2  datum_len     — the datum bytes below; 0 when the kind is none
  ----   56  fixed header
    56  addr_len   address bytes, raw
       ...         n_assets × { u32 policy_id, u8 name_len, name bytes, u64 quantity }
       ...         datum_len  datum bytes, raw — the inline datum, or a hash's 32 bytes
```

**The outref is 36 bytes, not a prefix.** This is the one place the family's
narrow-key trick must not be applied, and the measurement says why: a prefix
needs a confirm read, and §5b measures a confirm read at ~2 ms (tx-index's 1,044
lookups in a two-second detour) against ~µs for a row already in memory. A fold
performs ~138 M spend resolutions over the chain; the difference between those
two numbers is 69 minutes versus 77 hours. **Spend resolution must confirm from
memory, so the row carries the full key.** The narrow prefix lives in the index,
where a hit is a candidate that the row then confirms — the same soundness shape
as tx-index's "8 bytes of the hash, re-hash the located body", one level in.

**Policies are interned; addresses are not.** A policy is a 28-byte value with
231,515 distinct instances, so the table costs ~7 MB and nets ~27 B on every
asset-bearing row (§"Where we are today" shows the arithmetic). An address is
29–57 bytes and its reuse within the live set is **unmeasured** — a dictionary
only pays when reuse is high, so the safe assumption is that it does not.

⚠️ **The address is the largest unmeasured component of the row.** The probe
decodes no addresses at all. It is also worth noting what a row does *not* need:
`WATCHED_UTXO_INDEX.md` establishes that interest matching is symmetric and
credential-based, so the module path wants the credential — which is inside the
raw bytes — and never the bech32 string.

## B3 — The indexes

Both indexes share one shape, which is tx-index's with the directory resized to
the corpus:

```
entry    { u64 prefix, u32 row }        12 bytes   — NOT padded to 16
dir      2^21 buckets × u32 → entry     ~8 MB
```

- **Narrow entries, not a cast.** `(u64, u32)` has alignment 8 and size 12, so
  `&[u8] -> &[Entry]` requires padding to 16 and wastes 25% (44 MB per
  index at this cardinality). Read the two halves with `from_le_bytes` over the
  mapped slice, as `crates/tx-index` and `crates/policy-index` already do. A
  cast is worth revisiting only if a profiled scan says the manual reads cost
  something.
- **`DIR_BITS = 21`, and the width follows the corpus.** At 10.9 M entries, 2^21
  gives ~5 entries per bucket and an 8 MB directory. `policy-index`'s own note
  is the authority: at 19 M records, 2^24 would hold one entry per bucket and a
  64 MB directory "would outweigh a sixth of the data it indexes". The same
  arithmetic here gives 21; it must be re-derived, not inherited, if the
  cardinality moves by an order of magnitude.
- **Neither prefix needs hashing.** Session-critical detail from
  `policy-index`: it hashes `name_prefix` because asset names are human-chosen
  and differ in their *last* bytes, and leaves `policy_prefix` alone because a
  policy id is already blake2b-224 and buckets evenly. A payment/stake
  credential is also already a hash, so `by_addr` uses the raw 28-byte prefix.
- **The builder's key is the outref, and the output index is part of it.** The
  build index is keyed on 96 bits — the hash's first 8 bytes, then 4 more mixed
  with the output index — and that mix is not decoration. An outref is
  `(hash, index)` and **a transaction's outputs all share its hash**, so a key
  taken from the hash alone merges output 0 with output 1 of every transaction.
  The first build of this index did exactly that, and `finish` caught it as two
  live rows on one key. `layout_version` is therefore **2**: a version-1 artifact
  has each transaction's outputs merged and must not be read.
- **Every hit is a candidate.** A prefix collision costs one extra row read and
  never a wrong answer, because the row's full outref (or full address) is right
  there. This is the property that lets the index stay 12 B wide — and it is not
  a rare path. Outputs of one transaction share a hash, so they share the
  artifact's 64-bit prefix too, and the measured cost is **1.5–1.6 row confirms
  per lookup** (Conway; 70 k and 111 k live rows, two windows). That is the price
  of the 12-byte entry, paid in reads of rows the caller wants anyway.

## B3a — `by_addr`'s key, and what mainnet turned out to hold

Built 2026-09-23. `by_addr` keys a row on the **credential its address carries**,
derived by `tools/fold-probe/src/address.rs` — which is also the function a
consumer must call, so the index and its readers cannot disagree about what a
credential is. (The file is named for its input: a path containing `credential`
is refused by the `at-peek` inspector, which reads it as a secret.)

| header nibble | shape | the credential | cost |
|---|---|---|---|
| 0–7 | base, pointer, enterprise | `address[1..29]`, a slice | free |
| 14–15 | reward | `address[1..29]`, a slice | free |
| 8 | Byron | `payload.root` — **a parse** | a CBOR decode |

**Ten shapes are a slice; one is a parse.** The dispatch deliberately mirrors
`pallas_addresses::bytes_to_address`, *including its bound* (pallas accepts a
payload of 28 bytes or more for every Shelley shape and never checks the upper
end), because a derivation stricter than the decoder that serves it would drop
rows the decoder would happily return.

One thing was known before this was written and one was not. **Known:** the
payload's length is whatever the chain says (`Sizes` measures a 22,600-byte one),
so a Byron credential cannot be read off the front of the address — the arm has to
parse. **Not known, and the reason it is arithmetic rather than a corner case:**
how many live rows are Byron at all. **Over the whole live set:**

| | rows | share |
|---|---|---|
| payment credential (types 0–7) | **10,527,049** | 96.48% |
| Byron root (type 8) | **384,625** | 3.52% |
| stake credential (types 14–15) | **0** | — |
| unkeyed | **0** | — |
| total | **10,911,674** | = the row count, exactly |

Three things follow, and the third is the one that would have been guessed wrong.

1. **`by_addr` is a permutation of the row set, checked not assumed.**
   `by_addr_entries == rows` is asserted at build time and again by `verify`, so a
   derivation with a hole in it fails loudly rather than serving an index that
   quietly omits rows.
2. **`utxos_by_payment_cred` is served by the same file** as `utxos_by_address`:
   a lookup takes the credential's prefix, scans the run, and confirms. The
   confirm is what separates them — for an address it is the row's full address
   bytes, for a credential it is the re-derived credential, and for a
   payment-credential query it also rejects a `Kind::Stake` row that happens to
   share the bytes.
3. **Byron is not rare — it is 3.5% of the live set**, because a Byron output
   that was never spent is still there. A `by_addr` that sliced bytes without
   parsing would have silently keyed 384,625 rows on the wrong 28 bytes, and the
   only reason that is not the current state is that the payload's variable length
   was measured first.

⚠️ **Reward addresses are zero in the live set, so the type 14–15 arm is
unexercised by real data.** It is two lines and it is tested against a synthetic
address; a book built from a chain that does hold one would exercise it. Recording
it here rather than deleting it, because "no UTxO has ever sat at a reward
address" is a fact about this chain, not a property of the format.

**Deliberately not built: a stake-keyed ordering over the delegation part.** A
base address's stake credential is at `[29..57]`, a different offset, so keying it
would mean a *second* entry for most rows and the loss of the
`entries == rows` identity above. `resolve_stake_for_payment_pkh` needs no such
ordering — it looks up the payment credential and reads the stake part out of the
row it already has — but Koios's `account_utxos` (`search_utxos(StakeCredential)`)
would. That is a separate ordering, and it should be a separate decision.

## B4 — Liveness, the spend log, and the delta chain

Two facts fight each other here, and the resolution is the design:

1. Liveness is **1 bit** per ever-created output — 355,490,215 bits = **42 MiB**
   — and the book is a **stream compaction** of the created array masked by it.
   That compaction is a prefix-sum plus a scatter: embarrassingly parallel.
2. A spend in chunk N+100 flips a bit belonging to chunk N. **Liveness is
   mutable**, so a bitmap is not an immutable artifact.

So: **the bitmap is a builder-side structure, not a file in the artifact.**
Persisting it would require persisting the 355.5 M-row created array behind it
(~20 GB) — an order of magnitude more than the book it produces. The bitmap
earns its place in a parallel builder's working set and nowhere else.

What *is* persisted, and what the format must define, is the delta:

```
book/
  base/                 ← immutable, published, replaced by rename
    rows.bin
    by_outref.idx  dir.idx
    by_addr.idx    dir.idx
    policy.tbl
    datum.tbl
  delta/00123.dlt       ← one per chunk since the base's boundary
```

A delta carries **appended rows, tombstoned row ids, and the index entries for
both**. Readers consult deltas newest-first and then the base; a tombstone
shadows a base row. The writer is single; readers take a snapshot pointer, so
they are at most one block stale.

⚠️ The alternative — rewriting the base at every chunk — is what makes a
database look attractive, and it is the wrong trade: a base rewrite is ~2 GB of
I/O per chunk, against a delta of ~1 MB. Compaction (merging deltas into a new
base) is then a background, idle-scheduled merge by prefix — the same code as
the builder, and parallel.

**⚠️ Corrected and measured 2026-09-23 — `DELTA_CHAIN.md`.** `tools/fold-probe
follow` builds this delta over a published book's boundary and prices it. Three
corrections to the above, all of them from mainnet rather than from the spec:

- **A delta is ~3.3 MiB a chunk at the tip, not ~1 MB** — and the *row* is what
dominates (182 B of the 248 B per creation), not the index entries.
- **The tombstones need a dense row id.** A base row's address is a byte offset into
a variable-length arena, so "tombstoned row ids" needs the row's `by_outref` entry
— `Base::outref_entry`, added for this.
- **The base's own bitmap is 1.36 MB**, one bit per live row, and that is the whole
of what a follower holds beyond the delta.

It resolves **100%** of the inputs a book cannot — cross-checked against
`resolve-inputs`' independently-built gap, exactly — for **+31% on the walk that
produces it** (measured against a no-sink control on the same chunks). What is
still missing is the delta's own `by_addr` / `by_policy` orderings; a linear scan
is 97.5 ms over a 21-chunk delta, which is not a request path.

## B5 — Format rules

- **A published file is swapped by `rename`, never written in place.** Every
  writer fills `<name>.new` and moves it onto the published name, which is atomic
  within a filesystem. This is not tidiness: a reader holds a mapping
  (`crates/live-book`), a mapping follows the *inode*, and a writer that truncated
  a live file would make every read past the new end a `SIGBUS` rather than an
  error. `book.meta` follows the same rule, and is written last, so its presence
  means everything before it succeeded.
- **`layout_version: u32` in every file**, and a mismatch is **REFUSED, not
  reinterpreted**. `crates/tx-index` already learned this: "a segment or base of
  the wrong format version is REFUSED... reading old bytes at the new width
  returns garbage locations instead of failing."
- **`book.meta`'s field set is part of the layout.** It is parsed into a typed
  struct with every field required, so a field the writer does not write is an
  error rather than a default — which is why a change to it is a version change
  like any other. **`base_slot` is the one exception, and it is a different kind of
  field**: the rest describe the rows, and this one says *where the rows are*, which
  is a fact about the build rather than about the bytes. An artifact built before the
  field existed is not a different layout, it is one that cannot answer `tip()` — so
  absence parses as `None`, a `tip` request on an unstamped book refuses, and no
  `layout_version` moves. A build writes it from the walk; `fold-probe
  stamp-boundary` fills it in from the boundary chunk for an artifact that predates
  it. See `PHONEBOOK_COVERAGE.md` §12 for the execution test that found this and
  `DELTA_CHAIN.md` §7 for why a delta can never supply it.
- **Little-endian** everywhere, stated rather than assumed. The row arena is
  byte-addressed, so it has no alignment requirement at all; the fixed-size
  arrays are read at their declared stride.
- **Fences**: each array records `first_prefix` and `last_prefix`, so a lookup
  can skip an array without touching its body. `policy-index` fences its runs
  the same way.
- **Canonical order** is `(prefix, tail)` ascending — the 96-bit build key, which
  is a function of the outref alone. **Not `(prefix, row)`**: a row offset depends
  on when compaction happened to run, so ordering by it would make the artifact's
  bytes depend on the builder's memory strategy. Everything that is built must
  agree on the order, because it is what makes two builders produce the same
  bytes (§B6) — verified by comparing whole artifacts byte for byte across runs
  at 0, 1 and 7 compactions.
- **The directory's buckets partition the entry array** — non-overlapping,
  contiguous, covering. A property `verify_structure` asserts, not a convention
  it hopes for.

## B6 — Two builders, one output

The format does not care how the bytes were produced. Two builders exist, they
have measured costs, and **they must agree byte for byte** — which is the
cheapest correctness evidence available for either:

| builder | shape | cost | intermediates |
|---|---|---|---|
| **fold** | serial apply, no created array | **1.26 h wall, measured** — decode 32.1 m + apply 9.2 m + read 3.1 m, against 75.6 m of wall; the ~30 m remainder is the CBOR frame walk, now timed | none |
| **join** | created ∧ ¬spent, hash-partitioned merge | parallel, I/O overlapped; `--index-all-outputs` measured ~40 M rows / ~15 min for a 90-day window | ~20 GB created + 4.1 GB spent |

The wall-clock gap between those two rows is the argument for the join. Note
what the fold's *own* instrumented breakdown says the gap is made of: not I/O
(**read is 3.1 m of 75.6 m**), but three CPU stages — a CBOR frame walk, a
decode, and a serial apply — of which only the last needs order. A pipelined
builder parallelises the first two and leaves the applier as the floor, measured
at **9.2 m for the whole chain**.

Both builders must be **consumers of a shared pass** rather than walks of their
own — §B7 — and the join's intermediates are themselves artifacts of the
family, which is the stronger reason to prefer it.

The fold is what `tools/fold-probe fold --chunks 0` is running now. The join is
the parallel route, and it is the one that makes incremental refresh natural:
`PROJECT_LEDGER_IMPORTER.md`'s measured finding — that 94% of unresolved inputs
point *inside* the walk's own window, so indexing in-range outputs as you go
closes essentially all of them — is the same observation at a smaller scale.

Three whole-artifact assertions come free and should be wired as tests:

1. `rows in the book == created − spent + misresolved` — **not** `created −
   spent`, which is low by 14,098 on mainnet. `tools/fold-probe` reports all
   three, and the identity held to the unit over the whole chain.
2. `Σ lovelace held == the fold's`, and the fold's is checkable against
   something outside this repo: **36,764,994,429 ₳** against a circulating
   supply of ~36–37 B ₳. A decode that double-counted or dropped outputs
   anywhere would break that, which makes it the cheapest strong check here.
3. `by_outref` and `by_addr` are permutations of the **same** row-id set — the
   "two views cannot disagree" property, checked rather than assumed. **It is now
   checked in two places**: `finish` asserts the `by_addr` entry count against the
   rows it counted, and `verify` asserts it against the row count `book.meta`
   declares, so a build and a reindex are each held to it independently. A
   derivation with a hole in it cannot pass either.

## B7 — One walk, many artifacts

The book must be a **consumer of a shared pass**, not a walk of its own. The
measured argument is already in the repo: `policy-index` rides `tx-index`'s
decode through `extract_chunk_observed`'s callback for **+1.8%**, where an
independent pass over the same chunks would cost **+100%**
(`crates/policy-index/README.md` — "Give this crate its own walk and the entire
economic argument evaporates"). `FOOTPRINT` §8 asks for the same thing at the
infra level: one generic Mithril walker with extractors.

**One pass therefore yields**: tx-index's segments, policy-index's segments, the
**created-output log**, the **spend log**, the aux tables (slot → unix, era,
protocol params — today every walker recomputes these locally), and — after the
merge — the book.

**The join builder is what makes this concrete**, and it is the better answer to
B6's question for that reason rather than for its wall clock: its two
intermediates *are* artifacts of the family. The created-output log is the
**inverse of tx-index** — `(tx hash, output index) → where the output lives` —
which nothing in the workspace provides today, and it is the generalised form of
`PROJECT_LEDGER_IMPORTER.md`'s measured `--index-all-outputs` finding: index
in-range outputs as you walk and 94% of the resolution work becomes a lookup.
The book is the first consumer of that log; every walker in `tools/` is the
second.

⚠️ **The open tension the seam has to resolve.** `crates/tx-index`'s extractor
deliberately never builds a `MultiEraTx` — it iterates raw transaction bodies
and computes hashes and spans from the borrowed slices, because "at 123.6 M
transactions that cost is the walk". The book needs inputs and outputs decoded,
which is *more* decode than a span and hash. So the pass has to decide how much
depth it pays for, once, on behalf of every consumer — and the honest way to
decide is to **measure both depths on the same box**: tx-index alone, and
tx-index with the book riding along. +1.8% is the number to beat and the number
to verify; whichever way it lands, the result belongs in the extractor's own
docs, because it is a property of the pass and not of any one artifact.

**How deep the pass pays is now half-priced.** `DECODE_SHAPE.md` §2.10
(2026-09-24, three 100-chunk windows) measures the rungs above tx-index's floor:
decoding inputs and outputs through `MultiEraTx` instead of the one-pass
primitives walk costs **~1.75–1.88×** on the decode stage (§2.6, §2.8), and
carrying **witness datums and aux data** on top of that costs a further
**+60–81% of the decode** (+55% store-wide — 18.4 min of a 90.6 min wall) for
**+4 MB of peak RSS**. Those fields are produced by nobody today, so they force
the eager projection, and with it the `MultiEraTx` walk: `--cursor` and
`--chain-decode` cannot be combined, because `decode_tx` takes a `&MultiEraTx`.
What is still unpriced is the join this section asks for — tx-index's own
extractor alongside either rung — which is where +1.8% has to be verified or
refuted.

**Coverage is recorded as ranges, not a cursor** (`POLICY_WALK_SCHEDULER.md` §1),
so a partial build is expressible, a gap is legible, and any chunk range can be
re-run without invalidating the rest.

## B8 — Verification

Following the two siblings, because a format nobody can check is a format that
rots:

- **Structural** (`verify_structure`): ordering, fences, directory buckets, each
  permutation really being a permutation of the row-id range, `n_assets` and
  `addr_len` consistent with the row's own offsets. `policy-index` does this in
  **0.2 s over 19 M records**; at 10.9 M rows this is a sub-second check that
  should run at every publish.
  **Built for all three orderings** (2026-09-23), through one function: `verify`
  reports an `Ordering` per index and holds each to the same properties, because
  the copy is where a reader stops reading. `by_addr`'s confirm is the strongest
  of the three — it **re-derives** the credential from the row's address bytes,
  so an entry pointing at the wrong row, or a prefix written from the wrong
  field, is caught by construction. What a re-derivation cannot catch is a wrong
  *derivation*, since builder and reader would be wrong together; that is what
  the credential's agreement with `pallas_addresses` is for (`B3a`).
  Whole store, all entries, no sampling: **10,911,674 `by_outref` · 10,911,674
  `by_addr` · 10,946,451 `by_policy`, 0 failures of any check.**
- **Sampled** (`verify_sample`): walk back to the chunks, decode the body
  **independently of the builder**, and confirm the row's address, value, datum
  and outref. Sample by **stride, not first-N and not random** — "a stride hits
  every era and bucket, which is where a systematic fault lives."
  `policy-index`'s 3,001-of-3,001 is the precedent, and its own first run
  finding 342 false faults *in the checker* is the warning that a verifier is
  code too.
- **Reproducibility**: the same chunks must produce identical bytes. That is
  what makes a published book *checkable* rather than trusted, and it is the
  property `INSTALLABLE_MITOS` I5 needs in order to be a re-derivation instead
  of a leap of faith.

## B9 — Serving, and the seam with the change feed

**Built 2026-09-23**, in `crates/live-book` — the crate the second consumer
forced, the same way `policy-index` and `chain-decode` were extracted. Every file
is mapped read-only and every answer is confirmed against the row it points at, so
a prefix collision costs a read and never an answer.

| what | measured, whole store |
|---|---|
| `Base::open` | **73 ms** — three directories validated, 10.9 M rows mapped |
| resident after open | the mappings; nothing is read until it is asked for |
| `utxo` (point lookup) | **1223 ns** over 589,363 distinct refs |
| `by_payment_cred` | **871 ns/ref** over a 19,768-ref run |
| `by_address` | **925 ns/ref** over a 19,724-ref run |
| queries through all three orderings | **110,498/s** — 589,363 refs in 5.3 s |

Those are the numbers the format was designed against, and the one to hold beside
them is the ⚠️ in §Sizes: a confirm that had to re-read the *block* would be ~2 ms,
and `B2`'s whole argument for carrying the full outref in the row is that 138 M of
them make the difference between 69 minutes and 77 hours. At 1223 ns the row is
not the bottleneck; the search is.

**The check that gives the reader its correctness argument is not `verify`.**
`verify` audits *entries* and never exercises the binary search inside a directory
bucket — the one piece whose behaviour could differ on a run of ten thousand
entries from a run of four. So `fold-probe lookup --cross-check` walks *queries*
from `policy.tbl`: for each sampled policy, `by_policy` returns holders, `utxo`
must find every ref it returned, the row must resolve the policy back, and
`by_address` on that row's address must contain the same ref. Whole store:
**21,067 policies · 589,363 refs · 0 disagreements**.

**And one number with an oracle outside this repository.** `SOFTBURN_REWARDS.md`
in `cnft.dev-workers` records the `$burnsnek` sink as "19,692 UTxOs accumulated and
none ever consumed (2026-08-29)". The book puts it at **19,724** at the snapshot's
height — a sink that is never spent can only grow, and it has. The same query
through `by_payment_cred` returns **19,768**, and the 44-row difference is the
distinction between the two primitives rather than an error: 44 outputs share that
payment credential at *other* addresses, which `by_payment_cred` is defined to
return and `by_address` is defined to reject.

**⚠️ What the trait costs on top of it.** `BookDataPlane`
(`crates/mitos-data-plane/src/impls/book.rs`) puts `TypedOutput` over the same
rows — bech32 addresses, hex policy ids, owned `Vec<AssetEntry>` — and the
projection is **17× the lookup**: 20.4 µs per output against the 1223 ns point
lookup, over 34,177 outputs read from 2,000 sampled policies. Measured at both
decode levels, `Lean` is 20,616 ns and `WithDatum` 20,406 — **so none of it is
resolution**. It is the trait's shape: a `String` address and two hex `String`s
plus a validated `PolicyId` per asset entry, ~29.6 asset entries per output when
the sample is driven through a policy (against 1.68 per row overall, because a
row holding N assets is N times likelier to be reached from a random policy).
That is a property of the trait, not of the book, and it is worth a decision
rather than an optimisation: the readers below it hand over borrowed bytes, and
this layer is where they become owned text.

What the book answers:

- **Read-only mmap.** A rebuilt base is swapped by rename and old mappings keep
  the old inode alive (`B5`), which is also why the writers are not allowed to
  edit a published file in place.
- `read_utxos` / `read_utxo` (live set), `utxos_by_address`,
  `utxos_by_payment_cred`, `utxos_by_policy`, and `holder_count` from the entry
  count of a run. `AssetState`-style facts only if a sibling projection supplies
  them — the book is a set of outputs, not the entity graph.
- **`read_script`** is `NotYetImplemented` in the data plane today. The book's
  datum table is the natural home for a script table too — the same hash →
  payload shape, the same refcounted store — which would close that gap as a side
  effect.

What it does not: `read_tx` / `tx_metadata` stay with tx-index and the chunks;
`read_block` stays with the tip ring.

The change feed is the delta chain, not this artifact. Block-level changes come
from the follower; what the book adds is a durable tail a restarted module can
re-read without a replay buffer.

**⚠️ What the reader deliberately does not do yet.** There is no `rows_in_creation
order` walk, no stake-keyed ordering (§B3a), and no cache of resolved policies —
`by_policy`'s confirm resolves each row's interned id through the mapped table per
candidate, which is a 28-byte read and is measured above. Each of those is a
consumer away, and the measurements say none of them is the current bottleneck.

## Sizes

Phase 1 has now built a whole-store artifact, so most of the table below is
**measured** rather than derived. The derived column is kept where it still is:

| piece | rows | size | basis |
|---|---|---|---|
| `rows.bin` | 10,911,674 | **1.69 GiB** | **measured** — 166 B/row |
| `by_outref.idx` | 10,911,674 | **130.9 MB** | **measured** — 12 B/entry + 12 B header; `(130,940,100 − 12) / 12` is the live row count, exactly |
| `dir.idx` | 2^21 | **8.39 MB** | **measured** — 8 B header + 2^21 × u32 |
| `by_policy.idx` + dir | 10,946,451 | **131.4 MB + 8.39 MB** | **measured** — 1.003 entries/row |
| `policy.tbl` | 231,737 | **6.49 MB** | **measured** — 12 B header + 28 B/row |
| `by_addr.idx` + `addr.dir.idx` | 10,911,674 | **130.9 MB + 8.39 MB** | **measured** — 12 B/entry, one per row |
| `datum.tbl` | ⚠️ unknown | ⚠️ | see Open |
| **base, total** | | **2.23 GB (2.08 GiB)** | **measured** — `du` over the eight files below |
| a delta | per chunk | ~1 MB | 9 MB of chunk in, a fraction out |

Measured on the artifact of record, `/srv/data/fold-book/full3`, as written:

```
  1808876205  rows.bin
   130940100  by_outref.idx
   130940100  by_addr.idx
   131357424  by_policy.idx
     8388616  dir.idx
     8388616  addr.dir.idx
     8388616  policy.dir.idx
     6488648  policy.tbl
```

A whole-store build now passes end to end: **49.2 min** over 9,181 chunks with
`--cursor --primitives`, **67 compactions**, 202 rehashes, **peak anon 2.42 GiB** —
**1.35 GiB of which is the `HashMap` this artifact replaces**, plus the 256 MiB
build index. The three identities agree: rows `created − resolved` =
10,911,674, held 36,764,994,429 ₳, 18,308,681 asset entries.

Two figures the phase-1 gate asked for by name:

- **The address cost is not the 29–57 B the row format assumed.** A Byron address
  is `[#6.24(payload), crc32]` and its **attributes map is `{* any => any}`** —
  unconstrained — so mainnet carries addresses of arbitrary length. Over chunk 81,
  4 of 16,013 created outputs hold payloads of **502, 682, 828 and 7,678 B**
  (`rows.bin`'s longest row is 7,689 B), against a plain address's ~43 B. They are
  authentic, not a decode artefact: each parses as an `AddressPayload` consuming
  exactly its bytes *and* satisfies its own CRC. Their attribute 1 carries text —
  Bible verses in plain ASCII, and one a hex-encoded JPEG. Early Byron had no
  metadata field, so the address was the data carrier.
- That is why **`addr_len` is `u16` and not `u8`** (`layout_version` 3). The `u8`
  held for 79,000 chunks of Shelley-through-Conway and failed at chunk 81 of
  9,181 — the first era, and only the first. Over the whole store only **22 rows
  exceed 255 B**, longest **22,600 B**; a `u16` reaches 65,535 B, so the headroom
  is real, and a longer address fails the build loudly rather than wrapping.

⚠️ **This bears on `by_addr`, and it is why that ordering had to parse rather than
slice.** A bootstrap (Byron) address cannot be keyed by slicing its bytes: the
credential is the `root` *inside* the CBOR payload, and the payload's length is
whatever the chain says. **`B3a` is the answer, and the measurement that settled
it: 384,625 rows — 3.52% of the live set — are Byron, so the arm is neither cold
nor rare.** The 22 rows over 255 B are all keyed by it, longest payload
included.

The variable payload is 88 B measured **− 26.9 B** of interning (1.795 assets ×
(28 − 13) B) **= ~61 B** — the subtraction and the addition both stated so the
number can be argued with. The spread in `rows.bin` is the address length, which
is the ⚠️ in §B2.

For scale: dolos's `state` is ~4 G *including* the entity graph, and its
`index` is ~39 G. The book is the part of that the phonebook actually serves,
at a twentieth of the index's size.

## Phasing

| # | what | gate |
|---|---|---|
| 1 | Builder emitting `rows.bin` + `by_outref.idx` + `dir.idx`, **riding an existing pass rather than starting one** (§B7); **measure, don't derive** the row size, the address cost, and the incremental cost of the extra decode depth | the ⚠️ figures above become real, and the +1.8% claim is verified or refuted |
| 2 | `verify_structure` + `verify_sample`; the three whole-artifact assertions | a publish is checkable |
| 3 | **`by_addr` ✓ (2026-09-23)** · policy interning · the datum table — **capture and reader ✓ (2026-09-24)**; the plane does not open it yet · the script table | the module-facing queries stop needing a fallback |
| 4 | The delta chain + serving (snapshot swap, newest-first read) — **the read side is built** (`crates/live-book`, §B9), **and the delta chain now exists and is measured** (`tools/fold-probe follow`, `DELTA_CHAIN.md`): rows + two bitmaps, 100% of the inputs the base cannot answer, +31% on the walk that builds it. Still missing: the delta's own orderings, publishing it as `delta/NNNNN.dlt`, and the merge | the tip no longer rewrites the base |
| 5 | Compaction, and I5's published book with a re-derivation check | a stranger can verify what they downloaded |

**What phase 3a actually cost.** `by_addr` was added to a *finished* whole-store
artifact — not by re-walking 9,181 chunks, but by re-deriving it from `rows.bin`:
**4.8 s, peak RSS 494 MB, 10,911,674 entries written**, and the two directories
it depends on rebuilt in the same pass. The rank ordering `B1` claims is the one
that decided it: the key was already **in the row**, so this was a reindex and not
a capture. Running it a second time left `by_addr.idx`, `addr.dir.idx` and
`book.meta` **byte-identical** (sha256 over the whole-store artifact), which is
`B6`'s "two builders, one output" extended to the repair path: a repair that
produced different bytes from a build would be a second implementation. The same is not true of the datum table or the mint ledger, which is
why those are separate phases:

| what a view is keyed on | source | cost to add later |
|---|---|---|
| the outref | already in RAM at `finish` | free |
| **the address** (its credential is a function of bytes already in the row) | `rows.bin` | **4.8 s measured** |
| a policy (inside the row's asset records) | `rows.bin` | 5.5 s measured |
| witness datums, aux data, the mint — in **neither** the rows nor the indexes | the block bodies | **a full 215 GB re-read** — or a walk that is already running, for datums: see below |

**The datum half is no longer "a full re-read".** `fold-probe fold --datum-tbl <dir>`
writes `datum.tbl` + `datum.bin` from the deep pass's own
`DecodedTx::witness_datums` — no second decode, no second read of the chunks, and
the capture is a set keyed by hash rather than a log. Measured 2026-09-24 on three
100-chunk windows (`DECODE_SHAPE.md` §2.10 carries the offered volumes): **34 KB per
chunk at the Conway tip · 983 KB in Babbage · 969 KB in Alonzo**, i.e. a
whole-history table in single-digit GB, against `rows.bin`'s 2.2 GB and the 215 GB
corpus it comes from. Dedup by hash folds the offered bytes by 43–56%, and the
table answers for **95–99%** of the hash-attached outputs created in the Babbage and
Conway windows against 68% in Alonzo — the unrevealed tail is small, era-dependent,
and that is the population a fallback is for.

## Decided

- **One walk, many artifacts** (§B7). An extra consumer riding a shared pass is
  measured at **+1.8%**; an independent pass over the same chunks is **+100%**,
  and the repo's own note is "give this crate its own walk and the entire
  economic argument evaporates". The book rides.
- **Not a database.** Four structural reasons, not taste: the input is
  immutable and per-file signed, so **re-derivation replaces the journal**; the
  write unit is a 6-hour file, so there is nothing for a compaction scheduler to
  schedule; a user-space cache is anonymous and unevictable where the page cache
  is not (`FOOTPRINT` §4.1 measured this); and a B-tree lookup on 10.9 M rows
  costs several page reads against a sorted array's one directory page plus
  1–3 entry pages.
- **The row carries the full 36-byte outref; the index carries 8 bytes.** The
  measurement that decides it is 2 ms per confirm read against ~µs in memory,
  over ~138 M resolutions.
- **12-byte index entries, no padding**, read manually as the siblings do.
- **The builder's live set is a compact index, not a `HashMap` of full outrefs.**
  Measured 2026-09-22 (`DECODE_SHAPE.md` §2.7): the fold's apply stage costs
  **146 ns/op at a 70 k-entry set and 787 ns/op at 10.9 M** — 551 s over 700 M
  ops in `fold-full.log`, i.e. 5.4× the cost for a 155× larger set. The table is
  a `HashMap` with a 36-byte key and a 24-byte value, ~700 MB at mainnet size,
  so every probe is a DRAM access with a TLB walk behind it. Hasher choice is
  worth ~3 s across the whole store, which is why this is a layout decision and
  not a hashing one: the build should answer spends through ~12 bytes per row —
  the same `{u64 prefix, u32 row}` the artifact already stores — rather than 60.
  The mechanism (a hash table over the prefix, with the bucket scan the directory
  already implies) is **open**; a 2–4× on this stage is 4–5 minutes of a
  whole-store pass.
- **Variable-length rows with interned policies.** Policies are 28 B × 231,515
  instances; interning is a 7 MB table that nets ~27 B on every asset-bearing
  row, once the per-asset quantity and length byte are counted back in.
- **The liveness bitmap is builder-side.** As an artifact it would drag a
  ~20 GB created array behind it.
- **The spend log is kept** (~4.1 GB at 12 B × 344.6 M) even though it is
  rebuildable in ~5 minutes, because it is what makes the join builder parallel
  and the book incrementally maintainable.
- **Canonical order is `(prefix, tail)`** — the 96-bit build key, derivable from
  the outref alone — so two builders can be compared byte for byte and the
  artifact does not depend on how the builder used its memory. Checked, not
  assumed: `rows.bin`, `by_outref.idx`, `dir.idx` and `policy.tbl` are
  **byte-identical** across runs at 0, 1 and 7 compactions.
- **The scratch compacts on dead weight, not on arena size.** The trigger is
  `scratch_bytes − live_bytes_at_last_compaction > --book-compact-mb`. A size
  trigger looks equivalent and is not: the arena can never shrink below the live
  set, so once the live set alone exceeds the threshold, *every insert* compacts
  the whole arena. Measured at `--book-compact-mb 1` over a 3-chunk window:
  **26,524 compactions**. On a whole-store build at the 1 GiB default that regime
  begins where the live set is ~1.6 GB — which is exactly the end of the chain.
  Peak arena is then `live + 1 GiB` ≈ 2.7–3.0 GB against `B2`'s 3 GiB `u32` cap,
  so a whole-store build has **almost no headroom**; a lower threshold trades I/O
  for room.

## Open questions

- **The address cost.** Unmeasured, and the largest single row component. Until
  it is decoded, `rows.bin` is a range rather than a number.
- **Where datums live.** Inline in the row (measured ~35 B/row on created
  outputs) versus a refcounted side store, as dolos's `DATUM_NS` is
  (`crates/mitos-data-plane/src/impls/local.rs:528-568` — and note the comment in
  `host_fns_v2/chain_data.rs:20-33` that a hash-referenced datum costs an extra
  lookup that inline ones do not).
- **Whole set or interest slice.** A `lean` box may want only its modules'
  watched credentials. The format supports it — `by_addr` over a filtered row
  set is a smaller book — but nothing here decides it, and
  `FOOTPRINT` §7's profiles are where it belongs.
- **Who publishes, and against which boundary.** A book is only meaningful
  relative to a chunk boundary, and the refresh cadence has to be tighter than
  the resolver's staleness budget for the same reason `TX_INDEX_DATASOURCE.md`
  R3.2 gives.
- **Entity facts the modules already query** (mint provenance, `AssetState`)
  are *not* in this artifact. They are a sibling projection, and
  `MITHRIL_PHONEBOOK.md` §6 still owes a measurement of what of dolos's entity
  graph is genuinely read.
- **Whether the spent log needs a slot.** This spec says no: "is it live" does
  not need it, and "what just changed" comes from the follower. Adding it is a
  `layout_version` bump and cheap, since the log is derived.
