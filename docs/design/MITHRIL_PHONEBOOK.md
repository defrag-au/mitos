# Mitos as a phonebook of the Mithril log

**Status: SKETCH with measurements** (2026-09-22). Not a work order. §5b is
measured, on the ARM box, by `tools/fold-probe` — which is free to run there
because the dolos import that was proving the node-shaped path was killed on
2026-09-22. Everything outside §5b is still a sketch.

*Two of the pieces this note argues for now exist, and they are documented where
they live rather than here: `docs/design/LIVE_BOOK.md` is the artifact format
(§B3a is the address credential, with the measured 384,625 Byron rows and 0
reward addresses in the live set), and `docs/design/PHONEBOOK_COVERAGE.md` maps
the module surface onto it and carries the whole-store verification figures. §3's
"other phonebook volumes" are the ones still outstanding.*

Owner: TBD
Related:
- `docs/design/FOOTPRINT_AND_MINIMUM_SPEC.md` — §7 “how far can dolos be
  pruned?”, §8 unified walker constraints
- `docs/design/INSTALLABLE_MITOS.md` — §4 import is the slow part; I5 ships
  an already-folded book
- `docs/design/TX_INDEX_DATASOURCE.md`, `crates/tx-index/` — the projection
  pattern this note generalises
- `docs/work-orders/WO-F5-mitos-bootstraps-itself.md` — today’s importer
  *is* dolos `import_blocks`
- `tools/fold-probe` — the tool §5b's figures come from (`count` for the
  book's size, `fold` for the decode/apply split and the RSS)

## 1. Forcing observation

Mithril’s cardano-db snapshot is a **log**: sealed `NNNNN.chunk` files,
slot order, a sidecar that maps slot → block offset, and nothing keyed by
tx hash, address, or policy. Walking it is cheap and parallel — `tx-index
build --threads 8` is decode-bound, tens of minutes for all of mainnet.

What we actually serve — to modules, to Cloudflare workers, to ourselves —
is a **phonebook plus a change feed**: given a name (tx hash, output ref,
policy, address), where is it, is it still live, and what just changed.

Dolos import is a different job that happens to use the same bytes as
input. It reconstructs a **node-shaped store** (full ledger, archive copy
of the log, dolos indexes, WAL) so the process can validate and submit.
We have been treating that store as the phonebook. Most of it is not.

The live UTxO set is invaluable and stays. Being a Cardano node does not.

## 2. What the 145 G is

Production dolos data dir, from FOOTPRINT:

| Piece | Size | Role |
|---|---|---|
| **state** | ~4 G | UTxO + entities. The book. |
| archive | ~93 G | a second copy of the log we already keep as ~217 G of chunks |
| index | ~39 G | dolos’s own address/tx indexes |
| wal | ~11 G | follow undo log (grows after import) |

Per block, `import_blocks` does not just spend/produce outputs. It crawls
accounts, assets, datums, DReps, epochs, pools, proposals, tx logs, writes
archive + index + state, then at epoch boundaries walks accounts in 32
**sequential** shards (`ACCOUNT_SHARDS`). Those shards are crash-resume
slices, not a thread pool. `cache = 500` in `dolos.toml` is **500 MB** of
fjall cache over a 4 G state keyspace — random `load_utxos` / `commit`
against an eighth of the working set.

Submit does not justify this. `services/tx-submit` is already Koios-first,
mitos fallback, because local phase-1/2 false-rejects Babbage script refs.
A submitter is a TxSubmission client talking to a relay, not a 147 G ledger.

## 3. Two ceilings

**Sequential apply is inherent for a live UTxO set.** Block N+1 depends on
N. A UTxO folder will never look like `tx-index --threads 8`. You cannot
outsmart consensus order.

**Dolos is close to the ceiling for a full ledger. It is not close to the
ceiling for this phonebook.** A purpose-built folder:

- reads the log sequentially (the chunks stay the source of truth);
- holds the live set in memory for the *build* (naive
  `HashMap<TxoRef, EraCbor>` is 2–3× the 4 G packed-on-disk figure — fine
  on 32 G if that is *all* you hold, not fine next to archive/index
  writers; MEASURED §5b: the set is 10.9 M rows at ~120–220 B/row,
  ~1.3–2.1 G anonymous. The 8–10 G fear is a property of the
  full-`EraCbor` row, not of a folded set — so the lazy default is the
  thing to avoid, not the in-memory idea);
- checkpoints at epoch grain (that is already when dolos moves the cursor);
- dumps a packed mmap file and serves from page cache, same as tx-index —
  RAM is for the fold, not a permanent 8–10 G anonymous heap;
- follows the tip as a small delta.

That is still hours of one-core apply, not tens of minutes. The only way
the fold becomes an hour is **not folding**: copy an already-folded book
(INSTALLABLE_MITOS I5, or a ledger-state snapshot).

⚠️ MEASURED §5b, and it moves this: decoding the whole log is **45.7 min on
one core**, and **5.3 min at 8 threads** for the whole store. The apply is
~25% of decode at 9.9 M rows and grows with the set, so a pipelined folder's
floor is the decode pool. "Not folding" is no longer the only route to an
hour.

The other phonebook volumes (tx-index, policy-index, ledgers, walker
extractors) *can* skip order. They stay projections of the log. FOOTPRINT
§8 already constrains the unified walker: mmap-friendly, bounded RSS,
`--threads`, Nice/idle, resumable per chunk.

Workers never open a chunk. They ask the book and they subscribe to diffs
(catalogue → R2/KV is already that pattern).

## 4. Keep / drop

| Keep | Drop or shrink |
|---|---|
| Chunk store + Mithril refresh (the log) | dolos archive (~93 G rewrite of the log) |
| Live UTxO set (~4 G, or a compact mmap of it) | Full CardanoLogic entity graph, 32-shard epoch walks |
| tx-index / policy-index / ledger walks | Local mempool, N2C, minibf-as-node |
| Auth, modules, worker-facing surface | Modules that *must* subscribe to dolos `TipEvent` — those become walker extractors |
| Thin tip follow (immutable lags by *k*) | Import-as-live-follow-engine (`import_blocks` under `write_chain`) |

`lean` / `standard` / `full` in FOOTPRINT stay: they are how much of the
*log* you hold locally, not whether you materialise a node next to it.

## 5. Evidence

### 5a. The import, 2026-09-21 — the node path

ARM (`v2202604354294454929`, 12× aarch64, 24 G, Debian 13): musl
`mitos bootstrap --fetch never` against the existing 217 G store.
Import is one core (~120% CPU) for the whole run; RSS walked off 24 G and
onto 32 G of swap by ~25% of tip (~50 M / 198 M slots) after ~9 hours.
Disk was never the constraint.

⚠️ Discharged 2026-09-22: that run was killed to free the box, so this
section's instruction (do not start a UTxO-only importer there, do not kill
the import) no longer applies. §5b is what the box ran instead. The nine
hours were `import_blocks` visiting a node-shaped store — not the chain, and
not the SKU.

### 5b. The probe, 2026-09-22 — this path

`tools/fold-probe`, native glibc build on the box. Store
`/opt/market-ledger/snapshot-full/db/immutable`: 9,182 `.chunk` files,
214.86 GiB, chunks `00000`–`09181`, the newest excluded as still-growing.
Sidecars are 1.5 G against 214.9 G of chunk — they can be ignored in any
footprint arithmetic.

```sh
# whole-store pass, no set held: the exact live-set size and the ingest floor
fold-probe count --immutable <store> --threads 8

# a recent window with a real set: the decode/apply split and the RSS
fold-probe fold  --immutable <store> --chunks 10

# the definitive serial fold (hours)
fold-probe fold  --immutable <store> --chunks 0
```

**The whole-store pass**, 8 threads:

| | |
|---|---|
| wall | **318.2 s (5.3 min)** |
| read | 214.86 GiB at 725 MB/s |
| blocks | 13,963,796 |
| txs | 123,870,518 (245 invalid) |
| outputs created, all time | 355,490,215 |
| outputs spent | 344,592,639 |
| live UTxO set | **10,897,576** from `created − spent`; the fold says **10,911,674** |

Two independent cross-checks over the same chunk set: `crates/tx-index`
counted 123,602,296 txs (+0.22% here — a newer snapshot) and
`cnft.dev-workers/docs/design/PROJECT_LEDGER_IMPORTER.md` counted 13,707,267
blocks (+1.9%). The device was 98% idle throughout, ~300 MB/s sustained,
2–5% iowait: **disk is not the constraint on this box.**

**A recent window** (chunks 9171–9180, Conway, 2.5 days):

| | |
|---|---|
| decode | 0.11 s/chunk · **84 MB/s** |
| apply | 0.01 s/chunk — decode:apply **11:1** |
| the whole log, one core, from 84 MB/s | **45.7 min** |
| outputs created / residues left | 148,511 / 39,072 |
| spends resolved locally | 72.5% |
| spends naming another chunk's output | **40.6%** |
| peak anon RSS | 8–29 MB |

⚠️ **The two modes differ by 14,098, and the fold is right.** The fold resolved
344,578,541 of its 344,592,639 spends, and `entries = created − spent +
misses` holds to the unit. The misses are spends naming an output **the log
never created** — Byron's genesis distribution and redeem-style inputs, which
have no creating transaction at all. A `created − spent` count cannot see them
and is low by exactly this, everywhere. The fold's first chunk carries 6,882 of
them (49.7% of its spends) and the count decays to zero by chunk ~60.

**The serial fold** (`--chunks 0`, single-threaded, complete): **1.26 h wall**,
decode 0.21 s/chunk · **120 MB/s**, apply 0.06 s/chunk at a peak set of
11,271,535 rows, peak anon **2.17 GiB**, `hit%` 100.0% (344.6 M resolutions),
**48.1%** of spends naming a different chunk's output, and 36,764,994,429 ₳
held — against a circulating supply of ~36–37 B ₳, which is the sanity check
that says the decode is right.

⚠️ Its stages do **not** sum to its wall clock, and the gap is the interesting
part: decode 32.1 m + apply 9.2 m + read 3.1 m = 44.4 m against 75.6 m. The
remainder is the CBOR frame walk (`split_blocks`), which was untimed until
2026-09-22 and is therefore a *subtraction* rather than a measurement. A warm
small window puts that walk at **667 MB/s**, which would be ~5 min for the
store, not ~30 — so something makes it 5× slower under a 2.17 GiB live set
(cache interference between a streaming walk and a randomly-accessed map is the
obvious candidate) and that is now measurable rather than arguable.

⚠️ **An earlier draft of this section claimed the fold was disk-bound and needed
a prefetch stage.** That was wrong, and it came from attributing the untimed
frame walk to I/O. The read is **~4% of the wall clock**. The stages are CPU.

**What the numbers settle:**

1. **The log is cheap.** 5.3 min to decode all 214.9 GiB at 8 threads; 45.7 min
   on one. §5a's nine hours are the node shape, not the chain.
2. **The apply is ~25% of decode at 9.9 M rows**, not the dominant cost. It
   does grow with the set (0.00 → 0.10 s/chunk), so a pipelined folder's floor
   is the decode pool, and §3's "hours of one-core apply" is now measured
   rather than assumed.
3. **The book is 10,911,674 rows** — the fold's count, not `created − spent`.
   Measured at **193 B/row** (2.17 GiB anon over a peak set of 11.27 M), which
   lands inside the ~120–220 B/row range the window predicted. A *resolvable*
   row — one that can answer `read_utxos` — adds address, value and datum: the
   window measured ~88 B/row of asset names and inline datum CBOR, so budget
   ~2.5 G.
4. **It is not monotone.** Cumulative net creation ran 7.89 M at chunk 3,000 →
   **11.13 M at chunk 6,040** → 10.91 M at the tip, so the live set peaked
   mid-2023 and has been declining since. ⚠️ That is a **usage** trend, not an
   architectural property — it reflects Cardano activity, and it would be
   wrong to size anything on it. The design-relevant fact is the *density*:
   10.9 M live against 355 M ever created, which is what bounds a book and
   makes delta-plus-compaction necessary regardless of the trend's sign.
5. **Speculative chunk-grain apply is dead.** **48.1%** of spends chain-wide
   name an output created by a *different* chunk — 40.6% in a Conway window,
   44% in the Byron era, so it is not an artefact of one period.
6. **A fold resolves its own inputs.** `hit%` is 100.0% once it has history
   (344,578,541 of 344,592,639), so no remote tier is needed at fold time — the
   resolution problem is a *window* problem, not a fold problem.

**Caveats, so nobody over-reads this:**

- The probe decodes **no addresses**, so 84 MB/s is a floor and the per-row
  figures omit the address.
- No datum hashing, no collateral modelling (the 245 invalid txs are skipped
  on both sides), so `live` is exact to within a few hundred rows of 10.9 M —
  and `created − spent` is **low by 14,098**, which is spent naming an output
  the log never created (Byron's genesis distribution, redeem-style inputs).
  `fold` reports that count; `count` cannot.
- `count` holds **no set**, so 5.3 min is an ingest floor, not a fold's wall
  clock. Only `fold --chunks 0` measures a fold.
- **Every figure here is from the native glibc build.** The same source
  cross-built for `aarch64-unknown-linux-musl` ran 2.2× slower on one thread
  and *slower with eight than with one*: 150 chunks in 31.7 s → 70.9 s, where
  glibc did 14.2 s → 1.9 s. 370k context switches/s at 98% idle disk is
  allocator contention. The allocator is part of the measurement.

## 6. Open

- Compact on-disk / mmap layout for the live set (tx-index is the template;
  full `EraCbor` per output is the lazy default). §5b gives the sizing input:
  10.9 M rows, ~120–220 B/row as a summary, ~88 B/row more for asset names
  and inline datum, plus whatever address and body a *resolvable* row needs.
- What of dolos state besides UTxO do we actually query (pparams, accounts,
  pools)? Measure before keeping entity visitors. (§5b measures the fold's
  own needs and nothing else — this one is untouched.)
- Tip window: headers-only follow vs a small dolos vs someone else's API,
  for “did this land in the last *k* blocks”.
- Submit: keep Koios/Maestro, or a tiny TxSubmission process; not a reason
  to keep `Domain::receive_tx`.
- I5 vs a from-chunks in-memory folder: I5 wins wall clock; the folder
  wins “we do not have to trust a published state dir”. §5b sharpens the
  wall-clock half: the log walk is minutes, so the folder's cost is the
  apply and the churn, not the read.
