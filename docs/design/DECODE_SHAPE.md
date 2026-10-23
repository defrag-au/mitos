# The decode shape — a budget, not a struct

Status: **measured, decision open** (2026-09-22). Every figure in §2 up to §2.9
comes from `tools/fold-probe fold --chain-decode` run against the 214.87 GiB
mainnet store on the ARM box; the raw logs are
`ab-{conway,babbage,alonzo,mary,shelley}.log` under `/opt/scratchpad/`. **§2.10 is
the same A/B re-run on macOS against a full snapshot downloaded locally**, and the
only section whose figures are not the box's. Nothing here is implemented.

Owner: TBD
Related:
- `docs/design/MITHRIL_PHONEBOOK.md` §3 (decode is the parallelisable ceiling)
  and §5b (the fold's figures)
- `docs/design/LIVE_BOOK.md` — the artifact whose builder this decode feeds
- `crates/chain-decode/` — the crate under discussion, extracted 2026-09-22
- `crates/mitos-data-plane/src/types/decode.rs` — `DecodeLevel`, the existing
  request-side knob on the *read* path, and the precedent for a budget on the
  *write* path
- `tools/project-ledger/src/walk.rs:738` and
  `tools/token-ledger/src/walk.rs:141` — the two call sites that re-walk the raw
  tx because `DecodedOutput` did not carry what they needed in the shape they
  wanted

## Summary

`decode_tx(&MultiEraTx) -> DecodedTx` is an **eager, fixed-policy projection**:
it materialises the union of every consumer's needs, and every consumer pays for
all of it. Measured on identical bytes, a caller that reads five facts per
output pays **45–231% more** than one that pulls only what it reads, and the
excess is *not* the payload — it is the shape. In the cleanest window the
projection had nothing to materialise at all (0 datums, 0 redeemers, 0 witness
datums, 1.9% of outputs carrying an asset) and still cost **231%**.

A partially-applied function is not the fix. The parse is already shared — one
`MultiEraBlock::decode` per block, handed to every consumer — so there is nothing
for a closure to amortise, and each consumer applies the projection exactly
once. What has to change is **who decides what gets built**.

The second finding is separate and belongs to the hand-rolled decoder thread:
**`MultiEraBlock::txs()` clones the block** (`pallas-traverse/src/support.rs` —
`transaction_bodies.get(i).cloned()`, `transaction_witness_sets.get(i).cloned()`),
and that one line is **15–23% of decode**. It reads like a borrow.

## 1. Why the closure is the wrong lever

The idea was: return a partially applied function rather than a struct, so the
consumer's work is fused with the decode instead of passing through an
intermediate `DecodedTx`.

Two things are being conflated there.

**Amortisation.** A closure pays off when it is called more than once. Every
consumer in this workspace calls it once per transaction, and all of them
already receive the same `&MultiEraTx` from the caller's block decode. There is
no second parse to save, so a closure would add a capture and an indirection to
buy nothing.

**Laziness.** Where deferral genuinely pays here is per *output*: 96% of outputs
are of no interest to any given walker, and the projection materialises all of
them anyway. That is a **predicate**, and the decode is the only thing that can
apply it before the work is done.

**And a lazy accessor API hides its cost at the call site.** This is not
hypothetical — it is what is happening now, three times over, and the
measurement prices two of them:

| looks like | is | costs |
|---|---|---|
| `tx.outputs()` | a borrow | builds a `Vec<MultiEraOutput>` |
| `tx.consumes()` | a borrow | a `HashSet` + a `Vec`, per call |
| `o.datum()` | a getter | CLONES the `KeepRaw` — for an inline datum, the whole `PlutusData` tree |
| `block.txs()` | a borrow | a full clone of every body and witness set (§2.2) |

A reader cannot tell any of those from a `&`-returning method. A budget makes
the cost explicit at the call site; a sink removes it.

## 2. What the measurement says

Command, per window:

```sh
/opt/scratchpad/fold-probe fold \
    --immutable /opt/market-ledger/snapshot-full/db/immutable \
    --start <chunk> --chunks 20 --chain-decode
```

`--chain-decode` runs the shared projection over every valid tx of the window
*as well as* the fold's lean pull, on the identical bytes, in the same process.
Both passes share one `block.txs()`, so the difference between them is the
projection and nothing else.

### 2.1 The projection's cost, by era

| window | era | lean | shared (`decode_tx`) | `chain`/lean | walker rate |
|---|---|---|---|---|---|
| 9161–9180 | Conway | 0.09 s/chunk · 86 MB/s | 0.04 s/chunk · 186 MB/s | **+46%** | 59 MB/s |
| 3348–3367 | Babbage | 0.30 · 133 MB/s | 0.13 · 298 MB/s | **+45%** | 92 MB/s |
| 1848–1867 | Alonzo | 0.20 · 86 MB/s | 0.15 · 116 MB/s | **+74%** | 49 MB/s |
| 1068–1087 | Mary | 0.04 · 174 MB/s | 0.08 · 76 MB/s | **+231%** | 53 MB/s |
| 208–227 | Shelley | 0.01 · 248 MB/s | 0.01 · 235 MB/s | **+106%** | 121 MB/s |

(`lean` is the fold's own pull: outref, lovelace, asset shape, datum length.
`walker rate` is what a tool that runs the shared projection instead sees, over
the same bytes — 59 MB/s against 86 is the whole point of the table. Each
`--start` is an era boundary: 208, 1068, 1848 and 3348 are the first chunks of
Shelley, Mary, Alonzo and Babbage, because those eras began on chunk
boundaries.)

(s/chunk is quoted to the 2 dp the report prints; the ratios are computed from
the full-precision accumulators. Shelley's absolutes sit at the display floor.)

**The Shelley and Mary windows are the whole argument.** Shelley has no assets,
no datums, no redeemers, no witness datums, no required signers and no aux data —
the projection's only per-output work is bech32-encoding an address and
allocating an empty `Vec<Asset>`, and it still doubles the time. Mary adds
multiasset but keeps every other column at zero (0 datum clones, 0 witness
datums, 0 redeemers, 691 KiB of aux, 7,574 asset entries across 399,384
outputs — 1.9%) and still costs 231%.

What both do per output, unconditionally: **94–102 bech32 characters** (13.3 M
across the Shelley window, 37.6 M across Mary, 89.6 M across Alonzo). What both
do per transaction: `consumes()` (a `HashSet` + a `Vec`), `outputs()` (a `Vec`),
a canonical-index sort, a required-signer collect, a witness-set scan, an aux
probe.

So the cost is not the payload a consumer reads. It is **the allocation of a
fixed-shape row per output and a fixed set of buffers per transaction, whether
or not anyone reads them.**

### 2.2 `MultiEraBlock::txs()` is a clone — 15–23% of decode

| window | `txs()` | share of decode |
|---|---|---|
| Conway | 0.0198 s/chunk | 22.2% |
| Babbage | 0.0690 | 23.2% |
| Alonzo | 0.0404 | 20.4% |
| Mary | 0.0070 | 19.4% |
| Shelley | 0.0017 | 14.9% |

`MultiEraBlock::txs()` is `support::clone_conway_txs(&block)`: for every
transaction index it does `block.transaction_bodies[i].cloned()` and
`block.transaction_witness_sets[i].cloned()`, wraps each in `Cow::Owned`, and
scans the aux set linearly per tx. `KeepRaw`'s derived `Clone` clones its
`inner`, so that is a full second materialisation of the block's structure —
`PlutusData` trees included — and it is why the fold has been paying for the
same bytes twice.

This is a decoder-level win, independent of the projection: hand out borrowed
transactions and ~20% of decode disappears. On the whole-store fold (decode
32.0 min of a 1.26 h wall) that is about **6.4 minutes**, 8.5% of the wall.

⚠️ Sampled from 100 chunks in 5 eras, 1.1% of the store. A whole-store run with
these timers (`--chunks 0 --chain-decode`) was started 2026-09-22 and is the
number to quote once it lands.

### 2.3 The two projections agree — a free differential check

`--chain-decode` also reconciles the two decodes over the same txs. Across all
five windows — **1,043,532 transactions, 2,725,905 outputs, 8,382,638 asset
entries** — the eager projection's counts equal the fold's counters exactly
(outputs and asset entries; both take the same `is_valid` guard off the same
`txs()`).

That is not a re-implementation check against an independent extractor (that is
`tx-index`'s 123.6 M `(offset, hash)` pairs, still the stronger oracle), but it
is a cheap differential that runs on every window from now on and would have
caught the projection drifting from the fold.

### 2.4 Per-field counts, for sizing what a budget would save

Store-wide, from the whole-chain fold (`fold-full.log`):

| | outputs |
|---|---|
| bare (no assets, no datum) | 200,873,336 |
| assets only | 98,053,735 |
| datum only | 12,528,081 |
| both | 44,035,063 |
| **total created** | **355,490,215** |

- asset-bearing = **142,088,798 (40.0%)** — each one re-encodes its own CBOR via
  `o.encode()` to price `min_utxo`, a field with **one** reader in the whole
  workspace (`project-ledger`)
- datum-bearing = **56,563,144 (15.9%)** — each one takes `o.datum()`, which
  clones; 3.53 GiB of that is inline CBOR
- 43.38 GiB of asset policy/name bytes are materialised into `Vec<Asset>` as
  (policy, name, quantity) triples, for consumers that mostly want to *match* a
  policy and only then read a quantity

### 2.5 Where the cycles actually go

`perf` on the Conway window (newest 20 chunks, 154 MB, 113,218 txs, 277,961
outputs), against an unstripped release build
(`CARGO_PROFILE_RELEASE_STRIP=none`, same codegen). Recording was warm-cache.

| | lean fold | with `--chain-decode` |
|---|---|---|
| wall | 2.47 s | 3.39 s |
| cycles | 6.48 G | 8.80 G |
| instructions | 11.69 G | 17.17 G |
| IPC | 1.80 | 1.95 |
| branches | 2.21 G | 2.95 G |
| **branch-misses** | **2.28%** | **2.19%** |
| cache-misses | 82.7 M | 90.0 M |

Inclusive shares of the lean run:

| | share |
|---|---|
| `MultiEraBlock::decode_conway` | 36.9% |
| **`malloc` + `cfree`** | **26.3%** (17.3 + 9.0) |
| `MultiEraBlock::txs` (the clone) | 16.4% |
| ↳ `FilterMap<..step_by..>` inside it | 14.9% |
| `BTreeMap` decode (the asset maps), two instantiations | 18.3% |
| `KeepRaw::decode` | 9.1% |
| `pallas_codec::tree::decode_tree<PlutusData>` | 8.9% |
| `metadatum::pairs` + `str::from_utf8` + aux drop-glue | ~4.5% |
| kernel page-fault handling | ~1.6% |

Read it as three findings, in order of size:

1. **Allocation is the largest single cost, and it is the *representation*
   choice that causes it.** `malloc`+`cfree` at 26% inclusive is the BTreeMaps
   (a node per policy, a node per asset name), the `Box<Cow>` per output in
   `txs()`, the `Vec<Asset>`, and the cloned structures' drop glue. Nothing here
   is fixed by adding a branch hint.
2. **`txs()`'s clone is 16.4% on its own**, and its teardown (
   `drop_glue<WitnessSet>` 1.7%, `clone_subtree<Hash, Bytes>` 2.9%,
   `drop_glue<Map<u64, KeepRaw<AuxiliaryData>>>` 1.5%) confirms that the clone
   *and the drop of what was cloned* are both paid.
3. **Branch prediction is not the problem.** 2.2% branch-misses, unchanged by
   adding a 47%-larger projection — the extra work is *more instructions*, not
   more mispredictions. The instruction count is high because of what gets
   built, not because the control flow is hard to predict.

**The projection's own profile** (top self): `pallas_addresses::encode_bech32`
**5.6%**, `malloc` 5.4%, `cfree` 5.3%, `PlutusData` hashing 3.8%,
`minicbor::Decoder::skip` 3.2%. The single largest named cost of the shared
decode is bech32-encoding an address that every consumer throws away — 5.6% of
the whole process for a string.

⚠️ **Probe artefact: `smaps_account` is 4.0% [k].** The RSS sampler runs every
128 blocks, and reading `smaps_rollup` makes the kernel walk page tables. In a
20-chunk window that is 4% of the wall *inside the thing being measured*; over
the whole-chain fold it is paid ~109k times. Sample every 1024 blocks instead.

### 2.6 The lean walk, written — 1.9× for no CBOR

`fold-probe fold --primitives` takes the lean pull straight off the decoded
primitives instead of through `MultiEraBlock::txs()`/`MultiEraTx`: identical
accounting, the era matched once per block, borrowed outputs, no clone and no
`Box`. Same window sizes as §2.1, on the same box.

| window (20 chunks) | `MultiEraTx` (control) | primitives | ratio |
|---|---|---|---|
| Conway 9161–9180 | 0.09 s/chunk · 86 MB/s | **0.05 · 162** | **1.88×** |
| Babbage 3348–3367 | 0.30 · 133 | **0.15 · 264** | **1.98×** |
| Alonzo 1848–1867 | 0.20 · 86 | **0.10 · 166** | **1.93×** |

The two walks produce **identical counters** on all three windows — Shape
buckets, asset entries, `lovelace` created *and* held, spends/hit/same-chunk,
txs. That reconciliation is what makes the ratio mean anything: it is the same
work, so the difference is what the structure costs.

⚠️ The controls ran on an idle box (08:08–08:09); the primitives runs ran while
`--chunks 0 --chain-decode` held a core and 534 MB of page cache. The bias is
*against* the primitives figures.

Where it comes from, against the profile: `txs()`'s clone is 0.0198 of the 0.09
s/chunk on Conway — about half the saving — and the rest is four pallas
accessors and a clone that the walk does not need: `consumes()` (a `HashSet`
plus a `Vec` per tx), `outputs()` (a `Vec` and a `Box<Cow>` per output),
`value().assets()` (a `Vec` per output), and `datum()` (a clone per output, the
`PlutusData` tree included). All of it on 355 M outputs.

The same A/B run locally on macOS — a different allocator — gives 1.82× on
Conway and 1.63× on Alonzo. The ratio surviving two allocators is the point: the
win is structural, not a glibc quirk.

**What it changes.**

- The decode stage is 32.0 min of the whole-chain fold's 75.6 min wall. At ~1.9×
  that is ~17 min, i.e. **~15 min off the wall, ~20%**, from a walk that
  hand-rolls no CBOR at all.
- What remains of decode is `MultiEraBlock::decode`'s own materialisation — the
  `BTreeMap` values, the owned `Bytes` addresses and asset names, the
  `PlutusData` trees, the metadata — which was ~37% of the old profile and is now
  the **whole** of the hand-rolled decoder's prize.
- **The sink's trait is no longer the risky part.** `plan_block_primitives` _is_
  the loop: era matched once per block, borrowed, allocating only what it asks
  for. Extracting a trait around it is a refactor of a loop that is already
  measured, not a bet on an unbuilt design.

⚠️ Byron falls back to the `MultiEraTx` walk — a Byron spend is self-contained
(`TxIn` embeds the output it spends) and its outrefs are computed rather than
read, so it needs `MultiEraInput`'s machinery. 208 of 9,181 chunks, but 32% of
the store's *blocks* (its blocks are ~1 KB). A whole-store `--primitives` run is
mixed and says so on the `fallback` line.

### 2.7 Micro-optimisations: two settled negatives, two live levers

With the primitives walk in place, the remaining per-stage costs are separable.
Same three windows, now with the frame walk and the hash cost timed on their own:

| window (20 chunks) | decode | of which blake2b | frame walk | apply | apply ns/op | set |
|---|---|---|---|---|---|---|
| Conway 9161–9180 | 0.05 s/ch · 161 MB/s | 0.0058 · **12%** | 0.01 s | 0.0041 | **146** | 70,082 |
| Babbage 3348–3367 | 0.15 · 261 MB/s | 0.0216 · **14%** | 0.03 | 0.0192 | **196** | 342,820 |
| Alonzo 1848–1867 | 0.10 · 176 MB/s | 0.0173 · **18%** | 0.02 | 0.0196 | **220** | 348,799 |

**Settled: the hasher is not worth a dependency.** FxHash against SipHash on the
same run: 23 ns/op against 27 ns/op locally, i.e. ~4 ns saved per operation. The
whole store is 700 M operations (344.6 M spends + 355.5 M creates), so the
lifetime saving is **~3 seconds**. `fx` stays as the probe's default because it
is free, but nothing here justifies adding a hashing crate to any crate that
matters.

**Settled: pallas's hash is not slow.** cryptoxide's blake2b measures ~0.9–1.2
GB/s here, within spitting distance of a SIMD implementation, so replacing it
buys under a minute of a whole-store wall. What the table *does* say is that
hashing is a **floor**: 12–18% of decode is 2.7 minutes of a whole-store pass at
these rates, and no decoder rewrite removes it — every tx hash is an outref key.

**Live lever 1: the frame walk is ~20% of decode — and it is removed in §2.8.**
`split_blocks` re-walks every byte generically (`Decoder::skip`) to find block
boundaries, and then the decode walks the same bytes structurally. It is
consistent across all three eras (0.01/0.05, 0.03/0.15, 0.02/0.10) and its rate
is high (752–1343 MB/s), so it was a fixed ~20% tax on the decode stage. The fix
turned out not to need the `.secondary` sidecar at all: a decoder knows where it
stopped, so the boundary is free — see §2.8.

**Live lever 2 — and the biggest: the apply is a memory-hierarchy problem, and
the fix is already specified.** Apply ns/op against set size, same binary, same
box:

| set | ns/op |
|---|---|
| 70,082 | 146 |
| 342,820 | 196 |
| **10,911,674** (whole-store `fold-full.log`: 551 s / 700 M ops) | **787** |

5.4× the cost for a 155× larger set. That is the price of random access into a
`HashMap` that has outgrown the cache: the table is ~700 MB, the key is 36 bytes
and the value 24, and every probe is a DRAM access with a TLB walk behind it.
No hasher touches that. `LIVE_BOOK.md`'s `by_outref` — 12-byte permutations plus
a 2^21-bucket directory — is exactly the structure this measurement argues for,
and the fold should build *in that layout* rather than materialise a `HashMap`
first and sort afterwards. A 2–4× on the apply stage is 4–5 minutes of a
whole-store pass, which is the largest single CPU cost left that is not
`MultiEraBlock::decode`.

**Also changed, below the resolution of these windows:** the chunk buffer is now
reused across chunks (`fs::read` allocated and freed 5–40 MB per chunk, which
glibc serves from `mmap`, so every chunk re-faulted its whole extent), and the
RSS sampler runs every 1024 blocks instead of 128 — the latter removes
`smaps_account`'s ~4% from every wall figure we have quoted so far. Neither is
visible in a 20-chunk window; both are per-chunk costs that a whole-store run
(9,181 chunks) pays.

### 2.8 The frame walk, removed: `--cursor`

`split_blocks` walks every byte with `Decoder::skip` to find block boundaries,
and then the decode walks the same bytes again. A `minicbor::Decoder` knows
where it stopped, so decoding the block and reading `position()` gives the
boundary for free — one pass instead of two, no `.secondary` sidecar to parse or
trust, and nothing a streamed snapshot lacks.

| window | `--primitives` (decode + split) | `--cursor` (one pass) | saved |
|---|---|---|---|
| Conway 9161–9180 | 0.05 + 0.01 = 0.06 s/ch | 0.05 s/ch · 164 MB/s | **17%** |
| Babbage 3348–3367 | 0.15 + 0.03 = 0.18 | 0.15 · 259 MB/s | **17%** |
| Alonzo 1848–1867 | 0.10 + 0.02 = 0.12 | 0.10 · 170 MB/s | **17%** |

Counters identical on all three windows, as in §2.6. The implementation is also
the skeleton of the era-dispatched decoder §5.3 argues for: the tag is read once
per block and the era's type is decoded monomorphically (`ShelleyBlock`), with
Byron and the epoch boundary falling back by construction rather than by a
runtime branch.

Store-wide the frame walk is 3.1 minutes (0.02 s/chunk at 1187 MB/s), which is
9% of decode+split over the whole chain against 17% on recent-era chunks — the
early chunks are cheap to skip, so the figure that matters going forward is the
17%.

### 2.9 The whole-store run, with the chain probe (`ab-full.log`)

90.6 min wall over 9,181 chunks, one process, the shared projection running
alongside the lean pull:

| stage | s/chunk | total |
|---|---|---|
| lean decode | 0.22 | 33.7 min |
| ↳ `txs()` clone (inside decode, not additional) | 0.0496 | 7.6 min (**22.5% of decode**) |
| frame walk (outside decode) | 0.02 | 3.1 min |
| read | 0.02 | 2.8 min |
| apply (peak set 11,271,535) | 0.05 | 7.7 min |
| `decode_tx` (the shared projection) | 0.12 | 18.4 min (**+55% of the lean decode**) |
| **wall, all of it** | | **90.6 min** |

Stages that are not subsets of another sum to **65.7 min**, so **24.9 min is
unaccounted for** — and the arithmetic points at the RSS sampler: 109,092
`smaps_rollup` reads over the run at 128-block intervals, i.e. **~13.7 ms of
kernel page-table walking per sample** on a ~2.17 GiB heap. That is the probe
measuring itself, a quarter of its own wall. It is consistent but not yet
confirmed: a whole-store run with the 1024-block sampler should leave a residual
under a minute, and until one exists the attribution is arithmetic. The stage
timings are unaffected either way (the sampler sits outside both timers), but
**every wall figure quoted before 2026-09-22 carries it**, including the 1.26 h
fold in `MITHRIL_PHONEBOOK.md` §5b.

The store-wide reconciliation held: the eager projection and the fold agree on
123,870,518 txs and 355,490,215 outputs.

### 2.10 A second machine, 100-chunk windows: what the deep walk costs, and what it carries

Re-measured locally — macOS aarch64, 10 cores, 24 GiB — against a full mainnet
snapshot pulled with `mithril-client` (215.02 GiB, 9,195 chunks `00000`–`09194`, on
an external SSD; two chunks newer than the box's). 100-chunk windows rather than
20, and peak RSS read from `/usr/bin/time -l` because the in-process sampler is
`/proc`-only:

```sh
/usr/bin/time -l ./target/release/fold-probe fold \
    --immutable /Volumes/cardano/mithril/db/immutable \
    --start <chunk> --chunks 100 [--cursor | --chain-decode]
```

Raw logs `.tmp/ab/{tip,bab,alo}-{cursor,chain}.log` are scratch — the command above
is the durable artefact.

| window | era | mean chunk | `--cursor` | lean (`MultiEraTx`) | `chain` | deep total | deep/cursor |
|---|---|---|---|---|---|---|---|
| 9094–9193 | Conway (tip) | 7.2 MB | 0.0132 s/ch · 546 MB/s | 0.0230 · 313 | 0.0147 · 490 | 0.0377 · **191** | **2.9×** |
| 5000–5099 | Babbage | 34.1 MB | 0.0499 · 684 | 0.0937 · 364 | 0.0561 · 608 | 0.1498 · **228** | **3.0×** |
| 2500–2599 | Alonzo | 63.5 MB | 0.0629 · 1009 | 0.1116 · 569 | 0.0908 · 699 | 0.2024 · **314** | **3.2×** |

`s/ch` is derived from each run's printed MB/s and mean chunk (two decimals round
these into uselessness). The columns are three walks of the same bytes:

- **`--cursor`** — one pass, no `MultiEraTx` (§2.8). The fastest lean walk that
exists today.
- **lean (`MultiEraTx`)** — the same accounting through `block.txs()`. This is what
a `--chain-decode` run prints as `decode` MINUS `chain`, i.e. the walk the
projection is bolted onto *today*.
- **`chain`** — `decode_tx`, the eager projection: the thing that yields
`witness_datums` and `aux_data` (§2.1's `shared` column, §2.9's `decode_tx`).

The projection is **+64% / +60% / +81%** of the lean decode it rides on, against
§2.1's 20-chunk figures for the same eras (46 / 45 / 74) and §2.9's store-wide
+55%. The walk-to-walk gap is machine-and-window sensitive — a second allocator and
5× the window — but the magnitude is stable across all of it: **the deep fields
cost about half again as much decode as the walk that produces them, and no more
than that.**

**RSS is not where the deep walk costs.** Peak RSS, same windows:

| window | `--cursor` | `--chain-decode` | Δ | window's live set |
|---|---|---|---|---|
| Conway tip | 95.5 MB | 99.3 MB | +3.8 MB | 239,360 |
| Babbage | 264.6 MB | 275.9 MB | +11.3 MB | 797,601 |
| Alonzo | 491.0 MB | 498.3 MB | +7.3 MB | 1,511,177 |

Peak RSS tracks the window — its chunk size and its live set — and the projection
adds single-digit MB on top (1.5–4.3% of the total). Materialising the deep fields
is a **CPU** cost, not a memory one, which is the form of the answer the 8 GB host
needs.

**What the deep walk carries, by era** — per 100-chunk window, from each run's
`chain` line. This is the sizing table for any sink built on these fields:

| window | era | inline datum CBOR | witness datums | aux data |
|---|---|---|---|---|
| 9094–9193 | Conway tip | 47 MiB | 42,657 · **6 MiB** | **101 MiB** |
| 5000–5099 | Babbage | 30 MiB | 933,244 · **183 MiB** | **161 MiB** |
| 2500–2599 | Alonzo | 0 KiB | 1,173,345 · **121 MiB** | **332 MiB** |

**The tip is the outlier, not the rule**: witness datums are 6 MiB per 100 tip
chunks against 121–183 MiB in the 2021–2023 windows, so a table sized from the
newest window comes out an order of magnitude small. And **`aux_data` is the larger
one** — 101–332 MiB per 100 chunks, tens of GB store-wide, against single-digit GB
for the datum set.

⚠️ Those are counts of *instances*, not distinct hashes: the same datum witnessed
twice is two entries and two lengths, so a hash-keyed table is smaller (never
larger). Three windows is 3.3% of the store — band-setting figures, not a census.

**A `datum.tbl` built from those fields** (`fold-probe fold --datum-tbl`, same three
windows, 2026-09-24) — the artifact a hash-attached datum's payload needs:

| window | distinct hashes | payload | entry array | dedup | attached outputs answerable at creation |
|---|---|---|---|---|---|
| Conway tip | 18,096 | 2.7 MB | 0.7 MB | 56% | **98.95%** |
| Babbage | 373,559 | 80 MB | 14 MB | 56% | **95.20%** |
| Alonzo | 625,471 | 69 MB | 24 MB | 43% | **67.55%** |

The entry array is `40 B × entries + 28 B` exactly (the file sizes check it), so
**34 KB per chunk at the tip against ~970 in the 2021–2023 windows** — a
whole-history table is single-digit GB. Verified two ways: every payload rehashes
to the key it sits under (1,504/1,504 in a 5-chunk table), and five sampled hashes
come back **byte-identical** from Koios, which reads the chain's own witness sets
— so the table's key space and bytes are the chain's, not this decode's idea of
them.

⚠️ **One counter disagrees between the Alonzo walks, and it should not.** Conway
and Babbage agree to the unit on txs, outputs, asset entries, created `lovelace`,
spends, same-chunk and misresolved. Alonzo: the deep run reports **7,666,530
spends / 1,006,971 misresolved** against the cursor run's **7,666,553 / 1,006,994**
— 23 transactions, all in the misresolved bucket, on a window identical in every
other respect (2,735,646 txs, 1,511,177 entries, 866,751,548,374 ₳ created). The
"identical counters" claims in §2.6 and §2.8 hold on their 20-chunk windows; 23 in
7.7 M would not show there. §2.3's differential is the right shape to catch this —
it needs the spends counter in the guard, and this window pinned as its test.

**What it changes.**

- **The deep walk is still decode-bound; the cursor walk is not.** In the pipelined
projection the cursor run reads `(blocked on apply)` at P=8 on Alonzo and P=16 on
Babbage, while the deep run reads `(blocked on decode)` at those widths (reaching
apply only at P=16 on Alonzo, never in the tip window). The projection hands decode
back the headroom §2.8 took out of it — which is what makes it affordable on a
many-core host, and why the apply stage is now what a cursor-speed walk waits on.
- **The two knobs cannot be combined today, and combining them is worth 1.3–1.4×.**
`--cursor` and `--chain-decode` conflict because `decode_tx` takes a `&MultiEraTx`
and so *requires* the walk §2.8 replaced. The bound is `cursor + chain` against
`deep total` — 0.0279/0.1060/0.1537 s/ch against 0.0377/0.1498/0.2024 — and the
gap is exactly the `MultiEraTx` materialisation the projection does not need. That
is the prize §5.3's hand-rolled decoder plays for.
- **It prices the sink.** A `datum.tbl` built from these fields costs +60–81% of
the decode it rides on — §2.9's whole-store run put the same projection at 18.4 min
of a 90.6 min wall — and single-digit MB of RSS. Affordable now, which means the
hand-rolled decoder is not a prerequisite for the sink: it is the 1.3–1.4× above,
and separately what keeps a datum-carrying walk on §2.8's one-pass path.


## 3. What the consumers actually read

| | market-ledger | project-ledger | token-ledger | pool-observe | the book (target) |
|---|---|---|---|---|---|
| address | bech32 | bech32 → credential | bech32 → credential | bech32 → credential | credential + delegation only |
| lovelace | ✓ | ✓ | ✓ | ✓ | ✓ |
| assets | ✓ | filter, then re-reads the raw tx | filter, then re-reads the raw tx | ✓ | (policy, name, qty) |
| index | ✓ | ✓ | ✓ | – | ✓ |
| datum_hash | ✓ | ✗ | ✓ | ✓ | ✓ |
| inline_datum | ✓ | ✗ | ✓ | ✓ | ✓ |
| **min_utxo** | ✗ | **✓ (only reader)** | ✗ | ✗ | ✗ |
| inputs / outref | ✓ | ✓ | ✓ | – | ✓ |
| **inputs.redeemer** | ✓ (only reader) | ✗ | ✗ | – | ✗ |
| witness_datums | ✓ | ✗ | ✓ | – | ✗ |
| **aux_data** | ✓ (jpg labels-50) | ✗ | ✗ | – | ✗ |
| **required_signers** | ✓ (only reader) | ✗ | ✗ | – | ✗ |

Three fields have exactly one reader each and are materialised for everyone.
`pool-observe` does not even consume a `DecodedTx` — it was given
`&DecodedOutput` directly, because the tx-level half was dead weight at its
call site.

The address column is the load-bearing row. Every consumer wants a **28-byte
credential**; the projection hands out a **bech32 `String`**, and every consumer
that wants the credential parses the string back
(`mitos_cohort::payment_cred`, `token-ledger/walk.rs:84`,
`market-ledger/row.rs:111`, `project-ledger/party.rs:82`,
`mitos-marketplace-decode/sales.rs:392`) — a base32 round trip, 94–102
characters wide, per output, to get back the bytes the decode started from.

## 4. There are three of these shapes today

| shape | where | consumer |
|---|---|---|
| `DecodedTx` / `DecodedOutput` / `DecodedInput` | `crates/chain-decode/` | market-ledger, project-ledger, token-ledger, pool-observe |
| `TxDraft` / `OutputDraft` | `crates/mitos-data-plane/src/block_events.rs::project_tx` | the live host's own event projection |
| `DecodeTx` / `TxInput` / `TxOutput` | `crates/mitos-marketplace-decode/` | the marketplace decoders, mapped into by callers |

The second is a near-duplicate of the first — same canonical-index redeemer
resolution, same pallas calls, kept in step by hand. The third exists because a
consumer needed a shape the first did not offer (a spent input carrying its
resolved value, a datum carrying payload/hash/kind, hex `AssetId` rather than
byte vectors) and built its own rather than extend it.

That duplication is the strongest argument for the redesign below, and it is not
a performance argument: **the projection should exist once, and its shape should
be decided by what consumers pull, not by what the first consumer happened to
need.**

### 4.1 The asset side has the same problem, and it now has a measurement

`cardano-assets` carries three shapes for *assets*: `Asset` (the metadata
flatten), `AssetWithId` (plus a CID list — the workaround for the media `Asset`
drops, and IPFS-only) and `AssetV2` (the published wire type: `Asset` plus an id).
A fourth, `DecodedAsset`, is written in that crate — and as of the walk's move, so is
the reader that produces it off the CBOR: `AssetEnvelope::into_decoded_asset` for a
document, `metadatum::decode_document` for a `Metadatum`
(`ROW_ADDRESSING_AND_SEARCH.md` §5.4). The two readers are compared asset for asset
over the whole of mainnet: **10,957,768 equal, 200,132 differ — 200,131 of them
`media_type`, split by a counter rather than derived: 187,791 a spelling table, 11,973
a declared value that is not a type, 367 a declared type with no medium to attach it to
— and 0 `traits` and 0 `image` differences — 157 declined, every one an id `AssetId`
refuses.** `ROW_ADDRESSING_AND_SEARCH.md` §5.5 has the tables, the media-type
catalogue (`NftMimeType` plus `Undefined` and `Custom`, and what the corpus puts in
each), the per-medium census (headline and `files[]` counted apart, and where a sniff
is the only answer) and what each difference turned out to be.

## 5. The proposed shape

Two mechanisms, and they are separable — the second is the one that matters, the
first is what makes it expressible.

### 5.1 A budget instead of a fixed union

```rust
/// What a caller wants materialised. Nothing here is on by default, and this
/// is a plain newtype — no `bitflags` dependency for four bits.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Want(u16);

impl Want {
    pub const NOTHING: Want = Want(0);
    pub const ASSETS: Want = Want(1 << 0);
    pub const DATUM_HASH: Want = Want(1 << 1);
    pub const INLINE_DATA: Want = Want(1 << 2);
    pub const MIN_UTXO: Want = Want(1 << 3);

    pub const fn or(self, other: Want) -> Want {
        Want(self.0 | other.0)
    }
    pub const fn has(self, other: Want) -> bool {
        self.0 & other.0 != 0
    }
}
```

`MIN_UTXO` stays in the vocabulary and stays off. `DecodeLevel` on the read path
is the precedent, and the doc that recorded choosing named tiers over a field
mask argued the axis there was *how much resolution work*, three tiers. Here the
axis is *which fields*, and the field set is genuinely heterogeneous (§3), so a
mask is right and a level ladder is not.

### 5.2 A sink, so the caller's accumulator is the projection

```rust
/// Decode drives; the caller accumulates. No intermediate `DecodedTx`.
pub trait BlockSink {
    /// Cheap facts always; asked once per output, before any payload is built.
    /// `None` means "nothing here, skip the output entirely".
    fn want(&mut self, o: OutView<'_>) -> Option<Want>;

    fn input(&mut self, tx: TxView<'_>, o: OutRef, redeemer: Option<&[u8]>);
    fn output(&mut self, tx: TxView<'_>, idx: u32, o: OutView<'_>, want: Want);
    fn end_tx(&mut self, tx: TxView<'_>);
}

pub fn walk_block(block: &MultiEraBlock<'_>, sink: &mut impl BlockSink);
```

`OutView`/`TxView` are **borrowed** — `&'b [u8]` for the address with the
credential decoded on demand, `&'b [u8]` for data, pallas's own borrowed
structures elsewhere. Nothing is copied that the sink does not copy itself.

Why a sink rather than a struct with a budget:

- It removes the per-output/per-tx **allocation**, which §2.1 says is the actual
  cost — a budgeted struct still builds a row per output, empty or not.
- The per-output decision is a *predicate over cheap facts*, which is a method
  on the sink, not a flag the caller sets in advance.
- It is what the phonebook needs: **one walk, N artifacts** is two sinks, or a
  `Tee<A, B>`. The fold's `BlockPlan` is already the shape (`plan_block` pulls
  five facts into flat arenas), and it is the fastest decode measured so far.
- It makes the cost visible. A method called `want` that runs per output, after
  the cheap facts are in hand, cannot be mistaken for a free getter the way
  `txs()` and `o.datum()` are.

The crate's own rules survive intact: no I/O, no `Result`, no runtime, no notion
of what a watched output is (the sink decides). A sink that fills a `Vec` is the
test fixture, so a single tx stays unit-testable.

### 5.3 Decoder principles for the hand-rolled path

Recorded 2026-09-22 as a direction rather than a measurement: **threaded decode
is the approach** (§7), and the decode should be written with **fewer branches
and fewer instructions**. Concretely, in this code, that means the following —
and the first thing to notice is what the current path costs *per output*:

| step | what it costs |
|---|---|
| `tx.outputs()` | one `Box<Cow<..>>` **per output**, plus the `Vec` |
| `o.address()` | an `Address` parse, era matched |
| `address.to_string()` | a bech32 `String`, 94–102 chars |
| `o.value()` | a `MultiEraValue` wrapper, era matched |
| `Vec<Asset>` | the vec, plus a `Vec<u8>` for each policy and each name |
| `o.datum()` | a `KeepRaw` **clone** — the whole `PlutusData` tree when inline |

Six polymorphic steps and six allocations before a consumer has read anything,
repeated for all 355 M outputs. That is the "complex branching" being paid for.

1. **One loop per era, chosen once per block.** Not `match era` inside the field
   loop. `MultiEraOutput`'s every accessor (`address`, `value`, `datum`, …)
   re-enters an era match and boxes its result; a monomorphic per-era loop has
   no enum, no `Box`, no `Cow`, and lets the compiler keep field offsets in
   registers.
2. **The budget as a const generic, not a runtime mask.** `fn decode<const W:
   u16>(..)` makes "this consumer never reads `min_utxo`" a compile-time fact, so
   the field's code is not branched around — it is *absent*. Cost: one
   instantiation per era × consumer. Worth measuring against icache before
   committing, since the goal is fewer instructions, not more.
3. **Positional reads against canonical key order.** A tx body is a CBOR map, and
   pallas's generated decoder loops its entries with a `match` on the key plus a
   generic `skip()` for anything unrecognised. Cardano's encoders emit canonical
   (ascending-key) maps, so a body reader can compare-and-advance with no
   dispatch table at all. ⚠️ **Not verified against mainnet bytes.** Assume it,
   then prove it on the corpus — a decoder that assumes it and is wrong fails on
   123.6 M recorded `(offset, hash)` pairs rather than silently.
4. **Borrowed everything.** `&'b [u8]` for the address and for data, with the
   28-byte credential decoded once and handed to every consumer that wants it;
   no `Box`, no `Cow`, no `Vec` the sink did not ask for.
5. **No `Result` on the hot path** — already the crate's rule, and it is also an
   instruction-count rule: every `?` is a branch the ledger has already made
   unnecessary by validating the block.
6. **Address decoding is multi-*type*, not multi-era — and we should not build
   an `Address` at all.** Knowing the block's era fixes where the address bytes
   live (Byron: a structured payload + CRC; Shelley onward: a byte blob), which
   is one branch per block, free in a monomorphised loop. It does **not** fix
   what the bytes *mean*: `Address::from_bytes` dispatches on the **high nibble
   of byte 0** across eleven arms, and type 8 is a **bootstrap (Byron-era)
   address**, which can appear in a Conway output because Byron-era funds are
   spendable forever. So the multi-era question is settled by the block, and the
   multi-type one is not.

   None of the consumers want an `Address`: they want a **28-byte payment
   credential** (+ script flag) and, sometimes, a stake credential. For types
   0–7 that is the fixed slice `b[1..29]`, and types 0–3 carry the stake part at
   `b[29..57]`. Only three shapes need more: type 8 (a real Byron decode), types
   4–5 (pointer — payment credential present, **no** stake credential in the
   address), types 14–15 (reward — no payment part, never a UTxO address).

   So the hot path is `b[0] >> 4` and a fixed slice, with a cold arm for 8.
   ⚠️ **How cold is unmeasured** — the type histogram across the store has not
   been taken, and it decides whether the bootstrap arm needs a fast path or
   just needs to be correct. Cheap to add to the probe: count `b[0] >> 4` per
   output, which needs the raw bytes and therefore a match on the output's era
   variant rather than `o.address()`.

**What this is not.** A claim that branch count is where the time is. §2.5
settles it: **2.2% branch-misses, unchanged by adding a projection 47% larger**,
while `malloc`+`cfree` take 26% of the run and `txs()`'s clone takes 16.4%. The
instruction count is high because of **what gets built** — BTreeMaps per value,
a `Box<Cow>` per output, cloned witness sets, `PlutusData` trees nobody reads —
not because the control flow is hard to predict. Fewer branches is the *shape*
of the fix; fewer structures is the *substance* of it.

## 6. What this buys

- **The walkers**: 45–231% off their decode, and the walkers re-derive
  everything the book cannot serve. `tx-index` already has its own lean
  extractor; the three `*-ledger` tools do not, and they are the ones paying.
- **The book's builder**: it can be one pass with two sinks (rows for the arena,
  counters for the report) instead of a fold plus a re-walk.
- **The decoder work**: ~20% of decode is `txs()`'s clone, available before any
  hand-rolling starts, and it composes with whatever else the hand-rolled
  decoder skips.
- **Not bought**: the projection is not the bottleneck of a fold. Apply is one
  core and serial by consensus; §2.1's savings are on the stage that was already
  the parallelisable one. Read §2.2's 20% as the part that shortens the *wall*.

## 7. Decided / Open

**Decided**

- A partially-applied function is not the technique. Not built, not proposed.
- `decoded` shape: the projection becomes one thing, not three (§4). Which
  arrives first is a scheduling question, not a design one.
- Borrowing over copying for the address, and a credential the consumers can use
  without a base32 round trip. The bech32 string stays available — two consumers
  store it — but behind an explicit call, not in the default row.
- **The lean walk becomes the primitives walk** (2026-09-22, §2.6). Measured at
  1.9× on three eras with identical counters, no CBOR hand-rolled. The
  `MultiEraTx` accessors go: `txs()`, `outputs()`, `consumes()`, `assets()`,
  `datum()` — each is either an allocation per element, a clone, or both.
- **Threaded decode is the approach** (2026-09-22). The unit of parallelism is
  the **chunk** — its own file, its own frame walk, its own decode — with N
  decoders feeding one applier through an ordered, bounded queue; `BlockPlan`
  is already the hand-off struct and is owned and flat by design. What leaves
  the applier's thread: `split_blocks`, `MultiEraBlock::decode`, `txs()`,
  `plan_block`. What stays: `apply_plan` and checkpointing. The scaling to
  expect is `count`'s on 150 identical chunks: 14.2 s at one thread → 1.9 s at
  eight, i.e. **7.5×**, 94% (`MITHRIL_PHONEBOOK.md` §5b). Do not quote the
  whole-store `count` rate as a scaling figure — 725 MB/s over the whole store
  is a different work mix from 120 MB/s of single-threaded decode and the two do
  not divide. The ceiling is `max(decode/P, apply)`. Ordering is not negotiable:
  chunks are applied in chunk order, txs in tx order within a block
  (`BlockPlan::spans`).
- **Build the sink (§5.2), not the budgeted struct** (2026-09-22), landed one
  consumer at a time. Rejected first: **A** (`decode_tx(tx, &Want) -> DecodedTx`,
  cheapest to land, keeps every call site's shape, but still allocates a row per
  output and does not touch the `Box<Cow>` or the BTreeMaps) and **C** (A then B,
  which only buys a second rewrite). §2.5 is the reason: the cost is what gets
  *built*, so a mask on a struct that still builds it recovers little.
  Staging is by consumer, not by mechanism — the trait plus a `VecSink` test
  fixture, then the fold's `plan_block` as the first real sink, chosen because
  its output reconciles against the whole-store counters (§2.3), so a
  mistranslation surfaces as a mismatch rather than as plausible rows. The three
  `*-ledger` tools follow once the shape has survived that.

**Open — needs a decision**

- Whether the budget is a **const generic** (§5.3 item 2) or a runtime value.
  Const-generic makes unread fields vanish at compile time — the strongest
  version of "fewer instructions" — at the cost of one instantiation per era ×
  consumer. Measure icache before committing. The sink removes the *allocation*
  either way; this only decides whether the read itself costs a branch.
- The one thing that could force a change of approach: **a sink must own what it
  keeps.** The threaded decode crosses a thread boundary, so a borrowed
  `OutView<'b>` can live only inside the decoder thread; anything the applier
  sees has to be owned. That is fine — the book packs bytes into its own arena
  anyway — and it means the trait must not pretend to lend. If a consumer turns
  out to genuinely need a borrow held across the boundary, that consumer keeps
  the budgeted-struct path rather than bending the trait.

**Open — measurement owed**

- The whole-store `fold --chunks 0 --primitives` and `--cursor` runs: the windows
  in §2.6–2.8 are 1.1% of the store, and their controls were idle-box. Byron
  falls back, so the run is mixed — which the `fallback` line reports.
- **The sampler residual, confirmed or refuted.** §2.9's 24.9 min is arithmetic;
  a whole-store run with the 1024-block sampler should leave a residual under a
  minute. That run is now free to start (`ab-full` finished 90.6 min,
  2026-09-22).
- Per-field attribution. The counters say *how many*; they do not say *which
  field costs what*. The cheap version is one window with each field
  independently disabled inside a scratch copy of `decode_tx`.
- The **address-type histogram** across the store (§5.3 item 6) — how often each
  of the eleven arms actually fires, and in particular how many outputs still
  carry a bootstrap address. It decides whether that arm is a fast path or a
  correctness path.
- Why `count`'s instantaneous rate falls 1642 → 725 MB/s across the store
  (`count-glibc.log`): richer Conway bytes per byte, or a cold page cache after
  the first few hundred chunks. It matters to a pipeline's floor — if the store
  reads at ~750 MB/s cold, no amount of decode threading gets under ~5 min.
- What `MultiEraBlock::decode` itself spends on `PlutusData` trees nobody reads.
  The `datum clones` counter (§2.4) prices `decode_tx`'s own `o.datum()`, not the
  block decode's. This is the corpus/harness work — a decoder that keeps a datum
  as bytes is the experiment.

## 8. Reproducing

```sh
# the store is on the ARM box; the probe builds there (native glibc — the musl
# cross-build is 2.2x slower and anti-scales, see MITHRIL_PHONEBOOK §5b)
BOX=root@v2202604354294454929.supersrv.de
for d in crates/chain-decode tools/mitos-chain-walk tools/fold-probe; do
    rsync -avz "$d/" "$BOX:/opt/scratchpad/mitos-src/$d/"
done
rsync -avz Cargo.toml Cargo.lock "$BOX:/opt/scratchpad/mitos-src/"
ssh "$BOX" 'cd /opt/scratchpad/mitos-src && cargo build --release -p fold-probe'
```

Window boundaries used above (chunk = slot / 21600): Shelley 208, Mary 1068,
Alonzo 1848, Babbage 3348, Conway = newest.
