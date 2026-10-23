# Phonebook coverage — the module surface against the book

> **Status: analysis (2026-09-22), with `by_policy`, `by_addr`, the serving
> reader, and a `BookDataPlane` over them built (2026-09-23).** A reading of the two
> surfaces that already
exist — the wasm `chain-data` host functions in
> `crates/mitos-platform/wit-v2/world.wit` and the `ChainDataPlane` trait they
> project from in `crates/mitos-data-plane/src/lib.rs` — against what the live
> book can serve. §7 onward is stronger: it is grepped from the *call sites* in
> `community-modules/`, so it is what shipped modules ask for rather than what
> the surface permits. **Read from the code, not executed**: no module has been
> run against a book. §6 and §9 are measured, and §11 says what is left to
> close the loop.

Related:

- `LIVE_BOOK.md` — the artifact this maps onto
- `MITHRIL_PHONEBOOK.md` — why the book exists at all
- `MITOS_DATA_PLANE_API.md` — the trait, and the transport story
- `FOOTPRINT_AND_MINIMUM_SPEC.md` — the box the book has to fit on
- `DECODE_SHAPE.md` — the walk that builds it

## 1. The question

Mitos is moving off dolos's backing stores. What modules call is a *named*
surface — 12 wasm host functions projecting a 21-method trait — and **none of it
has been checked against the book.** The risk is not that the book is slow; it
is that the book cannot answer a question the surface asks, and that this is
discovered after the data plane is cut over.

So: one row per method, and what the book would need to answer it.

## 2. Two surfaces, not one

| surface | where | who sees it |
|---|---|---|
| wasm host functions | `wit-v2/world.wit`, `interface chain-data` | community modules in wasm |
| `ChainDataPlane` | `mitos-data-plane/src/lib.rs` | `LocalDataPlane`, the v2 dispatcher, test stubs |

The trait is the superset; the wasm interface is a projection of it, plus two
composites the host owns (`cip25-metadata`, `cip25-metadata-batch`). **The wasm
surface is the one that decides** — it is what a module can ask for — so it has
its own table and the trait-only extras follow.

Legend for *today*: `shipped` · `NotYetImplemented` (the impl returns it today,
so the book not serving it is not a regression) · `narrowed` (implemented for
one shape only) · `—` (not checked in this pass).

## 3. The wasm surface (12 host functions)

| host function | what it needs | book | today |
|---|---|---|---|
| `read-utxos` | point + bulk lookup by outref → address, value, datum | `by_outref` + row | **built, verified (§6)** |
| `utxos-by-address` | refs by address, paged | `by_addr` (`B1`) | **built, verified (§6)** |
| **`utxos-by-policy`** | refs by 28-byte policy, paged | **nothing — absent from `B1`** | **built** (§4a) |
| `utxos-by-payment-cred` | refs by 28-byte payment credential, paged | `by_addr` | **built** (§4a) |
| `resolve-stake-for-payment-pkh` | one ref for a payment pkh, then parse its stake part | `by_addr` + row | **built** (§4a) |
| `read-output-datums` | datum per output, caller-blind to inline-vs-hash | inline: the row ✓ · hash: `datum.tbl` ✗ | — |
| `tx-metadata` | aux-data CBOR by tx hash | — (history) | — |
| `datum-by-hash` | CBOR by datum hash | `datum.tbl` | — |
| **`asset-state`** | per-asset mint ledger | — (**a different fold**) | — |
| `cip25-metadata` | `asset-state` → `tx-metadata` → label-721 decode | — | — |
| `cip25-metadata-batch` | as above, batched per page | — | — |
| `read-tx` | full tx rollup by hash | — (history) | — |

## 4. The gaps, by what kind of thing closes them

Four classes, and only the first is a matter of building what `LIVE_BOOK.md`
already specifies.

### 4a. A hole in the design — `utxos-by-policy`

`B1` names exactly three orderings over the rows: `by_outref`, `by_addr`, and
creation order. **There is no `by_policy`.** But the WIT describes dolos's
`BY_POLICY` as "a first-class index" and names the consumers:

> cold-start hydration of per-policy holder ledgers (`holder-distribution`),
> NFT-collection indexers, vesting-contract scanners

`holder_count` (§5) needs it too. The rows already carry the policy as an
interned `u32` (`B2`), so the material was present and only the *reverse* index
was missing — a third permutation in the pattern `B1` already describes.

**Built 2026-09-22.** `by_policy.idx` + `policy.dir.idx`, 12-byte
`{u64 prefix, u32 row}` entries like every other ordering, sourced from the
already-required scan of `rows.bin` so it costs **no extra I/O and nothing in
the hot path**. One entry per `(row, policy)`, not per asset name — a row holding
two assets of one policy is one entry, which is what makes an enumeration return
a ref once. Measured: 13,912 rows → 16,378 entries on three Conway chunks, but
**10,911,674 rows → 10,946,451 entries on the whole store — 1.003 per row**. The
window's 1.18 was that window's composition, not the shape: at full scale almost
every live row holds at most one policy.

Because it is derived from the row store it also needs no walk: `fold-probe
reindex` rebuilds it against the whole-store artifact in **5.5 s**, against
~50 min for a refold. That is the difference between adding a view and re-reading
the chain, and the same path repairs `dir.idx` in place without touching
`rows.bin`, `by_outref.idx` or `policy.tbl`.

The entry *could* be 8 bytes (`{u32 policy id, u32 row}`) if policy ids sorted
like the policy. They are assigned in creation order today, and making them
sort-preserving means rewriting the id inside every row's asset records — so the
44 MB saved would cost a second policy numbering, and the same 12-byte shape as
`by_outref` buys the shared writer and the shared directory fill instead.

**And the one `B1` did design: `by_addr`, built 2026-09-23.** Three wasm host
functions project onto it — `utxos-by-address`, `utxos-by-payment-cred` and
`resolve-stake-for-payment-pkh` — and all three are ref-then-confirm over the
same entries, which is why one ordering serves them (`LIVE_BOOK.md` §B3a). The
derivation is `tools/fold-probe/src/address.rs`: a slice at `address[1..29]` for
the nine Shelley shapes, a CBOR parse for Byron. What that turned up is the
finding worth carrying forward — **384,625 live rows (3.52%) are Byron**, so the
arm that cannot be sliced is not a corner case; and **zero are reward addresses**,
so the stake arm is unexercised by mainnet.

It cost the same as `by_policy` and for the same reason: the key is in the row, so
it is a reindex — **4.8 s over the finished artifact, 10,911,674 entries, peak RSS
494 MB**. A book's views are only expensive to add when their material is in
neither the rows nor the indexes (§4b), and this one was.

### 4b. Same walk, different artifact

`B7`'s "one walk, many artifacts" is the answer to these: the bytes are in the
log, a second pass is not needed, and the artifact is not the live set.

- **`datum.tbl`** — hash → payload, from witness sets. Closes
  `read-output-datums`' hash path, `datum-by-hash`, and `read_datum`. Phase 3,
  and still ⚠️ unknown in `LIVE_BOOK.md`'s Sizes.
- **tx location** — hash → (chunk, offset, len), for `read-tx`,
  `tx-metadata` and `slot_by_tx_hash`. **`tx-index` already holds 123.6 M of
  these records** (`TX_INDEX_DATASOURCE.md`), so this is a reuse question
  rather than a build.
- **script table** — `read_script` is `NotYetImplemented` today; the datum
  table is its natural home (`LIVE_BOOK.md` §B9).

### 4c. A different fold — the mint ledger

`asset-state` is not derivable from the live set. It answers *"which tx first
minted this asset, how many times has it been minted, and what is its net
quantity"*, which is history accumulated over every mint and burn, not a state
snapshot. `COLLECTION_MODULES.md` leans on it explicitly:

> Unlike `tx-metadata` this is *state*, not archive, so the mint pointer
> survives pruning

and `cip25-metadata` is built on top of it. **A live-set fold cannot answer it** —
but the same walk sees every mint in every tx body, so it is a *second bounded
artifact* rather than a new system: one row per `(policy, asset_name)`, with
`initial_tx`, `mint_tx_count`, net quantity. It is the largest genuine gap in
this table, and the one with a heavy consumer behind it.

### 4d. Not servable from the target box

- **`read-block`** needs a retained block body. `FOOTPRINT_AND_MINIMUM_SPEC.md`
  §7 already establishes that the 256 GB box cannot hold the 230.7 GB log, so
  this needs a decision — a retained window, or a remote source — rather than
  an implementation. It is trait-only, not exposed to wasm.
- **`archive_horizon_slot`** is a dolos-archive concept and changes meaning:
  under the book it becomes the depth of the delta chain, not an archive
  horizon.

## 5. The trait-only extras (12)

Not projected to wasm, but named by the trait, so IPC/gRPC transports and the
HTTP surface (`MINIBF_BRIDGE.md`) will meet them.

| trait method | book | today |
|---|---|---|
| `read_utxo` (singular) | `by_outref` | — |
| `search_utxos` | `by_addr` / `by_policy` + a predicate algebra | **narrowed** to `Match(asset: Policy(_))` |
| `output_datum_hashes` | the row (`datum_kind` + bytes) | — |
| `read_datum` | `datum.tbl` | — |
| `read_script` | a script table | **NotYetImplemented** |
| `total_supply` | a mint-aggregation index | **NotYetImplemented** |
| `holder_count` | `by_policy` | — |
| `read_tx`, `slot_by_tx_hash` | tx location (§4b) | — |
| `read_block` | a retained body (§4d) | — |
| `tip` | **served** — `book.meta`'s optional `base_slot`, answered as `ChainPoint::Slot`. A delta hangs *from* a boundary and cannot report one, so this was never the delta's to close (§12, `DELTA_CHAIN.md` §7) | — |
| `protocol_params` | era / governance state — not a UTxO fold | **NotYetImplemented** |
| `archive_horizon_slot` | the delta depth (§4d) | — |

Three of these are `NotYetImplemented` **today**, so the book's silence on them
is not a regression — it is the status quo. `search_utxos` is implemented for
one predicate shape, which the source says plainly is a query-planning gap
rather than an index gap.

## 6. What has been measured

`fold-probe verify` reads a published artifact the way a consumer does —
`dir.idx` → `by_outref.idx` → `rows.bin` — and holds the entries to the two
properties `B5` claims. Against the whole-store artifact:

```
entries    10911674
sampled    202069 resolved dir.idx → by_outref.idx → rows.bin
ordering   0 adjacent pairs out of canonical order
prefixes   0 rows whose own outref does not hash to the entry's prefix
rows       0 out of range · 0 unreadable
lookup     1458 ns · 0.3 s for 202069 samples
```

So `read-utxos` — the only wasm host function the book claims today — resolves
10.9 M rows with 202 k samples all correct. **One caveat on the timing:** it is
`seek + read_exact` per lookup with no mmap, against a 1.8 GB file that is only
partly in page cache, so 1458 ns is an upper bound rather than a like-for-like
comparison with a `redb` point read. It is the figure to beat, not the figure to
quote as the serving cost.

**2026-09-23 — all three orderings, every entry, no sampling.** The same check
now covers `by_policy` and `by_addr`, and each entry of each was resolved and
confirmed against its own row (10,911,674 + 10,946,451 + 10,911,674 = 32,769,799
confirms):

| ordering | entries | sampled | confirm |
|---|---|---|---|
| `by_outref` | 10,911,674 | all | **1247 ns** |
| `by_policy` | 10,946,451 | all | **31,288 ns** |
| `by_addr` | 10,911,674 | all | **1719 ns** |

Through the trait (`BookDataPlane`, 2,000 of 231,737 policies sampled, 34,177
unique outputs read): **1,926 ns per `utxos_by_policy` ref** and **20.4 µs per
`read_utxo`** — the second of which is 17× the raw lookup and, measured at both
decode levels, is entirely `TypedOutput`'s owned text rather than any resolution.
See §11 for what that implies.

`0` out of canonical order, `0` directory violations, `0` entries whose row does
not corroborate the prefix, `0` rows out of range or unreadable — on every one of
the three.

Two things about that table are worth reading carefully.

- **`by_policy`'s confirm is 25× `by_outref`'s, and it is not the index's fault.**
  Corroborating a policy entry means seeking into the row's asset records, reading
  each interned id, and resolving it through `policy.tbl` — several seeks per
  entry, against `by_outref`'s one. It is a *verifier* cost, not a serving cost:
  the checks are run once at publish over every entry, where 31 µs × 10.9 M is
  under six minutes. A lookup that only has to answer for the entries in one
  directory bucket pays it a few times, and the same walk is what a consumer's
  own decode does.
- **`by_addr` is 1719 ns — 1.4× `by_outref` — and it does strictly more work.**
  It re-reads the row's variable-length address and, for the 384,625 Byron rows,
  runs a CBOR parse. This is the number that decides whether `utxos-by-address`
  can replace a `redb` point read, and it is the first figure in this document
  that is about an ordering the module surface actually asks for.

**The reader found a writer bug on its first run.** `dir.idx` filled empty
buckets with the entry count instead of the next non-empty bucket's index,
which makes an empty bucket *precede* a non-empty one — not a partition, so not
what `B5` asserts and not what `verify_structure` (phase 2) will accept:

| artifact | directory violations |
|---|---|
| chunk 81, before the fix (7,776 rows) | 5,134 |
| chunk 81, after the fix | 0 |
| **whole store, before the fix (10,911,674 rows)** | **41,840** |

This was a **spec violation, not a data error**: the live set, the row store and
`by_outref.idx` are all correct — `prefixes 0` above is that, at 202 k samples.
Only the directory's fill was wrong. None of the three whole-artifact identities
could see it, because none of them reads the format. `dir.idx` is a pure
function of `by_outref.idx`, so an existing artifact's directory can be
regenerated without re-folding.

## 7. The consumer inventory

§3 was a reading of the WIT. This is what shipped modules actually call, grepped
from `community-modules/`:

| module | calls | book needs |
|---|---|---|
| `collection-holders` | `utxos_by_policy` | `by_policy` ✅ |
| `collection-metadata` | `utxos_by_policy`, `cip25_metadata`, `cip25_metadata_batch` | `by_policy` ✅ + mint ledger + aux |
| `holder-distribution` | `utxos_by_policy` — **twice**, the policy scan and the LP decomposition scan — plus `resolve_stake_for_payment_pkh` | `by_policy` ✅ + `by_addr` ✅ |
| `vesting-tracker` | `resolve_stake_for_payment_pkh` | `by_addr` ✅ |
| `burn-address` | `utxos_by_address`, `read_utxos` | `by_addr` ✅ + `by_outref` ✅ |
| `credit-address` | `read_utxos`, `read_tx` | `by_outref` ✅ + tx history |
| `cip-25-mint` | `tx_metadata` | aux, by tx hash |
| `asset-metadata-update` | `tx_metadata`, `datum_by_hash` | aux + `datum.tbl` |
| `cip-68-mint` | `datum_by_hash` | `datum.tbl` |

So five of the nine modules have everything they ask for except history and metadata,
and the three address-keyed call sites are unblocked. **None of this has been
executed** — the gap between "the index exists" and "the module works against it"
is §11, and it is the next thing to close.

Two things this changes about §3:

- **`utxos_by_policy` has five call sites**, more than any other gap, and it is
the only one missing from the *design* rather than merely unbuilt. §4a is the
highest-value gap here, and it now has callers rather than use-cases.
- **`datum_by_hash` and `tx_metadata` are each called by two shipped modules.**
They are not speculative, and neither can be served by a live-set artifact.

## 8. What the construction does not yet observe

§4 answers "which index is missing". This answers the harder half — **what the
walk never looks at**, which no amount of index work can compensate for.

`plan_block_primitives` — the fast path, and the one a whole-store artifact is
built with — reads exactly two things off a block: `b.transaction_bodies` and
`b.invalid_transactions`. Grepping the file for `witness`, `aux`, `script` and
`cert` returns only comments and counters.

| consumers need | what the builder must extract | today |
|---|---|---|
| `datum-by-hash` | witness-set datums, hash → CBOR | **already extracted** as `DecodedTx::witness_datums` — on the `--chain-decode` path only, summed into a count |
| `tx-metadata` | aux-data CBOR per tx | **already extracted** as `DecodedTx::aux_data` — same |
| `read-tx` | the tx rollup: inputs, outputs, aux, validity | mostly there; `tx-index` holds the locations |
| `asset-state`, `cip25-*` | the mint ledger — `body.mint` per tx | **not extracted anywhere**; `DecodedTx` has no `mint` field |
| `utxos-by-policy` | nothing new — policies are already read and interned | index only |
| `utxos-by-address`, `utxos-by-payment-cred` | the payment credential **derived** from the address, parsing the CBOR payload for bootstrap (type 8) | **built, verified** (§4a, §6) — in the row, so no capture was needed |

Three of the six are already being extracted on the slow path and thrown away.
That is the argument for the sink (`DECODE_SHAPE.md` §8): the bytes are in hand
and the destination is what is missing. The mint ledger is the one genuinely new
extraction; the credential is the one genuinely new derived field.

## 9. Answering them efficiently

**Measured through the reader** (2026-09-23), whole store, `crates/live-book`:

| query | cost |
|---|---|
| `Base::open` | **73 ms** — three directories validated, 10.9 M rows mapped |
| `utxo` (point) | **1223 ns**, over 589,363 distinct refs |
| `by_payment_cred` | **871 ns/ref** over a 19,768-ref run |
| `by_address` | **925 ns/ref** over a 19,724-ref run |
| all three, driven from `policy.tbl` | **110,498 refs/s**, 0 disagreements in 589,363 |

`read-utxos` is measured (§6) and so are the enumerations now. Three properties
make them cheap:

- **`by_policy` is 12 bytes an entry**, the same shape as `by_outref` — §4a has
why the 8-byte form was examined and dropped. Its size follows the *entry* count
rather than the row count, and that ratio is almost exactly 1: **10,946,451
entries for 10,911,674 rows** makes `by_policy.idx` **131 MB**, level with
`by_outref`, plus its own 8 MB directory. Like every ordering it is a permutation
over rows, so it reuses the row store rather than copying it.
- **Sequential, then random.** A policy's refs are a contiguous run in a sorted
permutation — a sequential, cache-friendly scan — and each ref is then one row
read at the measured cost. **A 1,000-ref page is ~0.9 ms** of row reads plus an
index slice: the 19,768-ref `$burnsnek` run enumerates at 871 ns/ref and its
rows read back at the same cost.
- **No materialisation.** The WIT says that for `utxos-by-policy` the host
"materialises it once at scan-start into a frozen native-memory cache and slices
it per page", because a dolos scan has no cheaper option. A memory-mapped
permutation needs none of that: it is sliced in place and the page cache does
the rest, so a NIKEPIG-class policy costs resident pages rather than a heap
allocation the size of its whole holder list.
- **The address orderings are the same shape**, and their confirm is cheaper to
state: `utxos-by-address` scans the run of entries sharing the credential and
compares the row's full address bytes to the query's; `utxos-by-payment-cred`
re-derives the credential and additionally rejects a `Kind::Stake` row. Both are
one row read per candidate, measured at **1719 ns** including the derivation
(§6), against `by_policy`'s several seeks.
- **The one thing that is not one-entry-per-lookup.** A wallet's outputs all share
its credential, so the run for a busy address is as long as its UTxO count — and
that is the answer, not a collision: every entry in the run is a real output the
caller asked for. The 12-byte entry stays sound because the confirm is against
the row, which is what `B3` claims and what the whole-store verify checks on
every entry rather than a sample.

## 10. Order of work

1. **`by_policy`** — five call sites, absent from `B1`, buildable from what the
walk already observes. Includes the policy-id ordering requirement above.
2. **`by_addr`** — three call sites, already designed in `B1`; needs the
credential derived at build time (§8).
3. **`datum.tbl` and the aux index** — two call sites each, and the extraction
already exists on the slow path, so this is a sink and a second artifact rather
than a new walk.
4. **The mint ledger** — the only genuinely new extraction, behind the heaviest
consumer (`collection-metadata`).
5. **Regenerate `dir.idx`** for the published artifact (seconds, no re-fold), or
let the next whole-store run rewrite it.

**Done since:** 1 (`by_policy`) and 2 (`by_addr`) are built and verified; 5 is
moot, since every reindex rebuilds `dir.idx` anyway.

## 11. What closing the loop actually requires

Every table above answers *"can the book supply this"* by reading code. None of
them answers *"does the module work against it"*, and that is the question the
next version of mitos turns on. Three things have to exist for the first execution
test, in this order:

1. **A reader, not just a verifier.** ✅ **Built 2026-09-23** — `crates/live-book`,
   with `Base::open` / `utxo` / `by_address` / `by_payment_cred` / `by_policy` and
   an opaque paging cursor. Extracted as its own crate on the day it was written,
   because a binary crate cannot be depended on and this is the piece the data
   plane needs. Its correctness argument is `lookup --cross-check`: 21,067 policies
   and 589,363 refs walked through all three orderings with **0 disagreements**,
   plus `$burnsnek` at 19,724 against an outside record of 19,692 (§6, §9).
2. **The two implementations must be comparable.** ✅ **The impl exists**
   (2026-09-23): `BookDataPlane` in `crates/mitos-data-plane/src/impls/book.rs`,
   beside `LocalDataPlane` behind the same trait. It answers **six** of the
   trait's methods from the artifact and refuses the rest by name:

   | answers | refuses, and what would close it |
   |---|---|
   | `read_utxo` / `read_utxos` | `tip` — **the artifact records no chain position** |
   | `utxos_by_address` | `search_utxos` — it returns a `Page`, and a `Page` carries a tip |
   | `utxos_by_policy` | `read_datum` — needs `datum.tbl` (a capture, not a view) |
   | `utxos_by_payment_cred` | `read_script` — the row has no `script_ref` column |
   | `resolve_stake_for_payment_pkh` | `tx_metadata`, `read_tx`, `slot_by_tx_hash` — history |
   | `holder_count`, `read_output_datums` | `total_supply`, `asset_state`, `protocol_params` |

   Those six are, method for method, the whole input of five of the nine modules
   in §7 — `collection-holders`, `holder-distribution`, `vesting-tracker`,
   `burn-address`, and `credit-address`'s `read_utxos` half.

   **Three findings from building it, all of them cheaper to act on now than
   after a cutover:**

   - **`tip` is the structural one.** A book is a snapshot at *some* chunk
     boundary and `book.meta` does not record which, so the plane cannot say what
     slot it is at — and anything a consumer does with a chain position
     (staleness, `TickEvery`, drift across a paged query) is unanswerable. This is
     a gap in the **artifact**, not in the adapter, and it belongs to
     `LIVE_BOOK.md` phase 4. It must not be papered over with `ChainTip::origin()`:
     `Origin` means genesis.
   - **The datum table is 16% of a policy scan, not a corner.** Of 34,177 outputs
     read through `utxos_by_policy`, **6,538 carry a datum and 5,497 of those are
     hash-referenced** — so `payload: None`, which is the documented meaning of
     "the plane could not resolve the bytes". Inline datums resolve completely
     (1,041 of them did).
   - **The trait's own shape costs 17× the lookup.** 20.4 µs per output against
     1223 ns for the raw row, and measuring both decode levels shows `Lean` at
     20,616 ns against `WithDatum`'s 20,406 — **none of it is resolution**. It is
     `TypedOutput`'s owned text: a `String` address, two hex `String`s and a
     validated `PolicyId` per asset entry, ~29.6 entries per output when sampled
     through a policy. This is a decision about the trait, not an optimisation of
     the book.
3. **Then one module, both ways.** `collection-holders` is still the cheapest first
   test — it calls `utxos_by_policy` and nothing else, and that ordering is served
   at **1,926 ns/ref** over 47,370 refs from 2,000 policies.

   **The harness exists and the first half passes** (2026-09-23):
   `crates/mitos-platform/tests/collection_holders_over_book.rs` boots the real host
   with `BookDataPlane`, runs the shipped artifact through the chunked cold-start
   pump, and checks what it emits against the book through the same trait the module
   uses — asset-name set equality and **Σ quantity per asset name**, not a bound.
   §12 has the run and the gap it found on the way.

   **The "both ways" half is still blocked on a reference, not on the book.** The
   dolos install on the ARM box (`/opt/mitos/mainnet`) is the *killed* import —
   `install-state.json` reads `"state": "importing", "cursor": 0`, and its state
   is 2.4 G against the ~4 G a complete one takes. A partial store is not a
   height, so diffing it against a tip-snapshot would produce differences that
   mean nothing. Two ways forward, and it is a decision rather than a task:
   either complete an import on that box and diff against it, or accept the book's
   own verification (32.7 M entry confirms, 0 failures, plus the `$burnsnek`
   oracle) as the reference and validate the module against expectations instead.

What this deliberately does not schedule: the mint ledger, the datum table and the
aux index (§4b, §4c) are all *known* gaps with named consumers, and none of them
blocks the first execution test. The point of running one module first is to find
the gaps that reading has not found.

## 12. Where the input-resolution gap closed (2026-09-23)

The question §7 raised — *can a module ask about a transaction that just landed, and
be answered about outputs that are no longer live* — is answered, and the evidence
is in `DELTA_CHAIN.md`:

| question | measured |
|---|---|
| does anything answer the inputs the book cannot? | **the log does, 100% of them.** `never-created` is 0 on a from-genesis base |
| what does the structure cost? | **3.3 MiB and +31% of the walk per chunk** at the tip, plus a **1.36 MB** bitmap over the base |
| how fast is a resolution? | **~240 ns**, against `book_utxo`'s 496 ns |
| is it the *right* structure? | cross-checked exactly against `resolve-inputs`' independently-built gap: 43.48% base + 56.52% delta = 43.48% + 40.70% + 15.82% |

That moves §11's list rather than shortening it, and it reorders it:

- **`tip` was the cheapest unblocked item on it, and it is now closed — by an
  execution test rather than by an argument.**
  `crates/mitos-platform/tests/collection_holders_over_book.rs` runs the *shipped*
  `collection-holders` module through the real host against a published book. Its
  first run got as far as loading the artifact, booting the follower, routing
  dynamic interest and sharing the onboard scope from the live instance to the
  cold-start pump's disposable one — and then **trapped, emitting nothing**:

  ```text
  backfill: rebootstrap trapped (non-retryable); refill may be partial
  emitted 0 begin · 0 chunk(s) · 0 end · 0 holding(s)
  ```

  The cause was one missing number. `HostStateV2::utxos_by_policy` freezes a fresh
  scan as of a tip (`scan_anchor_slot` → `tip().slot()`, `host_fns_v2/chain_data.rs`),
  `BookDataPlane::tip` refused because the artifact records no chain position, and
  the `?` became a host-fn error the guest calls without a `Result` in front of it —
  so the first page scan killed the whole cold start.

  **Closed 2026-09-23.** `book.meta` gained an optional `base_slot`; a build writes
  it from the walk (the slot of the last block applied, which no transaction in a
  book carries), `fold-probe stamp-boundary` reads it out of the boundary chunk for
  an artifact built before the field existed, and `tip()` answers
  `ChainPoint::Slot(slot)` — the variant the type already had for *"slot-only point,
  captured when the originating event source had only the slot"*. Absent is **not**
  a `layout_version` change: the rows and the three orderings are the same bytes
  either way. And an unstamped book still refuses rather than answering zero, because
  a tip of genesis is a different claim to "this book does not know".

  **The same book, after the fix:** `rebootstrap pump complete utxos_ingested=706
  steps=6 outcome="completed"` and `1 begin · 1 chunk · 1 end · 839 holding(s)`, with
  the emitted ledger matching the book's own answer exactly — 839 asset names and
  Σ 839 units from 706 live refs, through the scan, the trait's projection, the
  module's SUM merge and the emit path. **The first shipped module to run against the
  phonebook and be checked against it.**

  What is still open there is a different gap, and the run names it: the host's
  watched-UTxO seeding calls `search_utxos`, which the book refuses for want of a
  predicate algebra and a `Page` cursor. Non-fatal — the seed logs and carries on —
  but it is the next thing that test will notice.
- **The delta has no orderings.** Point resolution is built; enumeration is not, and
  a linear scan of a 21-chunk delta is **97.5 ms**. The fix is the book's own shape —
  12 B an entry per ordering — and until it exists the plane cannot answer
  `utxos_by_address` or `utxos_by_policy` at the tip.
- **`read_datum`, `read_script`, `tx_metadata`, `read_tx`, `slot_by_tx_hash`** are
  unchanged: a capture and a history problem respectively, neither of them about the
  delta. For `tx_metadata` specifically, `TX_INDEX_DATASOURCE.md` now carries the
  measured case for leaving it on tx-index rather than on the book — and its local
  tier already makes that path cost zero remote calls (`local_tx_index.rs`).
