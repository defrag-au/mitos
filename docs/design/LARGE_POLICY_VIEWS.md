# Large policy views — the two edge cases, measured

**Status: ANALYSIS + MEASURED** (2026-09-25). The measurements are done and
reproducible; the "what to publish" half is a proposal. Nothing here is a work
order.

Question: how do we serve a collection page for a policy with tens of thousands
of assets so it feels like OpenSea — and what breaks first at that size? The two
named cases are **ADA Handle** (`f0ff48bb…ca0fb9a`) and **HOSKY Cash Grab**
(`a5bb0e5b…e01559`), both 1–2 orders of magnitude larger than any collection
mitos currently tracks. Neither is hypothetical: HOSKY is the token that killed
holder-distribution for two weeks (`NATIVE_HOLDER_SNAPSHOT_PLANE.md`), and this
doc is the same edge case one layer down — with the rows in hand rather than
paged through wasm.

Measured on the local full book (`/Volumes/cardano/fold-book/local`,
layout_version 3, `base_slot` 198,590,382, 2.19 GB, 10,896,105 rows). Counts come
from two independent implementations — the `live-book` reader through
`fold-probe`, and a direct Python walk of `rows.bin` + `by_policy.idx` that knows
nothing about the reader — and they are compared wherever they can disagree.
Commands in §8.

Related:
- `MITOS_AT_THE_EDGE.md` §3 (workloads, priced), **§4.5** (the row window),
  **§4.8** (clustered arena), §4.9 (`policy.tbl` resident), §4.10 (multi-range),
  §7.1a (sealed deltas indexed by policy)
- `LIVE_BOOK.md` §B2/B9 (the row, serving costs), §Sizes
- `BULK_BACKFILL_AND_ON_DEMAND_RESOLUTION.md` §1 — the same 100K cap seen from the
  consumer side (`/_admin/utxos`), where it *is* reported (`truncated: false`)
- `NATIVE_HOLDER_SNAPSHOT_PLANE.md` — HOSKY is the token that took
  holder-distribution down for two weeks; this doc is the same edge case one
  layer down, with the rows in hand
- `ARTIFACT_SHARDING.md` — the shard/re-ingest format §6.2's split question belongs to
- `ROW_ADDRESSING_AND_SEARCH.md` — the measurements behind §0's fix shapes and §3's
  arena sizes: compression, per-policy addressing, and the name table a search
  without an external engine needs
- `OWNERSHIP_WITHOUT_SYNC.md`, `TX_INDEX_DATASOURCE.md`, `MITHRIL_PHONEBOOK.md`

---

## 0. The finding that had to come first

**The reader's generic path could not read a row longer than 8 KiB, and the failure
was silent.** `Base::with_row` reads a speculative 8 KiB window and maps a parse
refusal to `Ok(None)` — "not a match" — so a row that does not fit is skipped by
every enumeration and every point lookup that goes through it
(`by_policy`, `by_address`, `by_payment_cred`, `utxo`, `with_utxo`,
`outref_entry`, and therefore `read_utxo` / `read_utxos` on the trait).

**Fixed, 2026-09-26.** `with_row_from` in `crates/live-book/src/reader.rs` grows
the window geometrically on `RowRead::OutOfRange` — 8 KiB → 16 KiB → 32 KiB — and
keeps `Ok(None)` for the two states that are genuinely "not a row": fewer bytes
than a header, and a header that claims more than the source holds. Growth is
geometric, so the median 166 B row still costs one read and the 22,656 B row
costs three. Two tests pin it: one end-to-end through `by_policy` on a 20,481 B
row (over two windows, so a single doubling would not be enough), and one on the
read count, driven against a byte-slice source rather than a file. Re-measured on
the same book through the same commands: the longest row answers live, and both
whales are at the artifact truth below (§8).

Store-wide, on this book:

| | measured |
|---|---|
| rows in `rows.bin` | 10,896,105 (walked from offset 0; agrees with `book.meta`) |
| **longest row** | **22,656 B** (`book.meta`'s `max_addr_len` is 22,600 — that row) |
| rows over 8 KiB | **630** |
| `by_policy` entries pointing at one | **1,786** of 10,932,696 |
| `by_addr` entries pointing at one | **630** of 10,896,105 |
| `by_outref` entries pointing at one | **630** of 10,896,105 |

`fold-probe lookup --outref` on the longest of them answers **"is not in the live
set"** — for an output the artifact's own `by_outref` ordering says is live
(checked: exactly one `by_outref` entry points at that row). The row at offset
12,766,247 is a 1-lovelace era-1 output with a 22,600-byte address
(CBOR-wrapped, starting `82d818…`, carrying what is plainly a JFIF blob), and the
next four are 19.8–21.6 KB. The other shape of long row is the asset-heavy
whale: the two HOSKY rows over the window hold 343 and 352 asset records, and the
27 ADA Handle ones up to 504.

Two consequences before anything else in this doc:

1. **The 8 KiB `MAX_ROW` is a regression in the uncommitted `Source` seam.** HEAD
   parses the row from the mapping to the end of file, which is why the
   pre-refactor binary measured 36,948 and 170,062 for the two policies below.
   The working tree measures 36,946 and 170,035 — the difference is exactly the
   rows over the window. The book is written by the fold path, which does not
   read its own artifact; nothing has shipped with the windowed path.
2. **The window's own justification is falsified on this corpus.** It reads:
   "§4.5 measures the median row at 166 B and the longest in the whole store at
   7,689 B, so 8 KiB covers every row mainnet holds with room to spare" — the
   longest here is **22,656 B**, and 630 rows exceed the window. The other half of
   the sentence ("`Row::parse` still refuses a row that runs past what it was
   given, so a fixture that exceeded it would fail loudly") is true of
   `Row::parse` and false of the caller: `.ok()` turns the refusal into a miss.

This is a decision, not a patch, and it belongs in `MITOS_AT_THE_EDGE` §4.5's
terms: the window exists because a remote source cannot know a row's length
without a second dependent read, which is real. But the skip must be *only* for
"these bytes are not a row", never for "this row is longer than I guessed". The
shape that keeps both: read the 56-byte header first, take the length from it,
read exactly that (one extra round trip for a remote source, implementable as
one for `MmapSource`) — or keep the speculative read and grow it on
`RowRead::OutOfRange`, which preserves the fast path and can still fail at some
larger bound. Either way the two refusals have to stop being the same `Ok(None)` —
which is what §4.5 asks for in its own words ("the reader can report when it went
back rather than guess"), and the second correction in §7 traces where the
constant's size came from.

---

## 1. The two edge cases, measured

Artifact truth (Python, no window) beside what today's reader returns:

| | **HOSKY Cash Grab** | **ADA Handle** |
|---|---|---|
| entries in the `by_policy` run | 36,948 | 170,062 |
| rows holding the policy | **36,948** (0 collisions) | **170,062** (0 collisions) |
| **distinct asset names** | **420,420** | **313,019** |
| policy asset entries | 420,425 (5 names in two outputs) | 313,025 (6 names in two outputs) |
| distinct holder addresses | **20,309** | **74,129** |
| total live lovelace in those rows | 316,744,970 ₳ | 105,407,418 ₳ |
| rows over the 8 KiB window | 2 (8,397 B, 8,375 B) | 27 (longest 10,196 B) |
| what the windowed reader returned | 36,946 refs — **36,948 after §0's fix** | 170,035 refs — **170,062 after** |
| enumerate: pages of 20 · wall | 1,848 · 1.2–2.5 s | 8,502 · 1.7–2.8 s |
| reading every row back | < 0.1 s | 0.1 s |
| holder pass · asset pass | 0.02 s · 0.05 s | 0.08 s · 0.10 s |
| peak RSS (mmap-dominated) | ~0.7 GB | ~1.4 GB |
| rank among 198,344 policy prefixes | 50 | **5** |
| share of all 10,932,696 `by_policy` entries | 0.34% | 1.56% |

**⚠️ The run lengths are the row counts, exactly: zero prefix collisions for
either policy.** The general warning stands (a run is keyed on 8 bytes and two
policies can share one), but here every entry confirmed — so the difference
between 36,948 and 36,946 was *not* collisions, it was §0's window (since fixed).
Anyone
re-measuring these two should expect the run length to be the row count and be
suspicious of anything smaller that is not explained by a row length.

**Mean address is 56.9 B in both** — base addresses with the stake part inline,
so holder distribution (§3.4) needs no second lookup.

**The row is the collection's identity card.** It carries the address, every
asset record (interned policy + name + quantity) and the datum hash or inline
payload. For both policies the asset *name* — the handle, the Cash Grab token —
is what a grid renders. Only the image/metadata is elsewhere.

**Zero-copy is real here, and it is why the counts above are cheap.** The holder
and asset passes each re-read all 170,062 rows and finish in ≤ 0.1 s because the
row is a borrowed view of the mapping: no bech32, no hex, no allocation. The
by-policy *confirm* is the opposite kind of cost (§1.2).

**Store-wide, the largest holder sets are not these two.** `fold-probe catalogue`
(2026-09-26, `COLLECTION_CATALOGUE.md` §5) walked every policy in this book: six
policies have more than 50,000 holders and **five of them are single-name tokens** —
151,030 · 135,518 · 97,703 · 89,620 · 77,497 addresses carrying 1–6 asset names each.
ADA Handle is the sixth (74,129 holders, 313,019 names); HOSKY's 20,309 is not among
them. So these two are the edge cases for *page* size (rows, and the names a grid
renders), while the peak a holder pass has to hold in memory is a single-name
token's 151,030 — the two maxima are on different policies.

### 1.1 The same policies through the trait and the module host

| | **HOSKY** | **ADA Handle** |
|---|---|---|
| refs from `utxos_by_policy` | 36,946 (all but §0's two) | **100,000 — capped, 41% short** |
| enumerate cost | 36,479 ns/ref → 1.35 s | 12,493 ns/ref → 1.25 s |
| `read_utxo` cost (WithDatum) | 3,966 ns/output → 0.15 s | 2,553 ns/output → 0.26 s |
| `holder_count` | 20,308 (short by one address) | **41,827 — a lower bound** |
| wall, whole run | 2.39 s | 1.61 s |

⚠️ **The table is pre-fix** — measured 2026-09-25 through the trait, before §0's read
fix landed. Its HOSKY rows are exactly what the window cost: 36,946 refs is the run
minus the two long rows, and 20,308 holders is short by the one address those rows
held. The *fixed* reader measures **36,948 refs · 20,309 holders** for HOSKY (the
`catalogue` census, `/Volumes/cardano/fold-book/local`, `COLLECTION_CATALOGUE.md` §5),
and the trait wraps that same reader — so a re-run should reproduce §1's figures.
ADA Handle's 100,000 and 41,827 are the **module cap** below and are unaffected by the
fix; the true holder count is **74,129**. Re-run:
`./target/release/examples/book_plane --dir <BOOK> --policy <P> --limit 200000`.

`UTXOS_BY_ADDRESS_HARD_CAP = 100_000` (`impls/local.rs`) is applied to
`by_policy` as well as `by_address`, and the v2 host path truncates again at
`MAX_SCAN_REFS = 100_000` (`host_fns_v2/scan_cache.rs`) after sorting, with **no
flag to say it happened** — `next` is simply `None` once the truncated set is
exhausted. A module cold-starting ADA Handle today would believe it had
enumerated the whole collection, 70,035 rows short.

That cap is a documented contract ("Cap: 100K refs", inherited from the
unbounded host-fns), and for the 163 tracked collections — median 2,600 items,
max 18,594 — it has never bound. **These two policies are the first that exceed
it.** It is also the read-side twin of `BULK_BACKFILL_AND_ON_DEMAND_RESOLUTION.md`
§1: there the cap is at least *reported* (`truncated: false`) and a consumer can
refuse to prune against a prefix; here the caller gets a `Vec` of exactly the cap
and no signal at all. Whatever else is decided, the cap has to become
policy-aware (refuse loudly) or paged (the reader already is).

### 1.2 Why HOSKY is 3–4× the per-ref cost of ADA Handle

Same reader, same artifact: 30–39 µs/ref against 8.7–11.0 µs/ref. The difference
is the confirm. `by_policy` resolves each row's interned policy id through
`policy.tbl` and stops at the first match; HOSKY's rows carry **11.4** policy
assets each (420,425 / 36,948) and ~24 asset entries in total, ADA Handle's carry
**1.8** (313,025 / 170,062). Enumerating a policy is therefore *a row read plus
an asset walk* per returned ref, not a search — the same conclusion `LIVE_BOOK
§B9` reached from the other direction. It also means the trait's shape (enumerate
refs, then read them) buys nothing on-box: the row was already read to confirm it.

---

## 2. What a collection page asks for

"Feels like OpenSea" decomposes into five queries, priced as **one pass over the
policy's rows** — 0.1–0.5 s on-box for either of these, and §1 says that pass is
the whole cost:

| the page asks | answerable from the book? | how |
|---|---|---|
| item count, holder count, total value | **yes, one pass** | rows of the run; dedup addresses |
| the grid: name, owner, datum, quantity | **yes, one pass** | the row *is* all four |
| "which of my items are in this collection?" | **yes, no policy scan** | the wallet's own rows carry their assets — §2.1 |
| top holders / holder histogram | **yes, one pass, then publish** | a fold over the same rows; ranked views are bounded tables |
| traits, rarity, images, listing state | **no** | metadata is a projection off the mint ledger / CIP-25-68, listings are marketplace state — `MITOS_AT_THE_EDGE` §3.2 |

**Four of the five are the same pass.** A per-policy bundle — rows, holder table,
counts — is one artifact, generated at the policy during the walk (or as a
reindex of the published book, which is what `fold-probe reindex` does for the
orderings).

### 2.1 Ownership filters invert cheaply

A wallet holds ~50 outputs; a collection holds 170,062. Asking "does this wallet
hold any of this policy" by scanning the policy is 170,062 rows to answer a
question about 50. Asking it by reading the wallet's rows is 50 rows — and each
row already says which policies it holds, because the assets are interned *in
the row*. One `by_payment_cred` plus a multi-range read (§4.10) answers it, and
the same read answers it for every collection the page shows at once.

This is the concrete reason `resolve_stake_for_payment_pkh` has a fast path in
`BookDataPlane`: **ownership is volatile and derivable**, and the direction of
the fold matters more than the artifact size.

---

## 3. The arithmetic at this size

§3.1 of `MITOS_AT_THE_EDGE` prices a median 2,000-output collection at ~9,100
round trips as shaped, 1–2 with §4.8's clustered arena. ADA Handle is **85× that
collection**:

| | median collection | ADA Handle | HOSKY |
|---|---|---|---|
| outputs | 2,000 | 170,062 | 36,948 |
| as shaped: enumerate + confirm | ~2,000 RT | ~170,000 RT | ~37,000 RT |
| as shaped: `read_utxos` (×3.55/ref) | ~7,100 RT | ~604,000 RT | ~131,000 RT |
| **as shaped, total** | ~9,100 | **~774,000** | ~168,000 |
| **clustered arena (§4.8)** | 1 | **1** | 1 |

Derived from §3.1's per-ref unit costs, not measured off-box. At this size the
arena is not an optimisation, it is the only expressible form — and it is the
same answer §3.1 already reached; this doc just supplies the multiplier.

**What the arena is, for these two policies — measured** (`ROW_ADDRESSING_AND_SEARCH.md`
§3):

| | rows | arena bytes | mean row | 1 MiB parts |
|---|---|---|---|---|
| ADA Handle | 170,062 | **93.6 MiB** | 577 B | 94 |
| HOSKY Cash Grab | 36,948 | **29.5 MiB** | 837 B | 30 |

⚠️ **The first draft of this section said 28 MB and 14 MB, derived from the store's
mean row (165.6 B), and that was 3× low.** A policy's rows are size-biased: they
hold the *holder's whole balance*, so the bytes are driven by the ~1.8 M asset
entries in those rows rather than the 313 k belonging to the policy. Sizing any
per-policy object by `rows × store mean` will be wrong in the same direction.

Both are still single R2 objects, and the arena is what makes a **scan** cheap:
30 and 94 part reads for the whole collection, which is what cold start and
holder counts are. ⚠️ What it does *not* make cheap is a page drawn in any order
other than the arena's own — 50 rows spread through it touch 29–47 parts, so a
name-ordered or filtered browse wants a card table (that doc's §6.4), not a reach
into the arena. Offset-based paging (`rows [k×20, (k+1)×20)`) is one range read.
`policy.runs.idx` is 1.85 MB for **all** 231,810 policies and gives
`(offset, len)` in one hop.

**Scope: not one object per policy for all of them.** Of 231,810 policies, **165
hold ≥ 10,000 refs and 9 hold ≥ 100,000** (54% and 14.8% of all entries). The
long tail wants the shared arena; the 165 want their own slice; the 9 want the
decision in §7.1.

---

## 4. What this implies for the walk (one walk, many artifacts)

Per §B7, ordered by how much of a collection page each closes:

| artifact | shape | size at these two policies | closes |
|---|---|---|---|
| `rows.by_policy.bin` + `policy.runs.idx` (§4.8) | rows clustered by policy | 1.69 GiB total; 29.5 / 93.6 MiB for the two here | the grid, the counts, the holder table — **one range read** |
| per-policy bundle: `holders.tbl`, `top.tbl` | sorted arrays, publish-time fold | ~740 KB for ADA Handle (74,129 × 10 B) | ranked views, histograms, "N holders" with no scan |
| per-asset view: `name → (owner, qty, offset)` | §3.2's projection, row dropped | ~15 MB ADA · ~21 MB HOSKY | asset → owner in one hop; the item page |
| the datum table | built (`DECODE_SHAPE §2.10`) | small | CIP-68 discovery |
| metadata + traits | **not from this walk** | — | images, rarity — mint ledger + metadata source |

⚠️ **Build these from the builder's row stream, never through the reader's
`with_row`.** The arena and the per-asset view are exactly the artifacts that
would silently lose §0's 630 rows if they were generated by querying the book.

⚠️ **Freshness is not designed here.** A collection page promises *now*:
`MITOS_AT_THE_EDGE` §7.4 records the base/delta pairing hole as live, and §7.1a's
per-policy delta index is what closes large-policy views — the base arena is the
snapshot, the delta has to be indexed by policy or every query pays a full delta
walk. Per §7.2 the delta is ~2.5 KB/block chain-wide, so per policy it is tiny;
it is not zero, and serving a stale "313,019 items" is the failure mode.

---

## 5. The serving recipe, priced end to end

| step | today | with §4's artifacts |
|---|---|---|
| counts + holders snapshot | full policy pass (0.1–0.5 s on-box; ~170k–600k RT off-box) | **resident / KV**: one small object |
| first grid page (20 cards) | ~70 RT (20 × 3.55, scattered) | **1 range read**, near-head of the arena |
| "my items in this collection" | policy scan, or a per-wallet walk | **1 multi-range read** of the wallet's own rows (§2.1) |
| item page (name, owner, datum) | row read + resolve | **1 range read**, or a hop in the per-asset view |
| traits / image | not answerable | still not — metadata projection |

The two rows a *viewer* hits are grid pages and wallet intersections; both are 1
request when the rows are clustered, which is why §4.8's justification is request
count rather than bytes.

---

## 6. Open questions

1. ~~**§0's fix shape** — header-first (correct, costs a dependent read remotely but
   nothing on mmap), or grow-on-`OutOfRange` (keeps the speculative fast path,
   bounded again by whatever number replaces 8 KiB). Either way "not a row" and
   "longer than the window" must stop being the same `Ok(None)`, and §4.5's
   7,689 B figure needs re-measuring before it is quoted again.~~ ✅ **Decided
   2026-09-26 — grow-on-`OutOfRange`**, in `with_row_from`: 8 KiB speculative,
   geometric growth, `Ok(None)` only for the two states that are genuinely "not a
   row". Header-first costs a dependent round trip on every row to save two on the
   rare long one; growth keeps the median at one read. §4.5's figure is re-measured
   (630 rows over 8 KiB, longest 22,656 B) and §1.1 still needs its trait re-run.
2. **One object per large policy, or a shard?** ADA Handle's arena is **93.6 MiB** and
   an offset-based page touches a few KB of it. One object keeps the range arithmetic trivial;
   the split rule is `ARTIFACT_SHARDING.md`'s subject, and the 9 policies
   ≥ 100k refs are where it gets decided.
3. **The 100K cap: refuse or page?** Refusing is honest and breaks ADA Handle's
   cold start; paging is more work and the only shape that serves it.
4. **Should the per-asset view carry the row offset, or the outref?** Offset is
   one hop in the arena; outref survives a rebuild. Both is 36 B.
5. **Do the derived views belong in `live-book` or a sibling crate?** The row
   store is a set of outputs; a holder table is a projection over it — the
   question `TX_INDEX_DATASOURCE.md` already answered once for tx-index.
6. **What is a derived view's freshness contract?** `book.meta` stamps the base;
   whether a view carries its own stamp, and how a consumer merges base + delta,
   is unsettled.

---

## 7. Corrections this exercise forces

1. **`LIVE_BOOK §B9` — "`holder_count` from the entry count of a run"** is wrong.
   It is a per-address dedup over every row of the run (`impls/book.rs`), which is
   why it costs a row read per ref and why it inherits the 100K cap. The entry
   count is an upper bound on *outputs*, and at these sizes it is not even that
   — the entry count of ADA Handle's run is 170,062 and the rows are 170,062, but
   the *holders* are 74,129.
   ✅ **Corrected 2026-09-26**: `LIVE_BOOK §B9` now says what the primitive does,
   with the measured figures beside it.
2. **`MITOS_AT_THE_EDGE §4.5`'s longest-row figure — repeated in `with_row`'s
   comment — is a chunk-level number read as a store-wide one.** §4.5 says "only
   **22 rows in the whole store** exceed 255 B of address, longest row **7,689 B**
   (all measured)"; `reader.rs` renders that as "the longest in the whole store at
   7,689 B". The 22-rows count *is* store-wide and comes from `LIVE_BOOK.md`
   §Sizes; the 7,689 B is that same section's **chunk 81** longest row, and the
   store-wide tail is two bullets later in the same section — longest **address**
   22,600 B, which is the 22,656 B row of §0, with 630 rows over 8 KiB. So the
   number the window rests on was written down beside the one it quotes. §4.5
   also prescribes the right behaviour — "`book.meta` carries `max_addr_len`, so
   the reader can report when it went back rather than guess" — which is exactly
   what `with_row`'s `Ok(None)` does not do. (It suggests **1 KiB**; the code uses
   8 KiB.)
   ✅ **Corrected 2026-09-26**: §4.5 now carries the store-wide tail (630 rows over
   8 KiB, longest 22,656 B) and the implemented shape, and `with_row`'s comment no
   longer quotes 7,689 B.
3. **The run-length-is-not-the-row-count warning** (repo-wide) is right in
   general and wrong for these two: both runs are exactly their row counts. The
   warning should read "confirm, then quote", which is what the reader does and
   what the Python walk re-checks.

Nothing in `MITHRIL_PHONEBOOK.md`'s keep/drop thesis needed revising: every
artifact above is a fold over the same walked bytes.

---

## 8. Provenance

Measured 2026-09-25 against `/Volumes/cardano/fold-book/local` (rows 10,896,105 ·
policies 231,810 · `base_slot` 198,590,382 · layout_version 3 · `rows.bin` 1.80 GB,
`by_policy.idx` 131 MB, `by_outref.idx` and `by_addr.idx` 130 MB each,
`policy.tbl` 6.49 MB, three directories 8.4 MB each).

```sh
# §1 — the reader's view (add --sum --holders --assets for the passes)
./target/release/fold-probe lookup --book <BOOK> --policy <P> --all --sum --holders --assets

# §1.1 — the trait / module path
./target/release/examples/book_plane --dir <BOOK> --policy <P> --limit 200000

# §1 artifact truth, §0's row classes, §7's figures
/usr/bin/python3 .tmp/policy-truth.py <P>
/usr/bin/python3 .tmp/oversized-rows.py <BOOK>
/usr/bin/python3 .tmp/collision-check.py <P>     # superseded by policy-truth.py
/usr/bin/python3 .tmp/longest-row.py
/usr/bin/python3 .tmp/outrefs-of-long-rows.py

# §1's distribution over all policies, and the run lengths
/usr/bin/python3 .tmp/prefix-runs.py <BOOK>
```

The Python walks are not a second reader — they are a check that can disagree,
and did: they are what turned "prefix collision" into "row longer than the
window". Their row walk counts exactly the 10,896,105 rows `book.meta` claims,
and their per-policy counts agree with the reader on every row the reader can
see (5 duplicated HOSKY names, 6 ADA Handle, both reported by `--assets`).

`--holders` and `--assets` were added to `fold-probe lookup` for this analysis;
`book_plane` gained `--policy`. Prefix runs are stored little-endian
(`u64::from_le_bytes(policy[0..8])`), which `prefix-runs.py` handles and a naive
reader does not.

**Re-measured 2026-09-26**, after §0's fix landed, with the same binary and the
same commands: the longest row (`dea1a1ab…bc023d#1`, 22,656 B) answers live for
the first time; HOSKY returns 36,948 refs · 316,744,970 ₳ · 20,309 holders ·
420,420 names in 420,425 entries; ADA Handle returns 170,062 refs · 105,407,418 ₳
· 74,129 holders · 313,019 names in 313,025 entries. Every figure §1's Python
walk measured, now through the reader, with zero refs the row store could not
answer.

**Not measured:** any off-box round-trip figure (§3's "as shaped" column is
derived from §3.1's unit costs); the arena sizes in §3 (derived from row
geometry, not from a built artifact); everything requiring a delta (§4's
freshness note is from the design docs, not a run).
