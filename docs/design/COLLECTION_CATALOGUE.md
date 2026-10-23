# The collection catalogue — `policy → name, live assets, holders`

**Status: STARTED (2026-09-25).** The name column is measured (§3). The two count
columns are specified against the book and gated on one read fix (§4). No artifact
has been written yet, and nothing here is a work order.

Related:

- `MITHRIL_PHONEBOOK.md` — the phonebook this is a projection of
- `ROW_ADDRESSING_AND_SEARCH.md` — the metadata corpus, the name ordering, and what
  a name search over it costs
- `LIVE_BOOK.md` — the artifact both count columns come from
- `LARGE_POLICY_VIEWS.md` — the large-policy edge cases; **§0 is the gate**
- `PHONEBOOK_COVERAGE.md` — the module surface this has to serve
- `NATIVE_HOLDER_SNAPSHOT_PLANE.md` — why the holders column is not a set per policy

## 1. What a collection page needs

Three facts per policy, and nothing else:

| column | the question |
|---|---|
| **name** | what is this called — for a browse list, a title, and a search box |
| **live assets** | how many of its assets are in the live set |
| **holders** | how many distinct wallets hold at least one |

The Luffy flow needs the first column at search time ("Luf" → Luffy in *Black Flag*);
a collection page needs all three to render something that is not a spinner.

## 2. Where each column comes from

| column | source | state |
|---|---|---|
| name | label-721 metadata: a declared field where there is one, else the assets' display names | **measured (§3)** |
| live assets | the live book, per policy | gated (§4) |
| holders | the live book, distinct payment credentials per policy | gated (§4), with an exactness decision (§5) |

## 3. The name column, measured

`fold-probe cip25 --immutable <store> --stride 1 --extract compare`, whole store,
2026-09-25:

```
  collections 169,235 policies · 11,157,900 typed documents · 0 with no display name
```

Every walked document produced a display name — the declared `name`, or the
on-chain name when it is text — so the denominator is the walk's own count.

**Declared.** The registry classifies all six of these as capture-only facets, so
none is a trait and none reaches `Traits`: `cardano_assets::document_fields` is the
way to read them.

| field | policies declaring it |
|---|---|
| `collection` | 8,699 |
| `project` | 2,739 |
| `publisher` | 2,237 |
| `series` | 1,041 |
| `collection name` | 74 |
| `vendor` | 27 |
| **any of the six** | **≤ 14,817 (8.8%)** — the sum is the bound; the exact union prints from the next pass |

So the metadata names the collection for roughly one policy in twenty, and the
derived candidate is what carries the other nineteen.

**Derived.** The display name with its edition marker removed (`FamousWolfClub6366`
→ `FamousWolfClub`, `Luffy #123` → `Luffy`, `3DArt` keeps its leading digit), tallied
per policy under a four-candidate cap:

| top candidate covers | policies | share |
|---|---|---|
| ≥ 90% of the policy's assets | **155,339** | 91.8% |
| 50–90% | 6,018 | 3.6% |
| < 50% | 7,878 | 4.7% |
| no candidate at all | 0 | — |

6,317 policies hit the four-candidate cap — names that never repeat, which is the
`<50%` row seen from the other side.

**The largest policies, and what the rule makes of them:**

| policy | documents | top candidate | share |
|---|---|---|---|
| `a5bb0e5b…` (HOSKY Cash Grab) | 481,524 | `HOSKY C(ash Grab)NFT` | 100% |
| `f0ff48bb…` (ADA Handle) | 271,085 | — | 0% |
| `fbfef0c4…` | 109,019 | `TheMandrillzWorldcup` | 100% |
| `dd5505a0…` | 66,058 | `Ape Society Frame Level` | 100% |
| `101dcbab…` | 65,000 | `Crypties cNFTcon2021` (declares `Non-Fungible Memories`) | 4% |
| `13e3f996…` (Clay Nation) | 121,963 | `Clay Nation Pitch` | 26% |

ADA Handle's 0% is the rule working rather than failing: every handle is its own
name, so there is no collection name to find — the same shape as `Hired Gun` and
`Mr. Lutz Rattlesnake`. A catalogue takes the declared field where there is one and
the candidate where its share is high, and says "no name" for the ~8% in between
rather than inventing one.

**17,673 policies are not in this table at all.** The label-721 corpus holds 186,908
policies; 169,235 have a declaration a reader can follow. The rest exist only as keys
no policy id matches or documents the extractor rejects (`ROW_ADDRESSING…` §5.4).

## 4. The gate on the two count columns — fixed

`LARGE_POLICY_VIEWS.md` §0: `Base::with_row` read a speculative 8 KiB and mapped a
parse refusal to `Ok(None)`, so a row that did not fit was skipped by every
enumeration that goes through it — including `by_policy`, which is exactly the query
both count columns are made of. Store-wide that was 630 rows over 8 KiB, and 1,786
`by_policy` entries pointing at one.

Those rows are the asset-heavy ones: HOSKY's 343- and 352-asset records, ADA Handle's
up to 504. Counting live assets or holders per policy before that read was fixed
would have under-counted precisely the policies this page exists for, and would have
done it silently — the failure mode was a plausible smaller number, not an error.

**Fixed 2026-09-26.** `with_row_from` grows the window geometrically on
`RowRead::OutOfRange` (8 KiB → 16 KiB → 32 KiB) and keeps `Ok(None)` for the two
states that are genuinely "not a row". Re-measured through the reader on the same
book: HOSKY returns 36,948 refs · 20,309 holders · 420,420 names, ADA Handle 170,062 ·
74,129 · 313,019 — every figure §3's Python walk measured, where the windowed reader
returned 36,946 and 170,035. See `LARGE_POLICY_VIEWS.md` §0 and §8 for the run.

## 5. Holders: exact, and the set is per policy

Holders is the column with a body count — `NATIVE_HOLDER_SNAPSHOT_PLANE.md` exists
because HOSKY took holder-distribution down for two weeks. The shape that avoids a
repeat is **one policy at a time**: `by_policy` groups by policy already, so a single
policy's run can be deduped with a set bounded by *that policy's* holder count rather
than by the store's, and the count that comes out is exact.

That was not the first shape proposed here. An external sort of every `(policy, key)`
record would also be exact, and was the plan while the peak memory looked like the
store's; it is not needed, because the grouping is already in the artifact's own
ordering. What it costs instead is time: every policy's run is walked, and each row is
read twice — once for `by_policy`'s confirm, once to get the address and the assets
out. A fused `by_policy` that handed the row to its caller would remove the second
read.

Both halves are measured by one command, which walks every policy in the book:

```sh
./target/release/fold-probe catalogue --book <BOOK> [--dump <path>]
```

It reports the store-wide distinct counts — (policy, name), (policy, address),
(policy, credential), and how many of those credentials are script hashes — the
largest single policy's holder and name sets (the peak an exact pass has to hold), the
distribution of policies over 100 / 1k / 10k / 50k / 100k / 1M holders, and the
meter's per-operation timings. `--dump` writes the per-policy numbers so the
distribution can be redrawn without a second pass.

**Measured 2026-09-26** — `fold-probe catalogue` over `/Volumes/cardano/fold-book/local`
(10,896,105 rows, `base_slot` 198,590,382), **29.0 s** single-threaded, 0 unreadable
refs:

| | store-wide |
|---|---|
| policies | 231,810 |
| refs (= `by_policy` entries) | 10,932,696 |
| asset entries | 18,295,428 |
| distinct (policy, name) | **10,340,117** |
| distinct (policy, address) | **6,336,198** |
| distinct (policy, credential) | **6,254,132** |
| of those, script credentials | 130,836 |
| largest holder set | **151,030** (`a0028f35…c481c235`, one asset name) |
| largest name set | **420,420** (HOSKY) |

Distribution over holders: **5,130** policies ≥100 · **869** ≥1,000 · **99** ≥10,000 ·
**6** ≥50,000 · **2** ≥100,000 · none ≥1,000,000.

Three things follow.

1. **Exact, no cap, no spill.** A cap would cut the tail and nothing else — 99 of
   231,810 policies have 10,000 or more holders. And the peak an exact
   pass holds is *one policy's* sets — HOSKY's 420,420 names (~13 MB of slice keys
   plus table overhead) and, on a different policy, 151,030 holders with 150,820
   credentials (~10 MB). No policy holds both maxima, so the pass is tens of
   megabytes — not the whole store's 10.3 M name keys and 6.3 M holder pairs held at
   once, which is what a fused pass would need.
2. **The largest holder sets are single-name tokens, not the multi-asset
   collections.** Six policies are above 50,000 holders, and **five of them carry
   1–6 asset names each** — 151,030 · 135,518 · 97,703 · 89,620 · 77,497 addresses.
   ADA Handle is the sixth (74,129 holders, 313,019 names); HOSKY, at 20,309, is not
   among them. `NATIVE_HOLDER_SNAPSHOT_PLANE.md`'s HOSKY story is about *page* size
   and this does not contradict it — but a holder-distribution plane sized on the
   NFT collections would size itself on the wrong policies.
3. **The cost is re-reading, not memory.** The meter: 10,932,696 `book_utxo` calls
   (each row is read twice — once for `by_policy`'s confirm, once to get the address
   and assets out), 231,823 enumerations, and **234,104,327** `policy.tbl` lookups —
   a row is visited once per policy it holds, and every visit rescans all of its
   asset records: 234 M scans for 18 M records. That is ~5 s of the 29, and it is the
   price of the per-policy shape; scanning each row once instead would need every
   policy's sets resident at the same time.

⚠️ **The meter's own clock is visible at this call count.** The wall clock (29.0 s)
exceeds the sum of attributed operations (≈20.0 s) by ~9 s, which is consistent with
two `Instant::now()` reads per observation across 245 M observations — so an
uninstrumented pass is ~20 s, and the means above are the honest cost while the
sub-quantum percentiles are not (`table_lookup` and `address_codec` both report
p50 = 0 ns, i.e. below this machine's clock granularity).

## 6. The proposed record, not built

One record per policy:

```
policy       [u8; 28]
name         u32 string-table offset · u8 flags (declared | derived) · u8 share
live_assets  u32
holders      u32
```

186,908 × 42 B ≈ 7.8 MB, plus a small string table: the whole catalogue is a ~10 MB
artifact, publishable with the discipline the book already uses (manifest,
`layout_version`, swap by `rename`, an attestation).

The count is the open half of that. 186,908 is the metadata walk's collection count;
the book holds **231,810** policies with live assets (§5), and neither set contains
the other — a policy can have metadata and nothing live, or be live with no metadata
(`PHONEBOOK_COVERAGE.md`). A record per book policy is 231,810 × 42 B ≈ 9.7 MB. What
the join's population should be is a decision, not an arithmetic one.

It should be **its own artifact rather than a table inside the book**. It is built by
two passes over two sources — the metadata walk for the name, the book for the counts
— joined on the 28-byte policy id, and either side has to be refreshable without
rewriting the other. A consumer that wants the live set need not carry the catalogue,
or the reverse.

## 7. Open

- The exact union of the six declared fields (bounded at 8.8% here; the next probe
  pass prints it).
- Whether the candidate rule needs to be better than "strip the edition marker". It
  is right for 91.8% of policies, and the interesting failures are in the 50–90% row
  — the `<50%` row is mostly namespaces and one-off names, where "no name" is the
  correct answer.
- Holder exactness: the pair count is what `fold-probe catalogue` measures (§5); the
  run is the next step.
- Where the artifact is published and who reads it: that belongs to the phonebook
  plan, not here.
