# Row addressing, wire size and search — measured

**Status: MEASURED** (2026-09-25). Companion to `ARTIFACT_SHARDING.md` §4.1/§6
(which fixes parts at 1 MiB and re-addresses rows to `(chunk, ordinal)`) and to
`LARGE_POLICY_VIEWS.md` §0/§3. Nothing here is a work order.

The brief was three properties for the files we send over the wire — **small**,
**pageable**, **searchable without an external engine** — and this prices each
against the real book (`/Volumes/cardano/fold-book/local`, layout_version 3,
10,896,105 rows, 1.68 GiB). Scripts and commands in §9; every figure below is
measured on mainnet rows unless it says *model*.

---

## 1. What the neighbours already fix

`ARTIFACT_SHARDING.md` §6 sets **part size by substrate, not by format**: 1 MiB
for the hot and warm tiers, up to 25 MiB cold, uniform parts so a logical offset
resolves by arithmetic (`offset / PART`), and 1 MiB is where Cloudflare's depth
curve flattens (33 ms at 1 MiB, 69 ms at 16 MiB, 438 ms at 64 MiB). Two of its
consequences matter for everything below:

- **Ranges and compression are mutually exclusive.** Cloudflare will not map a
  range onto compressed bytes (`MITOS_AT_THE_EDGE` §4.10, trap 2), and
  multi-range works only *within one object*.
- **The log tier re-addresses rows** to `(chunk, ordinal)` (§4.1), which is the
  point at which "how do I find row N" stops being free: in `base/` the
  orderings carry a byte offset per row, but `log/created/<chunk>.bin` has no
  ordering at all, so it needs either an ordinal → offset table or a stride.

---

## 2. Size

| object | raw | zstd -19 | ratio |
|---|---|---|---|
| `rows.bin`, 128 MiB slice (cold bulk) | 128 MiB | **43.7 MiB** | 2.9× |
| the same slice through **gzip -9** (what a CDN applies) | 128 MiB | 69.4 MiB | 1.85× |
| ADA Handle arena (93.6 MiB) | 93.6 MiB | **24.5 MiB** | 3.8× |
| HOSKY arena (29.5 MiB) | 29.5 MiB | **5.64 MiB** | 5.2× |
| `by_policy.idx` (10.9 M entries) | 125 MiB | **31.0 MiB** | 4.0× |
| `policy.dir.idx` (2²¹ buckets) | 8.0 MiB | **0.42 MiB** | 19.7× |
| ADA Handle name table (313 k entries) | 4.63 MiB | 1.11 MiB | 4.2× |
| HOSKY name table (420 k entries) | 10.83 MiB | **11.5 KiB** | 940× |

Three findings, and the third is the interesting one.

**The derived files are not incompressible, even the sorted ones.** `by_policy.idx`
is a sorted permutation and still loses 75% — repeated prefixes, near-monotone
offsets. A directory is 20× smaller compressed, because it is a monotone u32
sequence. Nothing on the hot tier needs to be 8 MB on the wire.

**The codec matters more than the format does.** zstd -19 gets 2.9× on the cold
bulk where gzip gets 1.85×, but zstd is not free to decode (a WASM decoder,
unless the substrate does it), while gzip/brotli arrives at no cost on any
whole-object response. ⚠️ Either way this is a **whole-object** lever: a
range-read object must stay raw.

**Search data can be almost free.** HOSKY's name table is 10.8 MiB of
`HOSKYCashGrab000000NNN` and compresses to 11.5 KiB — a counter pretending to be
a table. Human handles do not (4.2×), but 1.11 MiB is still shippable (§5).

---

## 3. Pageable

A row is 56 B of header plus variable-length parts; at 4 KiB pages one row is one
page (p50 130 B across the store means 32 rows share a page), so locally the page
cache is doing most of the work already — `Base::open` is 73 ms and a point read
1,223 ns (`LIVE_BOOK.md` §B9).

What changes at scale is the *arena*, because that is the object a policy scan
reads whole:

| | HOSKY Cash Grab | ADA Handle |
|---|---|---|
| rows | 36,948 | 170,062 |
| arena bytes | **29.5 MiB** | **93.6 MiB** |
| 4 KiB pages | 7,551 | 23,971 |
| 1 MiB parts | **30** | **94** |
| a full scan (clustered) fetches | 30 parts | 94 parts |
| 50 scattered rows touch | 50 pages · **29 parts** | 50 pages · **47 parts** |

⚠️ **The scattered row is still the expensive one, and clustering does not fix
it.** 50 rows spread through an arena cost 29–47 *part* reads — 29–47 MiB to
answer a question about ~30 KB of rows. That is `MITOS_AT_THE_EDGE` §3.3's
finding, now with arena sizes attached, and it says the browse/point path wants a
**page-shaped artifact** (§6.4) rather than random access into the arena.

⚠️ **A policy's rows are 3–4× fatter than the store's mean row** (837 B and
577 B against 165.6 B). They are size-biased by construction: the rows holding
one asset of the policy hold the *holder's whole balance* — 890,009 and 1,831,805
asset entries, of which only 420,425 and 313,025 are the policy's own. Any
sizing that multiplies the store mean by a row count will be 3× low — the error
`LARGE_POLICY_VIEWS.md` §3's first draft made, and now corrects.

---

## 4. Addressing: four schemes, priced per policy

Every row read individually — the confirm-per-candidate shape a scan degenerates
into without clustering:

| scheme | HOSKY | ADA Handle | note |
|---|---|---|---|
| today: 8 KiB window | 0.28 GiB | 1.30 GiB | **and 2 / 27 rows are silently lost** |
| grow from 512 B | 42.9 MiB | 159.1 MiB | 43.3% / 42.9% of rows take a second read |
| stride 256 B + spill | 36.1 MiB (18.3% waste) | 123.1 MiB (23.9% waste) | 59% / 52% of rows spill |
| stride 512 B + spill | 42.9 MiB (31.2% waste) | 159.1 MiB (41.2% waste) | 43% / 43% spill |
| stride 1024 B + spill | 54.4 MiB (45.8% waste) | 198.6 MiB (52.8% waste) | 18% / 8% spill |
| **blocks 64 KiB, packed** | **30.1 MiB (2.0% waste)** | **94.9 MiB (1.3% waste)** | bounded 64 KiB read |

**Per-row stride is the wrong trade at these sizes**, and the reason is sharper
than "padding": at a 256 B stride, 59% of HOSKY's rows spill and those rows hold
**27.1 of the 29.5 MiB** — the spill *is* the data. The stride converts "most
rows fit" into "most rows take two reads and most bytes are in the overflow",
while costing 18–53% in padding. It is a fine design for a corpus of uniform
small records; this corpus is a fat-tailed one.

**Grow-on-failure is the same read profile as a 512 B stride with none of the
padding** (43% of rows take a second read in both), and it needs no format
change. **Block-aligned packs** are the version worth adopting if the format
moves anyway: 1.3–2.0% waste, one bounded 64 KiB read, no second read ever, and
it composes with the 1 MiB part model — a part is 16 blocks.

---

## 5. Search without an external engine

The live set already contains the searchable text: **every asset name is in its
row**. What it does not have is an ordering over names. A per-collection
`name → row` table is one permutation, the same shape as `by_policy`:

| | HOSKY | ADA Handle | whole store |
|---|---|---|---|
| name entries | 420,425 | 313,025 | **18,295,428** |
| name bytes | 8.82 MiB | 3.14 MiB | **206.5 MiB** |
| mean name | 22.0 B | 10.5 B | 11.8 B (longest 32 B) |
| table (name ‖ u32 row) | 10.83 MiB | 4.63 MiB | 276 MiB |
| compressed | **11.5 KiB** | **1.11 MiB** | — |
| exact lookup | 11–12 page probes | 10–11 page probes | binary search |

- **Exact name**: binary search — 10–12 page-sized probes, or *zero* network
  reads if the table is resident (4.6 MB is, comfortably; the whole-store table
  is not, which is why it is per collection).
- **Prefix**: the run is contiguous, so it is a bucket read plus a scan — but
  ⚠️ **the run can be the whole collection**: ADA Handle `'c'` → 18,990 names,
  and *every* HOSKY name shares its first six bytes, so `'H'`, `'HOS'` and
  `'HOSKYC'` each return all 420,425. Short prefixes need a result cap and a
  "keep typing" discipline, not a smarter index.
- **Substring/fuzzy**: a linear scan of 3–11 MiB of names, in memory, single-digit
  milliseconds. **At this size the smallest index is the data itself** — an
  inverted or trigram index would be larger than the thing it indexes.
- **Corpus scale is where it stops working**: 206.5 MiB of names, or 765 MiB with
  the policy column. A global name index is the wrong artifact; per-collection
  tables are right, and they are also what a browser can hold (§6.4).

**Not searchable from the live set**, and no index changes that: traits, rarity
and images (the mint ledger + metadata source — `CIP86_METADATA_ORACLES.md`),
listing state, and history. The book answers *who holds what now*; a filter by
trait is a join, not a lookup.

### 5.1 The card table, built and queried

Prototyped against both collections (`.tmp/card-table-probe.py`), one entry per
asset name: `u8 name_len ‖ name ‖ flags ‖ [32 B datum hash] ‖ u8 addr_len ‖
address ‖ u32 row`, sorted by name, with a `u32` page directory over 4 KiB pages.

| | HOSKY | ADA Handle |
|---|---|---|
| cards (one per asset name) | 420,425 | 313,025 |
| rows they cover | 36,948 | 170,062 |
| table | 34.57 MiB | 21.52 MiB |
| — of which **addresses** | 22.80 MiB | 15.43 MiB |
| mean entry | 86 B | 72 B |
| page directory | 0.034 MiB | 0.021 MiB |
| compressed (zstd -19) | **2.59 MiB** (7.5%) | **7.21 MiB** (33.5%) |
| validation | **420,425 / 420,425** agree with their row | **313,025 / 313,025** |
| browse page (20 cards) | 1,720 B over 2 pages | 940 B over 1 page |
| exact name | 2 page fetches (8 KiB) | 2 page fetches (8 KiB) |
| prefix `'HOSKYC'` / `'cro'` | 420,425 (the whole table) | 209 (15.2 KiB) |

It works, and it is cheaper per browse page than reaching into the arena by an
order of magnitude in requests and four in bytes (§3: 20 scattered rows are up
to 20 part reads, ~20 MiB at part granularity; a card page is 1–2 reads of ~1 KB).
Two things the build exposed:

⚠️ **The address column is 66% of the bytes and it duplicates.** HOSKY has
20,309 distinct addresses across 420,425 cards — every holder's address repeated
once per asset it holds. It compresses to almost nothing *because* of that
regularity (7.5%), but the duplication is a design smell rather than a feature.

⚠️ **A variable-length table is not searchable without the directory.** You
cannot bisect records whose lengths vary; the page directory is what makes the
exact lookup two fetches instead of a walk from the start. Any name-indexed
artifact needs one.

**So the cheaper artifact is a permutation, not a table.** Order a policy's
arena slice by its alphabetically-first asset name and publish the name ordering
in the shape every other ordering already has — `{u64 name prefix, u32 row}`, 12 B
an entry:

| | HOSKY | ADA Handle | all 18.3 M names |
|---|---|---|---|
| entries | 420,425 | 313,025 | 18,295,428 |
| bytes | **4.81 MiB** | **3.58 MiB** | 209 MiB (≈55 MiB compressed) |
| directory | one shared 8.39 MB | | |

- **browse**: a page's rows are a contiguous range of the name-ordered slice, so
the owner comes from the row in the same range — no address column, no table;
- **exact and prefix**: bucket read plus a confirm against the row, the same
shape and the same soundness argument as `by_policy`;
- **it is derivable by `reindex` from `rows.bin`** — a search artifact can be
added to books that already exist, with no refold;
- the global form is expressible too (209 MiB), which is what makes cross-collection
name search possible without a second system.

### 5.2 Would this search all of Cardano's assets by name?

Partly — and the measurements say which part. Over the whole live set,
**18,295,428 asset entries**:

| | |
|---|---|
| distinct 1-byte prefixes (of 256) | 256 · biggest run **1,870,328** names (10.2%) |
| distinct 2-byte prefixes | 38,710 · biggest **776,822** (4.26%) |
| distinct 3-byte prefixes | 123,154 · biggest **757,287** (4.15%) |
| empty names | 39,409 |
| quantity == 1 (the cheap NFT-ish proxy) | 11,384,638 (62.2%) |

- **Exact names: yes, and it is the capability nothing else here has.** One
  ordering — 18.3 M × 12 B = **209 MiB** plus a directory, ~55 MiB compressed,
  derivable by `reindex` from `rows.bin` — and a lookup is a bucket read plus a
  confirm, the `by_policy` shape. ⚠️ Names are unique only *within* a policy, so
  a global key returns a run of same-named assets across policies; the policy is
  what disambiguates a hit.
- **Prefixes: retrieval yes, selectivity no.** Three characters barely separate
  anything — the biggest 3-byte run is 757,287 names, and the twenty biggest
  prefixes span **2,917** policies (`'Car'`), 2,763 (`'The'`), 1,629 (`'ADA'`),
  164 (`'HOS'`). A global prefix query needs a minimum length, a collection
  scope, or ranking on top; the ordering itself is not the limit.
- **Substring/fuzzy: no.** A sorted table gives prefix, not infix. A scan of the
  206.5 MiB name space is fine on a node and not at the edge, and an n-gram
  postings index would be *larger than the 206.5 MiB it indexes* — the same
  "smallest index is the data" result as §5, one scale up.
- **And "NFT" is not a property the live set carries.** The 62.2% quantity-1
  figure is a proxy, not a definition — minted supply and CIP-25/68 metadata live
  in the mint ledger and the metadata projection — and 39,409 entries have empty
  names, with tickers among the biggest prefixes. The corpus is assets, not NFTs.

So: exact name → any live asset, globally, in one or two reads; prefix → bounded
by query length and by whether the search is scoped to a collection; "search all
NFTs by any substring, ranked" → a different product, needing an index larger
than the names and a mint-ledger join to know what an NFT is.

### 5.3 Substring and fuzzy: what it would take

⚠️ **The prototype killed the obvious design.** The standard shape is "index the
rare n-grams, verify the candidates against the names" — and on this corpus it
does not work, because the informative trigrams are the *common* ones: whole
collections are systematic (`HOSKYCashGrab000NNNNNN` × 420 k names, plus 774,380
CIP-67-labelled ones). Built for real at a df ≤ 1,000 cut
(`.tmp/ngram-index-probe.py`), **every trigram of `HOSKYCashGrab000210208`
exceeds the ceiling and the query is unanswerable** — while the *typo'd* variants
were answerable, via the rare trigram the typo created. Exactly backwards.

Index everything, then, and price the query (`query-scan-probe.py`):

| query | hits | collections hit | postings to read |
|---|---|---|---|
| `HOSKYCashGrab000210208` | 1 | 1 | **29.0 MiB** |
| `KYCash` | 420,433 | 7 | 6.6 MiB |
| `crownlocksmith` | 1 | 1 | 1.5 MiB |
| `rownlock` | 1 | 1 | 0.9 MiB |
| `0000` | 147,817 | 3,870 | — |
| `snekk` | 91 | 4 | — |

| | measured |
|---|---|
| distinct trigrams over all 18.3 M names | 1,999,325 |
| **indexing every trigram** | 179,116,123 postings = **683 MiB** at 4 B |
| the 1,000 most common trigrams alone | 44.8% of all postings |
| trigrams with df ≤ 1,000 (the cut that fails) | 14,360,522 postings = 55 MiB |
| the built artifact, df ≤ 1,000, complete | **433 MiB** — postings 55, keys 7.6, offsets 7.6, and a 293.7 MiB verification blob (names + a u32 row ref, which a reader could drop for a page read per candidate) |

So a corpus-wide trigram index is **683 MiB** (three times the name space, so not
a Worker-isolate structure), an exact lookup on a *systematic* name reads **29 MiB
of postings**, a six-character infix reads 6.6 MiB and returns **420 k hits across
7 collections**, and a short one (`0000`) returns 147 k hits across 3,870. Only
long, distinctive infixes are cheap (`rownlock`: 0.9 MiB, one hit). And that is
not a data-structure failure: the queries a user makes against a systematic
collection are *prefixes*, which §5's ordering already answers in one or two
reads.

**So it is three tiers, not one structure:**

1. **Exact + prefix, corpus-wide** → the 209 MiB permutation (§5.1), one or two
   reads. Covers the systematic majority of this corpus.
2. **Substring + fuzzy, per collection** → scan that collection's names: 3.1 MiB
   (ADA Handle), 8.8 MiB (HOSKY), tens of KB for the median tracked collection.
   **No index at all**, so the "smallest index is the data" rule holds at this
   scope — and it is small enough that a *client* can hold one collection and
   search it locally (shipped compressed: 11.5 KiB, 1.11 MiB).
3. **Substring, corpus-wide** → tier 2's index over everything: 683 MiB, 1–29 MiB
   read per query, node-side, with **ranking** on top — 420 k hits is a ranking
   problem, not a retrieval one.

Fuzzy is a client of tier 2 or 3: candidate generation by trigram (a one-edit
typo changes at most three of them), then a bounded edit distance over a handful.

What all three inherit:

- **Both projections come from the walk**: postings are doc ids, and the names
  they verify against are the §5.1 table. One `reindex` pass produces both.
- ⚠️ **113,021 names are shorter than three bytes and 39,409 are empty** —
  outside any n-gram index, so they need a small side index or a scan.
- ⚠️ **774,380 names begin with a byte below 0x10** — CIP-67 labels. **299,887
  carry label 100 and 296,451 label 222**, the one NFT signal that lives in the
  name itself, so a CIP-68 filter needs no metadata join. Labels should be
  stripped before display and before indexing.
- **Case is byte-exact**, and the corpus proves it matters: `cash` matches 462
  names, while the HOSKY family alone is 420 k names containing `Cash`. Insensitive
  search needs folded keys.
- **Ranking is not in here.** The index locates; ordering hits by
  holders/volume/rarity is a join over the collections' own projections (below).

**Once located, the rest is a join, and two of its three inputs already exist.**
Ownership is the row (plus the delta). Traits in the CIP-68 case are the datum —
inline in the row or in `datum.bin` (`DECODE_SHAPE.md` §2.10). Traits in the
CIP-25 case are the *mint log*, and `crates/policy-index` is already exactly
that: its records carry the minting transaction's auxiliary-data span, and
"asset → mint transaction → its metadata is then a single `pread`" (its own
header). What is missing on that path is the decoder from that metadata into
trait rows — a projection, the subject of `CIP86_METADATA_ORACLES.md` — not a
new walk. A thumbnail service can be handed the located identifier and the
metadata's image URI; the *pixels* still come from wherever that URI points.

### 5.4 The searchable corpus is metadata, not the asset name

⚠️ **Everything in §5–§5.3 indexes the on-chain asset name** — the ≤32-byte value
in the row. What a user types is usually the metadata `name` (CIP-25 `721`, or the
CIP-68 datum), a different corpus in different artifacts, and larger.

The two halves live in different places and only one is measurable from what is
already on disk:

| | measured | where |
|---|---|---|
| **CIP-68** metadata (a datum) | **~400–480 B per asset** | `datum.tbl` windows: Babbage 8,400 of 373,559 datums mention `image`+`name` (2.3%), mean **480 B**, p90 545; tip 3 of 1,504 (0.2%), mean 402 B; **Alonzo 0** — CIP-68 is Babbage-era, so the detector is measuring what it claims |
| **CIP-25** metadata (`721` aux) | **518 B per asset · 5.6 GiB** | `fold-probe cip25` over all 9,194 chunks (below) |

**The CIP-25 half, measured end to end** — one store-wide pass through the platform's
own decoder (`fold-probe cip25 --stride 1`, `cardano-assets` at rev `f24eb797`;
337 s, 1.65 GiB peak RSS):

| | |
|---|---|
| transactions walked | 123,964,864 |
| carrying auxiliary data | 38,309,568 (**30.9%**) · **13.09 GiB** of aux |
| declaring label 721 | 4,689,702 txs · **11,650,457 assets** · 2.5 assets a tx |
| **collections (policies) with CIP-25 metadata** | **186,908** |
| metadata CBOR | **5,758.5 MiB** · 518 B an asset · 641 KiB a chunk |
| **decoded by `cardano-assets`** | **11,158,057 (95.8%)** |
| **assets whose metadata yields traits** | **9,554,052 (85.6% of decoded)** |
| **trait pairs** | **61,036,216** · 5.24 a declared asset |
| **distinct (trait, value) pairs** | **2,466,655** · over the viewer's 512 bitmap cap: **474 collections** |

Three things fall out. **The metadata corpus is ~28× the name corpus** (5.6 GiB
against 206.5 MiB), and that is the CIP-25 half alone — the CIP-68 datum half adds
~140 MiB. **186,908 policies carry CIP-25 metadata**, which is the real size of
the collection catalogue: not the ~163 tracked today, and not 231,810 either.
(An earlier pass counted **188,122** by raw key; the smaller number is the correct
one, because canonicalising a policy key to its 28 bytes merges the collections
that appear both as bytes and as hex text.) And **2.5 assets a declaration**
means metadata is scattered across millions of mint txs rather than arriving one
blob per collection — the ledger's 16 KB tx limit at work, and the reason the mint
log is the right way to find it.

**What the decode costs, same chunks, same machine, three passes back to back:**

| pass | wall | what it adds |
|---|---|---|
| `count` | **227 s** | the walk, no metadata work at all |
| `cip25 --extract value` | **291 s** | + the 721 walk, `cip25_metadata_value` (aux CBOR re-decode → `Metadatum` → `Value`) |
| `cip25` | **337 s** | + the v2 extractor (`AssetEnvelope` + trait extraction) |

The platform decode is **+110 s, 48%** over the walk, and the split answers the
`serde_json` question: the extractor is **46 s** for 11.3 M assets (~4 µs an asset),
not the expensive part. The larger share is the crate's value path at **+64 s** —
and on an earlier session the same walk with a hand-rolled 721 reader added ~6 s,
which is the tell: `cip25_metadata_value` re-decodes the **whole aux CBOR for every
declared asset** (11.3 M decodes of ~366 B, where a walk needs 4.7 M — one a tx),
so an entry point taking the decoded `Metadatum` its caller already holds is the
upstream change worth asking for. None of this makes the walk the bottleneck: 5.6 GiB
of metadata still sits inside a 215 GiB read.

⚠️ **And before that, the old reader could not see the traits at all.** CIP-25 does
not define traits — the convention is the minter's — and the hand-rolled reader
matched exactly one shape of them (`attributes: [{trait_type, value}]`): 21,418
assets, 136,645 pairs. The v2 extractor finds **9.55 M assets carrying traits and
61.0 M pairs — 447× the pairs** — because the flat top-level trait map, which
`cardano-assets` documents Black Flag as using, is the common shape in the wild and
the reader was blind to it. The same extractor run decides the viewer's bitmap
question, and the answer is not marginal: **474 collections exceed the 512-pair cap**,
the largest at **75,893 pairs (148× the cap)**.

**What the 4.2% that does not decode actually is, now named.** Of 11,650,457 declared
assets, **335,800 (2.9%)** sit under a key that is neither a 28-byte policy id nor
56-char hex, and **156,600 (1.3%)** are values the v2 extractor rejects. The first
group is not a reader gap — it is malformed metadata, and the pass prints the shape
and a first-seen example of each:

| shape | assets | example |
|---|---|---|
| policy key, text, 5 chars | 134,276 | `asset` |
| name key, integer | 119,196 | `1062` |
| policy key, text, 12 chars | 54,471 | `${newpolicy}` |
| policy key, text, 6 chars | 19,623 | `tokens` |
| policy key, text, 58 chars | 4,127 | `{b38d94e35b3e638dce7e5e0` |
| policy key, text, 10 chars | 1,281 | `definition` |

An unsubstituted template variable and a brace-truncated id are minter bugs no
reader can navigate past — `cardano-assets` walks the same structure and misses them
identically, which is the point of measuring through it. The integer asset names are
the one shape here that is decodable in principle (an integer is a legal CBOR key)
and matched by neither path, so it is a decision rather than a note.

**A direct `Metadatum` walk agrees asset for asset and costs ~a ninth of the CPU.** The
classification never needed the document: the field registry is the crate's own
`classify_field`, and what a fast walk has to mirror is the shape dispatch and the
value coercion. It lives in `cardano-assets` now (`metadatum::decode_document`), and
`fold-probe cip25 --extract compare` runs it and the document path on every asset the
crate's lookup finds, in one pass:

| | |
|---|---|
| the platform decoder returned an `Asset` | 11,158,057 |
| the walk declined (an on-chain name `AssetId` refuses) | **157** |
| **compared, both walks** | **11,157,900** |
| **different in `traits`** | **0** |
| different in any field | **200,132** — every one a `media_type` rewrite or the one duplicated `name`, all accounted for in §5.5 |
| both rejected | 156,600 |

So on every asset either reader can produce, the walk's `Traits` is **identical** — the
two sets agree on all 11,157,900 of them. The `unsigned_algorithms` shape is no longer
a decline: the walk calls the document path's own `extract::unsig_traits` over the
`unsigs` subtree, so the 31,158 assets an earlier pass declined are compared, and they
are equal — which is the evidence that the bespoke branch was ported rather than
approximated.

One capability that pass had is given up deliberately. Comparing *traits alone*, the
walk could read 147,380 documents whose envelope the type check rejects, because it
did not build one. The asset walk must build the `DecodedAsset`, so it needs the
envelope, and those documents sit in the 156,600 both paths reject. A traits-only mode
would get them back; nothing needs one yet.

| mode | wall | user | instructions retired |
|---|---|---|---|
| `--extract platform` | 337 s | 2,044 s | 13.78 × 10¹² |
| `--extract compare` (both walks) | 302 s | 2,231 s | 14.19 × 10¹² |
| `--extract fast` | **175 s** | 1,344 s | 10.39 × 10¹² |
| `count` (the walk, no metadata work) | 227 s | 1,670 s | 10.71 × 10¹² |

⚠️ **Wall clock on this box is not the measurement.** Each pass re-reads 215 GiB
through a 24 GB page cache, and the platform pass is *slower* than the pass that
runs both walks on the same bytes — run-to-run variance exceeds what the walk could
cost. Instructions retired are the stable number, and they decompose the pass:

| 10¹² instructions | share |
|---|---|
| the chunk walk and the 721 enumeration | 9.99 |
| the crate's value decode (aux re-decode → `Value`) | 3.10 |
| the crate's extractor (`AssetEnvelope` + traits) | 0.70 |
| the direct trait walk | **0.40** |

So the trait walk is **~9× less CPU than the crate's decode + extractor** (0.40
against 3.80), and it is the aux re-decode — not `serde_json` — that dominates the
difference. The wall gap (337 → 175 s) is larger than the instruction gap, which is
what a `Value` tree per asset costs in allocator and cache pressure rather than in
instructions.

What this settles, and what it does not. The walk belongs in the crate and is there —
one implementation, with this comparison as its gate; the 157 declines are documents
`AssetId` refuses an id for, and a `DecodedAsset` cannot carry what its identity
depends on. A traits-only reader, which is what would recover the 147,380, is a
decision with no consumer yet.

Scaling the datum half: 296,451 live names carry the (222) label, at ~480 B each
⇒ **~140 MiB of CIP-68 metadata text**, the same order as the 206.5 MiB of names —
so the datum half is not the blow-up. The CIP-25 half is, and what it is *not* is
one blob per collection: at 2.5 assets a declaration it arrives scattered across
millions of mint transactions, which is what makes the mint log the way to find
it (above).

Three consequences, and the first is the load-bearing one:

1. **The index belongs on a metadata projection, not on the book.** The book
   answers *who holds what, now*; metadata answers *what is it called and what are
   its traits*, and its sources are the mint log and the datum table. Two
   artifacts, two jobs, joined on `(policy, name)` — which both sides already key on.
2. **Only the searchable fields need to travel.** Text search wants `name`; a
   filter wants the *attributes*; neither wants images, descriptions or `files[]`.
   A projection at ~100–200 B an asset is what keeps §5.3's per-collection tier
   alive: a 10k collection's searchable text is ~1–2 MB raw and a few hundred KB
   compressed, whatever its raw metadata weighs.
3. **Traits are not text.** The marketplace filter is
   `(trait_type, value) → assets` — a postings structure over the projection, an
   order of magnitude smaller than a text index, and closer to what makes a
   collection page feel like OpenSea than substring search is.

⚠️ **And metadata is not immutable.** CIP-86 lets an oracle key rewrite a CIP-25
asset's fields long after the mint (`CIP86_METADATA_ORACLES.md`), so the
projection is a fold over history rather than a write-once artifact — which makes
invalidation part of any index built on it, not an afterthought.

### 5.5 `DecodedAsset` — the shape the walk produces

The comparison above ran against `AssetV2`, the shape a consumer reads today, and
producing that from the walk cost nothing: the id is the one thing the JSON path
cannot carry, and the walk holds the policy and the on-chain name it is made of.
But the three things `Asset`/`AssetV2` do badly are the three the walk can fix, so
`cardano-assets` now has `DecodedAsset` (`decoded_asset.rs`), produced on the JSON
path by `AssetEnvelope::into_decoded_asset` and on the fast path by the probe's
walk:

| `Asset` today | `DecodedAsset` | why |
|---|---|---|
| `image: String` + `media_type: Option<String>`, `files[]` dropped | `media: Vec<Media>`, headline first, each with `src`/`media_type`/`name`/role | `AssetEnvelope` already parses `files[]` — it needs it for the media-type fallback — and throws it away; the workaround (`AssetWithId.cids`) is IPFS-only (`cid::extract_cids`, with a test named `skips_http_image_urls`) |
| `name: String`, `""` when the document declares none | `name: Option<String>` | the crate's own `Untitled` comment says callers must fill the display name in from the `AssetId` — `""` cannot say "no name" |
| `rarity_rank` + `tags` | gone | never set by a decoder (all eight `From<AssetMetadata>` arms and `into_asset` pass `None`/`vec![]`); they arrive from the cnft.io cache on the API path, and rarity already has a home in `EnrichmentRarityScore` |

**The media type: a catalogue, and the mess it catalogues.** `cardano-assets`
now has `NftMimeType` — the types a consumer can act on — and `MediaType` is
`Mime(NftMimeType)` / `Undefined` / `Custom(String)`. Every spelling mints
publish is a serde `alias` on the variant, so `image/jpg`, `image/PNG`, `img/png`,
`.png`, `png`, `iamge/png`, `image/image/png` and `video/mp4\t` resolve to one
variant each; case is folded before the lookup (RFC 2045), which is what collapses
`IMAGE/GIF` without an alias per case.

The three groups are the point — they make the mess countable:

| | assets | spellings | what it means |
|---|---|---|---|
| `Mime(…)` | **6,531,331 (58.5%)** | 43 | a catalogue type, however the document spelled it |
| `Custom(…)` | **1,186 (0.01%)** | 40 | `type/subtype`, not yet named — the promotion list |
| `Undefined` | **11,976 (0.11%)** | 35 | not a type: `Video` 9,005, the empty declaration 1,273, `image/` 485, `image/*` 397, `<mime_type>` 214 |
| *no declared type* | 4,613,407 (41.3%) | — | the document says nothing |

The line between `Custom` and `Undefined` is a shape test — one slash, both sides
RFC 6838 tokens, no wildcard — so `Custom` is a list somebody can promote and
`Undefined` is the corpus saying there is nothing to promote. What is left in
`Custom` is led by `application/lpf+zip` (1,000 assets, still unidentified) and
`image/mp4` (87); the rest is 38 spellings of ≤17.

**SVG is the one that was a judgement, and the answer settled it:** its registered
type is `image/svg+xml` (IANA, via the W3C) — `image/svg` is a truncation and
`text/svg+xml` is a wrong top-level type, not a different format, since `text/*`
is for human-readable text. Folding `text/svg+xml` (8,908 assets) onto `ImageSvg`
is why `Custom` fell from 10,094 to 1,186, and it puts the whole SVG family —
89,175 + 8,908 + 786 + 2 — on one variant.

**What it costs.** `--extract compare`, both walks on every asset of the store:

| | |
|---|---|
| equal, every field | **10,957,768 (96.8%)** |
| **differ** | **200,132** |
| — `media_type` | **200,131** |
| — `name` | **1** |
| — `image`, rarity/tags, `traits` | **0 each** |
| the walk declined | 157 |
| both rejected | 156,600 |

The 157 declines are documents the walk cannot give an id — `AssetId` refuses the
on-chain name, and a `DecodedAsset` without one is not one. The walk reads the
`unsigned_algorithms` shape the same way the document path does, so the 31,158 assets
an earlier pass declined are compared now, and they are equal.

Three mechanisms, and they are now *counted* rather than derived: the compare
classifies each `media_type` difference where it is produced, and the reasons sum
to the field count exactly — `rewritten 187,791 · declared not a type 11,973 · no
headline 367 · other 0`, against `media_type 200,131`.

- **187,791 are the spelling table** — the declared string rewritten to the
canonical one: `image/jpg` 172,564, `text/svg+xml` 8,908, `image/GIF` 1,877,
`img/png` 1,364, `image/svg` 786, `image/PNG` 575, `.png` 522, `iamge/png` 500,
`gif` 246, `image/image/png` 189, `png` 170, `video/mp4\t` 42, and sixteen more of
≤10. That is **1.7% of the 11,157,900 walked** — 2.9% of the 6,544,493 that declare
a type — and it is the deliberate trade: one medium, one representation.
- **11,973 declared a type the catalogue does not carry** — `Undefined` where the
old shape carried the string (`Video` 9,005, the empty declaration 1,273, `image/`
485, `image/*` 397, `image` 322, `<mime_type>` 214, and the rest of those 35 rows).
It is the same 11,973 the media census counts, and it is three short of the 11,976
the document histogram counts because three of those documents carry no image at
all — they land in the row below.
- **367 declared a type and produced no medium to attach it to — accepted.** `Asset`
carried `media_type: Some("…")` beside `image: ""`; the new shape has no
document-level type, so with no medium there is nothing to attach it to. 364 declared
a type the catalogue carries and 3 an undefined one — arithmetic that closes against
the two histograms above rather than a second estimate. It is **the one deliberate
projection loss**, and it is 0.003% of the corpus: a type attached to nothing cannot be
fetched or verified, so it is dropped rather than carried as a field for 367 assets.
It is not dropped at the walk — `DecodedDocument::declared_media_type` holds it for
every document, these included — so the question was only whether the *asset* carries
it, and it does not.
- **one `name`** is the duplicated declaration. `2f633081…/LoveForCrypto1of25` is
declared twice under its policy; the crate's `find_in_map` reads the first
(`"LoveForCrypto 1 of 25"`) and the walk enumerates the document as written, so it
reads `"LoveForCrypto 25 of 25"` too. `cardano-assets` pins both halves in a test
(`metadatum::tests::a_duplicated_declaration_is_read_twice_here_and_once_upstream`).

Nothing else moved on any of the 11.1 M compared assets: `image` 0, `traits` 0,
rarity/tags 0.

**What the media look like, medium by medium.** The histogram above counts
*documents* by their declared type; `Media` is what a consumer reads, and the
corpus's **14,887,719 media** (headline 11,140,928 · `files[]` 3,746,791) split
like this:

| | catalogued | custom | undefined |
|---|---|---|---|
| headline | 6,530,971 | 1,182 | 4,608,775 — of which **4,596,802 declared nothing at all** |
| `files[]` | 3,369,972 | **358,567 (9.7%)** | 18,252 |

**A `files[]` entry cannot omit its type.** `AssetFile.media_type` is a required
field, so an entry without `mediaType` fails deserialization and declines the whole
document — the walk mirrors that rule, and
`decode_document_declines_what_the_document_path_cannot_produce` pins it. Declaring
nothing is therefore a headline-only state, and it is the only way a minter can
force a sniff.

The `files[]` row is the finding: **9.7% of file entries declare a type-shaped
value no catalogue carries**, against 0.011% of headlines, and in a vocabulary the
headline side never uses — `application/pdf` 80,996, `audio/mpeg` 78,752,
`audio/wav` 68,119, `audio/mp3` 60,301, `audio/x-wav` 17,820,
`application/octet-stream` 6,127, `model/fbx` 3,288, `video/x-matroska` 118,
`font/woff2` 2. And 15,566 file entries declare the literal string `url`. A
catalogue chosen from the headline corpus does not cover the files beside it: they
declare audio, documents and models, and nothing else in the corpus does.

**Where the bytes are the only answer.** Of the **4,627,027 media with no usable
type** (headline 4,608,775 · `files[]` 18,252):

| | media |
|---|---|
| the `src` is a bare CID | **4,471,192 (96.6%)** |
| the `src` ends in an extension | 81,895 (1.8%) — `png` 56,684 · `jpg` 22,356 · `gif` 1,485 · `php` 1,313 · … |
| neither | 73,940 (1.6%) |

96.6% of the population is IPFS-addressed with no filename to read, which is the
size of the step where a sniff is load-bearing rather than a convenience — the one
the IIIF worker already takes (`image_info::ImageFormat::from_magic_bytes`). The
extension bucket is a shape test on the last path segment, not a promise: the `php`
rows are API endpoints, and the spellings are printed beside the count so that is
visible rather than assumed. For the 41.3% of documents that declare nothing, the
catalogue carries no type at all — `MediaType::Undefined` serialises as an omitted
field, which is the signal a consumer sniffs on. There is no MIME type for "auto",
and omission is the only spelling of it that exists.

---

## 6. What I would build

1. **Fix the read, not the file.** Speculate a small window; on `OutOfRange`
   take the length from the header already in hand and fetch exactly that. 43% of
   *these two policies'* rows take a second read and the store average would be
   lower; the bytes stay 1.68 GiB and nothing is rebuilt. `Ok(None)` must mean
   "not a row", never "longer than I guessed".
2. **If the format moves anyway** (sharding §4.1), make parts **block-aligned
   packs** rather than per-row slots, and carry the alignment in the manifest so
   a reader can over-read one bounded block.
3. **Publish a name ordering** — `{u64 name prefix, u32 row}` sorted by
   (policy, name), 3.6–4.8 MiB for these two collections and 209 MiB for the
   whole corpus. Order the policy's arena slice by name so a browse page is a
   contiguous range and the owner comes from the row rather than from a
   duplicated address column. It rides `write_ordering`, so `reindex` can add it
   to an existing book.
4. **Ship the client a name list**, not an index — `name ‖ u32 row` compressed
   (11.5 KiB for HOSKY, 1.11 MiB for ADA Handle) is small enough that a browser
   can hold a collection and search it locally — *including substring and fuzzy*,
   by scanning it. §5.3's tier 2 is deliberately not an index.
5. **Do not build a corpus-wide substring structure yet.** It is 683 MiB and
   1–29 MiB of postings per query (§5.3), and the corpus's own skew means the
   queries it would serve are mostly prefixes that item 3 already answers. If a
   global infix search is ever wanted, it is a node-side service with ranking,
   not a table beside the book.
6. **Compress where the fetch is whole**: cold `log/**` parts, hot-tier objects
   fetched whole, and these tables. Leave range-read arenas raw — or fetch them
   whole and pay a decode step per part.

---

## 7. Open

1. **Codec**: does the substrate decode zstd (Workers, browsers) or only
   gzip/brotli? The difference is 2.9× against 1.85× on the cold bulk.
2. **Card table columns**: is the owner address worth 57 B an entry, or a hop
   from the row? The answer depends on whether grids render owner.
3. **The tail**: 231,810 policies cannot each have a published table. On-demand
   derivation from the arena for anything below the ~165 big ones?
4. **`(chunk, ordinal)` locating**: per-chunk stride, or an ordinal→offset table
   per chunk? Both are cheap at 38,720 outputs/chunk (a table is 155 KB a chunk,
   1.4 GB over the store) — but check first that anything *addresses a created
   row by ordinal for its bytes*: the ordinal's consumer in the proposal is the
   dense liveness bitmap (§4.2), which needs no byte address, and the two byte
   readers are sequential (rebuild) or tx-index-directed (spend resolution).
5. **Does a name-ordered card table stay current?** It is a projection, so it
   inherits §7.1a's delta problem.

---

## 8. The viewer's per-collection bundle, and what full-Cardano would take

`~/code/hodlcroft/viewer` already ships a per-collection serving artifact —
`collection.bin` plus `sprites/` and `hcf/` shards — and it is the same animal as
§5's card table, arrived at from the other end: it ingests a curated collection
(17 configs under `configs/cardano/`, ~36 in `ingestion_list.txt`) and emits
~350 KB per 10k tokens with O(1) asset lookup (a perfect hash), instant trait
filtering (a per-token attribute bitmap *and* a `trait:value → token` inverted
index), a sorted string table for autocomplete, 4×4 sprite sheets for thumbnails,
and fixed-size image shards served by range request. Its incremental design
appends in mint order with stable token indices.

Asked of the local book, the two halves meet:

| collection | policy | chain says |
|---|---|---|
| Funplastic | `f7f5a12b…` | **10,000** live names · 2,515 rows · 1,724 holders |
| Toolheads | `285c0b8e…` | **6,600** live names · 2,720 rows · 872 holders |

So the token set a bundle indexes is derivable from the walk in one query, and
the join key is `(policy, asset name)` — which both sides already use. ⚠️ Rows
are not tokens: 10,000 tokens live in 2,515 outputs, so one row holds about four
of them, and a join has to expand the row's asset records rather than count rows.

**Where full-Cardano breaks the bundle** — all four are caps in its own format doc:

| cap | limit | measured on the live set |
|---|---|---|
| `u16` token indices | 65,535 tokens a collection | ADA Handle **313,025**, HOSKY **420,425** — 4.8× and 6.4× over |
| `u16` string-table offsets | 64 KB of deduplicated strings | HOSKY's names alone are **8.8 MiB** |
| bitmap U64–U512 | 512 trait:value pairs, beyond which `traits.ignore` is required | **474 collections over it**, the largest at **75,893 pairs** (148× the cap) — store-wide `cip25` pass (§5.4) |
| `u32` HCF offset | 4 GB of images a collection | 10k at ~100 KB is 1 GB, so headroom for now |

**Three inputs the bundle has no chain-side source for yet**, and one it should
not have: the token set (above — one query, works), mint order and mint block
(the mint log), and traits/names (the §5.4 metadata projection). Ownership is the
fourth and belongs to the book: a bundle is static and says "token #42 exists",
never "who holds it now".

**Scale.** The indexes are cheap — 231,810 policies at the tracked profile
(median 2,600 tokens) is single-digit GB, and the 165 at ≥10k tokens about 60 MB.
The images are the problem: a 10k collection at 100 KB an image is 1 GB, so the
sane shape is traits and names for everything and image bytes on demand — the
bundle's own shape, fed from the chain.

## 9. Provenance

```sh
# per-policy layout, addressing and search numbers (writes .tmp/arena-*.bin, .tmp/names-*.bin)
/usr/bin/python3 .tmp/arena-probe.py <policy>

# the whole store's name space
/usr/bin/python3 .tmp/name-space.py /Volumes/cardano/fold-book/local

# compression: zstd -19 --long=27 -T0 on the extracted objects, gzip -9 on the slice
zstd -19 --long=27 -T0 -f -o .tmp/arena-f0ff.zst .tmp/arena-f0ff48bb.bin
zstd -19 --long=27 -T0 -f -o .tmp/cards-f0ff.zst .tmp/cards-f0ff48bb.bin
gzip -9 -k -v .tmp/rows-slice.bin          # 128 MiB slice of rows.bin from offset 512 MiB

# §5.1 the card table: build, validate every card against its row, time the queries
/usr/bin/python3 .tmp/card-table-probe.py <policy>     # → .tmp/cards-<policy8>.{bin,dir}

# §5.2 the corpus-wide name space: prefix selectivity, empty names, quantity proxy
/usr/bin/python3 .tmp/name-search-probe.py /Volumes/cardano/fold-book/local

# §5.3 substring/fuzzy: trigram document frequencies, stop-gram filter, candidates
/usr/bin/python3 .tmp/ngram-probe.py /Volumes/cardano/fold-book/local

# §5.3 the rare-trigram index, built and queried (its negative result)
/usr/bin/python3 .tmp/ngram-index-probe.py /Volumes/cardano/fold-book/local

# §5.3 what indexing everything would cost per query: hits, collections, postings
/usr/bin/python3 .tmp/query-scan-probe.py /Volumes/cardano/fold-book/local

# §5.4 how big the metadata corpus is, from the datum tables already on disk
/usr/bin/python3 .tmp/datum-metadata-probe.py .tmp/datum-bab

# §5.4 the CIP-25 half: whole-store pass through the platform decoder
./target/release/fold-probe cip25 --immutable /Volumes/cardano/mithril/db/immutable --stride 1 --trait-cap 12000000
# the A/Bs: the walk alone, the crate's value path without the extractor, the direct
# Metadatum walk, and both walks compared asset by asset
./target/release/fold-probe count --immutable /Volumes/cardano/mithril/db/immutable
./target/release/fold-probe cip25 --immutable /Volumes/cardano/mithril/db/immutable --stride 1 --extract value
./target/release/fold-probe cip25 --immutable /Volumes/cardano/mithril/db/immutable --stride 1 --trait-cap 12000000 --extract compare
./target/release/fold-probe cip25 --immutable /Volumes/cardano/mithril/db/immutable --stride 1 --trait-cap 12000000 --extract fast
/usr/bin/python3 .tmp/datum-metadata-probe.py .tmp/datum-alo
/usr/bin/python3 .tmp/datum-metadata-probe.py .tmp/datum-tip5
```

`arena-probe.py` reads the rows directly (no reader window) so its spill counts
are exact; `name-space.py` walks all 10,896,105 rows and agrees with `book.meta`.
The one modelled line is "grow from 512 B", which charges 512 B per row plus the
exact length of every spill row.

**Not measured:** Brotli, the substrate's own decode support, the created-log
addressing schemes (§7.4), and anything requiring a delta.
