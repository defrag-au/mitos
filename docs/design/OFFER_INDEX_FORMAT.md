# The offer index — bidder → offer, published per book boundary

**Proposed 2026-09-30.** Consumer: the offers view in `live-book-edge`'s frontend
(`frontend/src/offers.rs`). Siblings: `DATUM_RESOLVER_FORMAT.md` — whose doctrine this
copies — `LIVE_BOOK.md`, whose boundary it is pinned to, and `MITOS_AT_THE_EDGE.md` §10.

## 1. Why an artifact, and not a scan

The offers view asks "what has this holder offered?" It answers it today by asking the
book for **every output at each venue's credential** and filtering locally, because the
bidder is a field *inside the datum*, and the book indexes attributes of the output —
outref, address credential, policy — never the meanings a datum carries.

Measured on a warm run, 2026-09-30:

| | |
|---|---|
| rows read at the venues | 151,880 |
| bytes | 65.23 MB |
| book arm, wall clock | 6.77 s |
| offers that matched the holder | 879 — **0.58%** |

and the population is one venue's:

| venue | credential the reader asks by | rows |
|---|---|---|
| jpg.store V1 | `9068a7a3f008803edac87af1619860f2cdcde40c26987325ace138ad` | 147,625 |
| Wayup | `27d46ecbec94b052d8f875cf3beafd0e8ca40e8ad069f677e0a128ea` | 4,255 |

⚠️ **The scan is the client's last mile, not the edge's.** Three independent transfers in
the same run sit on the same ceiling — the wasm bundle at 12.3 MB/s, the datum set's entry
arrays at 10.3 MB/s, the book arm at 9.6 MB/s — and Chrome does not keep the multi-range
responses (79.3 MB transferred against 91.3 MB resources in the panel), so every visit
pays the 65 MB again. Connections, workers and cache posture cannot move a byte of it.

⚠️ **No address-shaped key fixes it.** jpg's offer address is the venue's own, so its
bidder exists only inside the datum. Wayup's offer address does carry the bidder's stake
credential, which would make a stake-keyed ordering work *for Wayup* — **2.8%** of the
population. The other 97.2% is a datum-content question.

⚠️ **It does not belong inside the book.** Every ordering there is an attribute of the
output; a bidder is a meaning only a venue decoder can produce. Putting it in `book/v3`
would make the artifact's layout depend on which marketplaces we happen to decode, and
freeze that into a layout version. (A colo-side cache was the other candidate — it moves
the scan rather than ending it, and it must decode the same 147k datums inside a Worker's
CPU budget, which is the one thing that limit will not give.)

## 2. What it is

A derived set, published beside the book and the datum set: for every live output at a
registered offer venue whose datum decodes as an offer, one association —

> **the credential the venue's decoder names → the offer's outref**.

The naming is the venue's, from `mitos-marketplace-decode`: jpg addresses a bid to the
**payment** credential in its datum, Wayup to the **stake** credential. The set records
whichever the decoder returned, so a reader asks for the credentials it already has from
CIP-30 and finds its offers at both venues.

⚠️ **The entry does not carry the venue.** A reader derives it from the row's address —
the venue's own credential — and verifies the bidder there, exactly as the scanning reader
does today. A byte saying "jpg" would be a second definition of something the row already
decides, and it would have to be numbered and kept stable for no reader's benefit.

⚠️ **Only venues with a decoder are in it.** `venues()` already skips a registered offer
contract that no decoder reads, rather than showing a row nobody can interpret; this set
inherits that rule, and a venue without a decoder stays a scan.

## 3. Naming, the catalogue, and the flip

```
offers/v1/current                          ← the only mutable object
offers/v1/s-<base_slot>-<digest>.tbl       ← immutable, content-addressed
offers/v1/s-<base_slot>-<digest>.bin
```

- The digest is the file's own blake2b-256 — `DATUM_RESOLVER_FORMAT.md` §6.2's rule
  unchanged — so a key never names different bytes, each object earns
  `immutable_attributes()`, and a reader can check what it fetched against the key it
  asked for.
- `current` takes `pointer_attributes()` (`no-cache`): a pointer with the files' posture
  pins a stale artifact for a year.

⚠️ **A snapshot is replaced, never appended to.** The datum set is additive because a
datum never changes; an offer is *spent*, so "what is live now" is a snapshot. A new
publication writes new keys and flips the pointer. Superseded objects stay in the bucket,
because correctness depends only on which catalogue a reader was handed.

```
index_version 1
book <fingerprint> <base_slot>
snapshot <entries> <credentials> <layout_version> <tbl_len> <tbl_digest> <bin_len> <bin_digest>
```

⚠️ **The `snapshot` line follows the `book` line**, because the snapshot's key is made of
the boundary it belongs to. A text that inverts them is refused with that reason rather
than parsed into a key built from nothing.

⚠️ **`book` is the consistency clause, and it is what makes the index safe to read.**
Associations name rows by outref, and those rows are read from the book. An index built
against a newer boundary than the reader's book would name offers that are not there;
against an older one it would name spent offers. The reader pins both and, when they
disagree, **falls back to the scan** rather than showing an answer it cannot stand behind.

## 4. The pair's layout

- **`.tbl`** — a fixed header, then one 36-byte record per *distinct credential* in
  credential order:

  ```
  0      layout_version u32
  4      entries        u32   — associations in the blob
  8      credentials    u32   — records in this table
  12     256 × u32            — bucket starts: the record index of the first credential
                                whose first byte is at least the bucket number
  1036 … credentials × 36     — credential (28 B) · first entry (u32) · count (u32)
  ```

- **`.bin`** — one 36-byte outref per association (`row.rs`'s encoding: 32-byte hash ‖
  u32 LE index), contiguous per credential in table order.

The directory narrows the table to one bucket before the binary search, so a lookup is two
ranged reads of the `.tbl` — the header, then the bucket — and one of the blob. At 256
buckets the header is 1,036 B and a bucket is on the order of 400 records at the measured
population; ⚠️ **that number is a guess until measured** against a real snapshot, and it is
the one knob trading the header's size against how far a lookup searches (§8). Both table
reads are inside one object, so a client's plan can carry them in a single request.

Estimates from the measured population — 151,880 associations over (order) 100k distinct
credentials — are a ~3.6 MB table and a ~5.5 MB blob per snapshot. One credential's lookup
reads a few kB: **two to four requests for any holder**, against 65 MB and 280 today.

⚠️ **Checked before it reaches a bucket**, on `DATUM_RESOLVER_FORMAT.md` §6.2's doctrine:
records strictly increasing by credential, entry runs tiling the blob exactly, every
offset inside it. A pair that fails would answer with wrong bytes rather than fail.

## 5. What the reader does

1. `GET offers/v1/current` — small, no-cache.
2. Refuse an `index_version` it does not read; refuse when `book <fingerprint>` is not the
   book in hand.
3. Read the bucket header and the table runs for **its own credentials** — the payment
   credentials CIP-30 gave it, and its stake credential — in one multi-range plan; the
   reader has that machinery already (`plan`, `Batch`).
4. Read the entries for those credentials from the blob.
5. Fetch each offer's row from the book by outref (`by_outref`, already published), decode
   the datum as it does today, and render.
6. ⚠️ When the index is absent, unreadable, or built against another boundary: **fall back
   to the scan**, which stays correct. The index is an optimisation, and the view already
   names its boundary.

## 6. What this is not

- **Not the book's layout.** The book's orderings stay attributes of the output; this set
  holds what a datum *means*, so it lives beside the book, versions on its own, and can be
  dropped without touching `book/v3`.
- **Not a service.** Catalogue, table and blob are read from R2 directly — no worker in
  the path, and none needed: the answer is kilobytes.
- **Not the datum set.** `datum/v1` stays the general `hash → preimage` store; nothing
  here changes it.
- **Not exhaustive.** Venues without a decoder, and any datum that does not decode, are
  not in it.
- **Not `by_addr`'s replacement.** It answers a question the book cannot, not one the book
  answers slowly.

## 7. What we build, in order

| | step | file | state |
|---|---|---|---|
| 1 | Format and writer: the pair, the catalogue, the key functions (one definition), and the checks §4 names before anything is written | `crates/live-book/src/publish.rs` | **built** (2026-09-30) — `OfferCatalogue`, `publish_offers` (sort, dedupe, check, write, flip), `lay_out_offers`, `check_offer_table` / `check_offer_blob` |
| 2 | The reader: ranged lookup, catalogue parse, digest check | `crates/live-book` | **built** (2026-09-30) — `OfferTable` over the whole table; the ranged arithmetic is `parse_offer_header` / `offer_record_offset` / `offer_entry_offset`, which the client arm drives |
| 3 | The fold's pass: for each registered venue credential, decode the rows the fold already builds — needs `mitos-marketplace-decode` and `address-registry`, which `tools/fold-probe` does not carry today | `tools/fold-probe` | **built** (2026-09-30) — `src/offers.rs`; see "what the pass reads" below |
| 4 | Publish: the layout's `offers/v1` in the same `rclone` push as the datum set (one line, like `datum-push`) | `tools/fold-probe`, `HOWTO_DATUM_CAPTURE_AND_PUSH.md` | **half built** — `publish` lays `offers/v1` out beside the book and prints the two-line upload (the book's files under the book prefix, `offers/` at the bucket root); the runbook line is not written |
| 5 | The client arm: read the index instead of the scan, with the scan as the fallback | `live-book-edge/frontend` | not built |

Steps 1–3 make the set exist. Step 4 runs on the arm box, per the R2 rule. Step 5 is the
one that makes the view snappy, and it needs a published set to be measured against.

### What the pass reads, and what it does not

The pass (`offers.rs`) decodes a candidate — an output at a registered venue, matched by
address for a one-address contract and by payment credential for a per-bidder one — from
whichever of these the chain provides:

- **inline** on the output, read directly;
- **revealed in a witness set**, matched by datum hash. Every transaction that creates or
  spends a hash-attached script output supplies the preimage, and the walk feeds the pass
  **every** witness datum it passes — a shape test (`Constr 0 [ Bytes(28), … ]`, a parse that
  allocates nothing) decides which are kept, and 44.5 M witness datums store-wide cost a few
  seconds of a ninety-minute walk. Keeping them is what lets an offer created *after* an
  identical datum was revealed elsewhere name its bidder: the census puts that at a quarter
  of Wayup's live offers;
- ⚠️ **not jpg's labels-50+ metadata convention.** It is an off-chain agreement rather than a
  chain guarantee, and reading it needs the metadata decoder this pass deliberately does not
  pull in. A candidate it would have answered is counted (`unreadable` in the run report)
  rather than guessed at.

**Measured on the first store-wide run** (9,218 chunks, boundary slot 199,108,777, 2026-09-30),
which is what the sources above are worth:

| | |
|---|---|
| candidates (created at a venue, all time) | 615,998 |
| decoded — of which **54** read off the output and **433,562** from a reveal | 433,616 |
| unreadable at creation (almost all since spent) | 182,380 |
| live rows at the venues, read back from the book | 6,215 — jpg's address 1,967, Wayup's credential 4,248 |
| live associations in the set | **4,981** |

So the split is: **Wayup complete** — the census found zero unrecoverable Wayup creations over
the contract's life, so every one of its 4,248 live rows is named — and **jpg 733 of 1,967**.
The missing 1,232 are jpg offers whose preimage was published only under labels 50+, which is
not chain data and is where this deliberately stops: the fold reads **exactly the sources a
generic `hash → preimage` index holds** — the output's own datum and the witness sets — and
nothing venue-specific. That residue is not unreachable, it is just not *this* artifact's:
`index/tx/v1` resolves a transaction hash to its body **and its aux-data offset**, the chunk
mirror is in the same bucket, so a consumer fetches the create tx's metadata generically and
applies the joining rule where it lives (`mitos-marketplace-decode`). R2 ships facts; the
domain layer composes them.

⚠️ **The reveal index is the mechanism, not a nicety.** 99.99% of the decoded offers needed
it (54 came off the output itself), and it costs memory in the fold: 44,762,927 witness datums
seen, 13,281,528 kept, peak anon 4.66 GiB on a 23 GiB box. Gating retention on transactions
that touch a venue would cut the eras where no venue existed to nearly nothing at the price of
a rare re-create whose sibling was revealed only by its spend.

Liveness is **not** the pass's: an offer created and spent inside the walk is still a
candidate, and `Book::is_live` is asked at the boundary. What crosses into `live-book` is a
pair — 28 bytes of credential and an outref — written as `offers.assoc` in the book
directory, and `publish` lays the pair out against the fingerprint and base slot of the book
it is publishing. The venue never enters the artifact (see §9).

⚠️ **One thing this step needs from outside the workspace**: the registry's enumeration of
its payment credentials (`address_registry::registry::all_known_credentials`). Wayup's offer
contract has no address anywhere in the registry — the credential is the only way to name it
— so a build that only sees `all_known_addresses` indexes jpg and silently writes nothing for
the venue that holds 4,213 of the 4,291 live offers. That accessor landed in `shared-crates`
on 2026-09-30, and mitos pins the pushed rev (`697a3d32…`, `Cargo.toml`'s shared-crates
block): the pin is a bump a consumer of this set has to carry, not a local patch.

## 8. Open questions

- **Bucket count, and whole header vs ranged reads**, measured against a real snapshot —
  the same question `DATUM_RESOLVER_FORMAT.md` §11 leaves open for its own tables.
- **Retention.** Superseded snapshots accumulate, and nothing needs them for correctness
  (§3). The datum set's GC question, inherited.
- **Many offers per credential.** Nothing caps a key's entry count; the reader should
  expect N entries per credential rather than one.

## 9. Decided

- **The set is separate from the book**, so marketplace semantics never enter the
  artifact's layout, and dropping the set costs nothing but the scan.
- ⚠️ **And it is not published as an R2 dataset.** A materialisation whose keys are a
  marketplace decoder's output — and whose completeness leans on one venue's off-chain metadata
  convention — is not a chain fact: it cannot be described without naming a marketplace, and it
  would need re-deriving whenever one changes its datum. The bucket holds chain facts (`book`,
  `datum/v1`, `index/tx/v1` over the chunk mirror) and a consumer composes them; the fold's
  pass stays a measurement harness, and `publish` refuses to lay a domain set out beside the
  book (a test asserts it). What this costs: an offers view needs either the scan or its own
  composition — which is the price of keeping the published data domain-neutral, paid
  deliberately.
- **Keyed by what the decoder named**, so the client asks for credentials it already has.
  ⚠️ Asked as an open question — Wayup's bidder is in the address as well as the datum — and
  answered by construction when the pass was written: the datum, both venues. Indexing the
  address would have made the pass cheaper for Wayup and made "whose offer this is" have two
  definitions that could disagree.
- **Values are outrefs, not rows.** The rows stay in the book, read by outref; the datum
  decode stays in the reader, where it already is.
- **The entry is the outref alone** — no venue byte, and no datum hash either. The venue is
  derived from the row's address, so carrying it would be a second definition of what the row
  already decides; and the reader decodes the datum to render the offer anyway, so a hash
  beside the outref would be a third of the payload for a check it already performs.
- **Snapshot plus flipped pointer**, replaced per book boundary, content-addressed, with
  the book's fingerprint in the catalogue as the consistency clause.
- **The scan stays the fallback**, so the view is correct with or without the set.