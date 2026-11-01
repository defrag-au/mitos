# The fold plane — aggregate artifacts as a mitos capability

**Date**: 2026-11-01
**Status**: Proposed. The contract (`crates/mitos-fold`, aggregates and views), the native
walker (`crates/mitos-fold-walk`) and the first aggregate — which is also the first view —
(`crates/mitos-holder-table`) exist as of 2026-11-01, and the pilot's parity holds: the walk
and the published book both report 294 holders / 68,994,134 units for `407ca4…`, matching
`holder-distribution`. `fold-probe pilot` runs both phases and fails the run if the two
shapes disagree. No artifact writer or publisher yet; the book is not yet a sink in this
crate.
**Purpose**: Lock down the second job mitos does — a deterministic walk that produces
aggregate artifacts — so an aggregate is a definition on a shared substrate rather than
another pipeline with its own format, publisher, reader and freshness rule.

This is not a replacement for the flow plane. It is the other half of the same node.

---

## 1. The problem

Mitos has two jobs:

- **Flow plane** — follow the tip, decode blocks, dispatch per-block events to wasm modules.
  This is the original design and it is sound: modules see transactions and UTxOs.
- **Fold plane** — walk a range deterministically and produce an aggregate artifact.

The second job already exists in pieces. `fold-probe` walks the chunks and writes the live
book; `policy-index` harvests mints alongside it; `collection-catalogue` is built from the
policy index; `policy-archive`, `token-tape` and the ledger wires each carry their own
artifact. The aggregates are individually right. What is messy is that **each one is a
pipeline**: its own walk or trigger, its own on-disk format, its own publisher, its own
reader, and its own idea of how fresh it is.

The number of aggregates is not the problem. The number of substrates is.

The shape to lock down: **one walk, many aggregates, one artifact contract, one publisher,
one reader, one freshness rule.** Adding a view becomes a schema and a fold, not a service.

---

## 2. Two planes, one source

| | Flow plane | Fold plane |
|---|---|---|
| Trigger | tip follow, per block | deterministic walk over a range |
| Unit | block / tx / UTxO event | aggregate row |
| Consumer | wasm modules, companions | artifacts, views, summaries |
| Output | events | stamped files |
| Failure | trap/retry per module | rebuild or resume from cursor |
| Determinism | not required across runs | required — bytes are the contract |

Both read the same decoded chain. Neither goes through the other.

Rules that keep the boundary honest:

- The flow plane never reads an artifact to answer a per-block question.
- The fold plane never goes through wasm. Aggregates are native Rust; a module never sees
  the bulk (`WASM_BUDGET_CHUNKING.md` is the long version of why).
- Every aggregate rides the decoded stream or an upstream artifact. It never re-reads and
  re-decodes the chunks. `policy-index/README.md` MEASURED the economics: harvesting
  alongside a walk costs **+1.8%**, because the block decode is already paid; a second
  independent pass costs **+100%**.
- The walk's base stamp is the shared coordinate. "As of slot S" means the same thing to
  every aggregate and to the flow plane's `since S` answers.

---

## 3. The fold contract

The shape, not the final signatures:

```rust
pub trait Aggregate {
    type Artifact;

    fn schema(&self) -> SchemaId;
    fn observe(&mut self, block: &DecodedBlock) -> Result<()>;
    fn finish(self: Box<Self>, stamp: Stamp) -> Result<Self::Artifact>;
}

pub trait View<S> {
    type Artifact;

    fn schema(&self) -> SchemaId;
    fn view(&mut self, source: &S) -> Result<()>;
    fn finish(self: Box<Self>, stamp: Stamp) -> Result<Self::Artifact>;
}
```

- A **registry** runs every registered aggregate over one walk. Order is explicit, not
  incidental. A **view registry** does the same for views over one substrate.
- A lean **`BlockSink`** may observe the decoded `MultiEraBlock` beside the registry. The
  book takes this path: it traverses the block itself and does not pay for `DecodedBlock`.
  The sink sees the block first; a sink failure stops the walk before the aggregates.
- **A view reads the substrate the walk built, and runs after it.** `View<S>` is generic over
  the substrate for the same reason `Aggregate<B>` is generic over the block: the contract
  crate stays dependency-free, and a view can be exercised against a fixture. `S` is
  `live_book::Base` in v1.
- **Determinism is part of the contract**: same input and same base stamp produce
  byte-identical output. Canonical ordering, no hash-map iteration, no wall clock, no
  environment. If a rebuild differs, that is a bug in the aggregate, not a tolerable
  difference.
- Aggregates are pure over the decoded input; views are pure over the substrate. I/O is the
  walk's and the reader's job; encoding is the artifact writer's.
- The walker's block type is `chain_decode::DecodedBlock`: owned, so a registry can live
  across the whole walk and no aggregate inherits a borrow of the chunk buffer.

The book is **aggregate #0**: the row store every other aggregate may depend on. That is a
statement about *shape*, not only about order — an aggregate sees what happened in the
window, a view sees what is live at its end, and some artifacts are only one of the two.

### The build is two phases

```text
walk (chunks → DecodedBlock → BlockSink + Registry)
  └─ book.finish()            — the substrate now exists
       └─ ViewRegistry.view(&book) — every projection from that one book
```

A view cannot run before its substrate is complete, which is why it is a separate trait
rather than a flag on `Aggregate`. It also means the marginal cost of a per-policy view is a
page of `by_policy`, not a second pass over the chain: the walk has already paid to write the
rows.

---

## 4. The artifact contract

One envelope, generalising what already exists rather than inventing a fourth format:

| Field | Source of the shape |
|---|---|
| kind + schema version | `collection_catalogue::CATALOGUE_VERSION`, `live_book::LAYOUT_VERSION` |
| base slot + hash | `live_book::Meta::base_slot`, `policy_archive::Stamp` |
| source fingerprint | `live_book::BaseFingerprint` |
| counts, file list, orderings | `live_book::Meta` |
| extension slots | `collection_catalogue::Extension` |

Three physical shapes cover the current ledgers:

| Shape | Answers | Precedent |
|---|---|---|
| **point** — key → row | point lookups | `by_outref`, `policy-index` |
| **ordered** — run + directory + cursor | paged scans | `by_policy`, `by_addr` |
| **summary** — one small typed record | counts, top-N, histogram, cohort digest | `collection-catalogue` |

A fourth shape (a **log**) is allowed where the artifact is a sequence rather than a
table — `policy-archive` is the existing example. The envelope names the shape; the reader
dispatches on it.

The reader surface is deliberately small:

```rust
read(key) -> Option<Row>
page(run, after, limit) -> Page
summary() -> Summary
```

No query language. If a question needs a join, it is a different aggregate.

### The manifest, as built

`Artifact::encode` / `Artifact::decode` in `crates/mitos-fold`, `key value` text in the same
convention as `live_book`'s `book.meta` — inspectable, diffable, and no codec dependency, so
the contract crate stays dependency-free. Scalars first in a fixed order, then one `file` line
per file and one `extension` line per extension:

```text
version 1                                  # the manifest's format, not the schema's
kind ordered
schema holder-table/v1
base_slot 43199989
base_hash 5195dfa0…                        # omitted when the source has none
source 000000000000000000000000000007cf
rows 294                                   # the primary table's row type
entries 294                                # what the file holds, line by line
bytes 52603
file 52603 holders.tsv
extension 1 407ca40a…                      # tag 1 = the holder table's policy
```

**A reader names the schema it requires; the manifest cannot introduce one.**
`decode(text, expected)` refuses a schema that is not `expected`, by name, because a schema
id is a `&'static str` in the reader's own code — a version it has never heard of is a
refusal, not a guess. `require_stamp` is the other half and is exact on all three fields.

Interpretation notes, both load-bearing:

- **`version` numbers the manifest; the schema's version numbers the rows.** A new field in
the manifest bumps the first; a new column in a row bumps the second.
- **`rows` and `entries` are different numbers on purpose.** In the holder table `rows` is
the row type (a holder) and `entries` is what the file physically holds (one line per
`(holder, asset)` balance). A reader sizing a page asks the first; a reader checking the
file asks the second.

An artifact whose *subject* is not a general field — the holder table's policy — carries it
as an extension with a tag assigned once (`HolderTable::POLICY_EXTENSION`). That is what the
slot is for, and a reader that does not know the tag ignores it.

One publisher, one index, atomic swap, meta last. `cf-publish` and `live-book` already have
the mechanics; the fold plane should not grow a second path.

---

## 5. Freshness

An aggregate is **as of slot S**. The flow plane is what happens after S.

- The tip follower maintains a **delta** for aggregates that need sub-cadence freshness.
  This is the book's base/delta pattern applied to a derived view, and it is why tip follow
  belongs in this shape rather than being a separate concern.
- Aggregates that tolerate staleness are rebuilt on a cadence: full replace, same envelope,
  new stamp.
- A reader names the base it requires. A mismatched stamp is refused by name — never an
  implicit "latest". `BaseFingerprint` exists for exactly this failure mode.
- A delta carries the base fingerprint it hangs from. The same rule as the book's delta:
  pairing, then integrity, and the two are different mechanisms.

First version: **full rebuild at an epoch boundary**; add a delta only where a consumer
demonstrably needs it (holder table, mint ledger). Do not build incremental machinery for
aggregates that are cheap to rebuild.

---

## 6. The registry is the anti-mess

One inventory, published with the artifacts:

| field | why |
|---|---|
| aggregate name + schema | what it is |
| base slot + fingerprint | what it was built from |
| row/entry counts, bytes | what it costs |
| owner | who rebuilds it |
| publish location | where a reader finds it |
| cadence | how fresh it is meant to be |
| dependencies | which upstream artifact it needs |

Adding an aggregate is an entry plus a small crate implementing `Aggregate`. It is not a
new service, a new publish script and a new reader.

---

## 7. The pilot — the holder table

`holder-distribution` is the implementation check, not the design.

**Current state.** `crates/mitos-holder-table` is the first aggregate: holders grouped by
stake credential, classified by `mitos-cohort`, sorted by holder key and asset name. It does
**not** yet do LP-pool or CrowdLock-vesting decomposition — that is the module's snapshot-time
transform and needs the pool datum and lock set as separate inputs. The caller supplies the
pool and lock-credential registries; empty registries classify a pool or lock as `Script`,
which is honest but not the final answer.

**It is a position, not a history.** The aggregate keeps the policy's live outputs and removes
one when it is spent, so an asset created and later moved never reaches the table. The first
version accumulated creates and never subtracted spends, and the error was large rather than
subtle: for `407ca4…` over chunks 0..=1999 it reported **362 holders and 1,421,293,459 units**
against the module's **294 and 68,994,134**. Every one of the module's holders was present
with at least the module's quantity — a superset that looks plausible row by row, which is
why the total is the assertion and the rows are the diagnosis.

Three implementations now agree on the same 396 live outputs of that policy:

| source | how | holders | quantity | cost |
|---|---|---|---|---|
| the walk | `Aggregate<DecodedBlock>`, live set per policy | 294 | 68,994,134 | one probe per input |
| the book | `fold-probe holders`, `by_policy` over the published book | 294 | 68,994,134 | 396 row reads, 1 ms |
| the module | `holder-distribution` snapshot chunks | 294 | 68,994,134 | — |

The book projection is the independent check: it never observes a block, so it cannot share
a mistake with the walk. `tests/module_parity.rs` asserts the module against either table
(its reader is keyed by column name, not position); `tests/book_view.rs` asserts the two
shapes against each other from the artifacts alone; and `fold-probe pilot` fails the run if
they disagree, naming the first holder that differs.

Both shapes share one `table_from` fold, so they can only disagree about *which* outputs are
live — never about what to do with them.

The structural pilot is `crates/mitos-holder-table/tests/walk.rs`: one walk over the fixture
chunk feeds a `BlockSink` and the holder table. On the local mainnet store, chunk 1848
decodes **1,044 blocks / 18,156 txs in 1.22 s in a debug build** — the walker's throughput,
not yet the table's. `fold-probe pilot` is the full-window version: phase one walks and
writes the book, phase two projects the table from it, and the run fails if the stream
aggregate and the view disagree.

- One walk writes the book and one artifact directory: `holders.tsv` plus `artifact.meta`,
  the manifest written last. `summary.tsv` is gone — the manifest carries the schema, the
  stamp and the counts, so it *is* the summary. Build from the builder's stream, never
  through the reader's `with_row` — the warning in `LARGE_POLICY_VIEWS.md` §4 exists because
  that is how 630 long rows would silently disappear.
- **The rows are TSV, temporarily.** The row encoding is the one part of this artifact that
  changes: a binary sorted array is smaller and pages without parsing. Everything around it
  is encoding-agnostic — the manifest names the file and its length, `counts` says what is
  in it, and a reader checks the length before trusting it — so replacing
  `HolderTable::to_rows` is the whole of that change.
- The module's snapshot becomes a read of the summary/page; its delta path stays in wasm.
  That read is a **new module**, `community-modules/holders-from-fold`, rather than a mode
  on `collection-holders` — one ledger cannot have a frozen artifact and a moving tip as
  two sources of truth. `collection-holders` keeps its interest handling, its live events
  and its scan; the fold reader has no interest to keep and no delta to compute.
- Parity: the artifact summary against the module's 396-ref snapshot, byte-identical where
  they overlap. The 995-ref and 9,625-ref policies exercise the delta path and the
  no-cap enumeration, not the snapshot builder.

This removes the fuel trap and the 100K cap as a side effect, without making either the
subject. The per-problem detail remains in `NATIVE_HOLDER_SNAPSHOT_PLANE.md`; this document
supersedes it as the general shape.

Measured basis for the pilot (`LARGE_POLICY_VIEWS.md` §1, §2):

| | HOSKY Cash Grab | ADA Handle |
|---|---|---|
| rows | 36,948 | 170,062 |
| distinct holder addresses | 20,309 | 74,129 |
| holder pass | 0.02 s | 0.08 s |
| asset pass | 0.05 s | 0.10 s |
| current module cap | 100,000 refs (silent) | 100,000 refs (silent) |

---

## 8. The local gate

**Nothing is built on a box, published to R2, or deployed until this gate passes locally.**
The box is a validation environment, not a first execution.

The gate:

1. **Contract tests** — envelope round-trip; schema/version mismatch fails by name; stamp
   refusal; registry ordering; determinism (same input → byte-identical artifact across two
   runs). **Done**: 32 unit tests in `mitos-fold`, 13 in `tests/registry.rs`, 13 in
   `tests/views.rs`.
2. **Pilot parity** on the local shadow pair: the walk, the book and the module agree on
   294 holders / 68,994,134 units for `407ca4…`. **Done** — `pilot` fails the run if the two
   shapes disagree, `tests/module_parity.rs` checks the module against either table, and
   `tests/book_view.rs` re-projects the book and compares the rows byte for byte.
3. **A rebuild** — wipe the artifact and rebuild from the same range; compare bytes with
   the first build. A difference is a failure. **Done**, 2026-11-01: two independent runs over
   chunks 0..=1999 produced a byte-identical `artifact.meta` and `holders.tsv`, and a
   byte-identical book (`diff -r`, no output, 568 MB each).
4. **The local stack** — `deploy/local` serves the artifact and the flow plane together;
   `container-compose` is pinned (the `observe()` fix must be committed and re-pinned, or
   the lab is one machine's working tree); the module reads the artifact and emits the same
   events. **Done, 2026-11-01** — except the subscribe half, which is the consumer's:
   `deploy/local/up.sh --fold <dir>` mounts the artifact, exports `MITOS_FOLD_ARTIFACT`,
   stages the new `community-modules/holders-from-fold`, and the host logs
   `fold artifact served`. The read-and-emit path is proven by
   `crates/mitos-platform/tests/holders_from_fold.rs`: the real host boots, the real built
   module activates, `start(module, rebootstrap = true)` drives it to `done`, and the
   emission is compared with `holders.tsv` parsed independently in the test —
   **1 begin · 1 chunk · 1 end · 294 holdings · 294 holders · Σ 68,994,134**, matching the
   pilot exactly. No chain, no book, no Dolos, `NullChainDataPlane` underneath so a stray
   chain call would fail rather than pass silently. What remains unwired is a *subscriber*:
   the module starts when a companion names it, and minting-engine names only
   `cip-25-mint`, so on the stack it is activated and idle. Wiring it in belongs to the
   consumer, in the consumer's repo.
5. **No production path** — the first publish target is a local directory or a test prefix.
   `mitos-watch`, the box, and the edge are untouched. **Holds.**

Once the gate passes, the contract is frozen at version 1. Additive changes (extension
slots) are allowed afterwards; a field change is a version bump.

---

## 9. Migration order

Only after the pilot and the local gate, and one archive at a time:

1. `collection-catalogue` — the closest in shape (one summary artifact, versioned, published
   to R2 + KV), and the smallest.
2. `policy-index` — point/ordered tables already; the shared-pass economics are the reason
   it exists.
3. `policy-archive` / `token-tape` — logs and per-policy projections. These may stay
   distinct shapes; the envelope describes them, the registry names them.
4. `tx-index` — the largest, and the one whose reader other tools depend on.

Each migration is its own slice with a byte-parity check against the current artifact, or a
recorded reason why the bytes legitimately differ. Do not migrate an archive in the same
slice that changes the substrate.

---

## 10. Non-goals

- A query language or a general SQL surface over artifacts.
- Merging the ledgers into one crate. They stay specialized; they share a substrate.
- Replacing the flow plane or the module wire format in the first slice.
- Incremental deltas for every aggregate.
- Production rollout, R2 publication, or a box build as part of the first slice.

---

## 11. Decisions

Settled:

- **Crate home**: `crates/mitos-fold` is the contract — the aggregate trait, the registry,
  the view trait, the view registry and the artifact envelope, dependency-free and I/O-free.
  `crates/mitos-fold-walk` is the native walker — chunk selection, the pallas decode, the
  owned `DecodedBlock` every aggregate observes, and the lean `BlockSink` path the book will
  take. `crates/mitos-holder-table` is the first aggregate, and also the first view — the two
  shapes shipped together because agreement between them is the pilot's cheapest assertion.
  The split is deliberate: the contract links into a worker or a test without a chain stack,
  and the walker is the only place that reads a chunk. `live-book` stays the row store and
  the format crate. The builder moves out of `tools/fold-probe` once the second writer
  exists — which is this slice; `live-book`'s `lib.rs` states that trigger explicitly.
- **Two inputs to one walk**: the book gets the lean `MultiEraBlock` (via `BlockSink`);
  aggregates get the owned `DecodedBlock`. The book does not pay for the aggregate view,
  and an aggregate does not re-read the chunk.
- **A book projection is a first-class aggregate kind.** `View<S>` runs after the walk has
  finished the substrate, so the build is two phases (walk → book → views). The holder table
  is implemented both ways; the stream shape stays for artifacts a view cannot express —
  window counts, per-epoch histograms, logs.
- **The fold reader is a new module, not a mode on `collection-holders`.**
  `community-modules/holders-from-fold` reads the artifact; `collection-holders` keeps its
  chain scan and its delta path. One module holding a frozen artifact *and* a moving tip
  would have two sources of truth for one ledger, and the delta walk would have to know
  which one its state came from. Two modules, one documented base: the fold module reports
  the artifact's slot in `SnapshotBegin`, and a consumer that wants freshness pairs it with
  the chain-scanning module. See §7.
- **`mitos-fold` is not a `fold`-feature dependency of `mitos-holder-table`.** The envelope
  helpers (`POLICY_EXTENSION`, `policy_of`) are part of the artifact's contract, not of
  either fold shape, and `mitos-fold` is dependency-free and builds for
  `wasm32-unknown-unknown`. `fold` gates exactly the two shapes whose dependencies cannot
  cross into wasm (`chain-decode`, `live-book`).
- **Migration waits** until the pilot and the local gate.

Open:

- Base cadence: full rebuild at an epoch boundary is the starting assumption; confirm
  against the publish path's cost.
- Which aggregates get a delta in v1: proposed holder table and mint ledger only.
- Publisher: `cf-publish` as the single path; confirm it can carry the inventory as well as
  the objects.
- The pilot summary schema: counts, top-N, histogram, cohort totals. Confirm against what
  the companion actually consumes before freezing it.

---

## 12. Related

- `LIVE_BOOK.md` — the row store, its sizes, and the base/delta split.
- `LARGE_POLICY_VIEWS.md` — the per-policy bundle, the one-pass measurements, and the
  warning about building views through the reader.
- `WASM_BUDGET_CHUNKING.md` — the history of bulk work in wasm, and why it traps.
- `NATIVE_HOLDER_SNAPSHOT_PLANE.md` — the holder-specific design this generalises.
- `PLANE_LANDING.md` — the L1 cutover. The fold plane is not on its critical path; M2's
  parity evidence stands on its own.
- `crates/policy-index/README.md` — the shared-pass economics.
