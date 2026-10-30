# Landing the data plane — scope, gates and sequence

**Date**: 2026-10-30
**Status**: Proposed. A plan, not a measurement. Every claim below points at the doc or
code that carries the evidence.
**Purpose**: Define what "the mitos plane is landed" means, what is actually built, what
is left, and the order that closes it without re-litigating the programme around it.

This document is deliberately a **ledger**, not another design. If it disagrees with
`LIVE_BOOK.md`, `DELTA_CHAIN.md` or `PHONEBOOK_COVERAGE.md`, those documents win on
measurements and this one is stale — fix it rather than adding a fourth status line.

---

## 1. What "landed" means

There are two levels, and conflating them is how this becomes a year-long programme.

### L1 — the base serves the hot set

The host answers the UTxO-shaped questions from the book. Dolos remains the tail for
anything at or after the base boundary, and the metadata and history methods still fall
back to `tx-index` / `policy-index` / Maestro. The delta's point resolution is built and
measured, but it is not integrated into `BookDataPlane` yet, so L1 does not depend on it.

This is the level that takes the 145 GB Dolos store out of the hot path and makes the
first real modules run against owned artifacts.

### L2 — the plane is the source of truth

The plane answers the **whole module surface**, including the tip and the metadata
trio. The base and delta are published on a boundary, a reader can name and refuse a
mismatched pairing, and Dolos is either a tail-only process or gone. This is the level
that makes the 8 GB / 256 GB box possible.

The distinction matters because L1 is a cutover, while L2 is a data-plane replacement.
Most of the risk lives in L2's tip and mint ledger, not in the base.

---

## 2. Where it actually is

| Piece | State | Evidence |
|---|---|---|
| **Base artifact** (`rows.bin`, `by_outref`, `by_policy`, `by_addr`, `policy.tbl`, `dir`, `book.meta`) | **Built and verified whole-store**: 10,911,674 rows, 2.23 GB, 32.7 M entry confirms with 0 failures | `LIVE_BOOK.md` Sizes; `PHONEBOOK_COVERAGE.md` §6 |
| **Reader** (`Base::open`, point + three orderings, opaque cursors) | **Built**; long-row (>8 KiB) bug found and fixed 2026-09-26 | `crates/live-book`; `LARGE_POLICY_VIEWS.md` §0 |
| **`BookDataPlane`** | **Built**; answers the UTxO/address/policy/holder methods, `search_utxos` for the three caller shapes, datum resolution from a `datum.tbl`, and `tip()` from `base_slot`; the rest refuse by name | `crates/mitos-data-plane/src/impls/book.rs` |
| **Host wiring** | **Wired.** `MITOS_DATA_PLANE=book` opens the artifact in `bundle.rs` and every host query delegates through `SelectedPlane`; a missing `MITOS_BOOK_DIR`, an unstamped book, or an unknown mode fails at startup by name. The admin routes answer from the same plane, so `/_admin/status` reports the book's boundary rather than Dolos's tip | `crates/mitos-core/src/bundle.rs`; `crates/mitos-platform/src/selected_plane.rs` |
| **First module execution** | **Passes**: shipped `collection-holders` runs against a published book, emits 736 holdings over 220 live refs | `crates/mitos-platform/tests/collection_holders_over_book.rs`; `PHONEBOOK_COVERAGE.md` §12 |
| **`datum.tbl`** | **Capture, reader and plane wiring built**: `read_datum` and the hash arm of `read_output_datums` resolve from a table beside the book or via `open_with_datums`; without one the plane keeps the hash and `payload: None` | `LIVE_BOOK.md` Phasing 3; `DATUM_RESOLVER_FORMAT.md`; `book.rs` |
| **Delta** | **Point resolution built and measured**: 100% of base-unanswerable inputs, ~240 ns/input, +31% walk, 3.3 MiB/chunk | `DELTA_CHAIN.md` §3–§5 |
| **Delta orderings** | **Not built.** Enumeration at the tip is a full scan: 97.5 ms over 288 K rows | `DELTA_CHAIN.md` §6 |
| **Delta publication / merge** | **Not built.** No `delta/NNNNN.dlt`, no rebase | `DELTA_CHAIN.md` §6, §11 |
| **Live tail source** | **Open.** The bundle still uses Dolos's `LagTolerantSubscription`; no production tail over a book | `bundle.rs:421`; `DELTA_CHAIN.md` §11 |
| **Mint ledger** (`asset_state`, `total_supply`, `cip25_metadata`) | **Not built for the book.** Dolos provides `asset_state`; `total_supply` is `NotYetImplemented` even there. `policy-index` has mint provenance, not net supply | `crates/mitos-data-plane/src/impls/book.rs`; `policy-index/README.md` |
| **History** (`tx_metadata`, `read_tx`, `slot_by_tx_hash`) | **Not on the book.** `tx-index` holds locations; the chunk holds the bytes | `DELTA_CHAIN.md` §9; `TX_INDEX_DATASOURCE.md` |
| **`search_utxos`** | **Built** for `Match` of policy, exact address and payment credential, paged with an opaque cursor; other shapes refuse by name. The host's watched-UTxO seed now fills instead of logging and carrying on | `book.rs`; `PHONEBOOK_COVERAGE.md` §3/§4a |
| **Publish path** | Components exist (`live-book::publish`, `cf-publish`, `mithril-snapshot`, `mithril-source`); the edge read path is deployed against a windowed book. No cadence or ownership decision | `LIVE_BOOK.md` Open; `MITOS_AT_THE_EDGE.md` §12 |
| **Release / install** | **Not landed.** No release workflow; the 2026-09-29 review found production on an older source build; `INSTALLABLE_MITOS.md` still says "nothing built" | `MITOS_UPGRADE_REVIEW_AND_PLAN.md` §2; `.github/workflows/` |

The honest summary: **the base is a finished artifact with a reader, one passing module,
and — since 2026-10-30 — a host that can select it. What the selection does not carry is the
tip: the tail is still Dolos's, the delta is neither published nor merged, and the mint
ledger and history are not on the book.**

---

## 3. The workstreams

Six, in dependency order. Each one is a proper cutover on its own, not a task list.

### A. Surface closure — what the modules ask for

The book answers the UTxO-shaped questions. The rest are either a different artifact or
a fallback. In order of how many shipped modules need them:

| Gap | Consumers | Closure | Notes |
|---|---|---|---|
| `search_utxos` | host watched-UTxO seeding (every dynamic-interest Add) | predicate algebra for the three shapes the callers use + a `Page` cursor | the cheapest unblocked item; `tip` is already answered |
| `read_datum` / hash-attached outputs | `collection-metadata`, `asset-metadata-update`, `cip-68-mint` | open the captured `datum.tbl` in `BookDataPlane` | 16% of a policy scan's outputs carry a **hash-referenced** datum (5,497 of 34,177), which the book currently answers as `payload: None` |
| `tx_metadata`, `read_tx`, `slot_by_tx_hash` | `cip-25-mint`, `asset-metadata-update`, `credit-address` | `tx-index` location + chunk body, with a local tier | history, not a UTxO fold; do not put it in the book |
| `asset_state`, `total_supply`, `cip25_metadata` | `collection-metadata` (the heaviest consumer) | a mint ledger: one row per `(policy, name)` with initial tx, mint count, net quantity | the only genuinely new extraction; `policy-index` is provenance, not net supply |
| `read_script` | not called by the shipped cohort | script table, or a named fallback | the row has no `script_ref`; do not pretend otherwise |
| `protocol_params`, `read_block`, `era_*`, `stake_pool_info`, `delegation`, `drep_state` | inventory first | fallback or out of scope | several are `NotYetImplemented` today, so the book is not a regression |

**Gate A**: the four UTxO-only modules plus `credit-address`'s `read_utxos` half run
against the book, with `search_utxos` and `read_datum` closed. This is the L1 cohort.

### B. The tip — the part that actually decides L2

The delta answers point resolution, and that is the correctness win: `read_utxo` can
now tell "spent", "never existed" and "created since my snapshot" apart
(`DELTA_CHAIN.md` §7). What it cannot do is enumerate.

1. **Delta orderings.** Add `by_addr` and `by_policy` permutations over the delta's
   own rows, in the book's 12-byte shape. Until they exist, `utxos_by_address` and
   `utxos_by_policy` at the tip miss post-boundary outputs and can return a spent base
   row. This is the last thing between the plane and a tip that answers in a request
   path rather than a 97.5 ms scan.
2. **Sealed delta format.** `delta/NNNNN.dlt`, one per chunk, written by the same walk,
   swapped by `rename`. The delta carries the base fingerprint it hangs from, and a
   reader refuses a mismatched pairing rather than answering across it.
3. **Merge / rebase.** Folding a delta's spends into the base's live set. The builder
   exists; the merge is the piece with no measurement behind it. Decide whether it is
   the same builder over a smaller input or a merge of two artifacts.
4. **The live tail.** This is the open question and the one to decide first. Options:
   - **Dolos as a tail-only follower** — smallest change, keeps the node-shaped
     process but shrinks it to follow; L1 can land without deciding this.
   - **A pallas chain-sync/blockfetch tail** — the gateway already does this in
     `augminted-bots`; it removes Dolos entirely but is a new process to operate.
   - **Chunk catch-up** — only for a lagging follower; not a tip source on its own.

   The delta is defined relative to a base boundary, so whatever supplies the tip must
   also supply the rollback depth the delta assumes. `TAIL_REFRESH.md`'s 300-slot
   safety depth and 30 s poll are a proven shape in `cnft.dev-workers`, even though that
   tail is a different artifact.

**Gate B**: a module sees a transaction created after the base boundary, a spent base
row is masked, and an enumeration at the tip answers from the delta rather than a scan.
A rollback within the safety depth is undoable.

### C. Host integration and cutover

`bundle.rs` currently builds `DomainDataPlane` and a Dolos subscription factory. The
book plane is a test-only concern. Landing means:

1. **A selectable plane** — `MITOS_DATA_PLANE=book|dolos` (or a `mitos.toml` block)
   with `MITOS_BOOK_DIR` / a published-artifact locator. The selection must be loud:
   a book that fails to open must not silently fall back to Dolos.
2. **A hybrid plane** — base + delta for UTxO; `tx-index` for history; the mint ledger
   where built; a named fallback for the rest. Each refusal is a metric, not a surprise.
3. **A tail independent of the plane** — or Dolos retained only for follow. Do not
   couple the book's selection to the subscription factory.
4. **Shadow mode** — run both planes for the cohort and diff answers. The test harness
   already does this for one module; production needs it per module and per method.
5. **Rollback** — the Dolos plane stays compiled and selectable until L2 is trusted.

**Gate C**: the L1 cohort runs in production on the book in shadow, then default, with a
documented fallback rate and a one-switch rollback.

### D. Artifact pipeline

The artifacts exist as `fold-probe` outputs, not as a service.

- **One walk, many artifacts.** The book, delta, `datum.tbl`, the mint ledger, and the
  `tx-index` / `policy-index` segments all ride the same decode. A second walk over the
  same chunks costs +100%; the sink is the design (`DELTA_CHAIN.md` §8).
- **Boundary and identity.** `book.meta.base_slot` and `Meta::fingerprint()` exist. A
  delta names the base it hangs from; a publish names the chunk boundary. An artifact
  without a boundary is not publishable.
- **Verification at publish.** `verify_structure`, `verify_sample`, and the three
  whole-artifact identities. A remote reader trusts an attestation instead of running
  `Base::open`'s 2.1 M-bucket validation (`live-book/src/publish.rs`).
- **Cadence and ownership.** The book is meaningful only relative to a chunk boundary,
  and the refresh cadence must be tighter than the resolver's staleness budget. Decide:
  full base per epoch + delta per chunk, or a tighter base with a merge. This is
  `LIVE_BOOK.md`'s open question, and it is a decision rather than a task.
- **Build where?** A full fold is ~49 min on 8 threads (whole store) and ~1.26 h serial.
  On the 8 GB / 256 GB profile the log is a rolling window, so the base cannot be built
  from genesis locally: it is either downloaded, or built from a previous base + a
  window + the merge. **The merge is therefore on the critical path for the stated
  minimum box, not just an optimisation.**

**Gate D**: a base and its deltas are published on a boundary with an attestation, and a
second box can consume them without reading a chunk or owning the build.

### E. Operations

- **Health**: base age, delta lag, tip lag, fallback rate per method, module traps,
  anon RSS, disk.
- **Shadow → canary → default** per module, with a diff report.
- **One status page**: what is deployed, on which rev, which artifact boundary, which
  modules, which fallbacks. The upgrade review's sharpest finding was four contradictory
  status lines; a landing plan that adds a fifth is not progress.
- **Release**: if the plane is to be installed by anyone else, the missing release
  workflow (`release.yaml`, checksums, R2 mirror, `install-release.sh`) is a hard
  dependency. For a single owned box it can be a source build, but then L2 is not
  portable.

### F. What is deliberately not on the critical path

- **The edge distribution** (`MITOS_AT_THE_EDGE.md` phases 2–8: R2 parts, locator DO,
  regional buckets). A local node does not need it. The read path already deployed
  against a windowed book proves the shape; do not let the edge work gate the host.
- **The v3 instance machinery** (WO-01…09, refill, memory ceilings, GC). It is adjacent
  and unfinished; it does not change whether a module answers from a book.
- **Installable-mitos phase 4** and the phonebook/R2 artifact formats, unless the
  consumption model is "download a published book".
- **The token-ledger / token-tape work in flight** (`token-summaries`). It is family B,
  and its mint/burn fold is useful input to the mint ledger, but it is not the plane's
  critical path.

---

## 4. Sequence

| Milestone | What | Gate |
|---|---|---|
| **M0 — ledger and decision** | Write the one status page; decide the tail (Dolos-only vs pallas), the base cadence, and the first module cohort | The plan has no open question that changes the sequence |
| **M1 — L1 surface** | `search_utxos` for the three caller shapes; open `datum.tbl`; run `holder-distribution`, `vesting-tracker`, `burn-address` against the book | The cohort matches the Dolos plane |
| **M2 — host selection** | Selectable plane in `bundle.rs`; hybrid fallbacks; shadow diff in production | The cohort runs shadow, then default, with rollback |
| **M3 — delta point resolution** | Integrate the existing delta into `BookDataPlane`: `read_utxo` over base ∪ delta, spent-mask and created-since-boundary | A module distinguishes spent, never existed, and created after the boundary |
| **M4 — delta enumeration and publish** | `by_addr` / `by_policy` over the delta; sealed `delta/NNNNN.dlt` with the base fingerprint; rollback within the safety depth | `utxos_by_address` / `utxos_by_policy` answer at the tip in the request path |
| **M5 — metadata and history** | `tx-index`-backed `tx_metadata` / `read_tx`; the mint ledger; the metadata trio | `collection-metadata` and the mint modules match the Dolos plane |
| **M6 — L2 and footprint** | Merge/rebase; the chosen tail; publish on a cadence; shrink or remove the Dolos stores; minimum-box profile | The 8 GB / 256 GB profile holds tip and serves the cohort |

M1–M2 are the L1 cutover. M3–M6 are L2. Do not start M5 by re-walking the chain:
the mint ledger rides the same walk as the book (`DELTA_CHAIN.md` §8).

---

## 5. Decisions needed

1. **Tail: Dolos tail-only, or a pallas tail?** Recommendation: Dolos tail-only for L1,
   because it is the smallest change and keeps the rollback path; decide the pallas tail
   for L2, where the footprint argument starts paying. The gateway's `ChainTap` already
   proves a pallas follow in this fleet, so it is not a research project.
2. **Base cadence and publish ownership.** Recommendation: full base at an epoch
   boundary, delta per chunk, published with an attestation; the box consumes the
   published artifact rather than building from genesis. The merge follows when the
   minimum box matters.
3. **The mint ledger: a new artifact, or `policy-index` plus burns?** Recommendation: a
   new artifact. `policy-index` is mint provenance for CIP-25 and does not answer net
   supply; the `AssetMintState` shape wants initial tx, mint count and net quantity in
   one row. The token-ledger's `net_mint` fold is the place to reuse the arithmetic.
4. **First cohort.** Recommendation: the four UTxO-only modules plus `credit-address`'s
   `read_utxos` half, not the metadata trio. They are the shortest path to a production
   shadow and they do not depend on the mint ledger.
5. **"Both ways" reference.** The Dolos import on the ARM box is partial, so the
   comparison the first module test wanted does not exist. Decide: complete an import
   on a box, or accept the book's own verification (32.7 M entry confirms + the
   `$burnsnek` oracle) and validate modules against expectations. Recommendation: the
   latter for L1, an import for the L2 cutover if a full-surface diff is wanted.

---

## 6. What would make this balloon

- **Building the mint ledger with a second walk.** It must ride the book's walk, or the
  +100% economics eat the whole programme.
- **Making the edge path a prerequisite.** The host does not need R2 parts or a locator
  DO. Keep them separate.
- **Solving the tail and the merge at the same time as the first cutover.** L1 exists
  precisely so the base can prove itself before either is on the critical path.
- **Letting "L2" mean "remove every Dolos-shaped thing".** The tail is a legitimate
  remaining job; a node that follows with Dolos and serves from the book is still a
  landed plane for L1, and it is the version that can ship this quarter.
- **Another status doc.** The upgrade review's finding was that the documents
  contradicted the code. M0's ledger is the mitigation; keep it to one page and update
  it when a gate moves.

---

## 7. Immediate next step

The smallest useful package is M1 plus the M2 skeleton. The first three items landed
on 2026-10-30; the shadow diff is what remains:

1. **Done.** `search_utxos` for `at_address`, `under_policy` and payment credential
   with the opaque `Page` cursor. The watched-UTxO seed that previously logged and
   carried on now seeds 220 of 220 refs on the 1848–1851 window.
2. **Done.** `datum.tbl` is opened by `BookDataPlane`: `read_datum` and the hash arm
   of `read_output_datums` resolve from a table beside the book or via
   `open_with_datums`. Without one the hash is still returned with `payload: None`.
   The era-dependent unrevealed tail remains a named fallback (99% Babbage/Conway,
   68% Alonzo), not a completeness claim.
3. **Done.** `MITOS_DATA_PLANE=book` opens `MITOS_BOOK_DIR` (optionally with
   `MITOS_DATUM_DIR`) and delegates every `ChainDataPlane` call through `SelectedPlane`;
   a missing directory, an unstamped `book.meta`, or an unknown mode fails at startup by
   name, and the admin routes answer from the selected plane so `/_admin/status` shows the
   book's `base_slot`. Verified natively on the 1848–1851 window: the host logs
   `plane="book"`, status reports slot 40,003,168 with no archive horizon, and
   `/_admin/blocks/1` answers "no block at this slot", which is true of an artifact that
   carries no blocks.
4. A shadow diff for `collection-holders`, `holder-distribution`, `vesting-tracker` and
   `burn-address` against the same block range.

That is enough to know whether the L1 cutover is real, without betting on the delta,
the merge, the tail, or the mint ledger.

---

## 8. Local validation stack (container-compose)

A local stack already exists for the acceptance loop; the plane needs it **extended**, not
replaced. It is an **inner-loop** tool, not a substitute for the box or the edge. The plane
is local by construction; the roundtrip that hurts is the host and the companion, not the
artifact format.

### What it buys

| Loop | Today | With a local stack |
|---|---|---|
| Module against the book | Build on the box, run, read logs | Rebuild the host image, mount the book, run the module |
| `search_utxos` / `datum.tbl` / delta | Box roundtrip per change | Local, minutes |
| Shadow diff | Production or a test harness | Same stack, two host services, same inputs |
| Companion apply/recapture | Deploy a Worker or use the box | `wrangler dev` (workerd) with local DO storage |
| Publish/attestation read | R2 + the edge | Local R2 or MinIO, or a bind-mounted artifact |

The data is the part people forget is already local. The chunk store is
`~/data/mithril/immutable` (9,338 chunks, 219 GB, rsynced off the external volume), and
`fold-probe fold --immutable … --primitives --book <dir>` builds a real artifact from it for
the host to mmap. The inner loop uses a windowed book — chunks 1848–1851 is 92,165 rows at
tip 40,003,168, enough for a module to cold-start — while a whole-store book (10.9 M rows,
2.23 GB) is the artifact for a footprint or cutover run.

### What it does not buy

- **The 8 GB / 256 GB footprint.** A Mac is not the target box. `memory = "8G"` and
  `cpus` approximate it, but page-cache behaviour and the cgroup OOM path are what the
  footprint test is for. Do that on the box.
- **Real Dolos/Mithril freshness.** A fixture window or a copied chunk is fine for
  module behaviour, not for tail latency, snapshots, or a rollback deep enough to
  matter.
- **R2 and edge latency.** `live-book-edge` and `MITOS_AT_THE_EDGE.md` are about cold
  round trips and cache status; localhost answers neither.
- **Cloudflare lifecycle.** Workerd emulates DO storage, alarms and SQLite; it does not
  emulate eviction, isolate restarts, or production rate limits.
- **The full-chain build.** 49 minutes over 9,181 chunks is a box job, not a laptop one.

### What already exists — `deploy/local/`

The acceptance stack was migrated from `cnft.dev-workers/local/mitos-lab` on 2026-10-30.
It is two services, and it is the same substrate the plane run needs:

| Service | Image | What it does |
|---|---|---|
| `mitos` | `mitos-image/` (built) | A preprod host; `up.sh` cross-builds the binary (`cargo zigbuild`, musl) and stages three community modules |
| `worker` | `worker-image/` (built) | `minting-engine` under `wrangler dev --local`; its `MITOS_REPLICATE_URL` is a Cloudflare quick tunnel to the worker's port |

`up.sh` renders `generated/mitos.toml`, `generated/namespaces.toml` and the worker's
`.dev.vars`, because the host's artifact fetch is HTTPS-only (D8) and the worker's client
id is the tunnel hostname. The pieces that already pay for themselves:

- **The tunnel is solved once.** A quick tunnel gives a real hostname and certificate;
  without it the worker cannot be fetched at all.
- **Namespace staging.** `may_announce` refuses a client the operator has not listed, and
  a subscribe for a module with no artifact is refused at boot. `up.sh` stages
  `cip-25-mint`, `cip-68-mint` and `credit-address` from the tree.
- **The workerd gaps are named**, not discovered per session: no Secrets Store emulation,
  and the DO runtime reports no name so the dial-back pins one policy.
- **The book mount.** `up.sh --plane book --book .tmp/<artifact>` exports
  `MITOS_DATA_PLANE` and the container-side paths, and the stack mounts the repository's
  scratch directory read-only at `/books`. The bind source is the scratch directory, which
  `up.sh` always creates, so the plan check never depends on an artifact existing — and an
  empty directory named as a book fails at open with the path, rather than being read as
  "no rows".

The plane run extends this stack rather than adding a second one. The book mount and
`MITOS_DATA_PLANE=book` are already on the `mitos` service; a shadow diff adds a second host
service over the same bind mounts. `dolos` and `r2` stay optional and arrive only when the
tail and publish paths are on the critical path.

One trap worth knowing before editing the stack file: `container-compose plan` checks
that every bind source exists, and `up.sh` creates `generated/` and `.tmp/worker` as its
first steps. A `plan` on a clean checkout therefore refuses with a missing
`generated/mitos.toml` — that is the staging step not having run, not a bad stack file.

### The traps that will cost an afternoon

- **`stack.toml`, not compose YAML.** This is deliberate; do not translate one on the
  fly.
- **DNS is host-level.** `[dns] domain = "test"` plus `container system dns create test`,
  then `down`/`up` so containers are recreated. Without it, `<name>.test` does not
  resolve and the companion dial-back looks like a broken image. On an `isolated`
  network bare names do not resolve at all.
- **`mmap` over a bind mount is unmeasured.** The host's reader is mmap-based. If a
  virtiofs bind mount is slow or refuses the mapping, copy the book into a named volume
  once and mount that. Correctness first; the stack is not a latency benchmark.
- **No one-shot dependencies.** `container-compose` has no
  `service_completed_successfully`; run `fold-probe verify` (or `Base::open`) in the
  host's entrypoint before serving, rather than a `verify` service the host waits on.
- **The host still needs a Dolos domain today.** `bundle.rs` constructs
  `DomainDataPlane` unconditionally, so a book-only host needs M2's selectable plane
  first. Until then the stack can run the existing test harness rather than the bundle.
- **A workerd companion is not a DO.** The local Worker is enough to test apply, ack,
  recapture and the dial-back; it is not evidence about eviction or production limits.

### Recommendation

Extend `deploy/local/` as part of M1/M2, after `MITOS_DATA_PLANE=book` exists — not as a
separate project. The first version only needs `mitos` + `worker` + the local book;
`dolos` and `r2` follow when the tail and publish paths are on the critical path. Keep
the box for the full-chain build, the 8 GB ladder, and the final smoke.
