# Mitos upgrade — independent review, core definition, and action plan

**Scope:** the Platform v3 work-order programme (`docs/work-orders/WO-01…09` plus the
footprint orders F1–F5), the installable-mitos plan (`infra/docs/work-orders/PLAN-I-installable-mitos.md`),
and the R2/edge initiatives that grew alongside them.
**Reviewed at:** `damon-refactor` @ `62706fa` (2026-09-29).
**Method:** read the orders, strategy docs, code and git history; ran what is cheap to
run (evidence in Appendix A). Claims quoted from other documents are marked **(doc)**.
Nothing but this file was changed.

**Rev 2 (same day):** rev 1 under-weighted the redesign's central axis — *decoupling from
dolos's backing stores* (`PHONEBOOK_COVERAGE.md`, `MITHRIL_PHONEBOOK.md`, `live-book`,
`BookDataPlane`). §1, §2.4, §2.5 and §4 were revised to put the phonebook/plane migration
where it belongs: at the centre. §5 and §6 adjusted in step.

---

## 1. The short version

1. **The v3 engineering wave is real and, in the parts I could verify, sound.** WO-01…09 are
   implemented and were each independently reviewed; the decision layer holds up under test
   (`mitos-lifecycle` 110/110, `mitos-config` 26/26, `cargo check` clean — Appendix A).
2. **The programme, not the code, is where it hurts.** Three agent tracks ran in parallel
   (the orders, the installer, the R2 designs). Status documentation has drifted in at least
   four places, one prescribed report was never written, and all five R2 design docs are
   untracked. Nobody — human or agent — can currently read the branch and know what is true.
3. **The upgrade's user-visible outcomes are not reached.** There is no release pipeline
   (`release.yaml` does not exist), the install path is half-landed (config/init/doctor/bootstrap
   exist; `mitos modules` and the rest of Phase 4 do not), nothing v3 is deployed, and
   production still runs a 2026-09-18 source build.
4. **The redesign's centre is a decoupling, not an instance model.** A big part of the
   current work is moving mitos *off dolos's backing stores* onto owned folds over the
   Mithril log (the "phonebook": `live-book` + siblings), with dolos shrinking to the live
   tail. That programme is real and partly proven (`BookDataPlane`, the passing
   `collection_holders_over_book` test) — yet it has no accepted strategy doc, no status
   line, and its own audit (`PHONEBOOK_COVERAGE.md`) says the plane cutover is not yet
   checked against the module surface. §2.4 and §4 put it where it belongs.
5. **The instance machinery is heavy and its payoff is deferred.** Gated behind WO-10
   (refill), F4 (memory ceilings) and WO-12 (GC) — none written — while the *installable
   mitos* headline stopped half-way. Combined with 4, the programme's effort and its
   visible outcome are pointing in different directions.
6. **"Core" is not defined anywhere in the repo.** §4 now proposes one that matches the
   decoupling: the plane mitos owns, the node that serves it, and the shrinking tail.

---

## 2. Track by track — what is actually true

### 2.1 Platform v3 (WO-01…09) — built, reviewed, nothing deployed; gates open

v3 changes the unit of execution from the module to the **instance** —
`(module, wasm_sha256, config_sha256)` — so two builds of one module can run side by side
while a client is moved across them. Every lifecycle *decision* is a pure reducer in
`crates/mitos-lifecycle` (`step(state, event, ctx) → Transition { next, effects }`); the
orders wired the host to translate events and execute effects **(doc)**.

- All nine orders carry "Implemented" and "Review" sections with real defect findings —
  including defects in the *orders themselves* (WO-09's stranded-quiesce deadlock and
  first-subscribe loop; WO-06's rename window) **(doc, WO-09 read in full)**.
- Gates still open, by the index's own account: WO-10 (refill — a crash loses a recapture),
  F4 (per-instance memory ceilings; `max_memory_bytes` is fuel-only), WO-11 (client lease),
  WO-12 (GC; delete `auto_resume`), plus an "small, unowned" row **(doc)**.
- **Deployment state: none.** Production runs `8cf37ae29bee-dirty`, built 2026-09-18 — before
  the WO wave completed (verified on the box in the ops session, 2026-09-29). No deploy note
  was executed.

### 2.2 Footprint track (F1–F5)

- F1 (ARM box rebuild) — implemented; operator mid-provisioning with Amendment A **(doc, infra)**.
- F2 (cache knobs + `MITOS_DELIVERY=shadow`) — "ready", not written. **I verified it is
  absent from the code.** This is the cheap, standalone one, and half of the mainnet gate **(doc)**.
- F3, F4 — not written.
- F5 (`mitos bootstrap`) — the index says "ready / not written", but the code **exists**:
  `crates/mitos-core/src/bootstrap.rs` (plan/inspect/classify + 8 tests) and `bootstrap_host.rs`,
  added by `37fad1c "more installer work (grok)"`. Status drift, not missing work.

### 2.3 Install track (PLAN-I) — planned end to end, partially executed, no report

PLAN-I was the deliberate answer to "write → implement → review keeps going wrong": **one plan,
no reviewer, the attack pass folded in**, driven from `infra`. Its own definition of done
requires `infra/docs/work-orders/PLAN-I-report.md` **(doc)**. That file does not exist —
searched both repos. So:

| PLAN-I phase | State (verified) |
|---|---|
| Phase 1 — release CI, checksums, signature, R2 mirror, `install-release.sh` | **absent**: `.github/workflows/` holds only `ci.yaml` |
| Phase 2 — `mitos.toml`, `mitos init`, `mitos doctor` | **substantially present**: `crates/mitos-config` (file/resolve/derive/network/install/catalogue/digest; 26 tests), `Init`/`Doctor` subcommands in `bundles/default/src/cli.rs` |
| Phase 3 — F5 + units + `user-data.sh` | **partially present**: bootstrap code + tests; `release/user-data.sh` exists (7 lines, satisfies the ≤10 gate); units noted in the tree |
| Phase 4 — signed module catalogue, `mitos modules sync` | **absent**: no `modules` subcommand; `catalogue.rs` is the type only |
| Report / "what the operator must now do" | **absent** |

Commits name the author lane: `ae5cd96 "self guided installer (grok build)"`, `37fad1c
"more installer work (grok)"`. `docs/design/INSTALLABLE_MITOS.md` still reads "Status:
PROPOSED. Nothing here is built." **(doc)** — false as of now.

### 2.4 The phonebook / R2 cluster — the redesign's other half

Not a side quest: this is the programme that replaces dolos's backing stores as the
node's data plane, with R2 as the distribution layer for what the folds produce.
`MITHRIL_PHONEBOOK.md` — "SKETCH with measurements", 2026-09-22 — is the spine;
`live-book`, `fold-probe`, `BookDataPlane` and the passing `collection_holders_over_book`
test are it executing. All five design docs are **untracked working-tree files** on a
branch where the code they describe is committed. State of each **(doc + code)**:

| Initiative | What it is | State |
|---|---|---|
| `MITOS_AT_THE_EDGE.md` | serve the live-book from R2 + Workers; locator DO for the tip | design; a read path was deployed ahead of its own phases (the doc amends itself); code: `live-book` publish/plan + tx-index hosted |
| `ARTIFACT_SHARDING.md` | chunk-sharded `log/` + derived `base/<epoch>/` format | proposal only |
| `DELTA_CHAIN.md` | a published delta over the book | instrument **built and measured** (`fold-probe delta/follow`); artifact not built |
| `COLLECTION_CATALOGUE.md` | live-set catalogue (~10 MB) | measured via `fold-probe catalogue`; artifact not built; overlaps the *existing* `crates/collection-catalogue` shipped from `cnft.dev-workers` |
| `BULK_BACKFILL_AND_ON_DEMAND_RESOLUTION.md` | admin pagination + aux surfaces | pagination ask **not implemented** (`/_admin/utxos` still uncursored); tx-index `/aux` landed (adjacent work) |

Plus the two that *are* real code on this branch: **`mithril-source`** (read Mithril chunks
over HTTP range requests; sidecar-addressed) and **`mithril-snapshot`** (validate/sync/publish
a snapshot to `r2:mithril-snapshots`) — both with CLIs and tests, neither yet wired to a
consumer; and **tx-index hosted** (`locate.rs`, `hosted.rs`, publish tool), the most-built of
the directions, consumed by the edge frontend in `cnft.dev-workers`.

And the piece that makes the cluster one programme rather than five: **`BookDataPlane`**
(`crates/mitos-data-plane/src/impls/book.rs`) answers six `ChainDataPlane` methods from a
published book and refuses the rest by name; `collection-holders` — a shipped module —
cold-starts over it and emits with Σ-quantity equality (the `MITOS_BOOK_DIR` test). R2 is
how the artifact reaches other machines; it is not a separate architecture.

### 2.5 The dolos pin — now scoped to a shrinking surface

mitos pins dolos at tag **v1.2.0**; upstream stable is **v1.6.1** (2026-09-17), and the
CF-backbone incident of 2026-09-28 was a wedge in dolos's own follow/pull layer. But the
version question is *not* "bump the whole dependency": the redesign is deliberately
reducing what dolos is used for (see §4's table — stores being replaced, types already
owned, serve stack already bypassed). What remains is the **follower/import path**, and
that is exactly where the incident lived and where a bump decision should be aimed. The
decoupling reduces this surface over time; it does not remove today's exposure.

---

## 3. Why the results feel unsatisfying — five structural causes

1. **No single source of truth.** Four status lines contradict the branch: V3 says "Unbuilt"
   while WO-01…09 are done; the WO index says F5 "not written" while its code exists;
   INSTALLABLE says "Nothing here is built" while half of it is; PLAN-I's report is absent.
   Every reader (including the next agent) must re-derive the truth from code.
2. **Parallel agent lanes with no lane discipline.** fable architected, opus implemented and
   reviewed, grok built the installer — each did real work, but the seams show: two repos,
   three registers, and the one track explicitly designed to run end-to-end *without* hand-off
   (PLAN-I) is the one with no record at all.
3. **Value deferred behind unstarted gates.** The instance machinery is the largest artefact
   of the wave; its deployable payoff needs WO-10 + F4 + WO-12, none of which exist. Meanwhile
   the headline — *a node anyone can install* — stopped at Phase 2/3 with Phase 1 (the part
   that must exist first: a release artefact to install) absent.
4. **R2 work has no core to attach to.** Five designs, one bucket, overlapping artifacts
   (`collection-catalogue` twice; delta vs sharding vs edge each holding part of the same
   answer), and no register of which format is a contract and who consumes it. One consumer
   shipped ahead of its own plan because there was nothing to hold the sequencing.
5. **Repo hygiene undermines all of the above.** 26 untracked paths including every R2 design
   doc; commit messages mostly "."; the status documents are the things that must be true and
   are the things that are stale.

**What is genuinely good and should be protected:** the pure-decision core
(`mitos-lifecycle`) with its evidence and test style; the review discipline that caught
real bugs rather than ticking boxes; PLAN-I itself as a document (it is the best process
artefact in the repo); and the real, measured code in `mithril-source`/`mithril-snapshot`,
`tx-index` hosted and `live-book` publishing. None of the above asks to throw work away —
it asks for boundaries and one truthful ledger.

---

## 4. What "the core" should be

Rev 1 of this section said "the node plus embedded dolos". **That is the thing the redesign
is moving away from.** `PHONEBOOK_COVERAGE.md`: *"Mitos is moving off dolos's backing
stores."* `MITHRIL_PHONEBOOK.md`: the chain of record is the Mithril log; current state is a
**fold over the walked chunks** — the live UTxO set is ~4 G against dolos's 93 G archive +
39 G index + 4 G state, "the rest is a rewrite of a log we already hold". The redesign is
already executing: `BookDataPlane` over `live-book`, and `collection-holders` (a shipped
module) cold-starting and emitting off the book.

So the core is the thing mitos *owns end to end*:

**Mitos core = the phonebook plane it owns · the node that serves it · the shrinking tail.**

**1. The plane (the redesign's centre).** Walk the Mithril log once; fold it into owned
artifacts — `live-book` first, then the siblings the module surface still needs (`tx-index`,
`datum.tbl`, the aux index, the mint ledger, the delta) — and answer `ChainDataPlane` from
those. One walk, N artifacts, no join. The plane's coverage is the spec:
`PHONEBOOK_COVERAGE.md` §3 is one row per method of the module surface, with what closes
each gap.

**2. The node.** `bundles/default` + `mitos-core` / `mitos-platform` / `mitos-config` /
`mitos-lifecycle` / `mitos-protocol`(+WIT). Duties: **execute** (sandboxed modules),
**deliver** (typed emissions to companions), **install/describe itself** (`mitos.toml`,
`init`, `doctor`, `bootstrap`), **report** (v3 D11) — unchanged by rev 2, except that
"follow" moves to 3 and the data plane stops being dolos's.

**3. The tail (dolos's remaining job, and the open question).** Today dolos supplies the
live follow; `lag_tolerant` already bypasses its broken tip adapter, and the ledger tools
run their own pallas chainsync tails. `DELTA_CHAIN.md` prices the mechanism that would let
book+delta answer at the tip, but **nothing yet specifies how a production node sources
live blocks** — dolos's follower, a pallas tail, or chunk catch-up. That question is the
frontier of the decoupling, and the honest state is "open".

**The dolos dependency, part by part** — the actual deliverable of "decouple from parts of
dolos":

| dolos part | role today | decoupling state |
|---|---|---|
| state/index stores (`redb`/`fjall`) | what `LocalDataPlane` answers from | **being replaced** by the book; gaps enumerated in `PHONEBOOK_COVERAGE.md` §3 |
| archive (93 G) | history + anchors | replaced by `tx-index` + the log itself (`ARTIFACT_SHARDING` direction) |
| WAL + tip subscription | module follow | partially bypassed (`lag_tolerant`); delta is the candidate replacement |
| serve stack (minibf/gRPC/TRP) | — | already decoupled: mitos mounts its own minibf |
| types | module-facing records | already decoupled (`mitos-data-plane::adapters::dolos`; v2 type ownership) |
| follower + import | the node's tail; `mitos bootstrap` | **not decoupled** — the remaining dependency, where the version question belongs |

**Contracts — frozen, versioned, everything else keys off these:** the WIT world; the
`mitos-protocol` wire types; emission/cursor semantics; the module manifest and the signed
catalogue; and one **artifact register** naming every published format (live-book parts +
attestation, tx-index shards, mithril chunk addressing, delta, sharding, catalogue) with its
format crate, builder, publisher and the plane method it serves. That register **is** the
plane's contract surface — R2 is its distribution, not a separate architecture.

**Periphery — deliberately outside the core:** the snapshot walkers and read planes
(family B: `tx-index`, `policy-index`, `policy-archive`, `live-book`, `mithril-*`, the
ledgers, `fold-probe`); R2 and the edge readers; companion DOs; the dev tools. These are
programs with their own contracts, not node features.

**Rules of thumb** (use these to settle every future boundary argument):
- If it names a venue, policy, DEX or collection → module or walker, never core.
- If it must exist for `mitos` to follow, execute, deliver or install → core.
- If a stranger can regenerate or consume it from a published artifact → periphery with a
  registered contract.
- Keep **local-first**: the data plane reads local stores; R2 is distribution for derived
  artifacts, never the node's dependency for following the chain.
- If a module-facing question can only be answered from dolos's stores, log it in
  `PHONEBOOK_COVERAGE.md` §3 — it is plane coverage to close, not a store to keep.

**Where the existing work fits:** the WO-01…09 instance machinery is core-internal — it is
the *upgrade mechanism* of the core, not a second product. The phonebook/R2 cluster is not
periphery: it is the plane being moved, with R2 as distribution. The install track is the
core's distribution face, and its I5 ("published data snapshot") should be re-scoped to the
phonebook artifact set, not a dolos data dir. The dolos pin is a decision about the tail.

**Anti-goals:** core is not a multi-tenant control plane; core does not absorb walker
workloads; core does not grow a second deployment story beside `mitos init`/"systemd + one
binary"; and the core does not quietly keep two data planes alive — the book is the plane,
or it is not.

---

## 5. Action plan

### Phase 0 — truth repair (hours; do before anything else)

1. **Commit or move the untracked design docs** (all five R2 designs + the rest of the 26
   paths). They are the only record of live work.
2. **Write one status ledger** — `docs/work-orders/STATUS.md` — a single table:
   track | state | gate | next action | owner session. It supersedes the status lines in
   V3, the WO index, and INSTALLABLE; those get one-line pointers to it and stop being
   sources of truth.
3. **Write `infra/docs/work-orders/PLAN-I-report.md`** — even if its content is "not executed;
   Phases 2–3 partially landed by the installer lane; Phases 1 and 4 absent" with pointers.
   A missing report is how this programme lost its thread.
4. Stop the "." commit messages for anything a status doc will cite.

### Phase 1 — certify the branch (a day)

Run, at one sha, and record the output verbatim in STATUS.md:
`cargo fmt --check` · `clippy --workspace --all-targets --all-features -- -D warnings` ·
`check --workspace --all-targets` · tests for `mitos-lifecycle`, `mitos-config`,
`mitos-platform`, `mitos-core` · the wasm32 check for `mitos-protocol`/`mitos-companion` ·
goldens 69/69 (rebuild `mitos-run` and the modules first). Fix what is red, or record why
it stands. This produces the "certified at `<sha>`" line every later step can cite.

### Phase 2 — finish the upgrade's shippable spine (the recommendation)

The upgrade's visible outcome is *a node anyone can install*. The shortest path to it,
in PLAN-I's own order:

1. **Phase 1 of PLAN-I** — release workflow (both targets), checksums + minisign,
   `install-release.sh`, `deploy.sh --from-release`. This must exist before anything else
   in the track can be exercised.
2. **Verify and complete Phases 2–3** — audit what the installer lane left against the plan
   (the config-equivalence test against `infra`'s real `dolos.toml`s; install-state recovery
   tests through the serialised form; the systemd `Condition`/`Requires` semantics check).
3. **Operator acceptance** — rebuild the ARM box from `release/user-data.sh`, no SSH, find
   it at tip (PLAN-I's I3 gate). That is the upgrade, demonstrated. Two phonebook decisions
   land on the same box: `PHONEBOOK_COVERAGE.md` §11's "one module, both ways" is blocked on
   a reference — complete an import there, or accept the book's own verification — and that
   box's killed import is the artefact the decision is about.

### Phase 3 — decision memos (each ≤ 2 pages, each decided not drifted)

- **Dolos version**: bump v1.2.0 → the 1.6.x line, or don't. Include WAL/storage compat
  (the `Cargo.toml` note about schema versioning and forced resync), what it buys against
  the chain-sync failure modes we have observed, and a spike plan on preprod/ARM first.
- **Instance machinery funding**: fund WO-10 (+ F4) now, or park it with an explicit
  "not before X" in STATUS.md. Either answer is fine; silence is not.
- **Phonebook register + freeze**: fill the artifact register (§4) from the phonebook
  cluster; status each row built/designed; no new design docs until every row has a format
  crate, builder, publisher and the plane method it serves.

### Phase 4 — adopt the core

Move §4 of this document into `docs/design/MITOS_CORE.md` (or keep this file as the seed;
commit it either way), and make every new work order state which core duty, contract or
periphery slot it touches. That is the sentence that was missing. Also settle the
phonebook's own status: `MITHRIL_PHONEBOOK.md` is still marked SKETCH while its pieces
execute — accept it as the direction, or say what would.

**Process guardrails, learned here:** one agent lane at a time; the status ledger entry and
the report file are part of Definition of Done (no report ⇒ not done); design docs land
tracked, not untracked; a plan's report file exists before its next phase begins.

---

## 6. Decision points for the operator

1. Is the instance/move machinery wanted on mainnet *soon* (→ fund WO-10 + F4 now) or
   *later* (→ park it in STATUS.md with a date)?
2. Is the install spine (PLAN-I Phases 1–3) the priority over further R2/edge work?
   (My recommendation: yes — it is the upgrade's own headline.)
3. Dolos bump: green-light the spike?
4. Who/which lane executes Phase 0–2 — and does this review get committed as the seed
   for `docs/design/MITOS_CORE.md`?
5. Phonebook reference: complete a dolos import on the ARM box, or accept the book's own
   verification as the reference for the first module-over-book diff
   (`PHONEBOOK_COVERAGE.md` §11)? And does `MITHRIL_PHONEBOOK.md` graduate from SKETCH to
   the accepted direction?

---

## Appendix A — evidence run at this sha (2026-09-29)

| Command | Result |
|---|---|
| `cargo test -p mitos-lifecycle` | **110 passed, 0 failed** |
| `cargo test -p mitos-config` | **26 passed, 0 failed** |
| `cargo check -p mitos-config -p mitos-core -p mitos-platform` | clean (Finished, no warnings in output) |
| `grep -rn "#\[ignore\]"` in platform/lifecycle/core/config | none |
| `ls .github/workflows/` | `ci.yaml` only (no release pipeline) |
| `bundles/default/src/cli.rs` subcommands | `Init`, `Doctor`, `Bootstrap` — no `modules` |
| `ls release/` / `networks/` | `user-data.sh` / mainnet+preprod |
| PLAN-I report search (both repos) | not found |
| untracked paths (`at-recall state`) | 26, incl. all five R2 design docs |
| `BookDataPlane` (`mitos-data-plane/src/impls/book.rs`) | exists; 6 methods answered, the rest refused by name |
| `crates/mitos-platform/tests/collection_holders_over_book.rs` | exists; passes with `MITOS_BOOK_DIR` set, skips without |
| `PHONEBOOK_COVERAGE.md` §11 | item 3 (one module over the book) marked passing 2026-09-23 |

Quoted-but-not-rerun (marked **(doc)** above): WO acceptance numbers, F-track statuses,
R2 implementations' internal test counts, the box's deployed build sha (verified in the
2026-09-29 ops session, not re-checked here).

## Appendix B — the ledger this should become

STATUS.md's first draft is Appendix B of this review: one row per track — v3/WO, footprint,
install, each R2 initiative, dolos — with {state, gate, next action}. Kept in one file, it
would have prevented four of the five problems in §3.
