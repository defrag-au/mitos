# Roadmap

What's done, what's next, in what order. Items further down the list have
more open questions.

## Phase 0 — Scaffolding (DONE)

- Workspace layout
- `Indexer` trait defined
- Stub `JpgCoIndexer` that logs events
- Bundle main with TODO for Dolos init
- README + design docs

State: builds (or will once dolos crate resolution settles); doesn't run
chain events end-to-end.

## Phase 1 — Embedding viability spike

The actual proof point. Replicate `dolos/src/bin/dolos/common.rs::setup_domain`
in `crates/mitos-core/src/domain.rs`, get a bundle running against an
existing Dolos data directory.

- [ ] Copy + adapt `setup_domain` wiring
- [ ] Wire up Dolos's chain-sync stage as a tokio task in the bundle
- [ ] Confirm `domain.watch_tip(None)?` yields `TipEvent`s that hit the
      stub indexer's `handle_event`
- [ ] Verify the data directory format is identical to stock Dolos
      (so we can point at an already-bootstrapped one for local dev)

Decision gate: if the embedding works cleanly, continue. If it surfaces
unexpected friction (private types, lifetimes that don't compose, etc.),
reassess the embed-vs-fork-vs-greenfield tradeoff.

## Phase 2 — First real indexer (jpg-co)

- [ ] `bootstrap` against a Dolos data dir: enumerate UTxOs at the
      jpg.store CO contract via `domain.indexes()`, hydrate via
      `domain.state()`, decode datums, populate redb materialized view
- [ ] `handle_event(Apply)`: parse block via pallas-traverse, scan TX
      outputs, decode datums (inline or witness-set-resolved), upsert
- [ ] `handle_event(Undo)`: reverse the same block's effects
- [ ] HTTP routes: `/jpg-co/by-creator/{pkh}`, `/jpg-co/by-policy/{p}`,
      `/jpg-co/{tx_hash}/{idx}`
- [ ] Cursor persistence keyed off `Mark` events

Validation: spike's "find my COs" use case answered end-to-end. The
`dolos-spike` worker can be retired once this serves the same query.

## Phase 3 — Bundle deploy story

- [ ] `Cargo.toml` profile for ARM64 cross-compile (aarch64-unknown-linux-gnu)
- [ ] Build pipeline: produce a bundle tarball with binary + data-dir
      conventions
- [ ] Shiku integration: deploy the bundle as a Shiku-managed app
- [ ] systemd unit template
- [ ] Health check that aggregates per-indexer cursor lag
- [ ] Operational runbook (where dolos data lives, how to re-bootstrap,
      how to swap bundle versions)

Open question: do we need a Shiku rethink for compose (build-time module
selection) or is "one bundle = one Shiku app" sufficient? Lean toward
the simple answer until proven otherwise (per
`CARDANO-SHIKU.md`'s "Shiku rethink" section).

## Phase 4 — Second indexer + framework hardening

The second indexer (probably `jpg-listings-indexer`) is what proves the
framework actually composes — Phase 2 alone could be done as a single
binary without a trait.

- [ ] `JpgListingsIndexer` skeleton + bootstrap
- [ ] Validate the dispatcher fans out cleanly to two indexers
- [ ] Identify framework abstractions worth lifting from indexer code
      into `mitos-core` (e.g. shared address-watcher pattern, common
      cursor table schema)
- [ ] Reconciliation hook: optional `reconcile()` method on indexers
      that runs nightly, compares materialized view against fresh
      bootstrap, repairs drift

## Phase 5 — Operational maturity

- [ ] State backup hooks (snapshot the data dir periodically)
- [ ] Bundle parallel-run + atomic swap orchestration (per
      `CARDANO-SHIKU.md`'s schema migration section)
- [ ] Cursor-aware Shiku health checks
- [ ] Multi-bundle deploy: different boxes running different indexer
      mixes for partitioned workloads
- [ ] Multi-region / horizontal scaling validation

## Open questions parked for later

- **Should we also expose UTxO RPC?** Probably yes eventually — gives
  external clients a stable interface. But our immediate consumers are
  CF Workers calling our typed REST endpoints, so not urgent.
- **What about the spike's `dolos-spike` worker?** Stays as the "verify
  from edge" tool until phase 2 lands; then retire or repurpose for
  comparing-output validation against the indexer.
- **Mithril bootstrap from inside the bundle?** Currently the bundle
  expects Dolos's data dir to already exist. Re-bootstrapping requires
  invoking the Dolos CLI's bootstrap command (or replicating its logic
  inline). Defer until phase 3 deployment story makes it concrete.
- **Framework's reaction to Dolos crate API breakage?** Right now: pin
  to a tag, rebase deliberately. If breakage rate becomes painful, look
  at vendoring the specific traits we depend on.
- **WASM module support à la Balius?** Not on the roadmap. Add only if
  there's a real use case for hosting third-party untrusted indexers.
- **Storage layout for multi-tenant boxes?** Open. If one bundle hosts
  many indexers, each indexer's redb file is its own. If a single
  bundle's indexers want to share a transaction (cross-indexer atomic
  updates), that's a framework feature, but no current use case asks
  for it.
