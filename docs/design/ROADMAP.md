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

## Phase 1 — Embedding viability spike (VALIDATED)

Replicated `dolos/src/bin/dolos/common.rs::setup_domain` in
`crates/mitos-core/src/domain.rs`. Workspace builds clean (`cargo check`
+ `cargo clippy --all-targets -- -D warnings`) and runs end-to-end
against a snapshot of the production mainnet Dolos data directory.

- [x] `load_config(path) -> RootConfig` — TOML + `DOLOS_*` env overrides,
      compatible with stock `dolos.toml`
- [x] `setup_domain(&RootConfig) -> DomainAdapter` — opens stores, loads
      genesis, initializes `CardanoLogic`, runs `domain.bootstrap()`
- [x] `spawn_sync_pipeline(domain, &cfg, exit) -> JoinHandle<()>` —
      builds `dolos::sync::pipeline`, wraps in `gasket::Daemon`, runs
      with cancellation handoff
- [x] Bundle `main.rs` composes: load → setup_domain → spawn sync →
      indexer bootstrap + dispatcher → axum HTTP → graceful shutdown
- [x] `Indexer<D>` dispatcher accepts `Arc<Mutex<dyn Indexer<D>>>` so
      bundle can hold one handle for both `routes()` and the dispatcher
- [x] `Cargo.lock` seeded from upstream Dolos to pin transitive
      dependencies (mithril-client semver violation between 0.12.2 and
      0.12.34 made this necessary; documented in the lockfile copy)
- [x] **End-to-end run on cardano-infra box against snapshot of
      `/opt/dolos/mainnet/data`**: state + index keyspaces recover
      cleanly, `dolos_cardano` initializes, chain rolls forward from
      snapshot point, `jpg_co_indexer` stub receives `TipEvent::Apply`
      log lines as expected.

### Lessons banked during empirical validation

These are non-obvious enough to be worth capturing, so we don't relearn
them later.

**Mitos's Dolos pin must match the version that wrote the data dir.**
The WAL schema is versioned. Pointing a `tag = "v1.0.3"` mitos build at
a data dir written by `dolos v1.1.0` fails fast with
`WAL schema not compatible: found=Some(N) expected=M`. The fix is to
bump (or downgrade) the workspace pin in `Cargo.toml` to match. There
is no in-place WAL upgrade — the schema bump is a deliberate breaking
change in Dolos.

**The Dolos data directory is an atomic unit.** WAL, state (fjall),
and index (fjall) must be a consistent snapshot. Two failure modes we
hit:

- *rsync while running*: even with `Restart=no` and a clean stop, files
  written during the rsync window after Dolos's graceful-shutdown flush
  produce a state where `state` is ahead of `archive`/WAL. Dolos
  refuses to bootstrap on the inconsistency and `dolos doctor reset-wal`
  can't bridge a large gap.
- *partial re-copy*: trying to "fix" a divergent snapshot by copying
  only `state/` while leaving the older WAL in place produces the same
  inconsistency in the other direction.

The reliable mechanism is: stop Dolos cleanly (drop-in override
`Restart=no` if systemd is configured to auto-restart), confirm the
process is gone (use a `[d]olos daemon` non-self-matching pattern to
avoid `pkill` matching the SSH command itself), `sync(8)`, then
`cp -a` the whole data dir, `sync(8)` again, then restart Dolos. ~15
minutes for ~340GB on the cardano-infra box.

**`dolos doctor reset-wal` is the recovery tool** when WAL and state
diverge by a small amount, but it can't bridge an arbitrary gap. Treat
it as a tool of last resort, not a workaround for sloppy snapshots.

Open from this phase:

- [ ] Verify graceful shutdown actually drains the sync pipeline (we
      observed clean shutdown, but haven't tested that under-tip loss
      is zero on next restart).
- [ ] Decide on tracing format for production (currently bundle's
      defaults are dev-friendly compact + EnvFilter).
- [ ] Decide whether to keep the snapshot-clone workflow or run mitos
      directly off the production data dir (the latter would couple
      mitos's lifecycle to Dolos's, but avoids the 15-minute clone).

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

## Phase 4.5 — Cloudflare replication prototype

Wire format and consumer patterns are designed; this phase puts the
first one in production. Protocol is in `docs/design/CF_REPLICATION.md`.

**First migration target: `collection-ownership`.** Picked because the
existing DO schema is already a clean materialized view, writes are
idempotent, and the protocol's `Undo` semantics actually close an
existing reorg gap rather than just preserving current behaviour. The
code is at `cnft.dev-workers/workers/collection-ownership/`.

**Fork, don't feature-flag.** New worker at
`cnft.dev-workers/workers/collection-ownership-mitos/` runs in
parallel with the existing `collection-ownership` for as long as
convergence validation requires (target: 30+ days of byte-identical
read-API output before retiring the original). The fork avoids any
risk to the production path during prototype work; the cost of the
extra worker on CF is negligible.

**Server placement.** The replication WebSocket server is hosted by
the same axum app the bundle already runs for indexer HTTP routes —
new `/replicate/{indexer}` upgrade endpoint, no separate listener.
Each indexer's `routes()` Router continues nesting under
`/<indexer-name>/...` as today.

### Build order

Each step lands cleanly before the next becomes meaningful.

1. **Trait extension + bundle refactor** against the existing
   `JpgCoIndexer` (`type Scope = ()`). Proves the refactor works
   without introducing new behaviour; bundle still composes.
2. **`SubscribeReply` enum + `/replicate/{indexer}` WebSocket upgrade
   handler** in `mitos-core`. Drive from a Rust integration test with
   a synthetic CBOR client — validates framing, scope decode, cursor
   handling, retransmit buffer, ack-driven trim.
3. **`OwnershipIndexer` skeleton, watch-set-only.** Override
   `subscribe` to add `policy_id`; cold subscribe returns
   `cursor = current_tip` (no backfill yet). Proves end-to-end records
   flow for new mints/transfers.
4. **`collection-ownership-mitos` worker, minimum viable.** Hibernated
   WebSocket consumer, same SQLite schema as existing worker, same
   read APIs. Wire one test policy. **Diff `/api/check` and
   `/api/bundle` outputs against the existing worker hourly — that's
   the validation.**
5. **Backfill in `OwnershipIndexer`.** Synthetic-applies stream for
   cold subscribes (enumerate UTxOs via Dolos by-policy index,
   resolve owners, emit Apply per asset). Diff freshly-bootstrapped
   mitos DO against existing for same policy.
6. **R2 snapshot path.** Only when a real >50k-asset collection
   exposes the inline backfill limit.
7. **Reorg validation.** Pick a known historical reorg, replay both
   pipelines, confirm mitos side emits `Undo` records and converges
   correctly while existing side does not.
8. **Extract `mitos-protocol` crate** so the worker side stops
   hand-mirroring wire types. The first end-to-end test (Black Flag,
   2026-05-02) hit a wire-format bug because pallas's `Hash<32>`
   Serialize impl emits a hex *string*, not bytes, and the worker's
   `protocol.rs` declared the cursor's hash field as
   `#[serde(with = "serde_bytes")] Vec<u8>` — every record failed
   to decode. Mirroring is structurally drift-prone; a shared crate
   prevents the class.

   Shape:
   - New crate `crates/mitos-protocol` in the mitos workspace.
     wasm32-compatible (no dolos/pallas deps in the *protocol*
     types). Owns: `ChainPoint`, `ClientMessage`, `ServerMessage`,
     `SubscribeReply`, encode/decode helpers, the indexer-specific
     change types (`OwnershipScope`, `OwnershipChange`).
   - `ChainPoint::Specific(u64, String)` is canonical (the hex
     string matches what pallas emits on the wire). mitos-core
     defines `From<dolos_core::ChainPoint>` for the conversion at
     the dispatcher boundary.
   - Once mitos is a public repo, `cnft.dev-workers` adds it as a
     git or version dep. The hand-mirrored
     `workers/collection-ownership-mitos/src/protocol.rs` deletes.
   - Estimated ~30 minutes of work; bounded; no API change to
     existing consumers.

### How to run it

End-to-end recipes (Scenarios 1→3) are in [`../TESTING.md`](../TESTING.md).
Friendly admin API + a `mitos-tail` synthetic CBOR client + the
diff harness are all in place; `wrangler dev` or a `*.workers.dev`
deploy is enough on the CF side.

### Success criteria for "works in practice"

- [ ] One real registered policy (~5-10k assets) tracked in parallel
      for 7 days
- [ ] `/api/check` and `/api/bundle` outputs match byte-for-byte
      hourly
- [ ] DO active duration on mitos side >90% lower than existing
      (validates the hibernation cost projection)
- [ ] Mitos-side recovers cleanly after forced mitos restart; DO
      takes resume path, no data loss
- [ ] Simulated reorg produces correct `Undo` flow

### Deferred for the prototype

Don't get bogged down before learning anything:

- Authentication: hardcoded shared secret in upgrade header
- Per-block message batching: one record per message
- Multi-consumer testing: one DO, one connection
- `schema_version` evolution: pin v0
- Mints flow / holder-map: not until collection-ownership is
  converging cleanly

- [ ] `Indexer` trait extension: associated `type Scope`, default
      `subscribe`/`unsubscribe` impls, `SubscribeReply` enum with
      resume / snapshot-redirect / fork-recognition variants
- [ ] Bundle registration refactor: `Bundle::add_indexer<I: Indexer<D>>`
      generic helper that type-erases `Scope` into a `Box<dyn
      IndexerHandle>` adapter inside the framework (axum-style), so the
      trait can carry an associated type while the bundle stays
      heterogeneous. Replaces the Phase 1 `Arc<Mutex<dyn Indexer<D>>>`
      pattern.
- [ ] `mitos-core` `Snapshotter` helper: per-indexer R2 writer with
      cursor-stamped CBOR/zstd output, latest-pointer maintenance,
      old-snapshot pruning
- [ ] Push channel implementation: WebSocket via DO Hibernation API
      (mandated by CF billing — see `CF_REPLICATION.md`),
      authentication, per-consumer retransmit buffer with
      cursor-ack-driven trim, per-block message batching. **Mitos is
      the WS client (outbound dial); CF DO is the WS server.**
- [ ] `Replicator` outbound dial loop: tokio-tungstenite client per
      registered subscription, reconnect with backoff, hands the
      socket to the same `run_subscriber` protocol logic as the
      `/replicate/{indexer}` test surface uses
- [ ] `subscribe(last_cursor)` handler that picks between resume,
      snapshot redirect, and fork-recognition reply
- [ ] `OwnershipIndexer` in mitos that mirrors the cnft.dev-workers DO
      schema, populated from chain
- [ ] CF-side: modify the collection-ownership DO to consume the push
      channel instead of `POST /ingest` from classifier
- [ ] Run both pipelines in parallel, diff outputs continuously until
      they converge, then cut classifier ingest

Second target: **mint notifications**, gated on confirming where the
existing dedup mechanism lives (or adding one) and deciding the Discord
delivery path. Likely implies a small VPS-side relay process for
Discord webhooks, with CF owning a `(asset_id, channel_id) → sent_at`
dedup table. See `CF_REPLICATION.md` "Discord delivery" section.

Third target: **`holder-map` as a Pattern B validation.** The existing
frontend library already consumes WebSocket updates; we add a thin DO
that subscribes to mitos and re-publishes to browsers. Doesn't displace
any current code, just proves the relay pattern.

Open before phase start:

- [ ] Confirm dedup mechanism for current mint notifications (search
      classifier/notifier for the missing piece, or confirm absence)
- [ ] Decide where the Discord relay process lives (same VPS as mitos,
      separate VPS, or somewhere else)

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
- **Replication protocol to Cloudflare consumers?** Designed. Mitos
  pushes `Apply(cursor, change)` / `Undo(cursor)` records over a
  long-lived HTTP/2 (or WebSocket) channel; full replay uses an R2
  snapshot keyed by `(slot, hash)` plus a resume cursor. Cursor is the
  Cardano `(slot, block_hash)` pair, mirroring `TipEvent`. Consumers
  reconnect with their last-applied cursor; mitos picks resume,
  snapshot-redirect, or fork-recognition based on the gap. First
  prototype in Phase 4.5. Full protocol in
  `docs/design/CF_REPLICATION.md`.
