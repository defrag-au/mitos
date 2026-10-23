# Native holder-snapshot plane

Status: **PARKED** (2026-07-06). holder-map pivoted to
aliens-only (registered projects with custom frontends; generic-token
self-serve left to others filling the TapTools gap). With the ad-hoc
companions evicted, holder-distribution serves ~2 companions and the
wasm path handles it in milliseconds — the heavy-tail/scale problems
this plane solves no longer exist in production. The design stays
valid if self-serve ever returns; nothing below was invalidated.

Previously: proposed (2026-07-05). Supersedes the Phase-2 "scoped
per-policy refresh + decomp fuel fixes" track of
`RECAPTURE_RELIABILITY.md` and the *in-module* implementation plan of
`HOLDER_DISTRIBUTION_MULTI_DEX.md`. The multi-DEX doc remains the
source of truth for the on-chain facts (pool script hashes, LP
unit/total-supply per DEX, datum layouts) — this doc changes *where*
that logic runs, not *what* it computes.

## Problem — the boundary is in the wrong place

Every recurring holder-map failure since May shares one shape: a
whole-UTxO-set aggregate scan forced through the fuel-budgeted wasm
data path. The trap-poison incident, the budget-chunking slices, the
progress-cap livelock, and the HOSKY grind that killed
holder-distribution for two weeks (2026-06-21 → 07-05) are all the
same defect surfacing in the same subsystem:

- 100K+ UTxOs paged 64 rows per fuel-budgeted call, ~19s wasm
  re-instantiation per trap, `MAX_TRAP_REBUILDS=64` → HOSKY-class
  tokens structurally end `partial` (~17 min of grind, every time);
- decomp progress is resident-only (`DECOMP_STATE` thread-local; the
  durable cursor is a 1-byte marker) so any trap restarts
  decomposition from zero;
- `utxos_by_policy` truncates at a 100K hard cap → silent
  under-attribution for shared-LP-policy DEXes;
- a decode-failed pool still pays the full materialize pass
  (1000 individual `state_kv::set_value`s per step) for zero benefit.

Meanwhile the *event* half of the module — `handle_events` /
`flush_buffer` live deltas, interest routing, keyed emission — has
never been the failure. The sandbox earns its keep there (trap
isolation for the shared host, community-module surface). The
snapshot builder is first-party code doing batch analytics; the
sandbox buys it nothing and costs it the entire failure class. The
same box runs dolos, which performs these scans natively in seconds.

**Decision: move snapshot building (scan + LP/vest decomposition +
emit) host-side as native Rust. The wasm module keeps the event
path.** Per-DEX pool decoding lands natively in `mitos-dex-decode`
(already a native crate) — the multi-DEX slice is implemented once,
where it cannot trap.

## Architecture

### Components

1. **`mitos-platform/src/holder_snapshot/`** — the native builder.
   For one `(policy, chain-point)`:
   - **Scan**: stream the policy's UTxOs from the local data plane
     (paged native iterator, no 100K cap, no page floor — memory is
     bounded by streaming + the chunked emit, not by fuel). Classify
     each holder (wallet / pool / lock) exactly as the module does
     today: bech32 → payment/stake cred, CrowdLock detection, and
     **pool detection by payment-cred match against the native DEX
     table** (`mitos-dex-decode::dex_for_payment_cred`) — multi-pool
     from day one (`pools: Vec<PoolRef{oref, dex}>`).
   - **Decompose**: per pool — datum via `read_output_datums` +
     `datum_by_hash` fallback, `mitos_dex_decode::decode(dex, datum,
     pool_value) -> PoolDecode{lp_policy, lp_name, total_lp}`,
     reserve read from the pool UTxO's own value, LP holders via
     **`utxos_by_asset(lp_policy ++ lp_name)`** (exactly one pool's
     LP holders; see "Data-plane additions"). CSwap farm-staking
     resolution ports as-is; other DEXes' staked LP stays in the pool
     residual (logged), per the multi-DEX doc. Vesting decomposition
     ports as-is.
   - **Emit**: construct `SnapshotBegin / SnapshotChunk×N /
     SnapshotEnd` from `mitos-community-events` (native crate — the
     companions' wire format is reused byte-for-byte, policy-keyed)
     and push them through a dedicated `(sink, events_rx)` from the
     same `emitter_factory`, drained by the existing
     `run_emit_drain(storage, "holder-distribution", …)` →
     `InterestCache::companion_wants` fan-out. **Zero new delivery
     code; the worker DO's policy fence and Begin-wipe semantics are
     untouched** (verified: `feed_do_mitos.rs` treats Begin/Chunk/End
     as an authoritative per-policy replace).
   - **Seed module state**: write the resulting raw-SUM `ledger:`
     shards into the module's state-kv (host already writes module kv
     — same `(kv_factory)(id)` seam recapture uses for flags). The
     `LedgerEntry` CBOR shape is shared so the module's live
     read-modify-write path continues unchanged on top of the fresh
     base. Also write the per-predicate bootstrap-done flag so the
     host bootstrap machinery agrees the policy is populated.

2. **Build queue** — a host-side single-concurrency queue
   (`tokio::mpsc` + one worker task, dedup by policy). All triggers
   enqueue; sequential execution bounds box load. Per-policy build
   cost: sub-second for typical tokens, seconds for HOSKY-class
   (native full-set scan ≈ what dolos does for the 117K-UTxO
   rebootstrap today in 25s — minus wasm marshalling, minus traps).

3. **The slimmed wasm module** — keeps `init`, `update_interest`,
   `handle_events`/`flush_buffer` (live deltas, still `lp_amount: 0`
   — LP attribution stays snapshot-time, but refresh is now cheap and
   scoped). `rebootstrap(mode)` becomes a no-op stub for this module.
   The scan/decomp/emit machinery (~half the file) is deleted, along
   with its state families (`decomp:`, `meta:`, `onboard-predicates`,
   `rebootstrap-cursor`) — the native plane owns snapshot state.

### Triggers (all enqueue on the build queue)

| Trigger | Today | After |
|---|---|---|
| Subscribe/onboard | wasm Onboard pump (self-heal scope) | host enqueues the policy on interest-`Add`; data in seconds |
| LP/farm event (B1 debounce) | whole-module recapture (~22 min) | worker calls `POST /_admin/modules/holder-distribution/refresh-policy {policy}` → one scoped build |
| Scheduled backstop | hourly whole-module recapture | worker cron (relaxed to daily) calls `refresh-policy` per registered policy, or a `refresh-all` that enqueues each |
| Drift repair (operator) | whole-module recapture | `refresh-policy`; whole-module recapture remains as disaster recovery |

### Coordination-free delivery — retire the ack gate for this module

The dialer's Recapture-frame/ack protocol exists to order a
*whole-companion wipe* before refill. Each `HolderFeedDO` is
single-policy and `SnapshotBegin` already wipes-and-replaces exactly
that policy — so a per-policy snapshot needs **no coordination at
all**: no 30s ack gate, no paused deliveries, no dead-companion
laggard (`73cbf70e…` costs 30s of every recapture today). The gate
stays for other modules; holder-distribution simply stops using it.

### Data-plane additions (native-only — no WIT change)

`utxos_by_asset(subject)` from the multi-DEX doc is still required,
but only as a **`DataPlane` trait method + `impls/local.rs`**
(dolos's by-asset index already exists). The WIT host-fn, module
bindings, and rebuild-all-16-modules ABI step from the original plan
are **dropped** — no wasm consumer. Same for a new uncapped paged
scan iterator for the native builder: trait + local impl only.
Maestro/Koios fallback: best-effort or logged-unsupported; the local
plane is the production path.

### Consistency posture (unchanged from today)

Builds anchor at the chain point observed at scan start; live deltas
keep flowing during a build (the disposable-backfill era already has
scan-vs-live shard concurrency, tolerated via SUM-merge semantics +
chain-point-idempotent snapshot apply at the companion). The
companion applies Begin/Chunks/End then continues deltas — identical
to today's rebootstrap semantics. The one sharpening: the native
builder writes `ledger:` shards in one batched pass per policy,
narrowing the overlap window rather than widening it.

## What this kills / fixes

- **The entire fuel-trap class for snapshots** — no budgets, no page
  floors, no re-instantiation, no resident-state loss.
- **HOSKY becomes complete** for the first time (no 100K cap, no
  rebuild ceiling) — the admission guard demotes from "required for
  platform safety" to a UX/cost knob.
- **The whole-module hammer**: B1 + cron trigger scoped ~1s builds
  instead of 22-minute all-tenant rebuilds; blast radius of one token.
- **The multi-DEX gap** ($DIS's Minswap pool as an anonymous whale
  wallet): decomposition for Minswap V1/V2, SundaeSwap V1/V3,
  WingRiders V1/V2, Splash + CSwap lands natively in one pass.
- **Silent under-attribution risk** (shared-LP-policy truncation) via
  scoped `utxos_by_asset`.
- The `rebootstrap-stale` watch alert stops being permanently
  `partial`.

## Follow-on end-state: shares, not amounts (reset-free LP attribution)

The plane above makes snapshot rebuilds cheap and scoped; this
follow-on makes them *unnecessary* for LP attribution — leaving
snapshots only two jobs: onboarding and disaster recovery.

**Why attribution is snapshot-time today:** we bake a decomposed
token amount (`lp_amount = share × pool reserve`) into each holder
row. Reserves drift on every swap, so the baked number is stale the
moment it's written, and only a rebuild refreshes it. No incremental
scheme can keep a *product* current without recomputing on every
pool touch.

**The fix is representational — store the factors, multiply at read
time:**

- **Per-wallet LP `shares`** (LP-token units, per pool). LP tokens
  are ordinary native assets: mint/burn/transfer/stake/unstake are
  discrete UTxO events the live path already sees. The module (or
  the DEX modules it rides on) emits them as normal incremental
  deltas — `LpShareDelta { pool_id, holder, units }`. CSwap
  farm-staked LP resolves to the staker via the staking datum at the
  farm address — also a discrete event.
- **Per-pool ticker** — `PoolReserveTick { pool_id, dex,
  reserve_tracked, total_lp, slot }`, emitted on pool-UTxO spends
  (every swap/add/remove recreates the pool UTxO; the DEX modules
  already watch these). One small event per pool touch; `total_lp`
  comes from the datum (or, for WingRiders' mint-max scheme, from
  `2^63−1 − pool-held LP`, which the tick can carry precomputed).
- **Read-time multiply** in the DO/frontend:
  `lp_amount(holder) = shares/total_lp × reserve_tracked` — always
  current, never recomputed in bulk, never wiped.

**Schema ripple (the real cost):** `token_holders` drops the baked
`lp_amount` for `lp_shares` keyed by pool; a small `pools` table
(`pool_id, dex, reserve, total_lp, updated_slot`) lands in the DO;
two new wire variants; the egui holder-map renders the product.
Vesting can stay snapshot-time initially (locks are discrete and
rarer — same treatment available later if it ever drifts).

**Sequencing:** strictly after this plane ships — it reuses the same
native pool table/decoders for tick decoding, and the parity harness
gives the ground truth to validate share math against. Until then,
event-triggered scoped rebuilds (B1 → `refresh-policy`) bound
attribution staleness to ~seconds-per-event, which is already a
large step down from "hourly whole-module rebuild".

## Rollout

1. **Decoders first** (pure, golden-tested): complete
   `mitos-dex-decode` per the multi-DEX doc — Minswap V1/V2, Sundae
   V1/V3, WingRiders V1/V2 (arity-branched), Splash; goldens from
   live pool datums via `GET /_admin/blocks/by-tx`. Fix the
   swapped Minswap V1/V2 labels in `shared-crates/address-registry`
   in the same slice.
2. **Data plane**: `utxos_by_asset` + uncapped paged policy scan
   (trait + local impl + tests).
3. **Builder + queue + emit**: `holder_snapshot/` with the
   `run_emit_drain` delivery seam; parity harness — build a snapshot
   natively for aliens/nikepig and diff against the wasm path's
   current output (holder set, amounts, lp_amount, vests) before any
   cutover. `$aliens.vault` (staked-LP-only wallet) is the canonical
   LP fixture; SpaceBudz/CrowdLock for vesting.
4. **Cutover**: admin `refresh-policy` route; interest-`Add` enqueues;
   holder-distribution drops out of `is_chunked_cold_start_module`
   (manifest.rs:75) so the wasm Onboard pump skips it; slim the
   module; worker-side B1 + cron rewire (cron → daily).
5. **Cleanup**: retire the module's scan/decomp code + dead state
   families; note the ack-gate non-use; update
   `HOWTO_OBSERVABILITY.md` playbooks; relax/retune `mitos-watch` R4
   (rebootstrap-stale) to watch per-policy build results instead.

Each step deploys independently and rolls forward; step 3's parity
harness is the go/no-go gate for step 4.

## Risks

- **Shard-shape drift**: the native builder writes `LedgerEntry`
  CBOR the module must read back. Mitigation: share the type (module
  source already lives in-repo; extract the entry codec into a tiny
  shared crate or include-path module) + a round-trip test that reads
  module-written shards.
- **Decomp math divergence** vs the wasm implementation during the
  transition: the step-3 parity diff is the control; CSwap port is
  line-for-line.
- **Box CPU**: sequential queue bounds it; a HOSKY build is a few
  seconds of native scan vs 17 minutes of trap-thrash today — strictly
  better.
- **Two snapshot producers during rollout** (wasm rebootstrap still
  live until step 4): benign — same wire, same idempotent apply — but
  keep step 3 read-only (parity harness emits to a test sink, not to
  companions) to avoid double emission.

## Non-goals

- Live (per-swap) LP attribution **in this slice** — baked amounts
  can't be kept current incrementally (reserve drift is continuous);
  event-triggered scoped rebuilds bound the staleness here, and the
  real fix is the representational change in "Follow-on end-state:
  shares, not amounts" above.
- Touching other chunked modules (collection-holders/metadata,
  vesting-tracker): they haven't shown the heavy-tail failure; migrate
  only if/when they do, using this plane as the template.
- Replacing the wasm platform: community modules and all event-path
  logic stay exactly where they are.
