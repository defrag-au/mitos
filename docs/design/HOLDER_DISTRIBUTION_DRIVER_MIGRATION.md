# Scalable chunked bootstrap for sharded modules (SUM + replace)

**Status: design / open.** Drafted 2026-05-22 after the
`collection-metadata` migration to `ChunkedBootstrap` succeeded but the
`collection-holders` (SUM) migration failed and a scale review showed the
current driver doesn't reach the target. This is the concrete spec for the
follow-up work; fresh-session-ready.

(Filename kept from the earlier `holder-distribution`-only note for link
continuity; scope is now all sharded modules + the host-fn work they need.)

## Scope / goal

Make `mitos_module_kit::ChunkedBootstrap` scale to a **~420K-entry policy
(Hosky-Cash-Grab class)** for both:

- **replace** modules (`collection-metadata` — one entry per asset, last-write-wins), and
- **SUM** modules (`collection-holders`, `holder-distribution` — a holder×asset
  recurs across UTxOs/pages, balances add).

Hard requirements at that scale: **bounded fuel + memory per host call** (no
trap), **completes** (no `SnapshotBegin` loop), **O(n) total** (no O(n²)), and
**nothing resident across calls** beyond a small durable cursor.

## Background — what shipped, what failed

**Shipped + verified (2026-05-22):**
- `mitos_module_kit::ChunkedBootstrap` — re-entrant scan→shard→emit driver,
  pure state machine driven via a `BootstrapIo` trait. 9 unit tests.
- Platform: `list-values: func(prefix) -> list<string>` added to the v2
  `state-kv` WIT + host (`crates/mitos-platform/wit-v2/world.wit`,
  `src/host_fns_v2/state_kv.rs`). Additive.
- `collection-metadata` migrated → sharded per-asset store, replace semantics.
  **Deployed + verified 100% on Nikeverse (5554 assets).** 47 goldens pass.

**Failed + reverted:** `collection-holders` (SUM). The driver's `accumulates`
path does `shard_get`+decode+`merge`+encode+`shard_put` **per entry, per page**
(each a redb txn, writes `Durability::Immediate` → fsync), on top of holders'
per-output bech32 parse. Traps out-of-fuel even at the minimum page (64).
Reverted (was uncommitted).

**Findings from the failure:**
1. The *original* `collection-holders` ALSO traps on a large collection
   (Nikeverse 5554) — its last-page whole-ledger `persist_ledger` + clone +
   open-emit is the same single-call cost that broke metadata, just at a higher
   threshold. So holders was never safe at NIKEPIG/Nikeverse scale.
2. A trapping `companion=*` holders recapture **cross-contaminated** a dev DO:
   IslaNOVA's `collection-ownership` DO got Nikeverse's holdings. Metadata's
   recapture didn't contaminate → specific to the trap/re-instantiate-retry path
   interacting with per-companion interest filtering. **Independent bug; must be
   root-caused before relying on recapture at scale** (see §Recapture bug).

## Scale analysis (the 420K math)

For ~420K entries:

- **Resident-ledger hybrid is ruled out.** A 420K-entry ledger is ~30–200 MB of
  wasm linear memory; the original NIKEPIG incident was a `cabi_realloc`
  **memory** trap. Anything resident across calls dies here. → sharding (nothing
  resident) is the only viable model.
- **Current emit is O(n²).** `step_emit` calls `shard_list(prefix)` every chunk
  and indexes by offset: 420K keys × 420 chunks ≈ **176M list-ops**, and the key
  list is ~12 MB to materialize. (Also affects metadata — the 5554 fix wouldn't
  reach 420K.) → emit must **page** through keys, not list-all-per-chunk.
- **Per-entry write/read cost.** 420K `Durability::Immediate` writes = 420K
  fsyncs; SUM adds a get per entry. → **batch** per page (one txn per page).

Target shape at 420K: ~420 scan rounds + ~420 emit rounds (chunk≈1000),
~40–200 MB state-kv, one fsync per page (~840 total), no resident ledger, no
O(n²). A multi-minute one-time recapture that **completes**.

## Design

### 1. Host-fn enhancements (platform / WIT)

Add to the v2 `state-kv` interface (`wit-v2/world.wit`), implemented in
`src/host_fns_v2/state_kv.rs` for both `ModuleKv` arms (Redb delegates to redb
txns; InMemory mirrors with sorted semantics for golden parity). All are
additive — existing modules unaffected.

```wit
interface state-kv {
    get-value: func(key: string) -> option<list<u8>>;
    set-value: func(key: string, value: list<u8>);
    delete-value: func(key: string);
    list-values: func(prefix: string) -> list<string>;          // existing

    // --- new, for sharded state at scale ---

    /// Paginated (key, value) range scan over `prefix`, keys sorted,
    /// `after` exclusive (None ⇒ from the start), up to `limit`.
    /// Returns values too (redb range yields k+v together) so the
    /// emit needs no follow-up gets. THE load-bearing primitive —
    /// lets emit page through shards O(n) total, nothing resident.
    kv-scan: func(prefix: string, after: option<string>, limit: u32)
        -> list<tuple<string, list<u8>>>;

    /// Batched random-access get — one read txn for many keys.
    /// Used by the SUM merge to read a page's existing shards at once.
    /// Result is parallel to `keys` (None ⇒ absent).
    get-many: func(keys: list<string>) -> list<option<list<u8>>>;

    /// Batched write — one write txn (one fsync) for a page's shards.
    /// Replaces N per-entry `set-value` fsyncs.
    set-many: func(entries: list<tuple<string, list<u8>>>);

    /// Bulk delete by prefix — one txn. Replaces list+delete loops
    /// for `shard_clear`.
    delete-prefix: func(prefix: string);
}
```

**Durability decision (settled):** keep `Durability::Immediate`, but at the
**batched-txn** grain — `set-many` commits one txn (one fsync) per page. ~840
fsyncs for a 420K bootstrap is fine, and per-page commits stay crash-safe (a
crash mid-bootstrap re-runs the round; the driver's `shard_clear` on Scan
re-entry makes SUM re-scan idempotent). A separate "deferred durability for the
bulk burst" mode is **not needed for v1** — revisit only if per-page fsync ever
shows up as the bottleneck.

Host impl notes:
- `kv-scan`: redb `table.range(key_for_module(mid, prefix+after_excl)..)`, stop
  at first key not matching `prefix`, take `limit`, strip the `{mid}-` namespace.
  `after` exclusive: seek to `> after`. InMemory: sorted filter + slice.
- `get-many` / `set-many`: one `begin_read` / `begin_write`+`commit` over the
  key list. `delete-prefix`: range-scan keys under prefix, remove, one commit.

### 2. Kit driver changes (`ChunkedBootstrap`)

**`BootstrapCursor`** — replace `emit_offset: u64` with a **key cursor**
`emit_after: Option<Vec<u8>>` (the last entry-key emitted). Robust to concurrent
live-tail shard mutations during a long emit (offsets shift; a key-cursor
doesn't skip/dup). Encoding: version + phase + predicate_idx + anchor_slot +
length-prefixed `emit_after`.

**`BootstrapIo`** — page-oriented (the kit calls these; modules implement via the
new host-fns):

```rust
pub trait BootstrapIo {
    type Entry;
    fn scan_page(&mut self, predicate: &[u8], after: Option<&[u8]>) -> ScannedPage<Self::Entry>;

    // batched shard IO
    fn shard_get_many(&self, predicate: &[u8], keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>>;
    fn shard_put_many(&mut self, predicate: &[u8], entries: &[(Vec<u8>, Vec<u8>)]);
    fn shard_clear(&mut self, predicate: &[u8]);                       // → delete-prefix
    /// Paginated (entry_key, entry_bytes) scan for the emit phase.
    fn shard_scan(&self, predicate: &[u8], after: Option<&[u8]>, limit: usize)
        -> Vec<(Vec<u8>, Vec<u8>)>;                                    // → kv-scan

    fn encode_entry(&self, e: &Self::Entry) -> Vec<u8>;
    fn decode_entry(&self, b: &[u8]) -> Option<Self::Entry>;
    fn accumulates(&self) -> bool { false }
    fn merge(&self, prior: Option<Self::Entry>, incoming: Self::Entry) -> Self::Entry { incoming }

    fn load_cursor(&self) -> Option<Vec<u8>>;
    fn save_cursor(&mut self, bytes: &[u8]);
    fn clear_cursor(&mut self);

    fn emit_begin(&mut self, predicate: &[u8], anchor_slot: u64);
    fn emit_chunk(&mut self, predicate: &[u8], entries: Vec<Self::Entry>);
    fn emit_end(&mut self, predicate: &[u8], total: u64);
    fn chunk_size(&self) -> usize;
}
```

**`step` Scan phase** (per page, batched):
1. On Scan (re)entry with `after == None`: `shard_clear(predicate)` (SUM
   re-scan idempotency).
2. `scan_page` → `(entry_key, entry)` pairs (the module already sums within-page
   dups in its fold).
3. If `accumulates`: `shard_get_many(keys)` → `merge` each with prior → collect
   `(key, encode(merged))`. Else: collect `(key, encode(entry))` directly.
4. `shard_put_many(pairs)` — one txn.
5. Save cursor; more pages ⇒ continue, last page ⇒ flip to Emit + `emit_begin`.

So one batched read (SUM only) + one batched write per page. No per-entry fsync,
no per-entry get.

**`step` Emit phase** (per chunk, paged):
1. `shard_scan(predicate, emit_after, chunk_size)` → `(key, value)` pairs.
2. Empty ⇒ `emit_end` + advance predicate. Else: decode values → `emit_chunk`;
   set `emit_after = last key`; save cursor.

No `shard_list`, no per-entry get in emit. O(n) total, bounded per call.

**Snapshot consistency note:** at 420K the emit spans minutes; live-tail TXs may
mutate shards mid-emit. The snapshot is then eventually-consistent (some entries
pre-TX, some post). Acceptable for the wipe-and-replace contract — live deltas
after `SnapshotEnd` reconcile. Document for consumers; don't try to freeze.

`entry_count` for `SnapshotEnd`: count is no longer free (no full list). Either
(a) track a running count in the cursor as the scan shards entries, or (b) accept
`SnapshotEnd.entry_count` is best-effort and have the consumer not depend on it
(it already doesn't for correctness — it wipes on Begin). Prefer (a): add
`shard_count: u64` to the cursor, bumped per `shard_put_many` (net of merges is
fuzzy for SUM — use a `delete-prefix`-then-count or accept approximate). Decide
during build; lean (b) + approximate to avoid a count pass.

### 3. Per-module mapping

- **collection-metadata (replace):** `accumulates=false`; scan shards via
  `shard_put_many`; emit via `shard_scan`. Re-migrate to the page-oriented IO —
  net simpler, and it now scales past tens-of-thousands. Goldens must stay green.
- **collection-holders (SUM):** `accumulates=true`, `merge` sums `Holding.qty`;
  entry_key = CBOR `(HolderKey, asset_name)`; entry = `Holding`. scan_page folds
  the page (within-page SUM) then returns pairs; driver batches the cross-page
  SUM. Live-tail (`flush_buffer`) moves to `get-many`/`set-many` per TX's touched
  keys.
- **holder-distribution (SUM + decomposition):** **DEFERRED — fully specified
  in "SB5 — Plan A" below.** Raw ledger shards like holders (SUM); LP-token +
  vesting decomposition transform the emitted list via a new kit `decomp_step`
  hook (`Phase::Decomp`) writing a second `decomp:` shard prefix the emit pages.
  The most complex module (financial, no goldens) — parked to keep focus on the
  CNFT path; see the Plan A section for the complete pickup.

### 4. Consumer-side scaling (not just mitos)

A 420K snapshot is 420K events to the consumer. For `collection-ownership`:
- Apply is already per-chunk (one POST per `SnapshotChunk`) → ~420 POSTs. OK.
- DO SQLite holds 420K `ownership` rows — within budget, but confirm the
  `SnapshotBegin` wipe + chunked INSERT path and the `total_asset_count` /
  `COUNT(*)` reads don't choke. Bundle reads are already keyset-paginated.
- The queue `StatsUpdate` / `MetadataFinalise` debounce handles many applies →
  one finalise; confirm finalise (reconcile_traits + rarity) is itself bounded
  or chunked at 420K (rarity scoring over 420K tokens is heavy — may need its
  own chunking; out of scope here but flag it).
- **Product question for CNT-scale (Hosky):** is a full per-holder snapshot the
  right shape, or does the consumer want top-N / aggregates? Decide before
  shipping a 420K holder-distribution path — it may change the wire contract.

### 5. Recapture cross-contamination bug (FIXED — both layers, 2026-05-23)

**Status update:** both layers below are implemented + tested. Summary
of what shipped:

- **Consumer guard (SB6a, `cnft.dev-workers`):** `handle_configure`
  mirrors the DO's `policy_id` into `collection_meta`; the
  `CollectionHoldersChannel` + `CollectionMetadataChannel`
  `apply_event` now compare the event's `policy` to that and skip on
  mismatch (`policy_matches`). Fails open if no `policy_id` row yet
  (pre-guard DOs) — reconfigure populates it. Un-breaks dev
  immediately, independent of the host.
- **Host filter (SB6b, `mitos`):** `host_v2::drain_one` now applies a
  per-companion interest filter at fan-out. Collection modules emit
  with `partition_key = policy hex bytes` (`emit_event_keyed`);
  `drain_one` reads each companion's persisted `SubscribeRequest.
  interests` (cached by file mtime via `InterestCache`), projects to
  `watched_policies`, and only appends the row for companions whose
  bounded interest covers the event's policy. **Fail-open semantics:**
  empty/`Any` interest, a keyless event, or any read/decode error all
  resolve to "deliver" — the filter is never *narrower* than the
  pre-fix broadcast. Sentinel fallback now fires only when **no**
  companion is subscribed (so an all-filtered event isn't sentineled
  and later wrongly reclaimed by an unrelated subscriber). 7 unit
  tests (`interest_filter_tests`); the 3 v2 dispatch/bootstrap
  integration tests (broken since the multi-client-companions layout
  migration — they wrote flat companion files the two-level walk
  skips) restored via `common::write_companion_subscription`. 47
  goldens green (partition_key isn't part of the decoded payload).

Original root-cause analysis retained below.

#### Original analysis (ROOT-CAUSED — host dialer)

**Confirmed 2026-05-22 (after SB4 holders migration).** The contamination is
**NOT trap-induced** — it persists on a *clean, no-trap* `companion=*` recapture
of `collection-holders` with two tracked policies (IslaNOVA `43a056…` ~1454
holdings, Nikeverse `de79250…` 5554). After the recapture **both** DOs report
Nikeverse's numbers (5554/433) and IslaNOVA's DO holds `Nikeverse0001…`. The
worker tail showed only `SnapshotEnd policy=de79250` (×2 — once per companion),
no IslaNOVA SnapshotEnd delivered.

**Root cause:** the recapture's snapshot emissions are **broadcast to every
subscribed companion of the module, not interest-filtered per companion.** The
module emits each tracked policy's `SnapshotBegin→Chunk→End` in sorted predicate
order (IslaNOVA first, Nikeverse second). Each companion receives *both*
policies' sequences; the consumer's `SnapshotBegin` handler wipes its DO's
`ownership` table unconditionally (it's per-policy, so it doesn't check the
event's `policy` against its own), so the **last-emitted policy's SnapshotBegin
wipes + overwrites** whatever the companion had — leaving every DO with the
last predicate's data. Metadata "didn't contaminate" only because IslaNOVA has
~no CIP-68 (its metadata snapshot was empty), so the overwrite was invisible.

**Two-layer fix:**
1. **Host (proper):** `mitos_platform::dialer` / the recapture emission path must
   apply each companion's interest filter to snapshot emissions, so a companion
   only receives the policies it's interested in. Look at how `update-interest`
   policy filters are (not) applied to the `rebootstrap` broadcast.
2. **Consumer (defense-in-depth, cheap):** the worker's `CollectionHoldersChannel`
   (and `CollectionMetadataChannel`) should compare the event's `policy` to the
   DO's configured policy and **skip** (don't wipe/apply) on mismatch. This
   alone stops contamination at the consumer regardless of the host bug, and is
   a small change in `cnft.dev-workers .../ownership_do/mitos.rs`. Recommended to
   land first (un-breaks dev immediately), with the host fix as the real fix.

**Critical at scale:** a 420K recapture with multiple tracked policies would
mis-route massively. Fix before any multi-policy recapture at scale.

### 6. Dev recovery (immediate, separate from the design)

dev `collection-ownership` DOs are dirty after the churn (IslaNOVA contaminated
with Nikeverse holdings; Nikeverse holders refilled via a trapping recapture).
Recovery once the SUM work lands (or before, to unblock): reset the affected DOs
(`POST .../_internal/reset` via the worker) + re-wake each individually so each
cold-starts its own policy; or untrack Nikeverse from holders so a clean
IslaNOVA-only recapture runs. Note the *original* holders traps on Nikeverse, so
Nikeverse holders won't refill cleanly until this work ships.

## Phasing

1. **Host-fns** — `kv-scan` / `get-many` / `set-many` / `delete-prefix` in WIT +
   host (both arms) + tests. (`mitos-platform` builds locally;
   `nix develop -c cargo check -p mitos-platform`.)
2. **Kit driver** — page-oriented `BootstrapIo`, key-cursor emit, batched scan,
   `decomp_step` hook stub. Unit tests incl. a simulated 100k-entry mock run +
   re-instantiation mid-emit (key-cursor resume).
3. **collection-metadata** — re-migrate to the page-oriented IO; goldens green;
   re-deploy + re-verify Nikeverse (regression guard).
4. **collection-holders** — SUM migration on the new batched path; goldens +
   a large-collection live recapture (Nikeverse) that **completes**.
5. **holder-distribution** — SUM + decomposition (`decomp_step`); add goldens;
   real pool recapture diff. **DEFERRED 2026-05-23 — see "SB5 — Plan A" below
   for the complete spec.** (CNT side; parked to keep focus on CNFTs.)
6. **Recapture contamination** root-cause + fix — ✅ DONE (§5). Both
   layers shipped + tested; pending host+module deploy and a live
   multi-policy recapture verification on dev (SB7).
7. **Dev recovery** + a 420K-class soak test (if a real ~420K policy is
   available on dev) to validate the scale target end-to-end, consumer included.

## Acceptance criteria

- A ~420K-entry policy bootstraps to **completion** via recapture — no trap, no
  `SnapshotBegin` loop, bounded fuel + memory per call (check host logs for
  zero `out-of-fuel` / `cabi_realloc`).
- Emit is O(n) (paged), scan write is one txn/page, SUM is correct across pages
  (verify a known holder's summed balance).
- Golden parity for all three modules (wire emissions unchanged).
- Recapture delivers each policy's data only to its own companion (no
  cross-contamination), including under trap-retry.
- Consumer (collection-ownership) absorbs 420K rows and serves bundle/stats
  without choking; finalise (traits/rarity) bounded or chunked.

## Verification tooling (local)

- Build one module to wasm: `nix develop -c cargo build -p mitos-build` then
  `nix develop -c ./target/debug/mitos-build --module <path>.rs`.
- Golden tests: `nix develop -c cargo build --release --bin mitos-run` then
  `./scripts/run-golden-tests.sh` (`UPDATE_GOLDEN=1 …` to regenerate).
- Kit unit tests: `nix develop -c cargo test -p mitos-module-kit`.
- Host: `nix develop -c cargo check -p mitos-platform`.
- Deploy: `MITOS_HOST=root@<box> ./scripts/deploy.sh` (rebuilds changed module
  wasm on the box; the host change rebuilds `mitos`). Recapture:
  `POST https://mitos.defrag.cc/_admin/modules/<id>/recapture {"companion":"*"}`
  (Bearer `MITOS_AUTH_TOKEN` from the box's `/etc/default/mitos-mainnet`).

## SB5 — Plan A: full holder-distribution migration (SHIPPED 2026-05-23)

**Status: SHIPPED + verified on prod (build `76e1d205e916`).** Implemented
exactly as specified below: kit `Phase::Decomp` + `decomp_step` (no-op
default), `meta:` side-state shard, `decomp:` prefix, paged materialise,
sharded SUM raw ledger + sharded deltas; vesting-tracker migrated alongside
(REPLACE, sort eliminated). 3 new holder-distribution goldens authored
first (raw / LP-pool / vesting) — all green, byte-identical pre/post; 51
goldens total. Post-deploy: a `holder-distribution` recapture ran the new
sharded + decomp path on **real Aliens data (CSwap pool + CrowdLock
vesting), 11430 UTxOs, no trap**, all 4 epochify companions drained clean —
the real-on-chain exercise the synthetic goldens couldn't give. A
`collection-holders` recapture confirmed the kit `Phase::Decomp` change is
neutral for the CNFT path. The original "why it's hard / why deferred"
analysis is retained below as the audit trail.

### Original deferral analysis (resolved by the shipped work)

**Was parked 2026-05-23** (during the B1/B2 dialer-scaling work) because at
that point: the shipped driver only covered a straight `Scan → Emit`
module; holder-distribution needed the net-new kit `Phase::Decomp` /
`decomp_step` hook, a `meta:` side-state shard, the `decomp:` prefix, **and**
3 goldens that didn't exist (financial output had zero regression net).
Combined with the live evidence that there was **no scale pressure** — the
only consumer (epochify, `hooks.epochify.space` + dev) tracks 2 policies at
~33 emissions each, for which the resident-ledger model is entirely fine —
the call at the time was to
fold this into the dedicated fuel-exhaustion sweep as the documented
headline resident-ledger risk, and pick up this spec when a real
large-CNT (Hosky-class) consumer materialises. Don't re-litigate "is it a
quick port" — it isn't; the answer is captured here.

**Why it's the hard one.** Unlike the other two modules, it applies two
**financial** transforms *between* scan and emit — and the kit's
`ChunkedBootstrap` is a straight `Scan → Emit` machine with no hook in
between. It also has **no goldens** (no regression net for financial
output). Getting the redistribution wrong silently corrupts holder
analytics. Treat with the care the rest of this doc's modules didn't
need.

### Current code (the reality to migrate)

`community-modules/holder-distribution/holder_distribution.rs`:
- **Raw ledger:** `PolicyLedger { holders: BTreeMap<LedgerKey, AssetMap> }`
  where `LedgerKey = Stake([u8;28]) | Enterprise(String)` and
  `AssetMap = BTreeMap<asset_name_bytes, u64>`. Persisted as a single
  CBOR blob `ledger:<policy_hex>` via `persist_ledger`/`load_ledger` —
  **this whole-blob serialize is the trap** (same shape collection-holders
  had pre-SB4).
- **Cold-start** (`cold_start`, one host call): paged `utxos_by_policy`
  scan → `fold_page` into the resident ledger, collecting two side
  outputs — `pool_ref` (auto-discovered DEX pool UTxO, address ==
  `cswap::POOL_SCRIPT_ADDR`) and `vesting_lock_refs` (CrowdLock locks,
  which **bypass** the ledger). Then `persist_ledger` (raw) →
  `build_decomposed_holders` → `emit_full_snapshot`.
- **Recapture** (`rebootstrap`, re-entrant): a 3-thread-local state
  machine — `REBOOTSTRAP_STATE` (`ReentrantRound` scan) →
  `REBOOTSTRAP_DECOMP` (`DecompState`, paged LP-token scan) →
  `REBOOTSTRAP_EMIT` (`EmitState`, resident `Vec<HolderEntry>` drained
  one `SnapshotChunk`/call). The durable cursor is only the
  `predicate_idx` (`KV_REBOOTSTRAP_CURSOR`).
- **LP decomposition** (`decompose_or_plain` → `read_pool_datum` /
  `fold_lp_page` / `decompose_holders`): read the CSwap pool datum
  (`total_lp_tokens`, `lp_policy`); paged scan the **LP-token** policy,
  summing each holder's LP qty (`fold_lp_page`) — with CSwap **farm**
  staking-datum resolution (staked LP sits at one farm address; the
  staker comes from the farm UTxO's datum); then redistribute the pool's
  aggregate to LP providers proportional to `lp_share(lp_qty, reserve,
  total_lp)`, recording each provider's `lp_amount`, with the rounding
  remainder kept as a **residual pool entry**. Raw ledger untouched.
- **Vesting decomposition** (`decompose_vesting` → `attach_vests`): read
  each CrowdLock lock datum, resolve owner pkh → stake cred
  (`resolve_stake_for_payment_pkh`), build per-owner `Vec<LockEntry>`,
  attach to holders (creating vest-only `HolderEntry`s for owners with
  no liquid holdings).
- **Live tail** (`flush_buffer`): `load_ledger` → apply per-TX
  produced/consumed deltas (`ledger_add`/`ledger_sub`) → `persist_ledger`
  → emit `HolderEvent::Delta`. **Note:** deltas account against the RAW
  ledger; the emitted delta is currently *not* re-decomposed (a known
  simplification — LP/vest re-attribution happens at the next recapture).
- chunk size `SNAPSHOT_CHUNK_HOLDERS = 1000`.

### Target design (Plan A)

**1. Raw ledger → sharded SUM** (mirrors collection-holders SB4):
- `accumulates = true`. Shard granularity = **per holder**: `entry_key =
  encode(LedgerKey)`, `entry = AssetMap`. `merge` sums two `AssetMap`s
  per asset (`saturating_add`). `scan_page` folds a page into per-holder
  `AssetMap`s (within-page SUM); the driver does the cross-page SUM via
  `shard_get_many` + `merge` + `shard_put_many` (one txn/page). Prefix
  `ledger:<policy_hex>:<holder_key>`.
- This alone fixes every **no-pool, no-vesting** policy (NFT collections
  like NIKEPIG) — they take the plain `Scan → Emit` path, decomp is a
  no-op.

**2. Scan side-state → a meta shard** (no resident accumulator exists in
the kit). `scan_page` is `&mut self`, so it writes the side outputs to a
per-predicate **meta shard** as it pages: `pool_ref` (set once) and
`vesting_lock_refs` (appended). Key e.g. `meta:<policy_hex>`. The decomp
phase reads it. (Alternatively, dedicated `pool:`/`vest:` keys.)

**3. NEW kit hook — `Phase::Decomp` + `BootstrapIo::decomp_step`**
(`crates/mitos-module-kit/src/lib.rs`):
- Add `Phase::Decomp` between `Scan` and `Emit`. After the last scan
  page, `step_scan` flips to `Decomp` (not directly `Emit`).
- `BootstrapIo::decomp_step(&mut self, predicate, anchor_slot) ->
  DecompOutcome { done: bool, ingested: u64 }` — **re-entrant**, called
  once per `step` until `done`, then the driver flips to `Emit`.
  **Default impl returns `{ done: true }` immediately** (no-op for
  collection-metadata / collection-holders — zero behaviour change for
  them; their goldens stay green).
- The decomp sub-cursor (LP-scan `after`, sub-phase) lives in the
  module's meta shard (durable) so a trap mid-decomp resumes — the kit
  cursor only needs a `Phase::Decomp` marker; OR extend `BootstrapCursor`
  with an opaque `decomp_cursor: Option<Vec<u8>>` the module owns. Prefer
  the latter (keeps decomp progress in the one durable cursor).

**4. Sharded decomposition** (the decomp_step body, re-entrant):
- Read the meta shard → `pool_ref`, `vesting_lock_refs`. No pool & no
  vests ⇒ `done: true` immediately (NFT fast path; emit reads raw shards
  directly — see note in step 5).
- If pool: read pool datum (`total_lp_tokens`, `lp_policy`); paged
  LP-token scan, one page per `decomp_step` call, folding into a
  **resident** `lp_holders: BTreeMap<LedgerKey, u64>` (bounded by
  LP-provider count — hundreds–thousands, resident is fine; shard only if
  a pool ever has >~100k providers, which doesn't happen). Reuse
  `fold_lp_page` (incl. farm staking-datum resolution) verbatim.
- Vesting: `decompose_vesting` (resident `BTreeMap<owner, Vec<LockEntry>>`,
  bounded by vesting-owner count).
- **Materialise the decomposed set into a second shard prefix**
  `decomp:<policy_hex>:<holder_key>`, paging the raw `ledger:` shards: for
  each raw holder, copy assets, add their LP share if they're an LP
  provider (set `lp_amount`), attach vests if they're an owner; **drop**
  the pool's entry, writing the residual as the pool's `decomp:` entry;
  finally write `decomp:` entries for vest-only / LP-only owners not in
  the raw ledger. This pass is itself paged (re-entrant) so it's bounded
  at 420K. `lp_share` math + residual handling come straight from
  `decompose_holders`.

**5. Emit pages the decomposed prefix.** `shard_scan` in `Phase::Emit`
reads `decomp:<policy_hex>:*` (the full final holder set, incl. residual
pool + vest-only owners). After `emit_end`, **clear** the `decomp:`
shards (snapshot-only; the raw `ledger:` shards persist for deltas).
*Optimisation:* when decomp_step found no pool & no vests, skip writing
`decomp:` shards and have emit read `ledger:` directly (saves a full
copy for the NFT case) — gate on a flag in the meta shard / cursor.

**6. Live tail** (`flush_buffer`): `load_ledger` → `shard_get_many` /
`shard_put_many` on the TX's `touched` holder set (already tracked) —
exactly the collection-holders SB4 change. Keep the existing
"deltas account against the raw ledger, re-decompose at next recapture"
simplification unless a consumer needs live-decomposed deltas (flag as a
separate decision).

### Goldens first (mandatory — none exist today)

Author under `community-modules/holder-distribution/tests/fixtures/<scenario>/`
and capture with `UPDATE_GOLDEN=1 ./scripts/run-golden-tests.sh` BEFORE
migrating, then keep green through it:
1. **raw / NFT** (the NIKEPIG shape) — a policy held by several stake
   creds, no pool, no vesting. Reuse `tests/fixtures/186000000.block.cbor`
   (pick its highest-frequency policy). Proves the plain SUM path.
2. **LP-pool** — a fixture with a CSwap pool UTxO + LP-token holders
   (incl. a farm-staked one) so `decompose_holders` + `fold_lp_page` are
   exercised. This is the load-bearing financial golden.
3. **vesting** — reuse/adapt the `vesting-tracker/aliens-crowdlock`
   block fixtures (CrowdLock locks) so `decompose_vesting` + `attach_vests`
   are covered.

### Phasing (pickup)

1. Goldens (3 cases above) capturing **current** behaviour.
2. Kit: `Phase::Decomp` + `decomp_step` (default no-op) + `BootstrapCursor`
   decomp-cursor field + driver wiring + a mock-IO unit test (re-entrant
   decomp, re-instantiation mid-decomp resumes). collection-metadata /
   collection-holders unaffected (default no-op) — their goldens prove it.
3. Module: raw-ledger sharding (SUM) + meta shard + `decomp_step` (LP +
   vesting → `decomp:` shards) + emit-from-`decomp:` + flush get/set.
   Replace the 3 thread-locals with the kit driver (keep `TRACKED_POLICIES`).
4. Goldens green; deploy; **diff a real pool recapture pre/post** (a
   known CSwap-pooled token — verify a known LP provider's `lp_amount`
   and a known vester's `vests` match the pre-migration output exactly).

### Risk callouts

- **Financial correctness** is the whole game — the pre/post real-recapture
  diff (step 4) is non-negotiable, not just golden parity.
- The decomp `decomp:`-shard copy doubles transient kv for a pool policy
  during recapture (raw + decomp). Fine at current scale; at 420K it's
  ~2× the kv but still bounded + cleared post-emit.
- `lp_holders` / `vests` resident maps are bounded by provider/owner
  counts (small); only shard them if a real pool ever proves otherwise.
- **CNT product question (still open, from §4):** at Hosky-CNT scale, is a
  full per-holder snapshot even the right wire shape vs. top-N/aggregates?
  Decide before shipping a 420K *holder-distribution* path — it may change
  the wire contract and moot some of the above.
