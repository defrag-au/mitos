# Holder-distribution: the composed snapshot authority

**Status: design draft** (2026-05-19, rev 3). This doc began as
the LP-pool decomposition design; rev 3 broadens it — after the
LP slice shipped — into the full model for `holder-distribution`
as the *single, internally-consistent holder-snapshot authority*
for the holder-map: LP **and** vesting decomposition, burn
classification, and the module/worker split that keeps each
piece of work where its data lives.

> The filename still says `LP_DECOMPOSITION`; the doc outgrew it.
> Kept to avoid breaking cross-references.

Cross-references:
- `HOLDER_DISTRIBUTION_MODULE.md` — the module this extends.
- `cnft.dev-workers/docs/design/HOLDER_MAP_MITOS_CUTOVER_PLAN.md`
  — this re-scopes that plan's Phases 2–4 (see *Cutover plan
  reconciliation*).
- `WASM_BUDGET_CHUNKING.md` — the paged `chain-data` host-fns +
  re-entrant `rebootstrap` this builds on.
- `VESTING_TRACKER_MODULE.md`, `DEX_COMMUNITY_MODULES.md` — the
  per-domain modules whose decode logic + event types this
  composes.

## Context

The holder-map dApp renders a distribution donut with bands:
**Circulating**, **Vested**, **Treasury**, **Mothership** (a DEX
pool's retained holding), **Scout Vessels** (LP redistributed to
providers), **Burned**.

`holder-distribution` scans `utxos-by-policy(X)` — which returns
*every* current UTxO holding the policy: plain wallets, DEX pool
UTxOs, vesting-contract lock UTxOs, burn sinks. So the module
already ingests every band's raw material. The job of this
design is to turn that raw scan into a fully-attributed snapshot
in one place, rather than emitting a partial snapshot and making
the consumer reassemble the bands from joined event streams.

**The LP slice has shipped** — `mitos-dex-decode` crate, the
LP-decomposition phase in `holder-distribution`, deployed and
verified against prod (60/64 holders exact-match). This doc
records that as slice 1 and designs the rest.

## Principle 1 — one module, one snapshot

A holder snapshot's bands are only meaningful *against the same
wallet set, at the same anchor slot*. Splitting them across
modules forces the consumer to join async event streams with
independent anchors and recapture timing — fragile, and it
pushes real logic into a "dumb consumer." So the **whole
snapshot** — every band — is produced by `holder-distribution`
in the one cold-start / `rebootstrap` scan.

The per-domain modules (`cswap-dex`, `splash-dex`,
`vesting-tracker`, `burn-address`) are **not** retired: they keep
emitting **live delta events** (a trade, a lock, a burn as it
happens) for the trade feed and other consumers. What this
design moves into `holder-distribution` is the **snapshot** — the
resting, fully-attributed holder state.

## Principle 2 — decomposition in the module, classification in the worker

The new, load-bearing principle. Two kinds of work turn a raw
scan into bands:

- **Decomposition** — redistributing a contract's aggregate
  holding to its beneficial owners (a DEX pool → LP providers; a
  vesting contract → vest owners). This needs **chain data**:
  pool datums, LP-token enumeration, vesting datums, owner
  resolution. Only the module has chain access. → **module.**
- **Classification against project config** — "is this holder a
  burn sink? a treasury wallet? a known entity?" This needs only
  the holder's identity and the project's config. The holder-map
  worker already holds project config and already runs a
  `classify()`. No chain data required. → **worker.**

This cleanly explains the whole split: LP and vesting are
decompositions → module; burn is a classification → worker.

## The holder identity — generalised

Today `HolderEntry` identifies a holder by
`stake_cred_hex: Option<String>`, where `None` is a single
bucket aggregating *all* enterprise (no-stake-credential)
outputs — which the consumer then drops on the floor.

That collapse is wrong for this design in two ways:

1. A `/dev/null` burn sink (e.g. `$burnsnek`,
   `addr1w8qmxk…`) is an **enterprise address**. Collapsed into
   `None` and dropped, it never reaches the worker — so the
   worker cannot classify it as a burn.
2. Real holders *do* use enterprise addresses. Collapsing them
   makes them invisible in the snapshot — a latent
   under-counting bug, independent of burns.

So the holder identity generalises to surface every holder
distinctly:

```rust
enum HolderId {
    /// Delegated address — grouped by its 28-byte stake
    /// credential (56-char hex). The dominant case.
    Stake(String),
    /// Enterprise (no-stake) address — the full bech32. These
    /// share no stake-cred grouping, so each is its own holder.
    Enterprise(String),
}
```

`holder-distribution`'s in-memory ledger key generalises to
match (stake-cred bytes vs payment-cred bytes / address). The
`None` aggregate goes away.

## The `HolderEntry` shape

```rust
struct HolderEntry {
    id: HolderId,
    /// Liquid holdings of the tracked policy. Vested tokens are
    /// NOT folded in here — see `vests`.
    assets: Vec<AssetBalance>,
    /// The module's best knowledge of what this credential is,
    /// from the contracts it recognised while decomposing.
    role: HolderRole,
    /// LP-decomposition attribution — the portion of `assets`
    /// redistributed from a DEX pool (the Scout Vessels band).
    /// `0` for a non-LP holder. Already included in `assets`.
    lp_amount: u64,
    /// Vesting decomposition — the lock positions beneficially
    /// owned by this holder (the Vested band). Typed with
    /// `vesting-tracker`'s own `LockEntry` — the detail layer
    /// reuses the per-module vocabulary rather than inventing a
    /// parallel shape. NOT included in `assets`; the holder's
    /// true total is `assets + Σ vests.amount`.
    vests: Vec<LockEntry>,
}

enum HolderRole {
    /// A plain holder. The default; also what an enterprise
    /// holder gets until the worker classifies it.
    Wallet,
    /// A DEX pool the module recognised — either the residual
    /// after decomposition, or a pool kind it can't decompose
    /// yet (Splash before Splash support lands).
    DexPool,
    /// A vesting contract — the residual after decomposition.
    VestingContract,
}
```

Note the **detail layer reuses the per-module types** — `vests`
is `Vec<crate::vesting_tracker::LockEntry>`, not a re-declared
shape. Because every per-domain event type already lives as a
submodule of the one `mitos-community-events` crate, this reuse
is free: `use crate::vesting_tracker::LockEntry`. LP detail stays
a bare `lp_amount: u64` for now — unlike vesting's `LockEntry`,
the `dex` module has no resting "wallet's LP position" type to
reuse (its types are trade/liquidity *events*). LP detail could
grow to per-pool later; see *Open questions*.

`lp_amount` is part of `assets` (LP tokens are a live claim on
the pool reserve); `vests` is *separate* from `assets` (locked
tokens can't be moved — `assets` stays liquid-only). This
matches the legacy model — see cutover plan Phase 4, "`amount`
stays liquid-only; `amount + Σvests` is the correct total."

## Slice 1 — LP decomposition (shipped)

`holder-distribution` **auto-discovers** the DEX pool: the pool
UTxO is just a holder of the policy whose address is a known DEX
pool script (`mitos-dex-decode::cswap::POOL_SCRIPT_ADDR`). No
consumer config. During the holder scan it flags that UTxO; a
re-entrant decomposition phase then reads the pool datum
(`total_lp_tokens`, `lp_policy`), scans the LP-token holder set
(`utxos-by-policy(lp_policy)` — unstaked LP at wallets, staked LP
at the farm with the staker recovered from the staking datum),
and redistributes the pool's reserve proportionally:
`share = lp_held × reserve / total_lp_supply`. The pool
aggregate is dropped; each provider's `assets` gains `share` and
records it as `lp_amount`; the rounding remainder stays as a
residual `DexPool` entry.

The decode logic lives in the shared `mitos-dex-decode` crate
(CSwap pool/staking datum + script-address constants), extracted
from `cswap-dex` so both modules share one source of truth.
Splash decomposition follows once Splash's decoder + addresses
land in `mitos-dex-decode::splash`; until then a Splash pool is
recognised by address and tagged `DexPool` but not decomposed
(it shows as Mothership, undecomposed — acceptable, matches
legacy).

Verified against prod: the decomposition is numerically correct
(per-wallet amounts match the legacy Maestro reconciler to the
lovelace; the model is identical to `reconcile_lp_staking`).

## Slice 2 — vesting decomposition

Structurally identical to LP: a vesting contract holds the
policy on behalf of vest owners; decomposition redistributes it.

1. **Recognise the vesting contracts.** Shield project vests sit
   at one fixed contract address; CrowdLock user vests sit at a
   shared payment script with varying staking parts (prefix-match
   the payment credential). These addresses are platform
   constants — owned by a **shared vesting-decode crate**,
   extracted from `vesting-tracker` exactly as `mitos-dex-decode`
   was extracted from `cswap-dex`. Both modules then share it.
2. **Decode each lock UTxO's datum** → the owner. Shield/CrowdLock
   datums carry an owner PKH; the owner's stake credential is
   recovered via the `resolve-stake-for-payment-pkh` chain-data
   host-fn (already exists — `vesting-tracker` uses it).
3. **Decompose.** Drop the vesting contract's aggregate holder;
   each owner's `HolderEntry` gains a `LockEntry` in `vests`.
   `assets` is *not* touched (locked tokens stay out of the
   liquid balance). Lock UTxOs whose datum doesn't decode, or
   whose owner doesn't resolve, stay as a residual
   `VestingContract` entry.
4. The **Vested band** is `Σ vests.amount` across all holders.

This is a second re-entrant phase in the `rebootstrap` state
machine, alongside the LP phase, for the same fuel-budget
reasons (see `WASM_BUDGET_CHUNKING.md`).

It **dissolves the cutover plan's Phase-4 open question** — there
is no separate vesting stream to join and no `token_holders`
lock-row suppression problem, because the locked tokens never
enter `assets` in the first place.

## Slice 3 — burn classification (worker-side)

A burn is **not a decomposition** — the tokens are gone, owned
by nobody; there is nothing to redistribute. It is a pure
*classification*: "this holder's address is one the project
flagged as a sink → the Burned band." And burn sinks are
**per-project config** — `$burnsnek` is an address Aliens
designates; there is no on-chain "this is a burn" signal, so
nothing to auto-discover.

Therefore burn classification stays in the **worker**, which
already holds project config and runs `classify()`. The module's
*only* burn-related obligation is the identity generalisation
above: surface the enterprise burn holder distinctly (don't drop
it into `None`) so it reaches the worker. The module needs **no
burn config** and **no burn logic**.

The worker matches an `Enterprise(addr)` holder's address against
the project's configured burn addresses → `Burn`.

## The `role` field — module tags, worker refines

`HolderRole` is the *module's* knowledge — set only for what the
module learns for free while decomposing: `DexPool` (a recognised
pool / its residual), `VestingContract` (a vesting residual),
`Wallet` (everything else, the default).

The **worker** layers project-config classification on top:
`Burn` (config burn addresses), `Treasury` / known-wallet labels
(config), handle `display`. The consumer's final entity type is
`role` refined by config. The worker's `classify()` shrinks to
roughly: take `role`, override with any config match, attach
labels.

## Module / worker boundary

| Concern | Where | Why |
|---|---|---|
| LP / vesting decomposition | module | needs chain data (datums, enumeration, owner resolution) |
| surfacing every holder, incl. enterprise | module | it owns the `utxos-by-policy` scan |
| `role` for recognised contracts | module | a free byproduct of decomposing |
| burn classification | worker | config-only, no chain data |
| treasury / known-wallet labels | worker | config-only |
| `$handle` resolution | worker (async alarm) | HTTP — can't run in `apply_event` |

## The per-domain modules

- **`cswap-dex` / `splash-dex`** — keep. Live `DexAction` trade
  events feed the holder-map trade feed (cutover Phase 2). Their
  CSwap/Splash decode libraries are shared with
  `holder-distribution` via `mitos-dex-decode`.
- **`vesting-tracker`** — keep. Live `VestingEvent` lock/unlock
  deltas. Its datum decoders are extracted into the shared
  vesting-decode crate. Its *snapshot* (`VestingEvent::Snapshot`)
  is superseded for the holder-map by `holder-distribution`'s
  snapshot; it may still serve other consumers.
- **`burn-address`** — keep for other consumers. For the
  holder-map it is superseded: burns are classified worker-side
  from `holder-distribution`'s enterprise holders, and a burn
  TX surfaces in `holder-distribution`'s own `HolderDelta`
  anyway. Whether holder-map still subscribes to it at all is a
  Phase-3 wiring decision.

## Deltas vs the snapshot

Decomposition is a **snapshot-time transform**.
`holder-distribution`'s `HolderDelta` carries raw post-TX
balances; `lp_amount` / `vests` are recomputed only on
cold-start / `rebootstrap`. Between snapshots a delta can leave
the attribution slightly stale; the scheduled recapture backstop
(cutover plan R3) realigns it within a bounded window. This is
the same posture already shipped for LP and is a deliberate
trade — keeping decomposition off the per-TX path.

## Wire changes — `mitos-community-events::holder_distribution`

- `HolderEntry.id: HolderId` — replaces `stake_cred_hex:
  Option<String>`.
- `HolderEntry.role: HolderRole` — new.
- `HolderEntry.lp_amount: u64` — **shipped** (slice 1).
- `HolderEntry.vests: Vec<LockEntry>` — new; reuses
  `crate::vesting_tracker::LockEntry`.

Each is a wire change; the holder-map consumer rev-bumps. They
can land incrementally (slice by slice) behind `#[serde(default)]`
where shape allows.

## Cutover plan reconciliation

`HOLDER_MAP_MITOS_CUTOVER_PLAN.md` needs an editing pass to fold
this in (tracked separately). In summary:

- **Phase 2 (DEX / trades)** — unchanged for the trade feed
  (`cswap-dex` + `splash-dex` → `DexFeedDO`). LP decomposition is
  *not* part of Phase 2's `DexAction` wiring; it lives in
  `holder-distribution` (slice 1, done).
- **Phase 3 (burn)** — re-scoped. Burns are classified
  worker-side from `holder-distribution`'s enterprise holders.
  The separate `BurnAddressChannel` is likely unnecessary for
  the holder-map.
- **Phase 4 (vesting)** — re-scoped. Vesting is decomposed in
  `holder-distribution` (slice 2). The separate `VestingChannel`
  + the `token_holders` lock-row suppression open question are
  dissolved.

## Work breakdown / sequencing

1. **LP decomposition** — DONE, deployed (slice 1).
2. **Generalised holder identity** — DONE. `HolderId` enum is
   live (`Stake` / `Enterprise`); ledger key handles enterprise
   creds.
3. **`role` field** — DONE. Module tags `Wallet` / `DexPool` /
   `VestingContract` via `HolderRole`.
4. **Vesting-decode crate** — DONE. `mitos-vesting-decode`
   carries the Shield + CrowdLock datum decoders;
   `holder-distribution` consumes `decompose_vesting()`.
5. **Vesting decomposition phase** in `holder-distribution` —
   DONE. `HolderEntry.vests` is populated at cold-start and via
   `rebootstrap`.
6. **Worker** — consume `id` / `role` / `lp_amount` / `vests`;
   add burn classification; collapse the legacy `classify()`
   heuristics onto `role` + config. **Remaining work.**
7. **Verify** — dev donut vs prod, every band. **Remaining work.**

Steps 2–5 landed alongside the chunked-snapshot work
(`docs/design/WASM_BUDGET_CHUNKING.md`); slices 6 + 7 are the
consumer-side cutover.

## Open questions

1. **LP detail granularity** — bare `lp_amount: u64` vs a
   per-pool breakdown (`Vec<{pool_id, amount}>`) for a wallet
   providing to multiple pools. Bare for now; revisit if a
   tracked token gets multiple pools that matter to the UI.
2. **`vesting-tracker` / `burn-address` holder-map subscription**
   — once the snapshot is wholly `holder-distribution`'s, does
   holder-map still subscribe to those modules at all? Decide
   during the Phase 3/4 worker wiring.
3. **Handle resolution** — `$handle` `display` is still the
   separable worker-side async enrichment (carried over from
   rev 2's Q2); dolos-native vs the Handle.me service. Does not
   block any slice here.
