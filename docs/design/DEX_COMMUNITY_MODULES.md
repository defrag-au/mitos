# DEX Community Modules

## Goal

Move DEX chain-recognition out of `cnft.dev-workers`'s
classifier worker and into platform-v2 community modules in
mitos. Push past where the classifier could go by combining
the wasm-module ABI's structural-detection affordances with
brand-specific datum decoding for known contract families.
Enable a TapTools-alternative consumer worker that subscribes
per policy to a fan-out of rich DEX events — swaps in Phase 1;
liquidity provision, single-sided zap-ins, claim/stake events
in later phases. The module shape (`<brand>-dex`) accommodates
the broader DEX surface from day one even though Phase 1
limits emissions to swap-shaped flows.

Same posture as the marketplace + asset-transfer migrations:
chain recognition belongs in the platform layer, projections
belong in consumer workers.

## Current state (as of 2026-05)

The classifier worker's `handle_dex_tx` flow:

- **Recognition method**: address-registry lookup + structural
  UTxO analysis. Watches captain-hook's `is_exchange` filter →
  `txs-dex` queue → classifier. Detects swaps by looking for
  script addresses on both sides of the TX with net token /
  ADA flows. **No datum decoding** at recognition time.
- **Brand coverage**: Splash, Minswap V1+V2, CSWAP, SaturnSwap,
  DexHunter. Brand identified via address-registry lookup
  (`shared-crates/address-registry`). Unmatched addresses still
  emit as `dex_platform: "Unknown DEX"`.
- **Event shape**: `DexSwap { dex_platform, asset_in, asset_out,
  swapper }` (the classifier's own type — distinct from this
  doc's proposed `DexAction` enum). Fill-only — no order create /
  cancel / execute lifecycle, no liquidity events.
- **External state**: none. Detection is one-TX-only (no
  aux-data, no witness datum, no prior-UTxO resolution).
- **Decoders not used**: the per-brand datum decoders in
  `shared-crates/cardano-tx/dex/{splash,cswap}/` exist but
  are invoked by TX builders, not by classification.

The structural-only approach catches multi-hop routings,
weighted/dynamic-fee pools, and unregistered DEX launches that
aggregators whitelist themselves out of seeing. It's
load-bearing for catch coverage but **doesn't surface pool
state, effective price after fees, or batcher fees** — the data
a TapTools-alternative tool would want for useful UI.

## Architectural pick: per-brand modules, contract-family-keyed

One wasm community module per DEX **contract family**, watching
that family's script addresses, decoding its datums for rich
event payloads. Same posture as `jpg-store-{listing,sale,offer}`
for marketplace work.

For PR 1 / Phase 1:

- `minswap-dex` — Minswap V1 + V2 contracts (one module
  spanning both versions; same brand family)
- `cswap-dex` — CSWAP contracts (all 82 pools at single script
  address)
- `splash-dex` — Splash V1 / V2 / V3 contracts

### Why per-brand, not hybrid

**Taggable modules over forced centralisation** is the
principle. Each module is keyed on the underlying contract
family, not the marketing brand:

- A new DEX built on top of Minswap's contracts unchanged →
  add the new addresses to `minswap-dex`'s `[interest].addresses`
  + an internal `address → brand_label` map. Events emit with
  `dex_brand: "MinswapFork"` (or whatever). One module covers
  multiple branded products that share the contract family.
- A genuinely-new DEX with different contracts → new module
  in its own PR. Mature contract families don't get
  contaminated by experimental ones.

### Trade-offs vs the rejected hybrid alternative

| | Per-brand modules | Single hybrid module |
|---|---|---|
| Module count | 3 in PR 1 | 1 |
| Module size | ~700KB-1MB each | ~2MB+ |
| Brand isolation | ✅ Splash decoder bug doesn't affect Minswap | ❌ All-or-nothing |
| Testing pathway | ✅ Per-module golden fixtures | ❌ Shared fixtures, fault attribution harder |
| Drop-in for new contract families | ✅ Add a new module | ❌ Grow the hybrid |
| Catches unknown DEXes | ❌ `asset-transfer` covers this gap | ✅ Built in |
| Consumer subscriptions | 3 (one per module) | 1 |
| Host overhead | ~3MB wasm, 3 sets of redb files, 3 WS per consumer per policy | 1 of each |
| Independent retirement | ✅ Evict one without touching others | ❌ Module-wide |

The unknown-DEX coverage concern that drove the hybrid pitch
**is already handled by `asset-transfer`**. A swap through an
unregistered DEX still emits `Transfer { from: pool_addr,
to: user_addr, … }` events at the asset-movement layer.
Consumers wanting "catch everything" subscribe to
`asset-transfer` in addition to whichever DEX modules they
care about — same layering as the marketplace overlay sitting
on top of asset-transfer.

Host overhead at 3 extra modules is negligible (we comfortably
run 9 community modules today; 3 more is a yawn).

## Event shape: shared crate, per-brand modules emit it

`mitos_community_events::dex::DexAction`:

```rust
pub struct Swap {
    /// 64-hex consuming TX.
    pub tx_hash: String,
    /// Absolute slot.
    pub slot: u64,
    /// Brand label resolved from the module's internal
    /// address-to-brand map. For `minswap-dex`: "Minswap",
    /// "MinswapFork", "MinswapDerivative" (operator-defined
    /// names for contract-family-sharing DEXes). For
    /// `cswap-dex` / `splash-dex`: their own brand
    /// vocabulary. Always populated — the module knows what
    /// brand its addresses tag to.
    pub dex_brand: String,
    /// Contract version within the brand: "V1" / "V2" / "V3"
    /// for Minswap and Splash; `None` for CSWAP (single
    /// version).
    pub contract_version: Option<String>,
    /// Bech32 address of the wallet that received the
    /// `asset_out` (= the user).
    pub swapper_address: String,
    /// What the swapper sent in. Lovelace or native asset.
    pub asset_in: SwapAsset,
    /// What the swapper got out.
    pub asset_out: SwapAsset,

    // ===== Datum-driven richness =====
    //
    // Always populated when the swap routed through a pool
    // (every Phase 1 brand). Defensive `Option<>` so a
    // decoder fault degrades gracefully rather than blocking
    // emission.

    /// Canonical pool identifier — asset-pair fingerprint
    /// (sorted policy+name hashes). Stable across pool-contract
    /// versions for the same asset pair, so a consumer's
    /// `ADA/MIN` chart doesn't fragment when Minswap launches
    /// a new pool version with the same pair.
    pub pool_id: Option<String>,
    /// Pool reserves immediately before the swap. Read from
    /// the consumed pool UTxO's datum.
    pub pool_reserves_before: Option<PoolReserves>,
    /// Pool reserves immediately after the swap. Read from
    /// the produced pool UTxO's datum.
    pub pool_reserves_after: Option<PoolReserves>,
    /// Pool's configured swap fee in basis points (e.g. 30 =
    /// 0.30%). From pool datum. Per-pool — different pools on
    /// the same DEX can carry different fees.
    pub pool_fee_bps: Option<u32>,
    /// Lovelace paid to the batcher for executing the swap.
    /// Extracted from the batcher output ADA delta. `None`
    /// for pool-direct swaps (no batcher hop).
    pub batcher_fee_lovelace: Option<u64>,
    /// Effective price the swapper got after fees and
    /// slippage. Encoded as `(asset_out.quantity,
    /// asset_in.quantity)` rational so consumers can pick
    /// precision.
    pub effective_price: Option<(u64, u64)>,
}

pub struct PoolReserves {
    pub base_asset: SwapAsset,
    pub quote_asset: SwapAsset,
}

pub enum SwapAsset {
    Lovelace(u64),
    Native { policy: String, asset_name_hex: String, quantity: u64 },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DexAction {
    Swap(Swap),
    // Future variants (Phase 3/4 — additive, no wire break):
    //   LiquidityAdd(LiquidityEvent)
    //   LiquidityRemove(LiquidityEvent)
    //   ZapIn(ZapEvent)              // single-sided liquidity provision
    //   OrderCreate(Order)           // batcher order placement
    //   OrderCancel(OrderRef)
    //   StakeRewardClaim(ClaimEvent) // for DEXes with reward tokens
    // The `<brand>-dex` module naming accommodates this surface
    // from day one; Phase 1 ships only `Swap` to bound scope.
}
```

**Single shared event-type crate**, three modules emit it.
Consumers subscribing to all three see a uniform wire shape —
they can union events into one `Vec<DexAction>` and key on
`(tx_hash, dex_brand)` for stable per-trade identity.

The `tag = "kind"` shape means future variants land additively;
existing consumers reading `Swap` events continue to work
unchanged when `LiquidityAdd` etc. start appearing.

## Per-module structure

Each module follows the same template:

```
community-modules/<brand>-swap/
├── <brand>_swap.rs           # detection + decode + emit
├── <brand>_swap.toml         # interest = brand's script addresses
└── tests/fixtures/
    ├── pool-direct-swap/     # simple ADA → token swap, pool-only
    ├── batcher-routed-swap/  # order-via-batcher path
    ├── multi-hop-swap/       # if brand supports it
    └── pool-state-change/    # validates reserves_before/after decode
```

### Algorithm (per module)

Per TX (`handle_events`):

1. Buffer Consumed + Produced events into a TxBuffer (same
   pattern marketplace modules use).
2. **Per-output address categorisation** scoped to this
   brand's addresses (declared statically in `.toml`):
   - **Pool**: brand script address in both Consumed + Produced
     for the same UTxO ref pattern
   - **Batcher**: brand script address in Produced only (order
     placement) or Consumed only (order fill / cancel)
   - **User**: non-script address with asset deltas
3. Compute per-(address, asset) net deltas across the TX.
4. **Decode pool datum** from the consumed pool UTxO's
   `prior_datum` field (or from witness data if hash-only) →
   `pool_reserves_before`, `pool_fee_bps`, `pool_id` derivation.
5. **Decode pool datum** from the produced pool UTxO's
   `output.datum` field → `pool_reserves_after`.
6. **Compute** `effective_price`, `batcher_fee_lovelace`,
   `asset_in` / `asset_out` from the net deltas.
7. Emit one `Swap` event per User who participated.

Decoder failure (datum shape changed upstream, unexpected
variant) → log warn, emit the structural fields only, leave
richness `None`. Never blocks the emission.

### Interest

Each module ships with `[interest].addresses` populated with
its brand's known script addresses. Updating the address list
(adding a Minswap-fork DEX, or new Splash version) is a module
rebuild + redeploy + evict-and-replace.

The internal `address → brand_label` map lives alongside the
addresses in the `.toml` — operator-readable, no rebuild
required for label-only tweaks (TBD — could be runtime config
or compile-time const; lean compile-time for type safety).

## Lift map — what we can pick up

| Brand | Pool decoder | Order decoder | Notes |
|---|---|---|---|
| Splash V1 | `shared-crates/cardano-tx/dex/splash/pool.rs` ✅ | `shared-crates/cardano-tx/dex/splash/` ✅ | V2/V3 share payment script per memory bank — confirm coverage |
| Splash V2/V3 | TBD (build or confirm shared decoder works) | TBD | Same staking-cred difference as marketplace V2/V3 |
| CSWAP | `shared-crates/cardano-tx/dex/cswap/pool.rs` ✅ (constructor 0, 8 fields) | `shared-crates/cardano-tx/dex/cswap/datum.rs` ✅ | All 82 pools at single script address |
| Minswap V1 | build from CDDL (~2 hrs) | build from CDDL (~3 hrs) | Datum schema authoritative at `WingRiders/cardano-datum-registry/projects/MinswapV1/`. No public Rust ref impl exists (`minswap-sdk-rust` is private). CSWAP decoder is the template. |
| Minswap V2 | build from CDDL | build from CDDL | Same as V1; V2 datum is more complex (10 fields, asymmetric fees, dynamic-fee flag, per-pool staking credential). Order datum has 11 `OrderStep` variants including Deposit / Withdraw / ZapIn (see "Beyond Swap" below). |

**Address-registry lifting**: each module pulls just its own
brand's addresses from `shared-crates/address-registry`. No
shared `dex-address-registry` crate in Phase 1 — each module
is self-contained. If we end up needing the consumer worker
to share the same mapping, we can carve out a crate later.

### Minswap decoder build — investigation findings (2026-05)

Pre-PR investigation surfaced:

- **Schemas authoritative** at
  `github.com/WingRiders/cardano-datum-registry/projects/MinswapV{1,2}/`.
  V1 pool = Constr-0 / 6 fields; V1 order = Constr-0 / 6 fields
  with 5-variant `OrderStep` enum (tags 121-125). V2 pool =
  Constr-0 / 10 fields with **asymmetric fees** (`feeANumerator`
  + `feeBNumerator`, not a single `pool_fee`) and a
  per-pool `allowDynamicFee` bool. V2 order = Constr-0 / 9
  fields with 11-variant `OrderStep` (tags 121-131) +
  4-variant `OrderAuthorizationMethod` + 3-variant
  `OrderExtraDatum` (inline / hash / none).
- **No public Rust ref impl.** `minswap-sdk-rust` is private.
  Build from the CDDL using the existing CSWAP / Splash
  in-house decoders as the template (proven pattern:
  `pallas_primitives::alonzo::{PlutusData, Constr}` +
  `Fragment::decode_fragment()` + tag-based dispatch +
  defensive `BigInt → u64` conversion).
- **Datums inline** on Minswap UTxOs (V1 + V2) — no aux-data
  / hash-only resolution needed (unlike jpg.store CO).
  Simplifies the module's `apply_event` significantly.
- **Address registry is exhaustive** for mainnet. No factory,
  fee-bank, or staking-credential upgrades discovered.
- **Effort estimate: medium (~2-3 days)** for the full
  `minswap-dex` module covering V1 + V2 pool + order
  decoders + golden fixtures. V1 first (~5 hrs total),
  V2 next (~15 hrs given enum depth).

## Phased delivery

**Phase 1: three brand modules + shared event crate**

PR 1 (one PR or three sequential — operator preference):

- Define event types in `mitos_community_events::dex`
  (shared by all three modules + consumers)
- `community-modules/cswap-dex/` — lift existing decoders;
  smallest scope, validates the per-brand template
- `community-modules/splash-dex/` — lift existing V1
  decoders; confirm V2/V3 coverage or build
- `community-modules/minswap-dex/` — build Minswap V1 + V2
  decoders from datum-registry references
- Per-module golden fixtures (4-5 per module covering the
  brand's known TX shapes)
- Workspace `run-golden-tests.sh` extends to cover the new
  fixtures

**Phase 2: classifier handoff**

- Stand up the TapTools-alternative consumer worker (or
  retrofit an existing one) that subscribes to all three
  `*-swap` modules + `asset-transfer` (for the unknown-DEX
  gap)
- Parallel-run against the classifier for 24-48h on prod;
  verify event counts agree per brand + richness fields are
  populated as expected
- Retire the classifier's `handle_dex_tx` path (similar to
  the ownership-routing retirement that happened with the
  marketplace migration)

**Phase 3: extend brand coverage**

- Add `sundae-swap` (V1 / V2 / V3) and `wingriders-swap` —
  each its own PR, same per-brand template
- `genius-yield-swap`, `muesli-swap`, etc. as priorities
  surface

**Phase 4: extend event surface beyond Swap**

Each `<brand>-dex` module extends its `DexAction` enum with additional
variants as the consumer use cases emerge. Likely order
(prioritise by what the TapTools-alternative UI actually needs):

- `LiquidityAdd(LiquidityEvent)` — user deposits into a pool;
  enables LP-position tracking and TVL charts. Decoded from
  Minswap V2's `Deposit` OrderStep (et al.).
- `LiquidityRemove(LiquidityEvent)` — symmetric counterpart.
- `ZapIn(ZapEvent)` — single-sided liquidity provision
  (Minswap V2 `ZapIn` OrderStep). Enables "I added LP without
  pairing the assets myself" tracking.
- `OrderCreate(Order)` / `OrderCancel(OrderRef)` — order
  lifecycle. Enables "resting orders" UX (e.g. "show me all
  unfilled Splash orders for asset X"). Per-brand decision
  since matching consume-side priors back to their original
  creates is brand-specific.
- `StakeRewardClaim(ClaimEvent)` — for DEXes with reward
  tokens or staking pools.

Each variant lands as a separate PR with its own golden
fixtures. The `tag = "kind"` enum shape means consumers
reading only `Swap` keep working unchanged as new variants
appear on the wire.

## Reusing modules across brands — the tagging mechanism

When a new DEX launches on top of an existing contract family:

1. Operator confirms the new DEX's addresses route to the
   same payment script as an existing module (e.g. same as
   Minswap V2).
2. Add the new addresses to that module's
   `[interest].addresses` in the `.toml`.
3. Add the new brand label to the module's internal
   `address → brand_label` map (e.g. `addr1z...: "DexX"`).
4. Rebuild + redeploy + evict-and-replace.
5. Going forward, the module emits `Swap` events tagged
   `dex_brand: "DexX"` for those addresses while continuing to
   emit `dex_brand: "Minswap"` for the original ones.

This keeps known-good decoders unchanged when forks appear,
and lets the consumer / UI treat fork DEXes as first-class
brands without operating-system-level churn.

## Open questions to resolve before PR 1

1. **Module-launch ordering**: ship the three modules in one
   PR or three sequential? Recommend **sequential** — CSWAP
   first (existing decoders, smallest scope, fastest), then
   Splash (existing V1 decoders), then Minswap (build from
   CDDL, ~2-3 days). Each module's fixtures + decoder shake
   out in isolation; cleaner fault attribution if anything
   regresses.

2. ~~**Minswap decoder source**~~ — **resolved 2026-05**:
   build from `WingRiders/cardano-datum-registry/projects/MinswapV{1,2}/`
   CDDL using the in-house CSWAP / Splash decoder pattern.
   No public Rust ref impl exists. See "Minswap decoder build —
   investigation findings" above.

3. ~~**Beyond Swap — V2's broader OrderStep surface**~~ —
   **resolved**: module naming (`<brand>-dex`, not
   `<brand>-swap`) explicitly accommodates the full DEX
   surface (liquidity add/remove, zap-in, order lifecycle,
   future stake/claim). Phase 1 ships swap-only `Dex::Swap`
   emissions to bound scope; later phases add additive
   variants (`LiquidityAdd`, `LiquidityRemove`, `ZapIn`,
   `OrderCreate`, `OrderCancel`, etc.) without breaking the
   wire (tag-discriminated enum). Each new variant is its own
   PR with its own golden fixtures.

4. **Asymmetric pool fees (Minswap V2)**. V2 pool datum has
   `feeANumerator` + `feeBNumerator` (different fees per swap
   direction). The current event shape has
   `pool_fee_bps: Option<u32>` (single value). Options:
   - **(a)** Emit the **effective fee for this swap's
     direction** (look at which way the swap went, populate
     `pool_fee_bps` from the matching numerator).
   - **(b)** Add `pool_fee_bps_a` + `pool_fee_bps_b` to the
     event; consumer picks.
   - **(c)** Keep single `pool_fee_bps`, document that for V2
     it's the swap-direction fee.
   **Recommend (a) + (c)** — simpler wire shape; the
   directional fee is what consumers actually want for
   per-trade analysis. Symmetric pools (CSWAP, Splash V1,
   Minswap V1) keep emitting the same single value. Document
   that V2 swaps populate the directional fee.

5. **Address-brand map representation**: compile-time const
   `&[(&str, &str)]` in the module source (type-safe, rebuild
   on every addition) vs runtime config in `.toml` (label-only
   tweaks don't require rebuild). Recommend compile-time for
   Phase 1 — simpler, and label changes are rare enough that
   the rebuild cost is fine. Revisit if fork DEX proliferation
   becomes a thing.

6. **Multi-User detection edge cases**: a batcher TX can fill
   orders for multiple users in one go. Each user gets their
   own `Swap` emission (matches `asset-transfer`'s per-recipient
   pattern). Edge cases worth deciding now:
   - Multi-hop where intermediate hops touch user-controlled
     addresses (rare but exists). Emit per leg or collapse to
     input → final output? **Recommend collapse** — same as
     the classifier today.
   - Self-trades (user inputs from + outputs to the same
     stake credential). **Recommend emit** — on-chain state
     changed; consumer can filter if they care.

7. **`pool_id` derivation**: `blake2b(sort(asset_a) ||
   sort(asset_b))` for cross-version stability. Same hash
   across `minswap-dex` / `cswap-dex` / `splash-dex` for
   identical asset pairs so consumers can build cross-DEX
   asset views. **Confirm before PR 1.**

8. **Effective price encoding**: `(asset_out_qty, asset_in_qty)`
   rational keeps integer precision. Consumer divides for
   floating-point display. Alternative: f64 — loses precision
   on large-magnitude swaps. **Recommend the rational.**

## Non-goals

- Aggregator-style routing (DexHunter's "best price across
  DEXes" UX). That's a consumer-side projection, not chain
  recognition.
- DEX-builder primitives (`build_swap_tx` etc.). Those live in
  `shared-crates/cardano-tx/dex/` and stay there; community
  modules are read-only.
- "Catches every DEX-shaped TX" via a structural-detector
  module. `asset-transfer` already does this at the asset-
  movement layer; per-brand DEX modules sit on top for known
  contract families. Consumer subscribes to both for full
  coverage.
- Replacing the in-tree `none-match-indexer` — that stays as
  the synchronised-dispatcher's residual-pass coordinator
  (see `docs/strategy/COMMUNITY_MODULES.md`).

## References

- Classifier survey conducted 2026-05; results inline above
- `cnft.dev-workers/workers/classifier/src/lib.rs:1388-1523` —
  `handle_dex_tx` entry point
- `shared-crates/pipeline/tx-classifier/src/patterns/dex.rs` —
  structural detection algorithm (~470 lines, reference for
  the per-brand detection logic — each module implements its
  own scoped version)
- `shared-crates/address-registry/src/registry.rs:99-174` —
  brand registry to lift addresses from
- `shared-crates/cardano-tx/dex/cswap/pool.rs` — constant-
  product math + datum decode (CSWAP)
- `shared-crates/cardano-tx/dex/splash/` — Splash V1 decoders
- https://github.com/WingRiders/cardano-datum-registry —
  Minswap datum schema references
- `docs/strategy/COMMUNITY_MODULES.md` — layering rationale
- `docs/design/DOMAIN_REFACTOR.md` — domain-event taxonomy
  (`Dex` will live alongside `Mint` / `Burn` /
  `AssetMovement` / marketplace events)
