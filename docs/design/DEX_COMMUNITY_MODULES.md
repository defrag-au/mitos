# DEX Community Modules

## Goal

Move DEX chain-recognition out of `cnft.dev-workers`'s
classifier worker and into platform-v2 community modules in
mitos. Push past where the classifier could go by combining
the wasm-module ABI's structural-detection affordances with
brand-specific datum decoding for known contract families.
Enable a TapTools-alternative consumer worker that subscribes
per policy to a fan-out of rich DEX events covering the
**full common-action surface of a DEX in one module per
brand**: swaps, liquidity add/remove (including zap-style
asymmetric ratios), order cancellation, farm staking, and
reward distribution. Each `<brand>-dex` module owns its brand's
entire DEX-domain surface so a consumer subscribing to one
`<brand>-dex` sees everything that brand's users do, not just
trades.

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

/// Struct-shaped variants (rather than `Lovelace(u64)`) so the
/// `tag = "kind"` discriminator round-trips through ciborium —
/// newtype variants wrapping primitives trip the serialiser.
pub enum SwapAsset {
    Lovelace { quantity: u64 },
    Native { policy: String, asset_name_hex: String, quantity: u64 },
}

pub struct LiquidityAdd {
    pub tx_hash: String,
    pub slot: u64,
    pub dex_brand: String,
    pub contract_version: Option<String>,
    /// Wallet that supplied the liquidity and received LP
    /// tokens.
    pub provider_address: String,
    /// What the provider added on each side of the pair.
    /// Asymmetric ratios (zap-style) are encoded by one side's
    /// quantity being noticeably smaller; CSWAP and equivalent
    /// brands expose this directly via the same pool-reserves
    /// delta as a 50/50 add — no separate `ZapIn` variant.
    pub quote_added: SwapAsset,
    pub base_added: SwapAsset,
    /// LP tokens minted to the provider. Raw asset (policy +
    /// name + quantity) so consumers can map LP→pool off-line
    /// without us building a runtime LP-policy registry inside
    /// the module.
    pub lp_received: SwapAsset,
    pub pool_id: Option<String>,
    pub pool_reserves_before: Option<PoolReserves>,
    pub pool_reserves_after: Option<PoolReserves>,
    /// Informational — pool's swap fee. Doesn't apply to the
    /// LP-add path itself but useful so consumers can render
    /// "you joined a pool charging X bps".
    pub pool_fee_bps: Option<u32>,
    pub batcher_fee_lovelace: Option<u64>,
}

pub struct LiquidityRemove {
    pub tx_hash: String,
    pub slot: u64,
    pub dex_brand: String,
    pub contract_version: Option<String>,
    pub provider_address: String,
    /// LP tokens burnt or returned by the provider.
    pub lp_burnt: SwapAsset,
    pub quote_withdrawn: SwapAsset,
    pub base_withdrawn: SwapAsset,
    pub pool_id: Option<String>,
    pub pool_reserves_before: Option<PoolReserves>,
    pub pool_reserves_after: Option<PoolReserves>,
    pub pool_fee_bps: Option<u32>,
    pub batcher_fee_lovelace: Option<u64>,
}

pub enum OrderKind {
    Swap,
    LiquidityAdd,
    LiquidityRemove,
    /// Order datum shape didn't match any known variant —
    /// emit anyway so consumers see the cancel happened, but
    /// can't classify it precisely. Useful signal for "we
    /// found a new order shape we should add a decoder for".
    Unknown,
}

pub struct OrderCancel {
    pub tx_hash: String,
    pub slot: u64,
    pub dex_brand: String,
    pub contract_version: Option<String>,
    /// What kind of order this was. Cancel-only — for the
    /// matching fills we already emit `Swap` / `LiquidityAdd`
    /// / `LiquidityRemove`.
    pub order_kind: OrderKind,
    pub canceller_address: String,
    /// The order UTxO that was consumed by the cancel. Lets
    /// consumers reconcile a cancel against the prior submit.
    pub prior_order_tx_hash: String,
    pub prior_order_output_index: u32,
    /// Lovelace returned to the canceller — order's locked
    /// lovelace minus the network fee.
    pub refund_lovelace: u64,
}

pub struct FarmStake {
    pub tx_hash: String,
    pub slot: u64,
    pub dex_brand: String,
    pub contract_version: Option<String>,
    pub staker_address: String,
    /// LP token quantity added to the farm. Raw asset; LP→pool
    /// mapping stays off-module (same rationale as
    /// `LiquidityAdd::lp_received`).
    pub lp_token: SwapAsset,
}

pub struct FarmUnstake {
    pub tx_hash: String,
    pub slot: u64,
    pub dex_brand: String,
    pub contract_version: Option<String>,
    pub staker_address: String,
    pub lp_token: SwapAsset,
}

pub struct RewardClaim {
    pub tx_hash: String,
    pub slot: u64,
    pub dex_brand: String,
    pub contract_version: Option<String>,
    pub claimer_address: String,
    /// Reward asset(s) received by the claimer. Vector so
    /// multi-asset rewards (some farms pay out two tokens
    /// simultaneously) fit the same shape as single-asset.
    pub rewards: Vec<SwapAsset>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DexAction {
    /// Pool swap — opposing reserve deltas.
    Swap(Swap),
    /// Two-sided (50/50) or zap-style (asymmetric) liquidity
    /// provision. Both reserves grow in the same TX.
    LiquidityAdd(LiquidityAdd),
    /// LP burnt to withdraw liquidity. Both reserves shrink.
    LiquidityRemove(LiquidityRemove),
    /// Open order at a batcher / order script consumed via
    /// cancel redeemer.
    OrderCancel(OrderCancel),
    /// LP tokens locked into a farm script.
    FarmStake(FarmStake),
    /// LP tokens released from a farm script.
    FarmUnstake(FarmUnstake),
    /// Farm rewards distributed. Detection signal is
    /// brand-specific — for CSWAP it's an operator key wallet
    /// rather than a Plutus script (see "Detection-signal
    /// stability" below).
    RewardClaim(RewardClaim),
}
```

**What we deliberately don't emit:**

- **Order submission** (swap / LP-add / LP-remove orders) —
  the matching fill TX carries the actual outcome (`Swap`,
  `LiquidityAdd`, `LiquidityRemove`) with real prices,
  reserves, and batcher fees. Emitting a parallel "intent"
  event on submission duplicates with strictly less info.
  Cancellation is different because the cancel *is* the
  outcome — hence `OrderCancel` exists, `OrderCreate` doesn't.
- **Harvest request** (the 2A "claim ticket" TX users send
  on CSWAP / similar protocols) — same rationale; the
  distribution TX is the outcome event. The 2A overpay is a
  CSWAP-side curiosity (operator retains the spread between
  the flat fee and actual TX cost) — surface it as a
  consumer-side computation against `batcher_fee_lovelace`
  conventions if needed, not as its own variant.
- **Pool creation / liquidity pool init** — one-time admin
  events; consumers care about user-facing actions.

**Single shared event-type crate**, each `<brand>-dex` module
emits it. Consumers subscribing across brands see a uniform
wire shape — they can union events into one `Vec<DexAction>`
and key on `(tx_hash, dex_brand, kind)` for stable per-action
identity.

The `tag = "kind"` shape means new variants land additively;
existing consumers reading `Swap` continue to work unchanged
when (e.g.) Splash brings a `LiquidityRemove` shape we haven't
seen yet.

## Detection-signal stability

Not every action is gated by a Plutus script. CSWAP — and
likely Splash / others — manages reward distribution via
**operator-controlled key wallets** rather than a payout
contract. This affects how we detect those actions:

| Action | Detection key | Stability |
|---|---|---|
| `Swap` / `LiquidityAdd` / `LiquidityRemove` | pool script consume+produce | script-based — rock solid |
| `OrderCancel` | order/batcher script consume + cancel redeemer | script-based — but **no CSWAP cancels observed on-chain**, see below |
| `FarmStake` / `FarmUnstake` | farm script consume+produce + LP-token delta sign | script-based |
| `RewardClaim` | TX consumes from CSWAP's request-collection **key wallet** + user output carries reward asset | **operator-address-based — fragile** |

Operator wallets can rotate; if CSWAP changes their reward
distributor key the module misses harvests until the address
is added to interest. Mitigation: each per-brand module
documents its operator-wallet dependencies inline, and a
periodic check (operator publishes a new wallet, or we see a
sudden drop in `RewardClaim` emissions) prompts a module
rebuild. We accept this fragility because the alternative
(skipping `RewardClaim` entirely) leaves a real user-facing
hole in the consumer UX.

### TODO: `OrderCancel` deferred for CSWAP

The `DexAction::OrderCancel` variant is wire-defined in
`mitos_community_events::dex` but no module currently emits it.
Investigation against mainnet history (2026-05):

- The CSWAP TX-builder code in cnft.dev-workers
  (`workers/wallet-operations/src/dex.rs`) marks CSWAP order
  cancellation as `"CSWAP order cancellation not yet
  implemented"`.
- Every one of the ~30 most-recent CSWAP order-script consumes
  on-chain uses a fill redeemer (Constr 2 — `d87b9f…`), never
  the expected cancel redeemer (Constr 0 — `d87980`).
- The CSWAP UI surfaces a "cancel" affordance, but the TXs it
  produces are aggregator-mediated re-submissions, not on-chain
  script unlocks.

Best current theory: CSWAP's contract may not expose a
user-callable cancel path at all — orders sit at the batcher
until filled. `splash-dex` (where on-chain cancels are common
and well-documented, and shared-crates has a working cancel
builder) will be the first module to actually emit
`OrderCancel`. If a real CSWAP cancel TX surfaces, the
detection path is a single-fixture addition to `cswap-dex`.

## Per-module structure

Each module follows the same template:

```
community-modules/<brand>-dex/
├── <brand>_dex.rs            # detection + decode + emit
├── <brand>_dex.toml          # interest = brand's pool + farm + order
│                             #   script addresses + operator wallets
└── tests/fixtures/
    ├── fill-<pair>/          # swap fill — full event
    ├── order-*/              # order submission TXs — zero events
    ├── liquidity-add-fill/   # LP add fill — `LiquidityAdd` event
    ├── liquidity-remove-fill/# LP remove fill — `LiquidityRemove`
    ├── order-cancel/         # cancel a pending order — `OrderCancel`
    ├── farm-stake/           # `FarmStake`
    ├── farm-unstake/         # `FarmUnstake`
    ├── farm-harvest-request/ # no event (intent, not outcome)
    └── farm-harvest-fill/    # `RewardClaim`
```

### Algorithm (per module)

Per TX (`handle_events`), buffer Consumed + Produced events,
then at flush dispatch into per-variant detection. The shared
foundation is the same pattern across all variants:

1. Buffer Consumed + Produced events into a TxBuffer
   (marketplace-module pattern).
2. Decode pool datums where present (consumed prior + produced
   output) to gate base/quote asset identity per pool.
3. Identify per-action signals (see table below).
4. Emit per matched action.

Decoder failure (datum shape changed upstream, unexpected
variant) → log warn, emit structural fields only, leave datum-
driven richness `None`. Never blocks the emission.

**Per-variant detection signals:**

| Variant | Signal |
|---|---|
| `Swap` | Pool consume + produce; reserve deltas have **opposing signs**. Asset_in = side that grew in pool; asset_out = side that shrank. |
| `LiquidityAdd` | Pool consume + produce; reserve deltas are **both positive**. Find LP token minted to a user wallet output. |
| `LiquidityRemove` | Pool consume + produce; reserve deltas are **both negative**. Find the corresponding LP-token burn (mint with negative quantity) or LP returning to the user wallet. |
| `OrderCancel` | Order/batcher script Consumed event with cancel-redeemer constructor (CSWAP: `d87980`). Decode prior datum to determine `order_kind`. Refund recipient identified via wallet output. |
| `FarmStake` | Farm script consume + produce; LP-token quantity in farm UTxO **grew**. Staker = user wallet that held the LP token in an input. |
| `FarmUnstake` | Farm script consume + produce; LP-token quantity in farm UTxO **shrank**. Staker = user wallet that received the LP token in an output. |
| `RewardClaim` | TX consumes from the brand's reward-distribution operator key wallet (per "Detection-signal stability"). Reward asset(s) appear in a user-wallet output. |

### Interest

Each module ships `[interest].addresses` populated with:

- Pool script address(es) — fixed
- Order / batcher script address(es) — fixed (used for
  `OrderCancel` + transitively for batcher-fee derivation)
- Farm script address(es) — fixed (used for
  `FarmStake` / `FarmUnstake`)
- Operator key wallet(s) used for reward distribution —
  potentially mutable; see "Detection-signal stability".

For CSWAP all four address categories collapse to known
canonical values: one pool script, one order/batcher script
payment-cred prefix, one farm script, one reward-distribution
wallet. Splash will be more involved (per-pool staking-cred
variation requires prefix-match for the pool interest set).

The internal `address → (brand_label, role)` map lives in the
module source as a compile-time const — type-safe, rebuild on
every addition; label/role tweaks are rare enough that the
rebuild cost is fine.

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

**Phase 1: three brand modules with full DEX-action coverage**

Each `<brand>-dex` module emits the full `DexAction` variant
set from day one. Implementation lands sequentially within a
single PR (commit-by-commit) — module-by-module, then
variant-by-variant within each module.

Per-brand scope:

- `community-modules/cswap-dex/` — lift existing decoders;
  smallest scope, validates the broad-coverage template
- `community-modules/splash-dex/` — lift existing V1
  decoders; confirm V2/V3 coverage or build; per-pool
  staking-cred prefix matching
- `community-modules/minswap-dex/` — build Minswap V1 + V2
  decoders from datum-registry references

Per-variant scope (in each module):

| Phase | Variant | Notes |
|---|---|---|
| 1a | `Swap` | Reuses Phase-1-pre work for `cswap-dex` |
| 1b | `LiquidityAdd` / `LiquidityRemove` | Directional-guard split — same-sign pool deltas |
| 1c | `FarmStake` / `FarmUnstake` | New farm-script interest entry per brand |
| 1d | `RewardClaim` | Operator-wallet interest; document fragility per brand |
| 1e | `OrderCancel` | Order/batcher prefix interest + cancel-redeemer check |

Each commit ships fixtures + golden-test coverage for its
variant. Module-build wasm size stays comfortably under 1 MB
in release profile.

**Phase 2: classifier handoff**

- Stand up the TapTools-alternative consumer worker (or
  retrofit an existing one) that subscribes to all three
  `*-dex` modules + `asset-transfer` (for the unknown-DEX gap)
- Parallel-run against the classifier for 24-48h on prod;
  verify event counts agree per brand + richness fields are
  populated as expected
- Retire the classifier's `handle_dex_tx` path (similar to the
  ownership-routing retirement that happened with the
  marketplace migration)

**Phase 3: extend brand coverage**

- Add `sundae-dex` (V1 / V2 / V3) and `wingriders-dex` — each
  its own PR, same broad-coverage template
- `genius-yield-dex`, `muesli-dex`, etc. as priorities surface

**Phase 4: forward-looking variants**

Reserved for variants we don't yet have a fixture-driven case
for. Candidates:

- DEX governance events (veSPLASH stake/unstake, vote casts)
- Cross-pool routing markers (the aggregator hop that ties
  multiple `Swap` events from different pools into one user
  trade — currently consumers infer this from `tx_hash`)
- Pool fee changes (Minswap V2 `allowDynamicFee` paths)

Same wire-compatibility posture: additive variants only.

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
   **resolved**: Phase 1 broadened to include the full
   common-action DEX surface in each `<brand>-dex` module from
   day one. `DexAction` ships with `Swap` /
   `LiquidityAdd` / `LiquidityRemove` / `OrderCancel` /
   `FarmStake` / `FarmUnstake` / `RewardClaim` together.
   `ZapIn` is *not* a separate variant — asymmetric LP
   provision is just `LiquidityAdd` with uneven `quote_added`
   / `base_added` quantities (CSWAP exposes this directly).
   Order-submission TXs (swap / LP-add / LP-remove orders)
   stay silent because the matching fill carries the outcome.

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

9. ~~**LP-token → pool mapping in `LiquidityAdd` / `FarmStake`**~~
   — **resolved**: out of scope for the module. Events emit
   `lp_received` / `lp_token` as raw `SwapAsset { policy,
   asset_name_hex, quantity }`; consumers map LP-policy to
   pool off-line (the mapping comes from a one-time crawl of
   pool datums, which doesn't belong inside a per-action
   community module).

10. ~~**`RewardClaim` operator-wallet fragility**~~ —
    **resolved**: accept fragility. CSWAP-style brands manage
    reward distribution via operator key wallets, not Plutus
    scripts. If the operator rotates wallets, the module
    misses harvests until the new address is added to
    interest. Mitigation: document the operator-wallet
    dependency inline per module, and a monitoring check
    (sudden drop in `RewardClaim` emissions for an active
    brand) prompts a module rebuild. Better than skipping
    `RewardClaim` entirely and leaving a real UX hole.

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
