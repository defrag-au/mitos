# Price Oracle (DEX-module consumer)

## Goal

A token price feed for Cardano native assets, built as a **mitos consumer
companion** on top of the `<brand>-dex` community modules. Derive spot,
last-traded, and time/volume-weighted prices from the rich `DexAction::Swap`
events (pool reserves + effective price the modules already emit), aggregate
across pools and brands, and anchor to USD from on-chain stablecoin pools — so
we stop depending on Charli3.

This is the "TapTools-alternative consumer worker" anticipated by
`docs/design/DEX_COMMUNITY_MODULES.md` (Phase 2). Chain recognition lives in the
DEX modules; the price *projection* lives here, in the consumer — same layering
posture as every other companion.

## Why build it

- **Charli3 is underwhelming in practice** for the tokens we care about, and it's
  a paid external dependency feeding a single 30-line client
  (`cnft.dev-workers/projects/wallet-viewer/src/charli3.rs`) that only powers the
  wallet-viewer portfolio total.
- **We already own the inputs.** The live `cswap-dex` and `splash-dex` modules
  emit `Swap` events carrying `pool_reserves_before/after`, `pool_fee_bps`, and an
  exact `effective_price` rational. Those events already stream into a deployed
  consumer (`cnft.dev-workers/projects/holder-map/src/dex_feed_do.rs`) which today
  **discards** the pool/price fields and keeps only raw amounts.
- **We already do the pull-model version, narrowly.**
  `cnft.dev-workers/services/price-book` turns CSWAP pool reserves into a cached
  `PriceQuote` (FT) + Anvil floor (NFT), behind an HTTP endpoint consumed by
  `loan-book`. It is CSWAP-only, single-pool, raw spot, ADA-denominated. The
  oracle generalises this and flips it from pull to push.
- **It de-risks `loan-book`.** loan-book values collateral off price-book's raw
  single-pool spot, which is flash-manipulable — a real liquidation/exploit risk
  for a lending protocol. A manipulation-resistant oracle (VWAP/TWAP +
  multi-pool + min-liquidity) hardens it as a side effect.

## Current state (as of 2026-05)

| Piece | Where | State |
|---|---|---|
| DEX recognition (CSWAP, Splash) | `community-modules/{cswap-dex,splash-dex}` | live; emit full `DexAction` incl. price-bearing `Swap` |
| DEX recognition (Minswap) | `minswap-dex` | **planned, not built** (~2-3 days, see DEX doc) |
| Consumer subscribed to DEX modules | `cnft.dev-workers/.../dex_feed_do.rs` | deployed; **discards** `pool_reserves_*` / `effective_price` |
| Pull-model spot price | `cnft.dev-workers/services/price-book` | CSWAP-only, raw spot, ADA-only; used by loan-book |
| AMM math | `cnft.dev-workers/../cardano-tx/dex/{cswap,splash}` | `constant_product_swap`, `select_estimated_price`, `find_optimal_split` |
| Token decimals / metadata | `cnft.dev-workers/services/policy-info` | CF-registry resolver (name/ticker/decimals/logo) |
| ADA/USD | — | **does not exist anywhere**; everything is lovelace/ADA |
| Charli3 client | `cnft.dev-workers/projects/wallet-viewer` | only consumer; portfolio valuation |

## "Price" is not one number

The oracle must be explicit about which price it serves, because each is computed
differently and is fit for a different purpose:

| Price | Computed from | Good for | Weakness |
|---|---|---|---|
| **Spot (mid)** | latest pool reserves, fee-adjusted | display, "current price" | manipulable; per-pool |
| **Last-traded** | most recent `Swap.effective_price` | "last fill" | single trade, can be tiny/huge |
| **VWAP (window)** | trades in window, volume-weighted | portfolio valuation, charts | lags fast moves |
| **TWAP (window)** | reserve snapshots over time | manipulation-resistant valuation (loan-book) | lags; needs periodic sampling |
| **Aggregate** | liquidity-weighted across pools/brands | canonical token price | needs per-pool liquidity |

The DEX modules hand us the raw material for all of these on every swap, so the
oracle is mostly bookkeeping + weighting, not new chain work.

## Architecture: push-model companion

Prefer the push model (subscribe to DEX modules) over pull (poll pool UTxOs via
Maestro):

- **Pull** (price-book today): one Maestro call per cache-miss, point-in-time,
  consumer picks the pool. Fine as a fallback / cold-start.
- **Push** (this design): the modules recognise every pool-touching TX in real
  time and hand us `pool_reserves_after` + `effective_price` per swap. A companion
  DO keeps a live price table with **zero per-query Maestro cost**, and the trade
  stream gives VWAP / volume / 24h change for free.

Shape: a **Flavor-A mitos-companion DO** (per
`cnft.dev-workers/docs/howto/CARDANO_DO_APPS.md`) that subscribes to
`cswap-dex` + `splash-dex` (+ `minswap-dex` when it lands), maintains per-pool
reserves and a rolling trade window, derives the prices above, and serves an
ETag-cached HTTP read API.

```
 cswap-dex ─┐
 splash-dex ─┤  Swap{reserves_after, effective_price, pool_fee_bps, pool_id}
 minswap-dex ┘            │  (HTTP dial-back, per the companion contract)
                          v
                 PriceOracleDO (SQLite)
                   ├─ pools(pool_key → reserves, fee, slot)
                   ├─ trades(unit, slot, price, volume)   ← rolling window
                   └─ token_price(unit → spot/vwap/last/liquidity/ada_usd)
                          │
                          v
                 GET /price/{unit}, /prices, /ohlc   (ETag-cached)
```

## Data model (sketch)

```sql
-- One row per pool instance we've seen touched.
CREATE TABLE pools (
    pool_key       TEXT PRIMARY KEY,   -- (pool_id, dex_brand, contract_version) — see open Q1
    unit           TEXT NOT NULL,      -- the non-ADA leg, {policy}{asset_name_hex}
    base_reserve   INTEGER NOT NULL,   -- token reserve (base units)
    quote_reserve  INTEGER NOT NULL,   -- lovelace reserve
    fee_bps        INTEGER,
    updated_slot   INTEGER NOT NULL
);

-- Rolling trade window (prune > 24-48h). price stored as a rational to keep precision.
CREATE TABLE trades (
    tx_hash    TEXT NOT NULL,
    unit       TEXT NOT NULL,
    slot       INTEGER NOT NULL,
    price_num  INTEGER NOT NULL,       -- from Swap.effective_price (out_qty, in_qty)
    price_den  INTEGER NOT NULL,
    volume_ada INTEGER NOT NULL,       -- lovelace leg
    PRIMARY KEY (tx_hash, unit)
);

-- Derived, served. Recomputed on apply (cheap) or on a sampling alarm (TWAP).
CREATE TABLE token_price (
    unit          TEXT PRIMARY KEY,
    spot_lovelace REAL,               -- liquidity-weighted spot across pools
    vwap_1h       REAL,
    vwap_24h      REAL,
    last_lovelace REAL,
    liquidity_ada INTEGER,            -- summed across pools (the trust signal)
    ada_usd       REAL,               -- from the stablecoin sub-feed
    volume_24h_ada INTEGER,
    updated_slot  INTEGER,
    stale         INTEGER NOT NULL DEFAULT 0
);
```

Reserves are base units; **price per whole token needs `decimals`** from
`policy-info` — a required integration point, not an afterthought.

## ADA/USD without an external API

The genuinely missing primitive. Neat approach: derive ADA/USD **on-chain from
ADA/stablecoin pools** (DJED, iUSD, USDM) consumed through the *same* DEX modules
— no external feed. Use a **median across the stablecoins** to resist any one
depegging, and mark the USD leg `stale` if the stablecoins disagree beyond a
threshold. An external CEX ADA/USD reference is an optional cross-check, not a
dependency.

## Manipulation resistance

Don't serve raw spot for anything financially load-bearing:

- **Min-liquidity floor** — ignore pools below a TVL threshold; flag tokens whose
  total liquidity is too thin to price.
- **VWAP/TWAP over a window** as the canonical valuation number; spot is for
  display only.
- **Outlier rejection** — drop trades whose effective price deviates wildly from
  the window median (sandwich/wash artifacts).
- **Liquidity-weighted aggregation** across pools/brands so a tiny manipulated
  pool can't move the headline price.

`loan-book` should consume TWAP (with a deliberately long-ish window), not spot.

## API

```
GET  /price/{unit}            -> { spot, vwap_1h, vwap_24h, last, liquidity_ada,
                                   ada_usd, volume_24h, updated_slot, stale }
POST /prices                  -> batch (≤ N units)
GET  /prices?min_liquidity=…  -> top tokens by liquidity/volume
GET  /ohlc/{unit}?window=1h   -> candles (Phase 4, for charts)
```

Read API, ETag-cached off an `updated_slot`-derived generation (the
collection-ownership pattern). No keys, public read.

## Relationship to existing pieces

- **price-book** becomes a thin client of the oracle (or is absorbed by it). Its
  Maestro pull path survives as the cold-start / fallback source for a pool we
  haven't seen a swap on yet.
- **loan-book** swaps raw-spot collateral valuation for the oracle's TWAP.
- **wallet-viewer** drops Charli3, points portfolio valuation at the oracle.
- **policy-info** supplies `decimals` (and ticker/logo) — the oracle does *not*
  re-implement token metadata. (This supersedes the metadata half of the old
  `cnft.dev-workers/docs/design/TOKEN_REGISTRY.md`, which `policy-info` already
  covers; and the price half of `TOKEN_REFUND_ORACLE.md`.)

## Coverage reality check

The oracle is only as broad as the DEX modules feeding it. Today that's **CSWAP +
Splash**. **Minswap — the largest share of Cardano liquidity — is not yet a
module** (`minswap-dex`, ~2-3 days per the DEX doc), so a launch-now oracle has a
real coverage hole on Minswap-only tokens. The long tail of unknown DEXes is
catchable via `asset-transfer` (pool→user transfers) but without reserves, so
those degrade to last-traded-amount only. Be honest about coverage in the API
(`liquidity_ada` + `stale` are the signals consumers should gate on).

## Phased delivery

**Phase 1 — ADA-denominated, CSWAP + Splash.** Stand up `PriceOracleDO`
(or extend `dex_feed_do.rs` to stop discarding the rich fields). Persist pools +
trades; serve spot / last / VWAP in ADA. Validate against price-book + a couple
of TapTools/CEX spot checks.

**Phase 2 — USD + hardening + cutover.** Add the stablecoin-pool ADA/USD
sub-feed; liquidity-weighted aggregate; min-liquidity + outlier guards; the HTTP
read API. Cut loan-book over to TWAP and wallet-viewer off Charli3.

**Phase 3 — Minswap (and beyond).** Once `minswap-dex` lands, subscribe; then
`sundae-dex` / `wingriders-dex` as they ship (DEX doc Phase 3). Coverage widens
without consumer changes.

**Phase 4 — history + alerts.** OHLC candles for charts; price-threshold alerts
(naturally a producer for the alert pipeline in `cnft.dev-workers`).

## Open questions

1. **Per-pool keying.** The `Swap` event carries `pool_id` (asset-pair
   fingerprint, intentionally stable across brands/versions) + `dex_brand` +
   `contract_version`, but **not a unique pool-instance id**. For a pair with
   multiple pools on the same brand+version, the consumer can't distinguish them
   from the event alone. Most major pairs have one canonical pool per brand, so
   `(pool_id, dex_brand, contract_version)` is a workable Phase-1 key — but if
   multi-pool-per-pair matters, the module may need to surface the pool UTxO /
   pool NFT. Decide before relying on per-pool reserves for aggregation.
2. **Where it lives** — a standalone `PriceOracleDO`, or fold into the existing
   `DexFeedDO` (which already subscribes to the same modules)? Folding reuses the
   subscription; splitting keeps the holder trade-feed and the price projection
   independently evictable.
3. **ADA/USD basket** — which stablecoins, and the disagreement threshold for
   marking USD stale.
4. **Window lengths** — VWAP (1h/24h) and the loan-book TWAP window; trade-table
   retention.
5. **No-DEX-liquidity tokens** — fall back to Anvil floor (already in price-book)
   for NFT-ish assets; flag FTs with no qualifying pool rather than guess.
6. **Decimals dependency** — cache `decimals` from policy-info in the oracle, or
   resolve per request? (Cache; decimals are immutable.)

## References

- `docs/design/DEX_COMMUNITY_MODULES.md` — the modules this consumes; `DexAction`
  / `Swap` event shape (`pool_reserves_*`, `effective_price`, `pool_id`).
- `docs/strategy/MITOS_COMPANION_RUNTIME_V1.md`,
  `docs/HOWTO_CONSUMING_A_COMMUNITY_MODULE.md` — the companion runtime + consumer
  contract.
- `cnft.dev-workers/docs/howto/CARDANO_DO_APPS.md` — the worker-side
  Flavor-A companion shape this follows.
- `cnft.dev-workers/services/price-book` — the pull-model precursor (CSWAP spot +
  Anvil floor) the oracle generalises.
- `cnft.dev-workers/shared-crates/cardano-tx/dex/{cswap,splash}` —
  `constant_product_swap`, `select_estimated_price`, `find_optimal_split`.
- `cnft.dev-workers/projects/holder-map/src/dex_feed_do.rs` — existing DEX-module
  consumer (currently discards the price-bearing fields).
- `cnft.dev-workers/services/policy-info` — token decimals/metadata source.
- Supersedes: `cnft.dev-workers/docs/design/TOKEN_REGISTRY.md` (price half) and
  `cnft.dev-workers/docs/design/TOKEN_REFUND_ORACLE.md`.
