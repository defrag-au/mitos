# Holder-distribution: multi-DEX LP decomposition

> **PARKED (2026-07-06):** holder-map pivoted to aliens-only; aliens'
> liquidity is CSwap (already decomposed), so no multi-DEX consumer
> exists. The on-chain research below stays authoritative for
> whenever a keeper project lists on another DEX.
>
> **IMPLEMENTATION MOVED (2026-07-05):** the decoders, pool table,
> and `utxos_by_asset` facts below remain authoritative, but the
> decomposition now lands in the **native host-side snapshot
> builder** (`NATIVE_HOLDER_SNAPSHOT_PLANE.md`), not in the wasm
> module. Consequently the WIT `utxos-by-asset` host-fn + module
> bindings + `holder_distribution.rs` state-machine changes in the
> Design/Build-order sections below are dropped — data-plane trait +
> local impl only.

**Status: design draft** (2026-06-24). Extends
[`HOLDER_DISTRIBUTION_LP_DECOMPOSITION.md`](HOLDER_DISTRIBUTION_LP_DECOMPOSITION.md)
(slice 1, CSwap-only) to attribute LP across **all** the DEXes a
tracked token is pooled in, not just CSwap.

Cross-references:
- `HOLDER_DISTRIBUTION_LP_DECOMPOSITION.md` — slice 1; the
  financial redistribution maths (`lp_share`, residual) carries
  over verbatim.
- `DEX_COMMUNITY_MODULES.md` — the per-DEX delta modules (trade
  feed); this doc is the **snapshot** side.
- `mitos-dex-decode` crate — the shared decode unit this extends.

## Problem

Today only **CSwap** LP is decomposed. A pool's aggregate of the
tracked token is redistributed to LP providers only when the pool
sits at `cswap::POOL_SCRIPT_ADDR`. Every other DEX pool is treated
as a plain `Wallet` holder — it shows up as one giant fake holder
and the real LP providers are invisible. Splash is half-wired (its
pool is tagged `DexPool` in `holder_role_for` but has no datum
decoder, so it is never decomposed either).

A token like SNEK is pooled on Minswap, SundaeSwap, Splash and
CSwap **simultaneously**. Correct attribution requires decomposing
*every* pool the token is in.

## What the research established (all on-chain verified, 2026-06-24)

Every in-scope DEX mints a **real, wallet-held LP token** — even
SundaeSwap V3 (CIP-68 label-333; `circulating_lp` in the datum
equals the LP token's on-chain `total_supply`). So the slice-1
attribution model — *enumerate holders of the pool's LP token,
each share = balance / total_supply, redistribute reserves* — is
universal. No DEX needs a different method. The per-DEX
differences are exactly three:

1. **Pool recognition** — the 28-byte pool *payment* script hash
   (match on payment cred; stake part varies per pool).
2. **Datum decode** — field layout differs per DEX/version.
3. **LP unit + total-supply source** — see table.

### Pool payment script hashes (route on these)

| DEX | Pool payment hash | Notes |
|-----|-------------------|-------|
| CSwap | `…` (existing `cswap::POOL_SCRIPT_ADDR`) | per-pool LP policy (from datum) |
| Splash | (existing `splash::POOL_SCRIPT_ADDR`) | decoder TBD |
| Minswap V1 | `e1317b152faac13426e6a83e06ff88a4d62cce3c1634ab0a5ec13309` | PlutusV1, datum-hash style |
| Minswap V2 | `ea07b733d932129c378af627436e7cbc2ef0bf96e0036bb51b3bde6b` | Aiken |
| SundaeSwap V1 | `4020e7fc2de75a0729c3cc3af715b34d98381e0cdbcfa99c950bc3ac` | enterprise (no stake), datum-hash |
| SundaeSwap V3 | `e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b` | hash == LP/NFT mint policy |
| WingRiders V1 | `e6c90a5923713af5786963dee0fdffd830ca7e0c86a041d9e5833e91` | Plutarch |
| WingRiders V2 | (enterprise `addr1wxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhqczc0qfe`) | multiple datum arities |

> ⚠️ **Registry bug**: `shared-crates/address-registry` has Minswap
> V1/V2 **swapped**. The address/hash labelled "Minswap V2"
> (`e1317b15…`) is actually V1; the one labelled "Minswap"
> (`ea07b733…`) is V2. Verified on-chain (the `e1317b15…` address
> holds the V1 pool-NFT policy `0be55d26…`). Fix as part of this
> work.

### LP unit + total-supply source per DEX

| DEX | LP policy | LP name | total_lp |
|-----|-----------|---------|----------|
| CSwap | datum (per-pool) | datum | datum |
| Minswap V1 | `e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86` | = pool-NFT name (poolId) | datum field 2 |
| Minswap V2 | `f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c` (shared authen) | double-SHA3-256 of sorted pair **or read from pool value** | datum field 3 |
| SundaeSwap V1 | `0029cb7c88c7567b63d1a512c0ed626aa169688ec980730c0473b913` | `6c7020` ++ ident (ident = datum field 1) | datum field 2 |
| SundaeSwap V3 | `e0302560…` (== pool script hash) | `0014df10` ++ ident (ident = datum field 0) | datum field 2 (`circulating_lp`) |
| WingRiders V1 | `026a18d04a0c642759bb3d83b12e3344894e5c1c7b2aeb1a2113a570` | per-pool 64-hex (read from pool value) | `2^63−1 − poolHeldLP` |
| WingRiders V2 | `6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737` | per-pool 64-hex (read from pool value) | `2^63−1 − poolHeldLP` |

**Robustness choice: read the LP unit from the pool UTxO's own
value where possible.** Most of these DEXes keep the *unissued* LP
in the pool UTxO (mint-max pattern: Minswap V1/V2, WingRiders). So
at decomp time we already `read_utxos(pool_ref)` (for the reserve);
the LP token is the asset under the LP policy that is **not** the
pool NFT (NFT qty = 1, or distinguished by CIP-67 label prefix).
This sidesteps re-implementing the Minswap V2 SHA3 derivation and
the WingRiders blake2b name guess. SundaeSwap mints exact (pool
holds no unissued LP), so V1/V3 derive the LP name from the datum
ident + the CIP-67 label prefix (`6c7020` / `0014df10`) — no
hashing needed there either.

For WingRiders, `total_lp = (2^63 − 1) − poolHeldLP`, where
`poolHeldLP` is the LP amount in the pool UTxO value — also read
from the value we already have.

### Datum layouts (field index → meaning)

- **Minswap V1** — `Constr 0`, 5 fields: `[assetA, assetB,
  totalLiquidity:int (2), rootKLast:int, feeSharing:opt]`.
- **Minswap V2** — `Constr 0`, 10 fields: `[batchingCred, assetA,
  assetB, total_liquidity:int (3), reserveA, reserveB, feeA, feeB,
  feeShareOpt, allowDynamicFee:bool]`.
- **SundaeSwap V1** — `Constr 0`, 4 fields (datum-**hash**, not
  inline): `[Constr0[coinA,coinB], ident:bytes (1),
  totalLP:int (2), Constr0[feeNum,feeDen]]`. Each coin wrapped in
  its own `Constr 0`.
- **SundaeSwap V3** — `Constr 0`, 8 fields: `[ident:bytes (0),
  assets:[[polA,nmA],[polB,nmB]], circulating_lp:int (2),
  bidFee, askFee, feeManager:opt, marketOpen, protocolFees]`.
  Assets are **bare 2-arrays** (no Constr wrapper, unlike V1).
- **WingRiders V1** — `Constr 0`, 2 fields:
  `[requestScriptHash:bytes, Constr0[Constr0[assetA,assetB],
  lastInteracted, treasuryA, treasuryB]]`.
- **WingRiders V2** — `Constr 0`, **flat, multiple arities shipped
  over time** (5 / ~16 / ~21 fields). Branch on field count.
  `requestValidatorHash` at 0; asset symbols/tokens at 1–4;
  fees/treasuries follow. Decode defensively; tolerate the short
  form. (The reserve we need comes from the pool value minus
  treasury; the LP unit + supply come from the value, so the
  fragile datum tail is largely unneeded for attribution.)

ADA is the empty policy + empty name (`("","")`) in every layout.

## Design

### `mitos-dex-decode` — a uniform decode surface

One module per DEX (`minswap_v1.rs`, `minswap_v2.rs`, `sundae_v1.rs`,
`sundae_v3.rs`, `wingriders.rs`), each exposing its pool payment
hash + a `decode_pool` that, given the **datum CBOR and the pool
UTxO's asset list**, returns a uniform:

```rust
pub struct PoolDecode {
    pub lp_policy: [u8; 28],
    pub lp_name: Vec<u8>,
    pub total_lp: u64,
    // reserve is read by the caller from the pool value
}
```

Plus a top-level `Dex` enum + `dex_for_payment_cred(&[u8;28]) ->
Option<Dex>` and `decode(dex, datum, pool_value) ->
Option<PoolDecode>` dispatch. Splash gets its `decode_pool`
finished here too (closing the half-wired gap).

The existing `CswapPoolDatum`/`decode_pool_datum` stays; CSwap is
adapted to the uniform surface (its `lp_policy` is per-pool from
datum, so it ignores the pool value).

### `holder-distribution` changes

1. **`MetaState.pool_ref: Option<...>` → `pools: Vec<PoolRef>`**,
   each `{ oref, dex: Dex }`. Scan recognises a pool by matching the
   output's **payment credential** against the DEX table (covers
   the per-pool-stake-cred DEXes), and pushes it (dedup by oref).
   `holder_role_for` uses the same table → all DEX pools tag
   `DexPool`.

2. **Reserve per pool from the pool UTxO value**, not the ledger
   holder aggregate — so two pools that collapse to the same holder
   key (CSwap shares one stake cred across pools) don't merge. The
   tracked-token reserve = that pool UTxO's holding of the policy.

3. **LP scan by (policy, name)**, not policy alone. `fold_lp_page`
   filters on the full unit. Farm-staking resolution stays
   CSwap-specific (only CSwap has a farm); other DEXes have no
   staking indirection in scope (Minswap/Sundae yield farms are a
   later slice — their staked LP stays in the pool residual for now,
   logged).

4. **Decomp state machine iterates pools.** Cursor becomes
   `(pool_index, lp_after)`; each pool runs an LP scan +
   `compute_lp_plan` accumulating into shared `lp_shares`, with a
   per-pool residual. The materialise/tail passes are unchanged.

### Scaling — scan LP holders by asset subject, not policy

CSwap's LP policy is per-pool, so `utxos-by-policy(lp_policy)` is
naturally scoped to one pool. Minswap/Sundae/WingRiders use a
**shared** LP policy across *all* their pools, so
`utxos-by-policy(shared_lp_policy)` would return LP holders of
every pool on that DEX. Measured (Koios `policy_asset_list`,
2026-06-24): **Sundae V3 6,579 / Minswap V1 10,089 / Minswap V2
3,237 / WingRiders V2 115 distinct assets** under the shared
policy — and the *UTxO* count (the actual scan size) is larger
still (holders × assets).

This is not just a perf cost — it's a **correctness** risk:
`utxos_by_policy` truncates at a **100K-UTxO hard cap**
(`mitos-data-plane/src/impls/local.rs` `HARD_CAP`). A shared LP
policy that exceeds 100K UTxOs would scan a truncated holder set →
**silent under-attribution**. Minswap V1 (10k assets) is a real
candidate.

**Resolution: scan by asset subject.** Dolos already maintains a
by-asset index — `CardanoIndexExt::utxos_by_asset(subject)`, keyed
on `subject = policy(28) ++ asset_name` (dolos `indexes/delta.rs`).
It is **not yet surfaced** through mitos. Surface it:

1. `mitos-data-plane`: add `utxos_by_asset(&[u8])` to the
   `DataPlane` trait + `impls/local.rs` (mirror `utxos_by_policy`,
   call `indexes().utxos_by_asset(subject)`), plus the
   fallback/dispatch/trap/inspect impls.
2. `mitos-platform`: add the `utxos-by-asset` host-fn to
   `wit-v2/world.wit` + `host_fns_v2/chain_data.rs` (same paged
   `utxo-page` shape + replay-log handling as `utxos-by-policy`).
3. module bindings: expose `chain_data::utxos_by_asset`.

Then `fold_lp_page` scans `utxos-by-asset(lp_policy ++ lp_name)` —
exactly the one pool's LP holders (tens–hundreds), no filter, no
over-scan, no truncation. Maestro-fallback plane: best-effort (its
asset-holders endpoint) or a logged `Unsupported` — the local
dolos plane is the production path.

This is a small, self-contained host change (the index exists), so
it goes **in this slice**, not a follow-up.

## Build order (holder-map first, per priority)

1. **Host `utxos-by-asset`** plumbing (data-plane → host-fn → WIT →
   bindings). Small; unblocks correct, scoped LP scans. Land first
   so the decoders are tested against the real scan path.
2. `mitos-dex-decode`: SundaeSwap V3 + V1 decoders (priority), then
   WingRiders V1+V2, then Minswap V1+V2, then finish Splash. Each
   with a golden datum test from a real on-chain CBOR sample.
3. `holder-distribution`: multi-pool `MetaState` + payment-cred
   recognition + per-pool reserve + by-asset LP scan + pool loop in
   the decomp state machine.
4. Fix the `address-registry` Minswap V1/V2 swap.
5. Deploy: rebuild + redeploy the mitos host (new host-fn) **and**
   holder-distribution wasm, then `recapture` per affected token
   (LP attribution is recapture-time only).

> Deploy note: the host-fn addition means the **mitos binary** must
> be redeployed alongside the module wasm (preprod first). A module
> built against the new `utxos-by-asset` import will fail to
> instantiate on an old host — ship host before module.

The **trade-feed** side (per-DEX delta modules like a `minswap-dex`
/ `sundae-dex` mirroring `cswap-dex`) is explicitly **out of scope
for this slice** — the user prioritised the holder snapshot.

## Verification

- Per-DEX golden test: real pool datum CBOR → expected
  `PoolDecode`. Confirm one live Minswap V2 LP name reproduces via
  SHA3-256 (NIST, **not** Keccak) **only if** we ever derive rather
  than read it.
- End-to-end: pick a token pooled across ≥2 of the new DEXes,
  recapture, and check `sum(LP shares) + residual == pool reserve`
  per pool, and that known LP-provider wallets surface with their
  share.
