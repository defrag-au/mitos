# Holder-distribution LP decomposition

**Status: design draft** (2026-05-19, rev 2). Scopes the
enrichment-parity work for the holder-map mitos cutover — the
dolos-native port of LP-pool decomposition. Rev 2 replaces the
spec-delivery design with **auto-discovery** (the module finds
the pool itself; the consumer supplies nothing). Handle
resolution is noted as an adjacent, separable concern.

Cross-references:
- `HOLDER_DISTRIBUTION_MODULE.md` — the module this extends.
- `cnft.dev-workers/docs/design/HOLDER_MAP_MITOS_CUTOVER_PLAN.md`
  — this is the substance of that plan's Phase 2.
- `WASM_BUDGET_CHUNKING.md` — the paged `chain-data` host-fns +
  re-entrant `rebootstrap` this builds on.

## Context

`holder-distribution` emits a per-stake-credential holder ledger
for a tracked policy. The holder-map dApp renders a distribution
donut; two bands — **Mothership** (the LP pool's holding) and
**Scout Vessels** (LP tokens attributed back to wallets) —
require *LP decomposition*: redistributing a CSwap pool's
aggregate holding of the token to the wallets that provided the
liquidity.

The legacy (pre-mitos) holder-map did this in a Maestro-based
reconciler (`services/token-holders/src/lp_staking.rs`). The
mitos path doesn't — `feed_do_mitos::classify` hard-codes
`lp_amount: 0`, so Scout Vessels is empty and the pool shows as a
plain wallet.

**Maestro is not required.** The legacy pipeline used Maestro
only because it predates the mitos/dolos stack. Everything LP
decomposition needs is on-chain current state, which dolos serves
through the `chain-data` host-fns. This design is dolos-native —
it satisfies the cutover's Phase 5 (Maestro elimination) rather
than fighting it.

## Principle: one module, one snapshot

LP decomposition is a **transformation of the holder snapshot** —
"the pool's aggregate holding, redistributed to its LP
providers" — not a standalone fact. Its output (`lp_amount` per
wallet) is only meaningful against the same wallet set, at the
same anchor slot, as the holder ledger.

So the decomposition is produced **by `holder-distribution`
itself, as a step in the same cold-start / `rebootstrap` scan**
that builds the ledger — one module, one internally-consistent
snapshot.

It is explicitly **not**:
- a separate module — that forces the consumer to join two async
  event streams keyed by stake credential, with independent
  anchors and recapture timing; fragile, and it pushes real
  logic back into the "dumb consumer."
- a feature of `cswap-dex` — that module is event-driven and not
  token-scoped; its output would still need joining to the
  holder snapshot.

The DEX modules' genuine reuse unit is the **decode library**
(`mitos-dex-decode::cswap` — pool datum, staking datum, share
math), which both `cswap-dex` and this decomposition step call.
Composition over coordination.

> **Note (2026-05-19):** `mitos-dex-decode` does *not* exist yet.
> The CSwap pool/order/farm address constants and the
> `decode_pool_datum` / staking-datum decoders currently live
> **inline inside `cswap-dex/cswap_dex.rs`**, private to that
> module. Community modules share code only via a crate listed
> in their `.toml` `[deps]`. So the first implementation step is
> to extract that decode logic into a new `crates/mitos-dex-decode`
> crate and refactor `cswap-dex` to depend on it — see *Work
> breakdown* item 2.

## Principle: the consumer supplies nothing

The holder-map worker is a **dumb `holds-policy` consumer** — it
says "I'm interested in policy X" and writes back whatever the
module emits. LP decomposition must not erode that. The worker
must not have to know `pool_address`, `lp_policy_id`, or which
DEX a token trades on — those are chain facts, and recognising
chain shapes is the *module's* job, not the consumer's.

Everything LP decomposition needs is therefore either **derivable
from chain state** or a **protocol constant** the decode library
already owns. Nothing rides the registration. See *Auto-discovery*
below.

## Design

### The decomposition step

When `holder-distribution` cold-starts (or `rebootstrap`s) a
policy `X`, after building the holder ledger it additionally:

1. **Detects the pool.** The holder scan enumerates
   `utxos-by-policy(X)`; the pool's UTxO is *already in that set*
   — it is simply a holder of `X` whose address is a DEX pool
   script. As each holder UTxO is bucketed, its payment
   credential is also tested against the known DEX pool script
   hashes (`mitos-dex-decode` constants — see *Auto-discovery*).
   A match flags the pool UTxO ref and its `X` balance. No pool
   → no decomposition; the policy is emitted as today.
2. **Decodes the pool datum.** `read-output-datums` on the
   flagged pool UTxO → the CSwap pool datum yields
   `total_lp_supply`, `lp_policy_id`, `lp_asset_name_hex`. The
   pool's reserve of `X` is the flagged UTxO's own `X` balance
   (already computed by the holder scan).
3. **Enumerates the LP-token holders** — `utxos-by-policy(
   lp_policy_id)` — covering staked (at the farm contract) and
   unstaked alike. Staked UTxOs are decoded from the CSwap
   staking datum to recover the staker's stake credential.
4. Per LP holder: `share = lp_token_amount × pool_X_reserves /
   total_lp_supply` (u128 intermediate).
5. **Rewrites the ledger:** drop the pool's aggregate holder; for
   each LP provider add `share` to their balance and record it as
   `lp_amount`. The rounding remainder stays as a residual pool
   entry.

All chain reads are the same `chain-data` host-fns the holder
scan already uses, at the same `anchor-slot`. The LP-token
enumeration is itself a paged `utxos-by-policy` scan — it slots
in as an additional phase of the per-predicate re-entrant
`rebootstrap` state machine (see `WASM_BUDGET_CHUNKING.md`),
re-entrant for the same budget reasons.

### Auto-discovery — no spec, no registration change

The decomposition is **discovered, not configured**. The facts
it needs all come from one of two places:

- **Protocol constants** — the DEX pool script address(es) and
  farm contract address. These are *not* per-policy: every CSwap
  pool sits at one shared pool script address. They live in
  `mitos-dex-decode` alongside the datum decoders, exposed as e.g.
  `cswap::POOL_SCRIPT_HASH` / `cswap::FARM_SCRIPT_HASH`.
- **Chain state** — the CSwap pool datum *itself carries*
  `lp_policy_id`, `lp_asset_name_hex` and `total_lp_supply`. Once
  the pool UTxO is found, decoding its datum yields everything
  else. The pool's `X` reserve is the UTxO's own balance.

So the module needs **nothing from the consumer** beyond the
`holds-policy` interest it already registers. Detection is one
payment-credential comparison per holder UTxO — folded into the
scan the module already runs — against a small constant set of
DEX pool script hashes. Pool present → decompose; absent → emit
as today. A plain CNT with no pool costs only the comparisons.

This means **no WIT change and no second registration path.** The
"spec delivery" question that an earlier draft treated as the
first design item to settle simply dissolves.

### Splash and the discovery set

CSwap has a single pool script address — the cleanest case.
Splash pools sit at **multiple** script addresses across contract
versions, with no single discovery address. That is still a
*finite, known set of version addresses* — also a constant the
decode library can own. Auto-discovery generalises: the module
tests holder UTxOs against the **union** of known DEX pool script
hashes; the decode library owns "here are DEX D's pool
addresses" per DEX. The detected DEX kind selects the datum
decoder.

CSwap ships first; Splash follows when its constants + decoder
land in `mitos-dex-decode::splash`.

### Future: optional override

Auto-discovery covers every standard pool. A genuinely
non-standard or custom pool — a bespoke contract the decode
library doesn't know — would need an explicit override. If that
case ever arises, the registration can carry an *optional*
decomposition spec (`pool_address`, `lp_policy_id`, DEX kind)
that overrides discovery; that is the only reason to extend the
`interest-predicate` vocabulary. **Not day-one work** — add it
when a token actually needs it. Until then the consumer stays
dumb.

### Wire change

`mitos_community_events::holder_distribution::HolderEntry` is
currently `{ stake_cred_hex, assets }`. It gains an `lp_amount`
field so the per-wallet attribution travels with the holder. The
holder-map consumer (`feed_do_mitos`) writes it through to the
`token_holders.lp_amount` column it already has. This is a
`mitos-community-events` wire change — consumers need a rev bump,
same as the Phase 4 chunked-snapshot change.

This is the *only* consumer-visible change: a field that arrives
populated, written straight through. No new registration
argument, no new channel.

### LP-awareness surface — bounded

Auto-discovery means `holder-distribution` *does* recognise LP
pools — but the surface is deliberately narrow:

- It does **not crawl the chain for pools.** It inspects the
  holder set it already enumerated and tests each address against
  a **constant set** of DEX pool script hashes. Discovery is
  O(holders) comparisons against a handful of constants, no extra
  scan.
- The LP "knowledge" is two things: a constant address set per
  DEX in `mitos-dex-decode`, and a `match` on the detected DEX
  kind to a decoder. The module owns *when* to decompose; the
  decode library owns *how* to read a given DEX's datums and
  *where* its pools live.
- Adding a DEX is a new decoder + a new address constant in the
  shared library, plus a new `match` arm here — not a structural
  change to the module.
- The module gains **no per-policy config and no consumer
  coupling.** Every pool-specific fact is either a chain read or
  a library constant.

So `holder-distribution` becomes "LP-pool-decomposition-capable
by recognising known DEX pools," not "hard-wired to every pool on
chain" and not "configured per policy by the consumer."

## Handle resolution (adjacent — separate decision)

Wallet `display` showing ADA `$handle`s is the other legacy
enrichment. It never used Maestro (the legacy path hits Handle.me
directly, stake-keyed). Two routes:

- **dolos-native** — `$handle` is an NFT policy;
  `utxos-by-policy($handle_policy)` enumerates handle ownership.
  On-chain, no external service — but basic only; misses
  Handle.me's virtual-subhandle / default-handle logic.
- **Handle.me service** — port `cnft.dev-workers/services/handle`
  as a holder-map-side enrichment alarm (it's async + HTTP, can't
  run in `apply_event`).

Independent of the LP work and of Maestro. Decide separately; the
LP decomposition does not block on it.

## Vesting

`holder_vests` / the Vested band is covered by the
`vesting-tracker` module channel (cutover plan Phase 4) — out of
scope here.

## Work breakdown

1. `HolderEntry.lp_amount` wire field (`mitos-community-events`) +
   `feed_do_mitos` consumer write-through. No other consumer
   change — there is no spec to pass.
2. **Create `crates/mitos-dex-decode`** by extracting the CSwap
   decode logic from `cswap-dex/cswap_dex.rs`: the pool / order /
   farm address constants, `CswapPoolDatum` + `decode_pool_datum`,
   the staking-datum decoder, and the share math. Refactor
   `cswap-dex` to depend on the new crate (add it to
   `cswap_dex.toml` `[deps]`, replace the inline copies with
   `use mitos_dex_decode::cswap::*`). `cswap-dex`'s `tests/` dir
   is the regression net.
3. Decomposition step in `holder-distribution`'s cold-start +
   `rebootstrap` scan, as a re-entrant phase: pool detection
   folded into the holder scan, then datum decode + LP-holder
   enumeration + ledger rewrite, using `mitos-dex-decode::cswap`.
4. Verify — recapture → dev donut's Mothership + Scout Vessels
   match the legacy prod endpoint.

## Open questions

1. ~~**Spec delivery**~~ — **Resolved 2026-05-19: no spec.** The
   module auto-discovers the pool from the holder set it already
   scans, using protocol-constant DEX pool addresses + the pool
   datum's own contents. No WIT change, no registration argument.
   An optional override spec is a future extension, added only if
   a non-standard pool ever needs it.
2. **Handle resolution route** — dolos-native vs Handle.me
   service. Separable; decide later.
3. **Module ownership** — ~~extend the generic `holder-distribution`
   community module vs a holder-map-owned module.~~ **Resolved
   2026-05-19: extend `holder-distribution`.** Decomposition is a
   generic capability — recognise a known DEX pool, redistribute
   it. With auto-discovery there is no project-specific config at
   all; see *LP-awareness surface* above for the bound on what
   the module gains.
