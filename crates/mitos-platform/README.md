# mitos-platform

The wasm-isolated module runtime (v2) — **the largest crate in the repo**, and
the host half of everything in family A.

> **The crate map is `src/lib.rs`'s `//!` header**, not this file. It lists
> every module and what it does, and it is kept current because it sits next
> to the code. Read that first; this file is orientation around it.

Full design rationale:
[`docs/strategy/MITOS_PLATFORM_V2.md`](../../docs/strategy/MITOS_PLATFORM_V2.md).
The contract itself is the WIT world in `wit-v2/world.wit` — that file is
normative, and the generated host and guest bindings both derive from it.

## Reading order

1. `wit-v2/world.wit` — the ABI. Everything else implements or consumes it.
2. `host_fns_v2` — the host side of each imported interface (chain-data,
   state-kv, emit, logging, interest).
3. `driver_v2` → `follower_v2` → `host_v2` — per-block dispatch, the chain
   follower, lifecycle.
4. `bootstrap_v2` — how historical state hydrates **through the same dispatch
   path** as live activity, so a module sees no distinction.
5. `budget` + `supervisor` — what happens when a module misbehaves.

## What the module header does not tell you

- **This is where the live-chain coupling lives.** Everything else in family A
  is downstream of the follower here. A change to dispatch ordering is felt by
  every deployed module and every companion at once.
- **The dialer is a delivery system, not a function call.** Per-companion
  partition-keyed lane pools keep ordering *within* a key while parallelising
  across keys; 200 / 422 / 5xx map to Ack / Nack / transport-retry. See
  [`DIALER_CONCURRENCY.md`](../../docs/design/DIALER_CONCURRENCY.md).
- **`indexer_data_cache` is permanent, hash-addressed, and shared.** Aux-data
  by tx hash and Plutus datums by datum hash, in `indexer_data.redb`, resolving
  lazily via Maestro on a local miss — aux-data for transactions older than the
  Dolos archive horizon, datums for hash-only CIP-68 references whose preimage
  never landed in Dolos (the snapshot gap). It is what lets bootstrap resolve
  years-old state a local node cannot.
- **`vendored/` is upstream code** (Apache-2.0, see
  `vendored/balius/NOTICE`) — patch deliberately, not incidentally.

## ⚠️ Traps

- **A trap policy is author-declared.** The module exports `trap-policy()` and
  the supervisor consults it on *every* trap. Changing supervisor behaviour
  changes what module authors' declarations mean.
- **Resource limits are configured before any guest call** — fuel, epoch
  interruption, `ResourceLimiter`. A limit added after instantiation does not
  apply to the instance you are looking at.
- **Recapture is coordinated, not local.** It signals companions to drop
  projected state and re-runs bootstrap re-entrantly via `rebootstrap`. A
  module whose bootstrap is not re-entrant will not survive it. See
  [`RECAPTURE.md`](../../docs/design/RECAPTURE.md) and
  [`WASM_BUDGET_CHUNKING.md`](../../docs/design/WASM_BUDGET_CHUNKING.md).
- **The v1 block-CBOR dispatch path is gone** (retired May 2026). Documents
  describing modules receiving raw blocks are historical.
