# Fallback Provider abstraction + batch metadata prefetch

Status: **proposed** (2026-05-25). Coverage spike done (Koios verified). No code yet.

## Problem

CIP-25 cold-start (the `collection-metadata` onboard pump) is slow and
external-API-cost-heavy for large collections whose mints predate the dolos
archive horizon. Salty Seagulls (`5329a9b8…`, 7,408 assets) took ~13 min to
onboard metadata and made thousands of Maestro calls.

The slowness is **not** the cache. `cip25_metadata` (host_fns/mod.rs:127)
composes `asset_state` (dolos, local, fast) + `tx_metadata`. `tx_metadata`
checks the persistent `IndexerDataCache` (redb, instant) and on a miss calls
`MaestroClient::fetch_aux_data(tx_hash)` — a **synchronous, single-tx HTTP GET
of the whole tx CBOR** (~160 ms). The module calls `cip25_metadata` one asset
at a time, so misses are serial. The cache dedups by tx (assets sharing a mint
tx hit cache), so call count ≈ distinct mint txs — but a one-asset-per-tx
collection still pays N serial round-trips.

Two inefficiencies, both in the *fills*, not the cache:
1. **Serial single calls** — one blocking round-trip per cache-miss.
2. **`fetch_aux_data` pulls the entire tx CBOR** to extract one label-721 block.

Mitos uses Maestro for exactly three things, all single-item GETs today:

| Method (`maestro.rs`) | Endpoint | Used by |
|---|---|---|
| `fetch_aux_data(tx)` | `GET /transactions/{tx}/cbor` | `CachingDataPlane` (host_fns/mod.rs:617) — CIP-25 tx_metadata |
| `fetch_output(oref)` | `GET /transactions/{tx}/outputs/{i}/txo` | `MaestroFallbackPlane` (maestro_fallback_plane.rs:139) — archive-pruned prior-output resolution |
| `fetch_datum(hash)` | `GET /datums/{hash}` | `CachingDataPlane` (host_fns/mod.rs:555) — hash-only CIP-68 ref datums |

## Two changes (synergistic — do together)

### A. `FallbackProvider` trait (pluggable provider)

Generalise the concrete `MaestroClient` into a trait so a deployment can pick
Maestro, Koios, or none. Both injection points already hold
`Option<Arc<MaestroClient>>` (host_fns/mod.rs:471, maestro_fallback_plane.rs:186)
→ change to `Option<Arc<dyn FallbackProvider>>`.

```rust
#[async_trait]
pub trait FallbackProvider: Send + Sync {
    // existing single-item methods (keep for the non-prefetch paths)
    async fn fetch_aux_data(&self, tx: &str) -> Result<Option<Vec<u8>>, FallbackError>;
    async fn fetch_output(&self, oref: &OutputRef, level: DecodeLevel)
        -> Result<Option<TypedOutput>, FallbackError>;
    async fn fetch_datum(&self, hash: &str) -> Result<Option<Vec<u8>>, FallbackError>;

    // NEW batch methods — the win. Default impls fan out to the single
    // methods (bounded concurrency) so a provider without a native batch
    // endpoint still benefits from parallelism; Koios overrides with one
    // HTTP POST.
    async fn fetch_aux_data_batch(&self, txs: &[String])
        -> Result<HashMap<String, Vec<u8>>, FallbackError> { /* default: join_all, bounded */ }
    async fn fetch_outputs_batch(&self, orefs: &[OutputRef], level: DecodeLevel)
        -> Result<HashMap<OutputRef, TypedOutput>, FallbackError> { /* default */ }
    async fn fetch_datums_batch(&self, hashes: &[String])
        -> Result<HashMap<String, Vec<u8>>, FallbackError> { /* default */ }
}
```

- `MaestroProvider` = today's `MaestroClient` (single GETs; batch = default
  bounded-concurrency fan-out).
- `KoiosProvider` = native batch POSTs (below). Single methods wrap the batch
  with a 1-element array.
- Selection by env (`MITOS_FALLBACK_PROVIDER=maestro|koios|none`, key var per
  provider). Default stays Maestro until Koios is validated in prod; `none`
  degrades gracefully exactly as today (no key → `None` → no fallback).

#### Configuration (mirrors the Maestro key flow)

Same pattern as `MaestroClient::from_env()` + `shared()` (`maestro.rs:139`):
each provider lazy-inits a process-wide `OnceLock` from `std::env`, with the
key supplied as an env var in `/etc/default/mitos-mainnet` on the box (restart
to pick up) — identical to how `MAESTRO_API_KEY` is supplied today.

| Var | Maestro | Koios |
|---|---|---|
| selector | `MITOS_FALLBACK_PROVIDER=maestro` (default) | `MITOS_FALLBACK_PROVIDER=koios` |
| key | `MAESTRO_API_KEY` (**required** — absent ⇒ no fallback) | `KOIOS_API_KEY` (**optional** — absent ⇒ free-tier client) |
| auth header | `api-key: <key>` | `Authorization: Bearer <token>` |
| network/base | `MAESTRO_NETWORK` → `{net}.gomaestro-api.org/v1` | `KOIOS_NETWORK`/`KOIOS_BASE_URL` → `https://api.koios.rest/api/v1` |
| concurrency | `MAESTRO_MAX_INFLIGHT` | `KOIOS_MAX_INFLIGHT` |

Key difference: Maestro's `from_env()` returns `None` without a key (no
client). Koios's should return a **keyless free-tier client** when
`KOIOS_API_KEY` is absent (build the reqwest client without the Bearer header) —
so a community operator who just sets `MITOS_FALLBACK_PROVIDER=koios` gets
working fallback at free-tier limits, and the PRO key only raises the ceiling.
The spike ran entirely keyless against `api.koios.rest`, confirming this works.

### B. Batch metadata prefetch in the CIP-25 cold-start path

The module already resolves `asset_state` per asset (cheap, dolos-served),
which carries the mint tx. So in `collection-metadata`'s `scan_page` /
`decode_page`:

1. Collect the page's **distinct mint-tx hashes**.
2. Call a new host-fn `prefetch_tx_metadata(tx_hashes: list<list<u8>>)` once
   per page → host resolves them via `fetch_aux_data_batch` → **populates the
   `IndexerDataCache`**.
3. Decode the page as today — every per-asset `cip25_metadata` now hits cache.

Effect: ~256 serial GETs/page → **1 batched POST**. Because Salty is
batch-minted (one mint tx carried 20 assets in our spike), a single
`/tx_metadata` call resolves all 20 — the dedup compounds the win. Minutes →
seconds for a large collection.

This is provider-agnostic: even on Maestro the prefetch fans out concurrently
(bounded). On Koios it's one POST per page.

## Koios provider — verified coverage (spike 2026-05-25)

Against Salty (`5329a9b8…`, CIP-25, mints past our archive horizon), Koios free
tier (`https://api.koios.rest/api/v1`):

| Need | Koios endpoint (POST, batch) | Spike result |
|---|---|---|
| aux_data / 721 | `/tx_metadata` `{_tx_hashes:[…]}` | ✅ returned 721 with **20 assets** for mint tx `b950e72b…` — metadata-only (lighter than Maestro's full-CBOR GET) |
| prior-output (spent!) | `/utxo_info` `{_utxo_refs:[…],_extended:true}` | ✅ spent ref `1f3e111b…#0` → `is_spent:true`, addr + 95144528 lovelace — **identical to Maestro** |
| datum-by-hash | `/datum_info` `{_datum_hashes:[…]}` | ✅ returned cbor bytes |

So Koios covers all three with parity, all batch-capable, and is a full-history
indexer (resolves old mints + spent outputs — the two coverage risks both
pass).

### Cost / limits
- Koios PRO ≈ ₳128/mo (~$30), 500k req/day, 250 req/10 s (25/s), 60 s timeout.
- With batching (~50 refs/call), a 7,400-tx onboard ≈ 148 calls = seconds,
  trivially within rate limits. Once batched ~50×, modest collections fit
  Koios **free** tier — relevant for community mitos operators running their
  own modules.
- Cheaper than Maestro per-request tiers; flat-rate predictability.

## Long-term

The durable fix for the whole archive-horizon fallback class is a **persistent
aux/datum store in dolos** (the operator is building dolos), so there's no
external call at all. The `FallbackProvider` abstraction + batch prefetch is the
right interim — and the prefetch restructure is worth doing regardless of who
backs the provider (or even once dolos serves it natively, the batch host-fn
shape still helps).

## Phasing

1. **`FallbackProvider` trait + `MaestroProvider`** (no behaviour change;
   default batch = bounded fan-out). Swap the two `Option<Arc<MaestroClient>>`
   fields. Low risk, pure refactor.
2. **`prefetch_tx_metadata` host-fn + collection-metadata collect-then-decode.**
   The actual speedup; works on Maestro (parallel) immediately.
3. **`KoiosProvider`** (native batch endpoints) behind the env selector;
   validate in prod alongside Maestro before flipping the default.
4. (later) dolos persistent aux/datum store supersedes external fallback.

## Open questions

- Prefetch granularity: per-page (simple, bounds the batch to the adaptive page
  size) vs whole-predicate (bigger batches, but unbounded resident tx-hash set
  — page-level is the safe default).
- `fetch_output` batch on Koios `/utxo_info` returns `_extended` asset lists —
  confirm the `TypedOutput` mapping captures multi-asset values + inline datums
  for the `WithDatum`/`Full` decode levels (spike only checked a no-datum,
  ADA-only spent output).
- Reference scripts: Maestro `/txo` doesn't surface `script_ref` (noted in
  `fetch_output` docs); check whether Koios `/utxo_info` does, in case a future
  caller needs it.
- Should `prefetch_*` be CIP-25-specific or a general host-fn other modules can
  use (e.g. holder-distribution's LP/vesting decode also does fallback reads)?
  Lean general.

See [[../../docs/design/COLD_START_TRAP_ISOLATION.md]] (the onboard pump this
runs inside) and the auto-onboard reliability work (rebootstrap-mode) that made
onboard scoped + observable.
