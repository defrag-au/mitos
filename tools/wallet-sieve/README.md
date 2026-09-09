# wallet-sieve

One wallet's flow story, excavated from raw chain bytes on demand.

Family B (see the root README): reads Mithril immutable chunk files directly.

The trick is that **the sieve passes need no index at all** — a wallet's
28-byte credentials are searched for as raw bytes across the chunk files, and
CBOR is decoded only for blocks that hit. Only *sender resolution* needs the
tx-index. Scanning machinery lives in `crates/chain-sieve`, shared with every
other "needle in 225 GB" tool.

## Three passes

| pass | flag | what it finds |
|---|---|---|
| **A. cred scan** | always | every output paying the wallet, and — via change outputs — most spends |
| **B. sweeps** | `--sweeps` | change-less spends, by searching the wallet's own tx hashes (a spending tx names its source hash in raw bytes) |
| **C. resolve** | `--resolve` | the *senders* behind receipts: one tx-index point lookup per foreign input |

⚠️ **Pass B is off by default because it was measured near-useless** — 2 of 870
transactions on a real wallet. It is kept because "measured not worth it" and
"not implemented" are different claims, and the flag lets the next person
re-measure rather than re-derive.

MEASURED 2026-08-25 on cardano-infra ($djo, full history): pass A **71.5 s for
225.5 GB at 3.15 GB/s**, zero false-positive blocks; resolve 150.3 s with early
exit.

## Commands

```bash
# One-shot, JSONL out.
wallet-sieve scan --data-dir <db> --target <$handle | addr | stake> \
    [--resolve --index-dir <tx-index>] [--sweeps]

# Hosted read surface: per-wallet cache, incremental refresh from a chunk
# cursor, bearer auth. Same shape as market-ledger serve.
wallet-sieve serve --data-dir <db> [--index-dir <tx-index>] [--db <cache>]
```

| route | what |
|---|---|
| `GET /flows/{target}` | job state + what is cached |
| `POST /flows/{target}/refresh` | scan or top up this wallet |
| `GET /flows/{target}/events` | the flow feed |
| `GET /flows/{target}/tx/{hash}` | one transaction |
| `GET /health` | |

A target is a `$handle`, a payment address, or a stake key.

## Downstream

`serve` also maintains the **chain-tail spool** (`tail.db`) that `token-ledger`
reads every 300 s for its volatile tip passes — the stretch between the
immutable snapshot and the live chain. That is a real coupling: if the spool
stops advancing, token-ledger's archives silently stop seeing new movements
while still reporting themselves healthy over the immutable range.

## Traps

- ⚠️ **A phase-2 failed transaction's outputs were never created.** The sieve
  skips them in both `extract_cred_hits` and `extract_sweep_hits` — the sweep
  matters more, because an invalid transaction *names* inputs it never
  consumed, so without the guard an owned UTxO reads as swept away while it is
  still sitting there.
- Cache eviction needs a `VACUUM`; see the sieve-bounds notes.
- `canonical` target resolution is shape-dependent — a handle, an address and
  a stake key do not resolve through the same path.
