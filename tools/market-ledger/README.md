# market-ledger

Deep marketplace-history walker + ledger for CNFT venues. Walks certified
immutable-DB history locally, decodes with the shared `mitos-marketplace-decode`
crate (the same source of truth the live modules use), keeps the deep corpus
on-box (sqlite → Parquet), and pushes the hot window to D1.

**NOT a second follower store** — it reads Mithril immutable-DB chunk files
(plain files, no locks), never the live dolos/mitos redb; phase-3 `follow` is
a self-contained chainsync tail (not a mitos companion subscriber). See
`docs/design/MARKET_LEDGER.md` for the full design.

## Modes

```
market-ledger bootstrap --download-dir <dir>          # mithril-client CLI → immutable DB
market-ledger walk --data-dir <dir> --db ledger.db    # decode history → sqlite ledger (resumable)
market-ledger stats --db ledger.db                    # corpus-sanity report
market-ledger seal --db ledger.db --out-dir parquet   # completed months → Parquet (duckdb CLI)
market-ledger upload --db ledger.db --endpoint <url>  # hot window → worker D1 ingest
market-ledger serve --db ledger.db                    # HTTP read surface (compact binary)
# follow (phase 3) — not yet implemented
```

## Where it runs

Real runs are **on cardano-infra** (the mitos box): a Mithril snapshot is
~250–280G transient, and the box has the disk + bandwidth. Build/unit-test
locally; bootstrap/walk/seal/upload on the box, under `nice`/`ionice` to protect
the co-tenant tip-follow. On-box prerequisites: the `mithril-client` and `duckdb`
CLIs on PATH.

## Resume

`walk` checkpoints the per-venue cursor **and** the open-book buffer to sqlite
every `--checkpoint-every` blocks. A re-run reloads the buffer and continues
from the cursor — it never re-walks and never re-hits an indexer. `--fresh`
ignores the saved state and restarts from the venue floor.

**Crash-visible progress.** Each checkpoint also writes a small JSON mirror
(`<db>.checkpoint.json`, atomic temp+rename) with the resumable slot/height/hash
+ counters, and a `done:true` marker on completion — so after a crash/kill,
`cat <db>.checkpoint.json` shows exactly where the resumable point is (it never
runs ahead of what a resume would use). Override with `--checkpoint-file`.

**Clean restart.** `market-ledger reset --db … [--parquet <dir>] --yes` deletes
the ledger (db + `-wal`/`-shm`) + checkpoint (+ optional Parquet); without
`--yes` it dry-runs. For automation, `walk --reset-flag <path>`: if that file
exists at startup, the walk wipes + restarts fresh and consumes the flag
(`--reset-parquet` to also clear Parquet). `immutable/` is never touched.

Input/datum resolution is **local-first**: the outref buffer + the spending tx's
`plutus_data` witnesses resolve everything a normal walk needs with zero indexer
calls (a datum_cache → Koios fallback for the rare unrevealed hash-only datum is
a later addition).

## Serve

`serve` is the consumer path: a read microservice over the ledger answering
market-history queries in the compact `market-ledger-wire` postcard format
(consumers PULL from it; the D1 `upload` path is legacy/backfill). It opens the
db read-only per request, so running it against a ledger a walk is actively
writing is fine (WAL, single writer).

```
market-ledger serve --db ledger.db --listen 127.0.0.1:8183
```

- **`GET /health`** (open): row count, slot extent, `latest_block_time` +
  `freshness_secs` (= snapshot age for a walked corpus), per-venue walk
  cursors, sealed partitions.
- **`GET /events`** (bearer-gated): filters `venue`, `policy` (56-hex),
  `asset` (CIP-14 fingerprint — exclusive with `policy`), `name`
  (asset_name_hex, requires `policy`), `kind` (comma-separated:
  `kind=sold,offer_accepted`), `from_slot`/`to_slot` (inclusive), `limit`.
  Response is `application/octet-stream`: a postcard `EventsPage`
  (version byte first — see `crates/market-ledger-wire` for the format and
  its append-only evolution contract). `?format=json` returns the text-form
  rows instead — the curl/jq debug loop.
- **Pagination** is keyset `(slot, rowid)`: follow `next_cursor` until it is
  absent. Treat the cursor as opaque. A concurrent new-venue backfill can
  insert rows at slots a paging client already passed — pagination is a
  stream over the corpus as-of-passage; re-query for an authoritative read.
- **Auth**: `MARKET_LEDGER_TOKEN` env (own secret, deliberately not
  `MITOS_AUTH_TOKEN` so the two services rotate independently). Unset ⇒ open
  mode with a startup warning — fine on loopback, set it before tunnelling.
- Responses gzip when the client sends `Accept-Encoding` (use
  `curl --compressed`); CORS is permissive so browser/WASM consumers can call
  through a CF tunnel directly.

Smoke:

```
curl -s localhost:8183/health | jq
curl -s "localhost:8183/events?policy=<56hex>&kind=sold&limit=5&format=json" \
  -H "Authorization: Bearer $MARKET_LEDGER_TOKEN" | jq
curl -s --compressed "localhost:8183/events?policy=<56hex>&kind=sold" \
  -H "Authorization: Bearer $MARKET_LEDGER_TOKEN" -o page.bin && xxd page.bin | head  # byte 0 == 01
```

## Validation (acceptance — run on-box after a bounded walk)

1. **D1 cross-check.** Walk the slot range the D1 firehose already covers
   (`--from-slot <D1 window floor> --max-blocks <n>`), then row-diff the ledger's
   `sold` / `listed` / `offer_accepted` rows against a D1 export of the same
   window. Every divergence is a walk bug or a known Koios-era gap — both worth
   finding. This is also where the best-effort **stake fields** get tuned to
   firehose parity.
2. **Named fixture.** The SpaceBud #9668 sale (tx `1abb0f60…`) must land
   `price_lovelace = 980 ADA`, `buyer_price_lovelace = 1000 ADA` (the +20 ADA
   Wayup-settled on-top fee folded into buyer price).
3. **Resume-equivalence.** A single walk `A→C` must produce the same rows as
   `A→B`, checkpoint, then resume `B→C` (INSERT OR IGNORE + the persisted buffer
   make this hold). Verify with `stats` + a row count/hash over both runs.
4. **Corpus sanity** (`market-ledger stats`): fills/month vs known market
   history, venue split, and the buyer-price premium (jpg-era ≈ 0%, Wayup and
   Wayup-settled-jpg ≈ +2%).

## Deferred optimizations

Chunk-level fast-skip + `read_blocks_from_point` resume (avoid decoding pre-floor
blocks), a persistent `datum_cache` fronting Koios, and in-process DuckDB for
`serve` mode (union view over sealed Parquet + live sqlite, behind the existing
`serve/db.rs` seam; `--parquet-dir` is already plumbed).
