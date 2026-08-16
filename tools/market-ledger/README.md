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
market-ledger follow --db ledger.db                   # chainsync tip tail (resumes from walk cursor)
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
(`<db>.checkpoint.json`, atomic temp+rename; note the scope key is `scope`, not `venues`, since the mirror moved to the shared `mitos-chain-walk` crate) with the resumable slot/height/hash
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

- **`GET /health`** (open): row count, slot extent, per-venue walk cursors,
  sealed partitions, and two freshness stats — `freshness_secs` = follower lag
  (`now - block_time(tip_slot)`, where `tip_slot` is the newest block in the
  volatile window; seconds when following, `null` on a walk-only ledger) vs
  `last_event_secs` = time since the last marketplace event (naturally large in
  quiet periods, NOT a lag signal).
- **`GET /events`** (bearer-gated): filters `venue`, `policy` (56-hex),
  `asset` (CIP-14 fingerprint — exclusive with `policy`), `name`
  (asset_name_hex, requires `policy`), `kind` (comma-separated:
  `kind=sold,offer_accepted`), `from_slot`/`to_slot` (inclusive), `limit`.
  Response is `application/octet-stream`: a postcard `EventsPage`
  (version byte first — see `crates/market-ledger-wire` for the format and
  its append-only evolution contract). `?format=json` returns the text-form
  rows instead — the curl/jq debug loop.
- **`GET /listings`** (bearer-gated): current live listings for a policy —
  `?policy=<56hex>[&venue=][&limit=]`. Response is a postcard `ListingsPage`
  (cheapest-first, unpriced last; `floor_lovelace` + `count` in the header;
  `?format=json` debug). `price_lovelace` is the datum **ask** (payout sum);
  fold the venue fee for buyer-price (wayup `ask*50/49`, which matches Anvil to
  the lovelace). The table is a buffer-driven projection of the open book (the
  authoritative un-spent set), so it never carries ghosts; `follow` seeds it
  from the open book at startup and maintains it at tip. ~86% of jpg + ~all
  wayup listings are priced (the rest are hash-only jpg with no on-chain datum).
- **Pagination** (on `/events`) is keyset `(slot, rowid)`: follow `next_cursor`
  until it is absent. Treat the cursor as opaque. A concurrent new-venue backfill can
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

## Follow

`follow` keeps the ledger tip-current via a self-contained pallas-network
chainsync + blockfetch tail — the same `process_tx` pipeline as walk, fed from
a peer instead of chunk files. It intersects at the persisted walk cursor, so
the flow is: walk a fresh snapshot to near-tip, then `follow` (the buffer is
warm — the persisted outref buffer IS the inputs cache). Peer via `--peer` /
`MARKET_LEDGER_PEER` (default: the IOG backbone relay; point it at a localhost
dolos o7s listener if one exists).

Rollback safety = a volatile window: raw CBOR for the last `--volatile-blocks`
(k, default 2160) blocks is kept in sqlite; the boundary checkpoint (cursor +
buffer, same tables walk uses) trails k blocks behind tip. `RollBackward` →
truncate events/blocks past the point, rebuild the live buffer from boundary +
replay. Kill it any time — restart resumes from the newest volatile block
(validated: SIGTERM mid-run, clean resume, 0 rows lost). Rollbacks deeper than
k abort with a re-walk instruction (beyond Ouroboros finality — shouldn't
happen).

Measured (2026-07-22, backbone relay): catch-up ~75 blocks/s including
boundary checkpointing; a 28h gap (4.9k blocks) closed in ~66s.

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
