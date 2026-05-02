# Testing the CF replication prototype

End-to-end recipes for exercising the mitos ↔ CF DO replication path.
Three scenarios in increasing scope; do them in order on first
contact.

The architecture this is testing is in `design/CF_REPLICATION.md`.
The Phase 4.5 build order + success criteria are in
`design/ROADMAP.md`.

## Prerequisites

- A Dolos data directory you control (mitos can read it; pin must
  match — see ROADMAP Phase 1 lessons banked).
- `nix develop` shell available in `~/code/defrag/mitos` (via the
  repo's `flake.nix`). Provides `cargo`, `rustc`, `clippy`, plus
  `wrangler` for the CF worker side of Scenario 2+.

Pick a shared secret you'll use for `MITOS_AUTH_TOKEN` /
`MITOS_TOKEN` — both sides need to agree. A 32-char alphanumeric is
fine for testing.

```sh
export MITOS_AUTH_TOKEN="$(openssl rand -hex 16)"
echo "$MITOS_AUTH_TOKEN"   # save somewhere; you'll paste this into wrangler
```

## Scenario 1: protocol round-trip with `mitos-tail`

Validates the wire format, subscribe handshake, backfill, and live
tail against a real Dolos data dir. **No CF involved.** Fastest
sanity check.

```sh
# Terminal 1 — start mitos
cd ~/code/defrag/mitos
MITOS_AUTH_TOKEN=$MITOS_AUTH_TOKEN \
nix develop -c cargo run --release -p mitos -- \
    --config /opt/mitos/mainnet/dolos.toml \
    --listen 127.0.0.1:8181 \
    --data-dir /opt/mitos/mainnet/mitos-data
```

You should see:
```
INFO mitos: mitos starting
INFO mitos_core::domain: domain initialized
INFO mitos_core: chain-sync pipeline spawned
INFO mitos_core: indexer bootstrapped indexer="jpg-co"
INFO mitos_core: indexer bootstrapped indexer="collection-ownership"
INFO mitos_core::dispatcher: dispatcher started indexer="..."
INFO mitos_core::bundle: HTTP server listening
```

Pick a real policy ID (use any tracked PFP collection — `bedwars`,
`pfp-city`, etc.). Then:

```sh
# Terminal 2 — tail the collection-ownership feed
cd ~/code/defrag/mitos
MITOS_AUTH_TOKEN=$MITOS_AUTH_TOKEN \
nix develop -c cargo run --release -p mitos-tail -- \
    --mitos http://127.0.0.1:8181 \
    --indexer collection-ownership \
    --scope-json '{"policy_id":"<28-byte-policy-hex>"}' \
    --cursor origin \
    --validate \
    --max-records 200
```

Expected output:
1. **One** `subscribe reply` log line with a Resume cursor.
2. **N** `apply` lines (the backfill — one per asset under the
   policy currently held in a UTxO).
3. **Live tail** of `apply` / `mark` lines as the chain advances.
4. A summary on exit: counts of apply / undo / mark / error and any
   `undo_without_prior_apply` invariant violations (should be zero
   under steady-state).

If the backfill count matches the collection's known asset count
(or close to it — burn/CIP-68 reference NFTs may differ slightly),
the protocol path works.

## Scenario 2: end-to-end mitos → CF DO

Adds the actual CF Durable Object as the data sink, exercising the
full hibernation-API path.

### 2.1 Deploy the worker

```sh
cd ~/code/defrag/cnft.dev-workers/workers/collection-ownership-mitos

# Configure the auth token CF-side (same value as mitos):
nix develop -c wrangler secret put MITOS_TOKEN
# (paste $MITOS_AUTH_TOKEN when prompted)

# Either deploy:
nix develop -c wrangler deploy
# (note the *.workers.dev URL it prints, or your custom domain
#  ownership-mitos.cnft.dev)

# …or run locally:
nix develop -c wrangler dev
# (note the local URL, typically http://localhost:8787)
```

### 2.2 Verify mitos config offline (optional but recommended)

Before starting the long-running process, dump everything mitos
*will* load to confirm paths and env are set correctly:

```sh
cd ~/code/defrag/mitos
MITOS_AUTH_TOKEN=$MITOS_AUTH_TOKEN \
nix develop -c cargo run --release -p mitos -- \
    --config /opt/mitos/mainnet/dolos.toml \
    --data-dir /opt/mitos/mainnet/mitos-data \
    --print-config-only
```

Output is a one-shot summary: listen address, data_dir, dolos
storage paths, registered indexers, auth status, persisted
subscriptions. Exits 0 without starting the chain follower or HTTP
server. If anything looks wrong (e.g. `auth: OPEN` when you set the
env, or persisted subs missing after a restart), fix it before
moving on.

### 2.3 Start mitos pointing at it

Same command as Scenario 1, with `MITOS_AUTH_TOKEN` set.

### 2.4 Register an outbound subscription

Use `mitos-admin` (sibling tool, friendly args, no JSON
heredocs):

```sh
TARGET="wss://collection-ownership-mitos.<account>.workers.dev/_internal/replicate?policy_id=<hex>"
POLICY="<28-byte-policy-hex>"

cd ~/code/defrag/mitos
nix develop -c cargo run --release -p mitos-admin -- \
    --mitos http://127.0.0.1:8181 \
    add \
    --indexer collection-ownership \
    --target  "$TARGET" \
    --scope-json "{\"policy_id\":\"$POLICY\"}" \
    --cursor   origin
```

Expected output: `added subscription id=1`.

Watch mitos's logs for:
```
INFO mitos_core::replicator: outbound ws connected
INFO collection_ownership_indexer: subscribe policy_id=... new=true backfilled=N
```

Confirm via:

```sh
nix develop -c cargo run --release -p mitos-admin -- list
```

```
ID    INDEXER                   STATUS        BACKOFF   TARGET
1     collection-ownership      connected     -         wss://...
```

The `health` subcommand gives a roll-up:

```sh
nix develop -c cargo run --release -p mitos-admin -- health
```

```
status:        ok
uptime:        12m45s
indexers:      jpg-co, collection-ownership
subscriptions: total=1 connected=1 connecting=0 backing_off=0 disconnected=0
```

### 2.5 Probe the DO's read APIs

```sh
BASE="https://collection-ownership-mitos.<account>.workers.dev"

# Asset count for the policy:
curl "$BASE/api/stats/$POLICY"

# Owner of a specific asset (no auth needed for reads):
curl "$BASE/api/owner/$POLICY?asset=<asset_name_hex>"

# All assets a stake holds in this policy:
curl "$BASE/api/bundle/$POLICY?stake=stake1u..."

# Whether a specific stake owns a specific asset:
curl "$BASE/api/check/$POLICY?asset=<asset_name_hex>&stake=stake1u..."
```

Compare to the existing `collection-ownership` worker's responses
for the same policy — they should match.

### 2.6 Verify subscription persistence

```sh
# Confirm the subscription is registered:
nix develop -c cargo run --release -p mitos-admin -- list

# Restart mitos (Ctrl+C, then re-run the start command).
# The subscription should re-appear automatically:
nix develop -c cargo run --release -p mitos-admin -- list
# (same id and target_url)
```

You can also confirm offline via `--print-config-only` — it reads
the persisted redb without spinning up the rest of the process.

### 2.7 Tear down

```sh
nix develop -c cargo run --release -p mitos-admin -- remove 1
```

Mitos disconnects, the DO's hibernating WS gets a close.

## Scenario 3: convergence diff against the existing worker

Run both workers in parallel for the same policy and watch
divergence over time. This is the actual validation harness for
Phase 4.5.

```sh
cd ~/code/defrag/mitos
nix develop -c cargo run --release -p diff-collection-ownership -- \
    --baseline https://ownership.cnft.dev \
    --mitos    https://collection-ownership-mitos.<account>.workers.dev \
    --policy   <hex> \
    --probe-asset <asset_hex> --probe-stake stake1u...8 \
    --probe-asset <asset_hex2> --probe-stake stake1u...9 \
    --interval 3600
```

Successful convergence looks like:
```
INFO diff_collection_ownership: stats within tolerance policy=... baseline_assets=10000 mitos_assets=10000 drift_pct=0.00
INFO diff_collection_ownership: check matches policy=... asset=... stake=... owns=true
INFO diff_collection_ownership: bundle matches policy=... stake=... count=42
INFO diff_collection_ownership: all policies converged
```

A divergence looks like:
```
WARN diff_collection_ownership: stats diverged > 5% policy=... baseline_assets=10000 mitos_assets=8500 drift_pct=15.00
WARN diff_collection_ownership: bundle DIVERGED policy=... stake=... only_baseline=3 only_mitos=0
```

For one-shot mode (e.g. cron):

```sh
diff-collection-ownership --once --baseline ... --mitos ... --policy ...
```

Run this in a `tmux` / `nohup` for at least 24h to satisfy the
roadmap's "byte-identical hourly" success criterion.

## Cost validation (hibernation actually working)

After Scenario 2 has been running for an hour or more, check the CF
dashboard for the `collection-ownership-mitos` worker:

- DO → Active duration should be **~3 min/day per consumer**, not
  24 hours.
- DO → Request count should be one per WebSocket message
  (~dozens-to-hundreds per hour per active policy, depending on
  chain activity).

If Active duration is anywhere close to 24h/day, the hibernation
API isn't engaging — likely the DO is treating the upgrade as a
fetch handler and never calling `state.accept_web_socket(&server)`.
Re-check the upgrade path in `do_state.rs::handle_replicate_upgrade`.

## Reorg validation

Reorgs are rare on Cardano (1-2 blocks deep, ~weekly on mainnet).
With `mitos-tail --validate` running, any natural reorg during the
session will produce visible Undo records and the validator will
warn on any malformed Apply/Undo sequences.

For a *forced* reorg test you'd need a Dolos data directory from
just before a known historical reorg slot, which is a separate
fixture-setup task. Track that as part of the Phase 4.5
"reorg validation" success criterion.

## Common issues

**"401 Unauthorized" on register or upgrade.** Token mismatch
between `MITOS_AUTH_TOKEN` (mitos env) and `MITOS_TOKEN` (CF
secret). Reset both to the same value.

**"unknown indexer" on POST.** The bundle hasn't registered an
indexer with that name. Check `bundles/default/src/main.rs` lists
both `JpgCoIndexer` and `OwnershipIndexer`.

**"WAL schema not compatible".** Your mitos build's Dolos pin
doesn't match the data dir's writer version. Bump the pin in
`Cargo.toml` (`tag = "v1.0.3"` etc) and rebuild. Phase 1 lessons
banked has the full diagnosis.

**"consumer lagged by N records; reconnect"** in the mitos logs.
Backpressure: the broadcast channel filled because the consumer
couldn't drain fast enough (slow link, slow DO). The Replicator
will reconnect on the next dial loop iteration. If sustained, raise
`BROADCAST_CAPACITY` in `handle.rs`.
