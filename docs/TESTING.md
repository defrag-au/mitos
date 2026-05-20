# Testing mitos

End-to-end recipes for exercising the wasm-module + companion path.
Three scopes in increasing depth; do them in order on first
contact.

For the **unit-test / golden-fixture** loop on a single community
module (the fast inner loop most module work happens in) see
[`HOWTO_TESTING_COMMUNITY_MODULES.md`](HOWTO_TESTING_COMMUNITY_MODULES.md).
The scenarios below cover end-to-end host + companion behaviour
that golden fixtures can't reach.

## Prerequisites

- A Dolos data directory you control (mitos can read it; the
  workspace's `dolos-core` pin must match the writer that built
  the data dir — see
  [`reference_mitos_archive_horizon`](memory) for the horizon
  caveat).
- `nix develop` shell available in `~/code/defrag/mitos` (via the
  repo's `flake.nix`). Provides `cargo`, `rustc`, `clippy`, plus
  `wrangler` for the CF worker side of Scenario 2+.

Pick a shared secret you'll use for `MITOS_AUTH_TOKEN` — both the
mitos host and the worker need to agree. A 32-char hex string is
fine for testing.

```sh
export MITOS_AUTH_TOKEN="$(openssl rand -hex 16)"
echo "$MITOS_AUTH_TOKEN"   # save somewhere; you'll paste this into wrangler
```

## Scenario 1: bring up a host with community modules

Validates that the bundle loads, the data plane reads, and the
community-module auto-load path activates each module under
`community-modules/`. No companion involved.

```sh
# Terminal 1 — start mitos with wasm-module hosting + community-module auto-load
cd ~/code/defrag/mitos
nix develop -c cargo run --release -p mitos -- \
    --config /opt/mitos/mainnet/dolos.toml \
    --listen 127.0.0.1:8181 \
    --data-dir /opt/mitos/mainnet/mitos-data \
    --modules-dir /opt/mitos/mainnet/modules \
    --community-modules-dir ./community-modules
```

The bundle:
1. Opens the Dolos data dir, hands its domain to mitos.
2. Registers `none-match-indexer` (residual-pass coordinator —
   the three legacy in-tree indexers retired; see
   `docs/design/DOMAIN_REFACTOR.md`).
3. Activates each pre-built community module whose sha differs
   from `--modules-dir`. First boot will instantiate all 12
   shipped modules.
4. Starts the chain-sync pipeline + HTTP server.

You should see lines like:
```
INFO mitos: mitos starting
INFO mitos: wasm-module hosting enabled
INFO mitos: community-modules auto-load enabled
INFO mitos_platform: module activated id=asset-transfer
INFO mitos_platform: module activated id=jpg-store-offer
...
INFO mitos_core: chain-sync pipeline spawned
INFO mitos_core::bundle: HTTP server listening
```

Confirm via `mitos-admin`:

```sh
nix develop -c cargo run --release -p mitos-admin -- \
    --mitos http://127.0.0.1:8181 \
    --token "$MITOS_AUTH_TOKEN" \
    list-modules
```

Expect a table listing all activated modules with their sha, ABI
version (should be v2), trap strategy, and size.

For a quick offline pre-flight (paths, env, persisted state) use
`--print-config-only`:

```sh
nix develop -c cargo run --release -p mitos -- \
    --config /opt/mitos/mainnet/dolos.toml \
    --data-dir /opt/mitos/mainnet/mitos-data \
    --print-config-only
```

## Scenario 2: end-to-end mitos → CF companion

Adds the actual CF Durable Object as the data sink, exercising the
full HTTP delivery path against a real chain feed.

### 2.1 Deploy the companion

Use a worker that subscribes to a community module. The two
reference companions in `~/code/defrag/cnft.dev-workers/workers/`
are `collections-mitos` (subscribes to `asset-transfer`) and
`jpg-store-mirror` (subscribes to `jpg-store-offer`).

```sh
cd ~/code/defrag/cnft.dev-workers/workers/collections-mitos

# Configure the shared auth token CF-side (same value as mitos):
nix develop -c wrangler secret put MITOS_AUTH_TOKEN
# (paste $MITOS_AUTH_TOKEN when prompted)

# Either deploy:
nix develop -c wrangler deploy
# (note the *.workers.dev URL it prints, or your custom domain)

# …or run locally:
nix develop -c wrangler dev
# (note the local URL, typically http://localhost:8787)
```

Make sure the worker's `wrangler.toml` sets:

- `MITOS_HOST_URL` — base URL of the mitos host (e.g.
  `http://127.0.0.1:8181` for local tests).
- `MITOS_REPLICATE_URL` — dial-back URL template the host POSTs
  emissions to, e.g.
  `https://collections-mitos.example.com/_internal/{op}-{target}?key={key}`.
  All three placeholders (`{op}`, `{target}`, `{key}`) must be
  present.

### 2.2 Start mitos (same as Scenario 1)

Make sure `--modules-dir` is set so the community modules host and
`/api/companions/subscribe` is mounted.

### 2.3 Wake the companion

The worker's DO self-registers on first wake. Hit any endpoint
that routes into the DO — for `collections-mitos` that's typically
the read API at `/api/stats/<policy>` (the DO read-path
implicitly wakes the DO and triggers the subscribe call):

```sh
BASE="https://collections-mitos.<account>.workers.dev"
POLICY="<28-byte-policy-hex>"

curl "$BASE/api/stats/$POLICY"   # wakes DO, triggers subscribe
```

Watch the mitos logs for:
```
INFO mitos_platform::companions: companion registered module=asset-transfer
   client_id=collections-mitos.<account>.workers.dev companion_key=...
INFO mitos_platform::dialer: dial loop started target=asset-transfer
   companion=...
```

The host begins backfilling and POSTing emissions to the worker.

### 2.4 Inspect emissions

```sh
nix develop -c cargo run --release -p mitos-admin -- \
    --mitos http://127.0.0.1:8181 \
    --token "$MITOS_AUTH_TOKEN" \
    emissions --module asset-transfer
```

Expected: rows transitioning `queued` → `pending` → `acked` as
the worker's `apply_event` succeeds. `nacked` rows surface decode
or apply errors — inspect with `--json` for the full error string.

### 2.5 Probe the DO's read APIs

```sh
curl "$BASE/api/stats/$POLICY"
curl "$BASE/api/owner/$POLICY?asset=<asset_name_hex>"
curl "$BASE/api/bundle/$POLICY?stake=stake1u..."
curl "$BASE/api/check/$POLICY?asset=<asset_name_hex>&stake=stake1u..."
```

Compare to the production worker's responses for the same
policy — they should match.

### 2.6 Force a recapture

Verifies the `on_recapture` → `rebootstrap` → refill path end to
end:

```sh
nix develop -c cargo run --release -p mitos-admin -- \
    --mitos http://127.0.0.1:8181 \
    --token "$MITOS_AUTH_TOKEN" \
    recapture asset-transfer --reason "testing TESTING.md scenario 2"
```

Expected:
1. The mitos host POSTs `/_internal/recapture-asset-transfer` to
   each subscribed companion.
2. Each companion runs its `on_recapture` (typically scoped DELETE
   of `source_module = 'asset-transfer'` rows).
3. The host runs the module's `rebootstrap` (re-scans the
   declared interest set, re-emits events).
4. `mitos-admin recapture` returns once all companions reach
   `RecaptureDone` with `companions_targeted`, `events_emitted`,
   and `duration_ms`.

### 2.7 Tear down

```sh
# Surgically remove this companion's record:
nix develop -c cargo run --release -p mitos-admin -- \
    --mitos http://127.0.0.1:8181 \
    --token "$MITOS_AUTH_TOKEN" \
    delete-companion \
    --module asset-transfer \
    --client-id collections-mitos.<account>.workers.dev \
    --key <companion-key>
```

The worker can re-register simply by waking the DO again.

## Reorg validation

Reorgs are rare on Cardano (1-2 blocks deep, ~weekly on mainnet).
Any natural reorg during a running Scenario 2 session produces
visible `rollback-event` dispatches to the module and (if the
module emits cancelling events) downstream `nacked` rows if the
companion can't apply them cleanly.

For a *forced* reorg test you'd need a Dolos data directory from
just before a known historical reorg slot, which is a separate
fixture-setup task.

## Common issues

**"401 Unauthorized" on subscribe.** Token mismatch between
`MITOS_AUTH_TOKEN` on the mitos host and `MITOS_AUTH_TOKEN` on
the worker. Reset both to the same value.

**"module not registered" on emissions or recapture.** The
module's name doesn't match anything in `mitos-admin list-modules`.
Check the bundle started with `--modules-dir` and
`--community-modules-dir` pointing at the right paths; check
`activated` lines in the bundle log.

**"WAL schema not compatible".** Your mitos build's Dolos pin
doesn't match the data dir's writer version. Bump the pin in
`Cargo.toml` and rebuild.

**Companion never receives emissions.** Check `mitos-admin
emissions --module <id> --status pending` — if rows pile up in
`pending`, the dial loop isn't reaching the worker. Verify
`MITOS_REPLICATE_URL` on the worker side resolves to a host the
mitos box can reach, and that the worker accepts the auth header.
