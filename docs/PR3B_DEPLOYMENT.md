# PR 3b deployment notes — mitos host + collections-mitos worker

End-to-end deployment checklist for the first
companion-runtime live test. Two halves, deployable in either
order — the worker registers eagerly on first DO wake and the
host treats unknown registrations idempotently.

Authoritative ops reference for the netcup `cardano-infra` box
is `~/code/defrag/infra/docs/mitos-operations.md`. This doc is
the PR 3b–specific delta on top of that procedure.

## Mitos host (Netcup `cardano-infra`, `mitos-mainnet`)

Source-on-box build model — same procedure as
`mitos-operations.md` § "Deploy / upgrade", with two
PR 3b–specific additions to the env file.

### 1. Update env file `/etc/default/mitos-mainnet`

Companion-runtime delivery requires `BUNDLE_MODULES_DIR` so the
wasm-module hosting path is active (without it, mitos runs in
classic statically-composed-bundle mode and the
`/api/companions/subscribe` endpoint isn't mounted).

```diff
 MITOS_AUTH_TOKEN=<32+ char hex>
 DOLOS_CONFIG=/opt/mitos/mainnet/dolos.toml
 BUNDLE_LISTEN=127.0.0.1:8181
 BUNDLE_DATA_DIR=/opt/mitos/mainnet/mitos-data
+BUNDLE_MODULES_DIR=/var/lib/mitos/modules
 RUST_LOG=info,mitos=debug,mitos_core=debug,collection_ownership_indexer=debug
```

```bash
ssh root@159.195.57.187 'mkdir -p /var/lib/mitos/modules'
```

### 2. Rsync + build

```bash
# From your laptop
rsync -avz --delete \
    --exclude='target/' \
    --exclude='.git/' \
    --exclude='node_modules/' \
    /Users/damo/code/defrag/mitos/ \
    root@159.195.57.187:/opt/mitos/src/

ssh root@159.195.57.187 'cd /opt/mitos/src && cargo build --release -p mitos'
```

### 3. Restart

```bash
ssh root@159.195.57.187 'systemctl restart mitos-mainnet'

# Confirm.
ssh root@159.195.57.187 'systemctl is-active mitos-mainnet; \
    sleep 5; \
    curl -sS http://127.0.0.1:8181/health'
```

### 4. Cloudflare tunnel

The host is internal-only at `127.0.0.1:8181`. The worker's
`MITOS_HOST_URL` (HTTPS subscribe endpoint) and the
host-initiated dial-back currently both need an externally
reachable hostname. **PR 3b's first live test requires
provisioning** `mitos.defrag.cc` (or equivalent) via the
Cloudflare tunnel pattern that `dolos-mainnet.defrag.cc`
already uses (`mitos-operations.md` § "Outstanding ops
decisions" #2). Without the tunnel:

- The worker can't POST to mitos's `/api/companions/subscribe`
  (subscribe call fails with a network error).
- Even if the worker registered some other way, mitos's dial
  loop targets the worker's public hostname (the worker is
  publicly reachable), so dial-back works in only one
  direction.

Both directions need to land before end-to-end emission
delivery works. The host side is the blocker.

### What to look for in `journalctl -u mitos-mainnet -f`

- `companion ws connected target=wss://...` — dial-back
  succeeded. Should appear once per registered companion
  shortly after the worker first calls
  `/api/companions/subscribe`.
- `companion registered module=… companion_key=…` — host
  accepted a fresh subscribe call and persisted CBOR.
- `draining queued emissions count=N` — backlog flushing
  on reconnect.
- `companion dial errored; backing off` — dial target
  unreachable; check `MITOS_REPLICATE_URL` in
  `wrangler.toml` and that the worker is actually deployed.

## collections-mitos worker (Cloudflare)

1. Confirm `wrangler.toml` carries:
   - `secrets_store_secrets[].MITOS_AUTH_TOKEN` — same value
     as the host's `/etc/default/mitos-mainnet
     MITOS_AUTH_TOKEN`.
   - `[vars] MITOS_HOST_URL` — HTTPS endpoint for
     `POST /api/companions/subscribe`. Once the CF tunnel for
     mitos lands, set this to `https://mitos.defrag.cc` (or
     whichever hostname the tunnel resolves to).
   - `[vars] MITOS_REPLICATE_URL` —
     `wss://<worker-hostname>/_internal/replicate?policy_id={key}`
     template; mitos substitutes `{key}` at dial time. Today's
     hostname per the existing `routes` block is
     `ownership-mitos.cnft.dev`.
2. From `cnft.dev-workers/`:
   ```
   nix develop -c bash -c "cd workers/collections-mitos && wrangler deploy"
   ```
3. First request to any `/api/check/<policy_id>` (or
   explicit `POST /_internal/wake/<policy_id>`) wakes the DO.
   The runtime POSTs `SubscribeRequest` to the host with the
   dial-back URL filled in; mitos persists CBOR + spawns a
   dial loop.

## Smoke test (manual)

```bash
TOKEN=$(ssh root@159.195.57.187 'grep ^MITOS_AUTH_TOKEN= /etc/default/mitos-mainnet | cut -d= -f2')

# 1. Wake a DO so it registers with mitos.
curl -X POST -H "Authorization: Bearer $TOKEN" \
  https://ownership-mitos.cnft.dev/_internal/wake/<policy_id>

# 2. Confirm host saw it on the box.
ssh root@159.195.57.187 \
  'ls /var/lib/mitos/modules/<module_id>/companions/ 2>&1'
# Expect: <policy_id>.cbor

# 3. Watch the host pipe emissions back.
ssh root@159.195.57.187 \
  'journalctl -u mitos-mainnet -f' | grep -E '(companion|emission|drain)'

# 4. Confirm the worker is receiving Apply frames.
wrangler tail collections-mitos --format=pretty
```

## Known limitations (intentional for first deploy)

- **Mitos host needs a Cloudflare tunnel** (`mitos.defrag.cc`
  or similar). Tracked in `mitos-operations.md` § "Outstanding
  ops decisions" #2 — previously low-priority but now blocks
  PR 3b live test.
- `Interest` frames received by the host are logged but not
  yet forwarded to the running module's `update-interest`
  host call. Static interest sets work; dynamic per-emission
  filtering doesn't.
- No emissions compaction — `Acked` rows accumulate. Manual
  `redb` purge if the per-module `emissions.redb` gets large.
- Channel routing uses stringified u32 IDs from the WIT ABI;
  multi-channel modules will need to coordinate with
  companion-side channel tag mapping. v1 tests should stick
  to single-channel modules (`collections-mitos`'s
  `ownership` channel).
- Module-level `[companion] replicate_url` defaults aren't
  surfaced from `mitos.toml`; every companion must carry
  `dial_back.url` via `MITOS_REPLICATE_URL` wrangler env.
