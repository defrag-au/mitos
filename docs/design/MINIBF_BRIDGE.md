# Minibf bridge — expose Blockfrost-compatible queries from mitos

> **Status: design, not implemented.** Trigger: cnft.dev-workers
> `wallet-viewer` worker needs "all assets at this stake address"
> and we'd rather not take a Maestro dependency for it. minibf
> (Dolos's Blockfrost-shaped HTTP API) is already a transitive dep
> via `dolos = "v1.0.3"` and exposes a composable axum router —
> wiring it into the existing bundle is mechanical.

Cross-references:
- `MITOS_DATA_PLANE_API.md` — sister API. data-plane is the
  in-process Rust trait surface for wasm modules; minibf is the
  external HTTP surface for off-host consumers. Same underlying
  domain, different transports
- `ROADMAP.md` — captures "should we also expose UTxO RPC?". This
  doc is the concrete answer for the Blockfrost half
- `bundles/default/src/main.rs` — the wiring point

## Why now

The `cnft.dev-workers` `wallet-viewer` worker needs cross-collection
holdings for a stake address. Three options were on the table:

1. **Maestro** — works, but Maestro is exactly what mitos is supposed
   to displace. Adding a new worker on Maestro is a regression
2. **New wallet-keyed endpoint in collection-ownership** — only
   covers tracked policies, doesn't solve untracked-collection
   discovery, and is more code to maintain
3. **minibf in mitos** — Dolos has the data; minibf already has
   the routes; we just need to plumb it through

(3) is the right answer. The investigation below confirms it's a
small change.

## What minibf actually gives us

`dolos_minibf::build_router(cfg, domain)` returns an
`axum::Router` covering (non-exhaustive):

- `GET /accounts/{stake_address}` — controlled stake, rewards,
  pool, etc.
- `GET /accounts/{stake_address}/addresses` — payment addresses
  for the stake
- `GET /accounts/{stake_address}/utxos` — all UTxOs at the stake
  (this is what wallet-viewer aggregates client-side into the
  per-asset bag — Blockfrost's
  `/accounts/{stake}/addresses/assets` is NOT in minibf v1.0.3, so
  callers walk the UTxO list themselves)
- `GET /accounts/{stake_address}/rewards` — reward history
- `GET /addresses/{address}` — single-address info
- `GET /addresses/{address}/utxos[/{asset}]` — address UTxOs
- `GET /blocks/latest` + `/blocks/{hash_or_number}` family
- `GET /epochs/{epoch}/parameters` + `/epochs/latest/parameters`
- `GET /scripts/{script_hash}[/json|/cbor]` and
  `/scripts/datum/{datum_hash}[/cbor]`
- `GET /txs/{tx_hash}[/cbor]`
- `POST /tx/submit` — TX submission (**not in scope for v1 of
  this bridge** — see "auth" section below)

Full list: `dolos/crates/minibf/src/lib.rs::build_router`.

## Scope

**In scope:**

- Mount `dolos_minibf::build_router(...)` on the existing
  `bundles/default` axum app under a path prefix (default
  `/minibf`)
- Read minibf config from the existing `dolos.toml`
  (`config.serve.minibf`) — same path the dolos binary already
  uses
- Optional shared-secret header auth (single env var, single
  axum middleware layer) so it's not anonymously exposed when the
  bundle's port is reachable from elsewhere

**Out of scope:**

- TX submission via minibf (`POST /tx/submit`). Mitos isn't a
  submission gateway today; opening that surface is a separate
  decision. Mount the router with a route filter that rejects
  `POST /tx/submit`, or just document that it's reachable and
  decide later
- A standalone listener / separate port for minibf. Possible
  later; for now a path prefix on the same listener keeps
  deployment simple (one tunnel, one cert)
- Replacing the in-process `ChainDataPlane` (data plane stays —
  wasm modules use it as host functions, this surface is only for
  off-host HTTP consumers)
- Schema lock-step with stock Blockfrost. minibf is its own
  subset; we ship whatever Dolos v1.0.3 ships

## The wiring (concrete)

The key finding is that minibf is a **composable axum::Router**,
not a binary entrypoint. The dolos CLI's `dolos minibf` subcommand
(see `dolos/src/bin/dolos/minibf.rs`) does literally this:

```rust
let app = dolos_minibf::build_router(minibf.clone(), domain);
```

…and runs `oneshot` against it for one-off CLI queries. We do the
same thing but `app.nest("/minibf", router)` it onto the bundle's
existing app instead.

### Step 1 — Cargo.toml

Add `dolos-minibf` as an explicit workspace dep (currently
transitive via `dolos`):

```toml
# mitos/Cargo.toml [workspace.dependencies]
dolos-minibf = { git = "https://github.com/txpipe/dolos", tag = "v1.0.3" }
```

…and in `crates/mitos-core/Cargo.toml` (or `bundles/default/Cargo.toml`
— see step 3 for the decision):

```toml
dolos-minibf = { workspace = true }
```

### Step 2 — minimum trait-bound check

`build_router` requires:

```rust
D: Domain + SubmitExt + Clone + Send + Sync + 'static,
Option<AccountState>: From<D::Entity>,
Option<PoolState>: From<D::Entity>,
Option<AssetState>: From<D::Entity>,
Option<EpochState>: From<D::Entity>,
Option<DRepState>: From<D::Entity>,
```

`DomainAdapter` (constructed in
`crates/mitos-core/src/domain.rs::setup_domain`) is literally what
the dolos binary passes to `build_router` in `dolos minibf` —
trait bounds will satisfy. If they don't, that means the dolos
binary itself can't compile against the same version, which would
be surprising.

### Step 3 — Where to mount

Two reasonable options:

**Option A — mount inside `Bundle::run`** (in
`crates/mitos-core/src/bundle.rs::run`, around line 191 where
`let mut app = axum::Router::new();` happens). Pros: every bundle
that uses `mitos-core` gets minibf for free. Cons: hard-codes
the prefix and config-read; less flexible if a future bundle
wants minibf disabled.

**Option B — mount in `bundles/default`**, by extending `Bundle`
with a `with_extra_routes(router: axum::Router)` builder method
(or `nest_extra(path, router)`). Pros: bundle author chooses
what to mount; matches the existing additive `add_indexer` /
`enable_modules` pattern. Cons: slightly more surface area.

**Recommendation: Option B.** Matches how every other capability
in `Bundle` is opt-in. Keeps `mitos-core` agnostic of which
HTTP surfaces are mounted. Specifically:

```rust
// crates/mitos-core/src/bundle.rs
impl Bundle {
    /// Mount an additional axum router under `prefix`. Called
    /// during build, applied during `run` before indexer routes
    /// (so indexer route names can't accidentally shadow the
    /// prefix).
    pub fn nest_extra<S>(&mut self, prefix: &str, router: axum::Router<S>)
        // …
}

// bundles/default/src/main.rs
if let Some(minibf_cfg) = config.serve.minibf.clone() {
    let router = dolos_minibf::build_router(minibf_cfg, domain.clone());
    bundle.nest_extra("/minibf", router);
    info!("minibf bridge enabled at /minibf");
}
```

Note that `MinibfConfig.listen_address` becomes vestigial when
mounted via path prefix — minibf does its own bind in the dolos
binary, but here we're mounting it onto the existing listener so
the address field is ignored. That's fine; comment it in the
bundle wiring so a future reader understands why.

### Step 4 — Auth (optional but recommended)

minibf has no built-in auth. Two paths:

1. **Trust the tunnel / network boundary.** If the bundle's port
   is only reachable via a cloudflared tunnel with Access in front,
   we're done. Simplest.
2. **Shared-secret header.** Add a tiny axum middleware that
   checks `x-mitos-key: <env value>` and rejects on mismatch.
   Wrap the minibf router with `.layer(...)` before nesting.
   ~15 lines. Worth doing if minibf is reachable from anywhere
   other than known callers.

Recommendation: ship (2) from day one with a single env var
(`MINIBF_SHARED_SECRET`). If unset, log a warning and skip the
middleware (preserves "just works" for local dev).

### Step 5 — Verification

```bash
# Smoke test against a known stake address
curl -sH "x-mitos-key: $MINIBF_SHARED_SECRET" \
  http://localhost:8080/minibf/accounts/stake1.../utxos | jq .

# Compare structure against Blockfrost's published response shape:
#   https://docs.blockfrost.io/#tag/Cardano-Accounts
```

For CI: a single integration test that boots the bundle against a
fixture data dir, hits `/minibf/health`, and asserts 200.

## Open questions

- **`max_scan_items`** — `MinibfConfig` has a `max_scan_items`
  field. For wallet UTxO scans this matters at high-asset-count
  wallets. Default is unset (no cap). Decide before shipping; a
  cap with a clear 429 / partial-result behaviour is probably
  the right answer
- **Token registry URL** — `MinibfConfig.token_registry_url`
  feeds asset metadata. Default off, off-host consumers can do
  their own enrichment (mirror already does this for our case).
  Leave unset for v1
- **`/tx/submit` exposure** — see scope. Reject it for v1 with a
  route filter, or leave it documented-but-undefended. Decide
  before shipping

## Estimate

~4 hours including tests and a smoke deploy. The router function
is ready, the domain is ready, the config struct is ready — this
is plumbing, not invention.
