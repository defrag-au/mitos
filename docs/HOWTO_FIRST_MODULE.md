# HOWTO: build your first dApp-owned module

> **For most new work, start with
> [HOWTO_CONSUMING_A_COMMUNITY_MODULE.md](HOWTO_CONSUMING_A_COMMUNITY_MODULE.md).**
> The community-modules-first preference
> (`docs/strategy/LAYERED_RESPONSIBILITIES.md`) means most chain-
> recognition logic ships in `mitos/community-modules/<name>/` and
> dApps just stand up a companion that subscribes. This HOWTO
> covers the **dApp-owned module** pattern — still valid for
> one-off dApp-specific recognition that doesn't belong in a
> shared community module.

A walkthrough for building a paired wasm-module + CF-Worker-DO
companion against the current v2 platform ABI. If you've read
`MITOS_COMPANION_PATTERN.md` for the *why*, this doc is the *how*.

Cross-references:
- `strategy/MITOS_COMPANION_PATTERN.md` — architectural rationale
- `strategy/MITOS_COMPANION_RUNTIME_V1.md` — companion SDK design
- `strategy/MITOS_PLATFORM_V2.md` — current wasm-module runtime + dispatch model
- `strategy/MITOS_PLATFORM_DEPLOYMENT.md` — build + upload + recapture flow
- `design/MITOS_BUILD.md` — single-file-module build tool
- `design/SUBSCRIPTION_MECHANICS.md` — `Interest` vocabulary
- `design/MULTI_CLIENT_COMPANIONS.md` — `client_id` keying

## Prerequisites

- A CF Worker monorepo where your DO will live (e.g.
  `cnft.dev-workers/`)
- A running mitos host (or one you're prepared to spin up locally
  against testnet)
- `mitos-build` and `mitos-admin` available in the dev shell.
  From the mitos repo: `nix develop -c cargo build -p mitos-build`
  and `nix develop -c cargo build -p mitos-admin`. Resulting
  binaries live at `target/debug/{mitos-build,mitos-admin}`.
  (Use `cargo build --release -p ...` for production deploys.)
- Rust target: `wasm32-wasip2` for the module + `wasm32-unknown-unknown`
  for the worker.

## Repo layout

A dApp-owned module consists of three pieces in your worker
monorepo:

```
your-worker-monorepo/
├── workers/
│   └── <my-worker>/
│       ├── Cargo.toml          # the CF Worker DO (cdylib for CF target)
│       ├── wrangler.toml
│       ├── src/                # DO logic, RPC handlers
│       └── modules/
│           ├── <feature>.rs    # the wasm indexer module — your code
│           └── <feature>.toml  # its config + build-time deps
└── types/
    └── <feature>-events/       # shared event-shape crate
        ├── Cargo.toml
        └── src/lib.rs          # typed events both halves consume
```

The shared types crate at `types/<feature>-events/` is the
**single source of truth** for the event shape. Both halves
depend on it, so any wire-format change becomes a compile error
in the half that didn't get updated.

## Step 1 — write the shared types crate

```toml
# types/<feature>-events/Cargo.toml
[package]
name = "<feature>-events"
version = "0.1.0"
edition = "2021"

[dependencies]
serde = { version = "1", features = ["derive"] }
serde_bytes = "0.11"
```

```rust
// types/<feature>-events/src/lib.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum <Feature>Change {
    SomethingHappened {
        // ... fields the module extracts and the companion stores
    },
    SomethingUndone {
        // ... reverse delta
    },
}
```

Keep this crate **dependency-light**. It compiles for three
targets (native, `wasm32-unknown-unknown` for the CF DO,
`wasm32-wasip2` for the module). Heavy deps will hurt one of
those.

## Step 2 — write the wasm module

The module is **a single `.rs` file** at
`workers/<my-worker>/modules/<feature>.rs`. There is no
Cargo.toml for it — `mitos-build` synthesises one. See
`design/MITOS_BUILD.md` for the full materialisation rules.

### `<feature>.rs` skeleton (v2 ABI)

```rust
//! Brief description of what this module does.
//!
//! These leading inner-doc comments will be hoisted above the
//! injected `wit_bindgen::generate!` macro at materialisation time.

use serde::Deserialize;

use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{
    DispatchEvent, MintedEvent, ProducedEvent, ConsumedEvent, UtxoEvent,
};

use <feature>_events::<Feature>Change;

const LOG_TARGET: &str = "<feature>-module";

#[derive(Debug, Clone, Default, Deserialize)]
struct Config {
    // top-level keys from <feature>.toml
}

struct Module;

impl Guest for Module {
    fn module_version() -> (u32, u32) {
        (2, 0)  // v2 ABI handshake
    }

    fn trap_policy() -> (TrapStrategy, RetryPolicy) {
        (
            TrapStrategy::Replay,
            RetryPolicy {
                max_retries: 3,
                backoff_cap_ms: 1_000,
            },
        )
    }

    fn init(config: Vec<u8>) {
        let _cfg: Config = if config.is_empty() {
            Config::default()
        } else {
            ciborium::de::from_reader(&config[..]).expect("decode config")
        };
        logging::log(LogLevel::Info, LOG_TARGET, "init complete");
    }

    /// Hot path. The platform filters TXs against your declared
    /// interest set host-side and delivers matching events in
    /// deterministic order: `tx-context` → `referenced` →
    /// `consumed` → `produced` → `minted` per TX. `tick` and
    /// `rollback` events arrive in their own batches.
    fn handle_events(events: Vec<DispatchEvent>) {
        for event in events {
            match event {
                DispatchEvent::Utxo(UtxoEvent::Produced(p)) => handle_produced(&p),
                DispatchEvent::Utxo(UtxoEvent::Consumed(c)) => handle_consumed(&c),
                DispatchEvent::Utxo(UtxoEvent::Minted(m)) => handle_minted(&m),
                DispatchEvent::Rollback(r) => handle_rollback(&r),
                _ => {}
            }
        }
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        // Filter application happens host-side. Override only if
        // you keep interest-keyed module-side state.
        Ok(())
    }

    /// Event-driven modules return `done: true` immediately;
    /// the host's bootstrap walk over the declared interest set
    /// covers refill. Self-bootstrapping modules (which scan via
    /// `chain-data::utxos-by-*`) implement this as a paged scan —
    /// one call processes one bounded page, returning
    /// `done: false` until the scan is exhausted. See
    /// `docs/design/WASM_BUDGET_CHUNKING.md`.
    fn rebootstrap() -> Result<RebootstrapStep, String> {
        Ok(RebootstrapStep { done: true, ingested: 0 })
    }
}

fn handle_produced(p: &ProducedEvent) {
    // Decode datum, decide whether to emit.
    // emit::emit_event(channel, &cbor_bytes) for global ordering,
    // or emit::emit_event_keyed(channel, partition_key, &cbor_bytes)
    // when same-key events must drain serially. See
    // `docs/design/DIALER_CONCURRENCY.md`.
}

fn handle_consumed(_c: &ConsumedEvent) { /* ... */ }
fn handle_minted(_m: &MintedEvent) { /* ... */ }
fn handle_rollback(_r: &crate::mitos::platform_v2::types::RollbackEvent) { /* ... */ }

export!(Module);
```

The exact bindings come from the generated WIT bindgen. Run
`mitos-build prepare --crate-name <feature>` and point your editor
at the printed path for rust-analyzer completion.

### `<feature>.toml` (manifest)

```toml
# Opt into the v2 platform ABI.
abi_version = 2

# Initial interest set. The companion adds to this dynamically
# via `/api/_interest/<feature>/subscribe` once it knows what to
# track. Hardcodes here are a bootstrap fallback / dev convenience.
[interest]
addresses = []
policies  = []

# Top-level config keys = your `Config` struct. Whatever shape
# your `Config` deserialiser expects sits here outside [interest]
# / [deps].
# some_setting = "value"

# Build-time only. Stripped before CBOR-encoding the runtime config.
[deps]
<feature>-events = { path = "../../../types/<feature>-events" }
ciborium = "0.2"
serde    = { version = "1", features = ["derive"] }
```

`mitos-build` strips `[deps]` and `[interest]` before CBOR-encoding
the runtime config, so your `Config` deserialiser only sees its
own keys. See `design/MITOS_BUILD.md` for the full schema.

### What you import from `crate::mitos::platform_v2::*`

The synthesised crate's `lib.rs` invokes `wit_bindgen::generate!`
with the bundled host WIT (v2). Host interfaces available:

- `chain_data` — snapshot queries: `read_utxos`, `read_output_datums`,
  `utxos_by_address`, `utxos_by_policy`, `utxos_by_payment_cred`,
  `resolve_stake_for_payment_pkh`, `tx_metadata`, `datum_by_hash`,
  `read_tx`. Paged scans are budget-aware — call iteratively until
  `next: None`.
- `state_kv` — `get_value`, `set_value`, `delete_value` for
  module-private redb-backed state.
- `emit` — `emit_event(channel, cbor)` for global lane; or
  `emit_event_keyed(channel, partition_key, cbor)` for per-key
  serial / cross-key parallel dialer behaviour.
- `logging` — `log(level, target, message)` routed into the host's
  tracing subscriber.
- `types` — `DispatchEvent`, `UtxoEvent`, `ProducedEvent`,
  `ConsumedEvent`, `ReferencedEvent`, `MintedEvent`, `TxContextEvent`,
  `TickEvent`, `RollbackEvent`, `TypedOutput`, `TypedDatum`,
  `ChainPoint`, `OutputRef`, etc.

The world-level `use` clauses bring `DispatchEvent`, `TrapStrategy`,
`RetryPolicy`, `InterestOp`, `RebootstrapStep`, and the `Guest`
trait into your module's scope.

See `crates/mitos-platform/wit-v2/world.wit` for the precise
signatures.

## Step 3 — write the CF DO companion

```toml
# workers/<my-worker>/Cargo.toml
[package]
name = "<my-worker>"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
worker = "..."
mitos-companion = { workspace = true }    # the SDK
mitos-protocol  = { workspace = true }    # Interest / wire types
<feature>-events = { path = "../../types/<feature>-events" }
serde     = { version = "1", features = ["derive"] }
ciborium  = "0.2"
async-trait = { workspace = true }
anyhow    = "1"
```

The `mitos-companion` SDK provides `MitosCompanionRuntime<C>` which
absorbs HTTP delivery, cursor persistence, schema migration, and
dispatch. You implement two traits.

### `MitosCompanion` — top-level companion declaration

```rust
use mitos_companion::{MitosCompanion, MitosChannel, MitosChannelDyn};

pub struct MyCompanion;

#[async_trait::async_trait(?Send)]
impl MitosCompanion for MyCompanion {
    const NAME: &'static str = "<feature>";
    type Config = ();

    fn channels(&self) -> Vec<Box<dyn MitosChannelDyn>> {
        vec![Box::new(<Feature>Channel)]
    }

    /// REQUIRED: non-empty, stable per consumer instance. Lets
    /// two workers share the same `companion_key` without colliding
    /// in the host's per-companion store. The SDK derives a sensible
    /// default from the dial-back URL host; override only when that
    /// isn't unique (e.g. dev + prod share the same hostname).
    fn client_id(&self) -> Option<String> {
        Some("my-worker.example.com".into())
    }

    /// Optional: scoped state cleanup before a host-driven refill.
    /// Called when the operator runs `mitos-admin recapture <id>`.
    /// Multi-target companions MUST scope the DELETE by `module`.
    /// See `docs/design/RECAPTURE.md`.
    async fn on_recapture(
        &self,
        ctx: &mitos_companion::Ctx,
        module: &str,
        _reason: Option<&str>,
    ) -> mitos_companion::Result<()> {
        if module == Self::NAME {
            ctx.exec(
                "DELETE FROM my_table WHERE source_module = ?",
                vec![module.into()],
            )?;
        }
        Ok(())
    }
}
```

### `MitosChannel` — per-channel event handler

```rust
pub struct <Feature>Channel;

#[async_trait::async_trait(?Send)]
impl MitosChannel for <Feature>Channel {
    const NAME: &'static str = "<feature>";
    type Event = <Feature>Change;  // from your shared types crate

    async fn apply_event(
        &self,
        ctx: &mitos_companion::Ctx,
        event: Self::Event,
    ) -> mitos_companion::Result<()> {
        match event {
            <Feature>Change::SomethingHappened { /* ... */ } => {
                ctx.exec(
                    "INSERT INTO my_table (...) VALUES (...) \
                     ON CONFLICT(...) DO UPDATE SET ...",
                    vec![/* SqlStorageValue bindings */],
                )?;
            }
            <Feature>Change::SomethingUndone { /* ... */ } => {
                ctx.exec("DELETE FROM my_table WHERE ...", vec![/* ... */])?;
            }
        }
        Ok(())
    }
}
```

Events arrive as CBOR over HTTP POST; the SDK decodes into your
`type Event` automatically (compile-time-checked round-trip with
the module via the shared types crate).

### Wire the runtime into your DO

```rust
#[durable_object]
pub struct MyDO {
    runtime: MitosCompanionRuntime<MyCompanion>,
}

impl DurableObject for MyDO {
    fn new(state: State, env: Env) -> Self {
        Self { runtime: MitosCompanionRuntime::new(state, env, MyCompanion) }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        ensure_dapp_schema(self.runtime.state())?;
        match req.path().as_str() {
            "/my-rpc-endpoint" => self.handle_my_rpc(req).await,
            _ => self.runtime.fetch(req).await,
        }
    }
}

fn ensure_dapp_schema(state: &State) -> worker::Result<()> {
    let sql = state.storage().sql();
    sql.exec("CREATE TABLE IF NOT EXISTS my_table (...)", vec![])?;
    Ok(())
}
```

`MitosCompanionRuntime::fetch` owns:

- `POST /_internal/apply-<channel>?key=...` — one emission per
  request, returns 200 on Ack, 422 on Nack (apply errored), 5xx
  for transport retry.
- `POST /_internal/recapture-<channel>?key=...` — triggers
  `on_recapture`.
- `POST /_internal/wake` — operator-triggered re-subscribe.
- `GET /api/_health`, `GET /api/_meta` — introspection.
- `GET|POST /api/_interest[/subscribe|/unsubscribe]` — interest mutation.

The DO no longer overrides `websocket_message` / `websocket_close`
— delivery migrated to HTTP POST (see
`crates/mitos-companion/src/runtime.rs` for the canonical comment).

Your dApp owns the SQL schema; the runtime owns its own bookkeeping
tables (`mitos_companion_meta`, `mitos_companion_interest`).

## Step 4 — build the module

```bash
$ cd workers/<my-worker>
$ mitos-build --crate-name <feature> --module-id <feature>
# emits:  modules/target/mitos/<feature>/{<feature>.wasm, manifest.toml, config.cbor}
```

Release-only — debug builds fuel-exhaust on realistic blocks.
See `design/MITOS_BUILD.md` for the full CLI.

## Step 5 — deploy the module to mitos

```bash
$ mitos-admin \
    --mitos http://mitos-host:8080 \
    --token "$MITOS_AUTH_TOKEN" \
    upload-module --artifact workers/<my-worker>/modules/target/mitos/<feature>
```

Or the one-shot `deploy` subcommand (chains build + upload):

```bash
$ mitos-admin \
    --mitos http://mitos-host:8080 \
    --token "$MITOS_AUTH_TOKEN" \
    deploy --crate-name <feature>
```

`--mitos` and `--token` are **top-level** flags (positioned
before the subcommand), not subcommand args.

The upload POSTs the wasm + manifest + config.cbor multipart to
`/_admin/modules/<id>`. The host validates the manifest, verifies
the wasm SHA, parses the WIT world, and instantiates the module —
calling `init(config)` with the CBOR bytes from `config.cbor`.

Subsequent uploads under the same module ID **replace** the
running instance. State-kv and the chain-point cursor **persist
across replacement**.

## Step 6 — deploy the companion to CF

```bash
$ cd workers/<my-worker>
$ wrangler deploy
```

Standard CF Workers flow. The DO runs on first request.

Make sure `wrangler.toml` sets:

- `MITOS_HOST_URL` — base URL of the mitos host.
- `MITOS_REPLICATE_URL` — dial-back URL template the host POSTs
  to, e.g.
  `https://my-worker.example.com/_internal/{op}-{target}?key={key}`.
  All three placeholders (`{op}`, `{target}`, `{key}`) must be
  present.
- `MITOS_AUTH_TOKEN` — bearer token, via CF Secrets Store
  (preferred) or a worker secret.

## Step 7 — companion self-registers on first wake

There's no separate `mitos-admin` registration step. The runtime
POSTs a `SubscribeRequest` (CBOR) to mitos's
`/api/companions/subscribe` the first time the DO wakes:

1. Worker first wakes the DO (e.g. a read-API request lands).
2. Runtime checks its registration cache; if stale or missing,
   it POSTs `SubscribeRequest { targets, companion_key,
   client_id, resume_from, interests, dial_back? }`.
3. Mitos persists the registration in the per-module companion
   store, indexed by `(module_id, client_id, companion_key)`.
4. Mitos's dialer begins POSTing emissions to
   `<MITOS_REPLICATE_URL with substitutions>` — one POST per
   emission. The runtime decodes, runs your channel's
   `apply_event`, advances the persisted cursor, and ACKs.

## Step 8 — assert canonical interest at runtime

The TOML's `[interest]` block is a **first-init only** fallback —
used when the module's persisted interest state is empty. The
canonical Interest set is asserted by the companion calling its
own `/api/_interest/subscribe`:

```bash
# from a frontend / admin tool / migration script:
$ curl -X POST https://<my-worker>.workers.dev/api/_interest/subscribe \
    -H 'content-type: application/json' \
    -d '{ "kind": "policy", "value": "<policy-id>", "channel": "<feature>" }'
```

The companion writes to `mitos_companion_interest`, then POSTs
an interest-mutation body to mitos's
`/api/companions/{key}/interest`, which routes via
`update-interest` into the running module. Filter changes take
effect on the next block.

**Don't ship policy hardcodes in `<feature>.toml` for
production.** Drive Interest dynamically; the TOML's list is
observability / local-dev / first-boot only.

## Step 9 — iterate

Module changes:

```bash
$ mitos-admin deploy --crate-name <feature>
# Brief restart of the running instance. Cursor + state-kv persist.
```

Companion changes:

```bash
$ wrangler deploy
# Standard CF rolling deploy.
```

Both halves deploy independently of the mitos *host*; both deploy
together (same PR, same release) relative to *each other*, because
they share the types crate.

## Reference implementations

In-tree community modules are the closest working reference for the
module side:

- `community-modules/asset-transfer/asset_transfer.rs` — event-driven,
  simplest shape transform.
- `community-modules/jpg-store-offer/jpg_store_offer.rs` — datum
  decoding + multi-channel emission.
- `community-modules/holder-distribution/holder_distribution.rs` —
  self-bootstrapping (uses `utxos-by-policy`) + non-trivial
  `rebootstrap` + chunked snapshot emission.

Companion-side, the canonical examples live in cnft.dev-workers:

- `cnft.dev-workers/workers/collections-mitos/` — subscribes to
  `asset-transfer`, projects collection ownership.
- `cnft.dev-workers/workers/jpg-store-mirror/` — subscribes to
  `jpg-store-offer`.

## Common pitfalls

- **Adding `wit-bindgen`, `serde`, `ciborium`, or `mitos-protocol`
  to `[deps]`.** They're implicit; redeclaring causes Cargo
  duplicate-dep errors. See `design/MITOS_BUILD.md` for the implicit
  set.
- **Hand-editing files under `target/mitos-build/<id>/`.** They're
  regenerated on every `mitos-build` run. Edit `<feature>.rs` /
  `<feature>.toml` instead.
- **Path entries in `[deps]` not relative to the toml.** Paths are
  resolved relative to `<feature>.toml`'s parent directory and
  canonicalised at build time.
- **Skipping the shared types crate.** Inlining the event types in
  both halves means wire-format drift on every schema change.
- **Shipping policy hardcodes in `<feature>.toml` for production.**
  Bootstrap-only fallback. Drive Interest dynamically via the
  companion RPC.
- **Returning `(1, 0)` from `module_version`.** v2 modules return
  `(2, 0)`. The host rejects v1 modules at load time.
- **Forgetting `client_id()`** — the SDK falls back to the dial-back
  URL host, which is usually fine for prod but collides if dev
  and prod share an ingress hostname.

## What's not covered yet

- **`cargo cardano init` / `cargo cardano deploy`** — strategic
  shape per `MITOS_COMPANION_PATTERN.md`, not built. Use the manual
  flow above.
- **CIP-30 / CIP-8 helpers in `mitos-companion`** — deferred to v2
  of the runtime SDK. Roll your own for now.
- **Auto-derived Interest from the module's typed event surface** —
  also deferred. Today the dApp explicitly manages Interest via
  `/api/_interest/subscribe`.
- **Multi-host deployment, cross-tenant isolation, per-module API
  keys** — see `MITOS_COMPANION_PATTERN.md` "Open questions".
