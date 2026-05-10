# HOWTO: build your first companion module

A walkthrough for building a paired indexer-module + CF-Worker-DO
companion using **today's tooling**, not the future
`cargo cardano init` shape described in the strategy docs.

If you're trying to add chain-derived state to a CF Worker dApp and
you've read `MITOS_COMPANION_PATTERN.md` for the *why*, this doc is
the *how*. The reference implementation is
`cnft.dev-workers/workers/collections-mitos/` —
copy from it liberally.

Cross-references:
- `strategy/MITOS_COMPANION_PATTERN.md` — architectural rationale
- `strategy/MITOS_COMPANION_RUNTIME_V1.md` — companion SDK design
- `strategy/MITOS_PLATFORM_V1.md` — wasm-module runtime
- `design/MITOS_BUILD.md` — single-file-module build tool
- `design/SUBSCRIPTION_MECHANICS.md` — `Interest` vocabulary
- `design/CF_REPLICATION.md` — WS protocol between halves

## Prerequisites

- A CF Worker monorepo where your DO will live (e.g.
  `cnft.dev-workers/`)
- A running mitos host (or one you're prepared to spin up locally
  against testnet)
- `mitos-build` and `mitos-admin` installed:
  `cargo install --path tools/mitos-build` and
  `cargo install --path tools/mitos-admin` from the mitos repo
- Rust target: `rustup target add wasm32-wasip2`

## Repo layout

A companion-pattern dApp consists of three pieces in your worker
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

The shared types crate at `types/<feature>-events/` is the **single
source of truth** for the event shape. Both halves depend on it, so
any wire-format change becomes a compile error in the half that
didn't get updated. This is the structural fix for the "encoder /
decoder drift" the cnft.dev stack hit in production.

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
        // ... fields the indexer extracts and the companion stores
    },
    SomethingUndone {
        // ... reverse delta
    },
}
```

Keep this crate **dependency-light**. It compiles for three targets
(native, wasm32-unknown-unknown for the CF DO, wasm32-wasip2 for the
indexer module). Heavy deps will hurt one of those.

## Step 2 — write the indexer module

The module is **a single `.rs` file** at
`workers/<my-worker>/modules/<feature>.rs`. There is no Cargo.toml
for it — `mitos-build` synthesises one. See `design/MITOS_BUILD.md`
for the full materialisation rules.

### `<feature>.rs` skeleton

```rust
//! Brief description of what this module does.
//!
//! These leading inner-doc comments will be hoisted above the
//! injected `wit_bindgen::generate!` macro at materialisation time.

use std::cell::RefCell;
use serde::{Deserialize, Serialize};

use crate::mitos::platform::logging::{self, LogLevel};
use crate::mitos::platform::state_kv;
use crate::mitos::platform::block_context::ResolvedBlock;

use <feature>_events::<Feature>Change;

// CBOR'd typed config shipped via `<feature>.toml`. Mirror your
// runtime needs here.
#[derive(Debug, Clone, Default, Deserialize)]
struct Config {
    // top-level keys from <feature>.toml
}

thread_local! {
    static CONFIG: RefCell<Config> = const { RefCell::new(Config { /* ... */ }) };
}

// `wit_bindgen::generate!` produces a `Guest` trait. Implement it
// for a unit struct that mitos-build wires up.
struct Module;

impl crate::Guest for Module {
    fn module_version() -> (u32, u32) { (1, 0) }

    fn trap_policy() -> (TrapStrategy, RetryPolicy) {
        (TrapStrategy::Replay, RetryPolicy { max_retries: 3, backoff_cap_ms: 1000 })
    }

    fn init(config: Vec<u8>) {
        let cfg: Config = if config.is_empty() {
            Config::default()
        } else {
            ciborium::de::from_reader(&config[..]).expect("decode config")
        };
        CONFIG.with(|c| *c.borrow_mut() = cfg);
        logging::log(LogLevel::Info, "module", "init complete");
    }

    fn handle_event(channel: u32, block: ResolvedBlock) {
        // walk block.tx_count(), block.get_output(tx, idx), …
        // emit events via crate::mitos::platform::emit::emit_event(channel, cbor)
    }

    fn update_interest(op: InterestOp, items_cbor: Vec<u8>) -> Result<(), String> {
        // CBOR-decode Vec<Interest>, apply to your filter set,
        // optionally persist to state-kv for restart resilience.
        Ok(())
    }
}

crate::export!(Module);
```

The exact `Guest` trait surface comes from the generated bindings
— run `mitos-build prepare --module modules/<feature>.rs` and point
your editor at the printed path for rust-analyzer completion.

### `<feature>.toml`

```toml
# Top-level keys = your `Config` struct.
# Whatever shape your <Config> deserializer expects.
some_setting = "value"

# Build-time only. Stripped before CBOR encoding.
[deps]
<feature>-events = { path = "../../../types/<feature>-events" }
ciborium         = "0.2"
```

`mitos-build` strips `[deps]` before CBOR-encoding, so your
`Config` deserialiser never sees a bogus `deps` field. See
`design/MITOS_BUILD.md` for the full schema.

### What you import from `crate::mitos::platform::*`

The synthesised crate's `lib.rs` invokes `wit_bindgen::generate!`
with the bundled host WIT, producing host-fn imports under that
path. The five interfaces:

- `chain_data::read_utxos` — bulk UTxO lookups
- `state_kv::{get_value, set_value, delete_value}` — module-private KV
- `block_context::ResolvedBlock` — borrowed per-block context handed
  to `handle_event`
- `emit::emit_event(channel, cbor_bytes)` — push events to the host
- `logging::log(level, target, message)` — structured logs into the
  host's tracing subscriber
- `interest::InterestOp` — enum used by `update_interest`

See `crates/mitos-platform/wit/world.wit` for the precise function
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
mitos-protocol  = { workspace = true }    # Interest types
<feature>-events = { path = "../../types/<feature>-events" }
serde     = { version = "1", features = ["derive"] }
ciborium  = "0.2"
async-trait = { workspace = true }
anyhow    = "1"
```

The `mitos-companion` SDK provides a `MitosCompanionRuntime<C>` that
absorbs the WS lifecycle, cursor persistence, `/api/_interest/*`
endpoints, and Apply/Undo/Mark dispatch. You implement two traits.

### `MitosCompanion` — top-level companion declaration

```rust
use mitos_companion::{MitosCompanion, MitosChannel, MitosChannelDyn};

pub struct MyCompanion;

impl MitosCompanion for MyCompanion {
    const NAME: &'static str = "<feature>";
    type Config = ();

    fn channels(&self) -> Vec<Box<dyn MitosChannelDyn>> {
        vec![Box::new(<Feature>Channel)]
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

    async fn apply_event(&self, ctx: &Ctx, event: Self::Event) -> Result<()> {
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

Events arrive as CBOR over WS; the SDK decodes into your
`type Event` automatically (compile-time-checked round-trip with
the indexer module via the shared types crate).

### Wire the runtime into your DO

```rust
#[durable_object]
pub struct MyDO {
    runtime: MitosCompanionRuntime<MyCompanion>,
}

impl DurableObject for MyDO {
    fn new(state: State, env: Env) -> Self {
        // Wasm-module companion — `::module()` constructor. Use
        // `::indexer()` instead when subscribing to an in-tree
        // indexer like `marketplace` or `mint-burn`. See
        // `docs/design/UNIFIED_SUBSCRIBE.md`.
        Self { runtime: MitosCompanionRuntime::module(state, env, MyCompanion) }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        ensure_dapp_schema(self.runtime.state())?;  // your tables
        match req.path().as_str() {
            "/my-rpc-endpoint" => self.handle_my_rpc(req).await,
            _ => self.runtime.fetch(req).await,  // delegate /api/_interest/*, etc.
        }
    }

    async fn websocket_message(&self, ws: WebSocket, msg: WebSocketIncomingMessage)
        -> Result<()>
    {
        ensure_dapp_schema(self.runtime.state())?;
        self.runtime.websocket_message(ws, msg).await
    }
    // websocket_close, websocket_error similar — forward to runtime
}

fn ensure_dapp_schema(state: &State) -> Result<()> {
    let sql = state.storage().sql();
    sql.exec("CREATE TABLE IF NOT EXISTS my_table (...)", vec![])?;
    Ok(())
}
```

Your dApp owns the SQL schema; the runtime owns its own bookkeeping
tables (`mitos_companion_meta`, `mitos_companion_interest`).

## Step 4 — build the module

```bash
$ cd workers/<my-worker>
$ mitos-build --module modules/<feature>.rs
# emits:  modules/target/mitos/<feature>/{<feature>.wasm, manifest.toml, config.cbor}
```

Profile is release-only in v1 (debug builds fuel-exhaust on
realistic blocks). See `design/MITOS_BUILD.md` for the full CLI.

## Step 5 — deploy the module to mitos

```bash
$ mitos-admin upload-module \
    --artifact workers/<my-worker>/modules/target/mitos/<feature> \
    --mitos http://mitos-host:8080
```

This POSTs the wasm + manifest + config.cbor multipart to
`/_admin/modules/<id>`. The host validates the manifest, verifies
the wasm SHA, parses the WIT world, and (if a host is wired in)
instantiates the module — calling `init(config)` with the CBOR
bytes from `config.cbor`.

Subsequent uploads under the same module ID **replace** the running
instance. State-kv and the chain-point cursor **persist across
replacement** (`crates/mitos-platform/src/host.rs:101-103`).

## Step 6 — deploy the companion to CF

```bash
$ cd workers/<my-worker>
$ wrangler deploy
```

Standard CF Workers flow. The DO runs on first request.

## Step 7 — register the subscription

mitos dials the companion's WS endpoint. Tell mitos where to dial:

```bash
$ mitos-admin add \
    --indexer <feature> \
    --target  wss://<my-worker>.workers.dev/<feature>/replicate \
    --scope-json '{}' \
    --cursor  origin \
    --mitos   http://mitos-host:8080
```

The companion's `/replicate` endpoint accepts the inbound WS via
the SDK's hibernation-API handling. From there events flow:

1. Mitos opens WS → companion accepts (via runtime).
2. Companion advertises Interest via `ClientMessage::Interest`
   over the WS (driven by `/api/_interest/subscribe` calls).
3. Mitos applies the filter, pushes matching `ServerMessage::Apply`
   events.
4. Runtime decodes CBOR → calls your channel's `apply_event(ctx, ev)`
   → cursor advances on success.
5. On `Undo` / `Mark`, runtime dispatches to the appropriate
   handler.

## Step 8 — assert canonical interest at runtime

The TOML's bootstrap config (e.g. `policies = [...]`) is a
**first-init only** fallback — used when the module's persisted
interest state is empty. The canonical Interest set is asserted by
the companion calling its own `/api/_interest/subscribe`:

```bash
# from a frontend / admin tool / migration script:
$ curl -X POST https://<my-worker>.workers.dev/api/_interest/subscribe \
    -H 'content-type: application/json' \
    -d '{ "kind": "policy", "value": "<policy-id>", "channel": "<feature>" }'
```

The companion writes to `mitos_companion_interest`, then forwards a
`ClientMessage::Interest { op: Add, items: [...] }` over the held WS
to mitos, which routes it via `update-interest` into the running
module. Filter changes take effect on the next block.

This is the load-bearing operational fact: **don't ship policy
hardcodes in `<feature>.toml` for production deploys.** Drive
Interest dynamically; the TOML's list is observability /
local-dev / first-boot only.

## Step 9 — iterate

Module changes:

```bash
$ mitos-build --module modules/<feature>.rs
$ mitos-admin upload-module --artifact modules/target/mitos/<feature>
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

## Reference implementation

See `cnft.dev-workers/workers/collections-mitos/`:

- `modules/ownership.rs` — single-file indexer module
- `modules/ownership.toml` — runtime config with `[deps]`
- `src/do_state.rs` — `MitosCompanion` + `MitosChannel` impls
- `Cargo.toml` — companion-side deps (`mitos-companion`,
  `mitos-protocol`, `collections-mitos-events`)
- The shared types crate at
  `cnft.dev-workers/types/collections-mitos-events/`

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
  canonicalised at build time. From `workers/<w>/modules/<f>.toml`,
  the typed-events crate at `<repo>/types/<f>-events/` is
  `../../../types/<f>-events`.
- **Skipping the shared types crate.** Inlining the event types in
  both halves means wire-format drift on every schema change. The
  crate is small but load-bearing.
- **Shipping policy hardcodes in `<feature>.toml` for production.**
  Bootstrap-only fallback. Drive Interest dynamically via the
  companion RPC.
- **Profile = dev.** Release only in v1; debug builds fuel-exhaust
  under realistic block sizes.

## What's not covered yet

- **`cargo cardano init` / `cargo cardano deploy`** — strategic
  shape per `MITOS_COMPANION_PATTERN.md`, not built. Use the manual
  flow above.
- **CIP-30 / CIP-8 helpers in `mitos-companion`** — deferred to v2
  of the runtime SDK (`MITOS_COMPANION_RUNTIME_V1.md`). Roll your own
  for now.
- **Auto-derived Interest from the indexer's typed event surface** —
  also deferred. Today, dApp explicitly manages Interest via
  `/api/_interest/subscribe`.
- **Multi-host deployment, cross-tenant isolation, per-module API
  keys** — see `MITOS_COMPANION_PATTERN.md` "Open questions".
