# mitos-build

> **Status: partially current.** The single-file build flow,
> materialisation pattern, manifest schema, and artifact-dir
> shape described below are accurate. The v1 ABI references
> throughout (`mitos:platform/mitos-module`, `wit/world.wit`)
> are stale — v1 was retired May 2026; the build tool now
> emits v2 artifacts only (`mitos:platform-v2/mitos-module-v2`,
> `wit-v2/world.wit`) and rejects `abi_version = 1` with a
> clear migration error.
>
> Treat WIT-shape examples in this doc as historical; pull
> current shape from `crates/mitos-platform/wit-v2/world.wit`
> and `tools/mitos-build/src/main.rs` directly.

The single-file-module build tool. Materialises a Cargo crate around
a `<name>.rs` + optional `<name>.toml`, runs `cargo build` against
the bundled WIT, and emits a deployable artifact directory consumable
by `mitos-admin upload-module`.

This doc is the **integration contract**: TOML schema, materialisation
rules, generated-crate layout, and the manifest format. Everything
here is observable in `tools/mitos-build/src/main.rs` — citations are
included so future drift is detectable.

Cross-references:
- `MITOS_COMPANION_PATTERN.md` — why modules live in the consumer
  repo, not in mitos's tree
- `MITOS_PLATFORM_V1.md` — the host runtime that consumes the
  artifacts this tool emits
- `SUBSCRIPTION_MECHANICS.md` — the `Interest` vocabulary modules
  receive via the `update-interest` WIT export
- `crates/mitos-platform/wit/world.wit` — the WIT contract every
  module implements (bundled into mitos-build at compile time)

## Module shape on disk

A single-file module is **two files** sitting next to each other,
typically inside a CF Worker's `modules/` directory:

```
workers/<my-worker>/
└── modules/
    ├── <name>.rs           # the module source (entirely user-owned)
    └── <name>.toml         # optional config + build-time deps
```

`<name>.rs` is read verbatim — there is **no Cargo.toml** for the
module. `mitos-build` synthesises a Cargo crate around it at build
time. This is the "wrangler-style: human edits TOML, machine ships
CBOR" convention (`main.rs:239`). `<name>.toml` is optional; modules
with no per-deploy configuration can omit it entirely.

### Module ID

Derived from the file stem with `_` → `-` (`main.rs:377`). A module at
`modules/jpg_co.rs` becomes module ID `jpg-co`. Override via
`--module-id <id>` on the CLI; must match `[a-z0-9-]+`, max 64 chars
(enforced by `mitos-platform::manifest::validate_module_id`).

## TOML schema

`<name>.toml` is two layered concerns in one file:

1. **Runtime config** — top-level keys deserialised by the module's
   own `Config` struct in `init(config: list<u8>)`. Schema is owned
   by the module author; mitos-build doesn't validate it.
2. **Build-time directives** — the `[deps]` table, consumed by
   mitos-build and stripped before CBOR-encoding (`main.rs:254-261`)
   so the module's `init` deserialiser never sees it.

```toml
# Top-level keys = your module's Config struct.
# Whatever shape you've declared in your `.rs` with
# `#[derive(Deserialize)]` and decoded in `init`.
policies = [
    "b3dab69f7e6100849434fb1781e34bd12a916557f6231b8d2629b6f6",
]

# Build-time only. Stripped before CBOR encoding.
[deps]
# String form: simple version dep
hex = "0.4"
# Table form: path / features / etc. - same syntax as Cargo.
my-events = { path = "../../../types/my-events" }
```

### Top-level keys (runtime config)

The shape is yours. Whatever your module's `Config` struct expects.
The TOML is parsed once by mitos-build, the `[deps]` table is removed,
the rest is CBOR-encoded into `config.cbor` (`main.rs:262-265`), shipped
alongside the wasm, and handed to `init` at module load.

If the file is missing, `init` receives an empty byte slice. Authors
typically `#[derive(Default)]` on `Config` so missing config means
"sensible defaults".

### `[deps]` table (build-time directives)

Each entry becomes a line in the synthesised crate's
`[dependencies]` block. Two value shapes are supported
(`main.rs:546-572`):

```toml
[deps]
# String → version-only dep.
hex = "0.4"

# Table → full Cargo dep table syntax. Common keys:
#   path     — relative to THIS toml file's directory; resolved + canonicalised
#   features — string array of features to enable
#   default-features — bool
#   git, branch, rev, tag — git deps
my-events  = { path = "../../../types/my-events" }
some-crate = { version = "1", features = ["foo"], default-features = false }
```

Path entries are resolved relative to `<name>.toml`'s parent
directory and canonicalised (`main.rs:556-563`), so the synthesised
`Cargo.toml` works regardless of where it ends up under
`target/mitos-build/`.

### Implicit deps you don't declare

Every materialised crate gets these unconditionally (`main.rs:474-478`):

```toml
wit-bindgen   = "0.54"
serde         = { version = "1", features = ["derive"] }
ciborium      = "0.2"
hex           = "0.4"
mitos-protocol = { path = "<absolute path baked at mitos-build compile time>" }
```

Don't redeclare these in `[deps]`. Cargo will error on duplicates.

### What goes where — quick recipe

| Need | Where |
|---|---|
| A constant the module reads at startup | top-level TOML key, in your `Config` struct |
| A list of policies, addresses, etc. to watch | top-level TOML key |
| A shared types crate (event shapes used by both halves) | `[deps]` with `path = "../../../types/<name>"` |
| A crates.io dep | `[deps]` with version string |
| Anything `init` should validate against | top-level (gets CBOR'd, decoded by you) |
| `wit-bindgen`, `serde`, `ciborium`, `mitos-protocol` | nothing — implicit |

## Materialisation rules

Given `modules/<name>.rs` + optional `<name>.toml`, mitos-build
generates a workspace under
`<source-dir>/target/mitos-build/<module-id>/` with this layout
(`main.rs:405-525`):

```
target/mitos-build/<module-id>/
├── Cargo.toml              # workspace; members = ["module"]
├── wit/
│   └── world.wit           # copy of crates/mitos-platform/wit/world.wit
└── module/
    ├── Cargo.toml          # cdylib crate; deps = implicit + user [deps]
    └── src/
        └── lib.rs          # = doc-comments + wit_bindgen! + user source
```

Generated files use **content-hashed write-if-changed** semantics
(`main.rs:610-616`) so cargo's incremental compilation cache stays
warm across no-op rebuilds.

### Workspace `Cargo.toml`

```toml
[workspace]
members = ["module"]
resolver = "2"

[workspace.package]
version = "0.0.0"
edition = "2024"
publish = false

[profile.release]
opt-level = "z"
lto = true
codegen-units = 1
strip = "symbols"

[patch."https://github.com/defrag-au/mitos"]
mitos-protocol = { path = "<baked-in absolute path>" }
```

The `[patch]` block redirects any transitive `mitos-protocol`
references (e.g. from your `[deps]` entries that pull
`mitos-protocol` via workspace inheritance) to the same physical
crate, avoiding "two different versions of crate `mitos_protocol`"
errors (`main.rs:425-429`).

### Module `Cargo.toml`

Crate name = `<module-id>-module` (`main.rs:406`). Fixed
crate-type, fixed implicit deps, your `[deps]` appended verbatim.

### Module `src/lib.rs`

```rust
// AUTO-GENERATED by mitos-build — DO NOT EDIT.
<your inner doc-comments hoisted to the top>

wit_bindgen::generate!({
    path: "../wit",
    world: "mitos-module",
});

<the rest of your <name>.rs>
```

Inner-doc handling: `//!` comments at the top of your `.rs` are
hoisted above the `wit_bindgen::generate!` macro invocation
(`main.rs:489-518`) because Rust requires inner attributes to
precede all items.

The `wit_bindgen::generate!` macro produces a module tree your
code accesses via `crate::mitos::platform::*`. After expansion,
the host-fn imports your code calls (e.g.
`crate::mitos::platform::state_kv::get_value`,
`crate::mitos::platform::logging::log`) resolve to bindgen-generated
extern "C" stubs that cross the WIT boundary into the host.

### Where host-fn imports come from

The `wit_bindgen::generate!` macro reads
`target/mitos-build/<id>/wit/world.wit` (a verbatim copy of
`crates/mitos-platform/wit/world.wit`, bundled into mitos-build
via `include_str!` at compile time — `main.rs:34`). The macro
expands to a module tree mirroring the WIT package structure:

| WIT interface | Rust path in the synthesised crate |
|---|---|
| `mitos:platform/types` | `crate::mitos::platform::types` |
| `mitos:platform/chain-data` | `crate::mitos::platform::chain_data` |
| `mitos:platform/state-kv` | `crate::mitos::platform::state_kv` |
| `mitos:platform/block-context` | `crate::mitos::platform::block_context` |
| `mitos:platform/emit` | `crate::mitos::platform::emit` |
| `mitos:platform/logging` | `crate::mitos::platform::logging` |
| `mitos:platform/interest` | `crate::mitos::platform::interest` |

Module authors call these as if they were ordinary Rust functions;
wit-bindgen handles the boundary marshalling. See
`crates/mitos-platform/wit/world.wit` for the full surface.

### Datum access — `decode-level` is the single knob

Modules that need datum bytes ask for them by setting the dispatch
decode-level to `with-datum` (or `full`) and calling
`block_context::resolved_block::get_output_datum(tx_idx, output_idx)`
or `get_consumed_input_datum(...)`. The host returns `option<typed_datum>`
with the resolved CBOR bytes — **regardless of whether the on-chain
output carried the datum inline or as a hash reference**. Modules
must never branch on inline-vs-hash; that's the caller-blind
resolution principle from `MITOS_DATA_PLANE_API.md`, and there's
deliberately no `plutus_data_by_hash` host fn that would re-leak
the distinction.

Cost-wise: indexers that don't need datums (e.g. ownership-by-policy)
should stay at `decode-level::lean` and never call the `*_datum`
methods, paying zero datum-resolution cost. Indexers that do need
them pay one archive lookup per cross-block hash datum (most blocks
touch <50 datums of interest; lookup is sub-millisecond redb read).

### What the module must export

The bundled WIT world `mitos:platform-v2/mitos-module-v2` requires
six exports (`crates/mitos-platform/wit-v2/world.wit:488-544`):

| Export | When called | Purpose |
|---|---|---|
| `module-version() -> (u32, u32)` | Once, at module load before init | ABI handshake; v2 modules return `(2, 0)`. |
| `trap-policy() -> (trap-strategy, retry-policy)` | Once, at module load | Declares replay/skip-and-mark/quarantine + retry shape |
| `init(config: list<u8>)` | Once, after handshake | CBOR-decode `config.cbor` into your `Config` |
| `handle-events(events: list<dispatch-event>)` | Per dispatched TX (or bootstrap chunk, tick, or rollback batch) | Decide what to emit; mutate `state-kv` |
| `update-interest(op, items-cbor) -> result<_, string>` | Each time the companion mutates Interest | CBOR-decode `Vec<Interest>`, apply to module's filter |
| `rebootstrap() -> result<rebootstrap-step, string>` | Recapture refill; re-entrant — host loops calls per fuel budget | Self-bootstrapping modules re-scan their interest set + re-emit. Event-driven modules return `{done: true, ingested: 0}`. |

`wit-bindgen` generates the trait surface; you implement
`Guest`/`GuestExports` per the macro's docs. See
`community-modules/standard-burn/standard_burn.rs` for a
worked reference and
`community-modules/holder-distribution/holder_distribution.rs`
for a non-trivial `rebootstrap` implementation.

## CLI

```
mitos-build --module modules/<name>.rs [--module-id <id>] [--profile release]
            [--out <dir>] [--wasm-path <path>] [--dry-run]
mitos-build prepare --module modules/<name>.rs
```

| Flag | Effect |
|---|---|
| `--module <path>` | Single-file mode. Pass the `.rs` (or stem). Mutually exclusive with `--crate-name`. |
| `--module-id <id>` | Override the auto-derived module ID. Default: file stem with `_`→`-`. |
| `--profile <name>` | Cargo profile. **Release only is supported in v1** — debug builds fuel-exhaust on realistic blocks (`main.rs:87-89`). |
| `--out <dir>` | Override artifact output path. Default: `<rs-dir>/target/mitos/<id>/`. |
| `--wasm-path <path>` | Skip cargo, use this prebuilt wasm. CI escape hatch. |
| `--dry-run` | Validate + print manifest without writing the artifact directory. |
| `--crate-name <name>` (legacy) | Multi-crate workspace shape (the in-tree `mitos/modules/<name>/` examples). Not the canonical shape for new modules. |

### `mitos-build prepare`

Materialises the synthesised crate without running `cargo build`,
prints the crate path. Open it in your IDE to point rust-analyzer at
the bindgen-generated trait surface for completion (`main.rs:113-147`).

```
$ mitos-build prepare --module modules/jpg_co.rs
/Users/.../workers/jpg-store-mirror/target/mitos-build/jpg-co/module
```

## Build flow

1. **Resolve** — find `<name>.rs`, find `<name>.toml` if present,
   derive module ID (`main.rs:349-385`).
2. **Materialise** — write `target/mitos-build/<id>/{Cargo.toml,
   wit/world.wit, module/Cargo.toml, module/src/lib.rs}`. Idempotent
   (write-if-changed). User's `[deps]` rendered into the module's
   Cargo.toml (`main.rs:533-583`).
3. **Build** — `cargo build --target wasm32-wasip2 --profile <profile>
   -p <name>-module` against the materialised workspace
   (`main.rs:298-334`).
4. **Inspect** — wasmtime-load the produced wasm to extract ABI
   version + trap policy + verify the WIT world matches
   `mitos:platform/mitos-module` (`main.rs:189-193`,
   `mitos-platform::inspect::dry_inspect`).
5. **Manifest** — generate `manifest.toml` from inspected values +
   build metadata (rustc version, git SHA, RFC3339 build_id, crate
   version) (`main.rs:618-663`).
6. **Emit** — write the artifact directory:

```
<rs-dir>/target/mitos/<module-id>/
├── <module-id>.wasm        # the binary
├── manifest.toml           # auto-generated; never hand-edit
└── config.cbor             # only if <name>.toml exists; [deps] stripped
```

## Manifest format

Auto-generated by mitos-build (`crates/mitos-platform/src/manifest.rs:19-63`),
shipped with every artifact, validated by the host on upload. Schema
is stable; bumps require coordinated mitos-build + mitos-platform
deploy.

```toml
[module]
id          = "jpg-co"
sha256      = "abcd...ef01"     # hex of <id>.wasm
size_bytes  = 248_192

[abi]
version_major = 1
version_minor = 0
wit_package   = "mitos:platform"
wit_world     = "mitos-module"

[trap_policy]
strategy        = "replay"      # | "skip-and-mark" | "quarantine"
max_retries     = 3
backoff_cap_ms  = 1000

[build]
rust_version  = "1.95.0"
target        = "wasm32-wasip2"
profile       = "release"
build_id      = "2026-05-07T12:34:00Z"
git_sha       = "abc123def456"
crate_version = "0.1.0"
```

`abi.version_major` is the binding stability promise. Host enforces
match against `HOST_ABI_MAJOR`; mismatched majors are rejected at
upload time (`crates/mitos-platform/src/admin.rs`).

`trap_policy.strategy` is read once at module load by the host
supervisor and consulted on every trap. `replay` requires
`handle-events` idempotency (per-batch).

## Path baking

The `mitos-protocol` path is **baked into mitos-build at compile
time** via `concat!(env!("CARGO_MANIFEST_DIR"), "/../../crates/mitos-protocol")`
(`main.rs:43-46`). Every materialised crate has this absolute path
in its `Cargo.toml`, so the synthesised crate finds it without the
user threading paths around.

**Operational consequence**: if you move the mitos source tree,
re-run `cargo install --path tools/mitos-build` to rebake the path.

## What mitos-build does NOT do

- **No upload.** That's `mitos-admin upload-module` (chained in
  `mitos-admin deploy`). Build outputs are filesystem artifacts only.
- **No companion-side work.** The CF Worker DO half is built by
  `wrangler` against your worker's own `Cargo.toml`. mitos-build
  only knows about the mitos-side wasm module.
- **No shared-types-crate authoring.** You write `types/<name>-events/`
  yourself; mitos-build just lets your module's `[deps]` reference
  it via `path = "..."`.

## Worked example

For a module at `workers/jpg-store-mirror/modules/jpg_co.rs` +
`jpg_co.toml`:

```bash
$ cd workers/jpg-store-mirror
$ mitos-build --module modules/jpg_co.rs
# materialises:  modules/target/mitos-build/jpg-co/
# emits:         modules/target/mitos/jpg-co/{jpg-co.wasm, manifest.toml, config.cbor}

$ mitos-admin upload-module \
    --artifact modules/target/mitos/jpg-co \
    --mitos http://mitos-host:8080
```

Subsequent runs are incremental — write-if-changed keeps cargo's
cache warm; only files whose content actually changed are rewritten.
