# redb access pattern

Mitos's host process embeds dolos and runs five+ redb files
concurrently — per-module `cursor.redb`, `kv.redb`,
`emissions.redb`, the global `subscriptions.redb`, plus dolos's
own state stores. redb 3.x is **single-writer-per-process**:
calling `redb::Database::open` on a file that's already open in
the same process fails with

> Database already open. Cannot acquire lock.

This document is the contract every new redb file in mitos must
follow. We've already tripped this twice; the rules below are
what the codebase has converged on after researching the
ecosystem (redb upstream, dolos's own pattern, fjall, balius).

## The pattern: typed Stores around `Arc<redb::Database>`

Every redb file is wrapped in a domain-typed struct that owns a
**private** `Arc<redb::Database>`. The struct derives `Clone`
(cheap — clones share the Arc), exposes only typed read/write
methods, and never returns `&redb::Database` in a public
signature.

```rust
#[derive(Clone)]
pub struct EmissionsStore {
    db: Arc<redb::Database>,
}
impl EmissionsStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ...>;
    pub fn append(&self, ...);
    pub fn get(&self, id: u64) -> Result<Option<Record>>;
    // ... every other method takes &self, no &Database leaks
}
```

This is **exactly the pattern dolos uses** (`StateStore`,
`RedbWalStore` in `crates/redb3/src/`). It works because:

- redb's `begin_write` takes `&self` and serialises writers
  internally. No actor / mpsc / per-DB tokio task needed.
- redb readers are MVCC; multiple `begin_read()` calls run
  concurrently against one open handle.
- `Database` is `Send + Sync`, so cloning the outer `Arc` and
  handing it to N async tasks is the standard pattern.

We do **not** use an actor pattern. Adding mpsc adds latency,
introduces a SPOF, and re-implements what redb already
serialises correctly.

## Where opens happen

Exactly four sites in the workspace open redb files. Each is
annotated with `#[allow(clippy::disallowed_methods)]` and a
comment naming the public entry point:

| File                                                       | Owner                                  | Public surface                                          |
|------------------------------------------------------------|----------------------------------------|---------------------------------------------------------|
| `mitos-platform/src/storage.rs::CursorStore::open`         | `ModuleStorage::cursor_store(id)`      | `ModuleStorage::{read,write}_cursor(id, …)`             |
| `mitos-platform/src/emissions.rs::EmissionsStore::open`    | `ModuleStorage::emissions_store(id)`   | `EmissionsStore::{append,get,update_status,…}`          |
| `mitos-platform/src/vendored/balius/kv.rs::RedbKv::try_new`| `ModuleStorage::kv_store(id, …)`       | `RedbKv::{get,set,delete,list}_value(module_id, …)`     |
| `mitos-core/src/replicator.rs::Replicator::new`            | `Replicator` (process singleton)       | `Replicator::{add,remove,list,summary,…}`               |

All other redb-using code obtains a handle via the public
surface — never via `Database::create` / `open` / `Builder`.

## Per-process caching is mandatory

`ModuleStorage` holds three caches keyed by module id:

```rust
cursor_stores:    HashMap<String, CursorStore>,
emissions_stores: HashMap<String, EmissionsStore>,
kv_stores:        HashMap<String, RedbKv>,
```

`*_store(id)` is `get-or-open`: first call per module pays
~hundreds of ms for redb's file-format check + repair pass;
subsequent calls return a `Clone` of the cached typed wrapper
(O(1) HashMap lookup + Arc clone).

Subsystems that need access — the emit drain task, the dialer's
poll loop, the subscribe handler's `peek_next_id`, the host
fns' `state_kv` — all call `storage.<store>_store(id)` and get
the same underlying `Arc<Database>`. The single-writer
constraint is satisfied by construction.

`close_<store>(id)` drops the cached handle so a follower stop
+ restart cycle re-opens redb cleanly. `host::stop` calls all
three `close_*` methods.

## The lint that enforces this

`clippy.toml` at the workspace root bans the three open methods
across all crates:

```toml
disallowed-methods = [
    { path = "redb::Database::create", reason = "..." },
    { path = "redb::Database::open",   reason = "..." },
    { path = "redb::Builder::create",  reason = "..." },
]
```

Run `cargo clippy --all-targets -- -D warnings` and the lint
fires on any unannotated open. Legitimate sites add
`#[allow(clippy::disallowed_methods)]` with a one-line comment
explaining why (the table above is the exhaustive list).

## Adding a new redb file

When you add a new redb-backed file:

1. Define a typed `XxxStore` struct in `mitos-platform` (or
   `mitos-core` for global, non-module-scoped state).
2. `db: Arc<redb::Database>`, `#[derive(Clone)]`, never expose
   `&Database` in a public method.
3. Open site goes inside `XxxStore::open` (or equivalent
   constructor) with `#[allow(clippy::disallowed_methods)]` +
   reason comment.
4. Add a cache + getter on `ModuleStorage` (per-module files)
   or open it once at process startup (global files).
5. Add a `close_xxx` method symmetric to `close_cursor` /
   `close_emissions` / `close_kv` and wire it into
   `host::stop` so follower restarts re-open cleanly.
6. Add a row to the table above.

Reviewing? Check the lint passes. If a PR `#[allow]`s the
disallowed_methods lint without one of the four sanctioned
sites, that's the smell that triggers a request for changes.

## Why we didn't pull in a community crate

We surveyed crates.io / lib.rs for `redb-pool`, `redb-actor`,
`redb-service` — nothing exists at this layer. The closest
hits are `redb-bincode`, `redb_model`, `struct_db` —
serialisation/ORM helpers that don't address multi-handle
sharing. Upstream redb's docs explicitly recommend "open once,
share via `Arc<Database>`"; that's exactly this pattern. The
dolos codebase is the proof that a real-world Cardano node with
five+ redb files runs this way without an actor in sight.

## References

- redb `Database` docs — <https://docs.rs/redb/latest/redb/struct.Database.html>
- redb issue #811 (read-only second open) — <https://github.com/cberner/redb/issues/811>
- dolos `RedbWalStore` — <https://github.com/txpipe/dolos/blob/main/crates/redb3/src/wal/mod.rs>
- dolos `StateStore` — <https://github.com/txpipe/dolos/blob/main/crates/redb3/src/state/mod.rs>
- fjall README (same constraint, same answer) — <https://github.com/fjall-rs/fjall>
