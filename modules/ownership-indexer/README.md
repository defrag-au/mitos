# ownership-indexer (host test fixture)

**This is a host-side integration test fixture, not the deployable
ownership module.**

The production / deployable shape of this module lives next to the
CF Worker that consumes its emissions:

```
cnft.dev-workers/workers/collections-mitos/modules/
├── ownership.rs
└── ownership.toml
```

Built via the single-file flow (`mitos-build --module …`), uploaded
to the host via `mitos-admin upload-module`. See
`docs/PR3B_DEPLOYMENT.md` for the deploy procedure and
`docs/design/MITOS_COMPANION_PATTERN.md` for the colocation
rationale.

## Why this directory still exists

`mitos-platform`'s integration tests
(`crates/mitos-platform/tests/{integration,admin,lifecycle,
equivalence}.rs`) need a real wasm component to drive the
host through. They build this module directly and skip cleanly
when the artifact isn't present:

```rust
fn ownership_module_wasm() -> Option<PathBuf> { … }
// tests fall through with `eprintln!("skipping…")` if None.
```

To run those tests:

```bash
cd modules/ownership-indexer
cargo build -p ownership-indexer-module --target wasm32-wasip2 --release
# produces target/wasm32-wasip2/release/ownership_indexer_module.wasm
cd ../..
cargo test -p mitos-platform
```

## Drift between this fixture and the deployable module

This fixture *should* track the deployable module's behaviour
closely so the host's integration tests stay representative. If
they drift, integration tests pass against a stale shape and the
host can break in production for shapes the tests don't cover.

When the deployable `ownership.rs` changes meaningfully:

1. Update the fixture (`modules/ownership-indexer/module/src/lib.rs`)
   to match.
2. Re-run the host integration suite.

A future cleanup could replace this multi-crate shape with the
single-file shape (a `modules/test-fixtures/ownership.rs` that
mitos-build materializes at test-setup time) so there's no
duplicated boilerplate to drift. Tracked but not prioritized.
