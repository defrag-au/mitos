# Community modules

Wasm modules that ship with mitos and auto-load on host startup.
See `mitos/docs/strategy/COMMUNITY_MODULES.md` for the design.

## Layout per module

```
<name>/
├── <name>.rs          # single-file module source
├── <name>.toml        # manifest (deps, addresses, abi_version)
├── fixtures/          # test fixtures (optional)
└── build/             # produced by `mitos-build --module <name>.rs`
    ├── <name>.wasm
    ├── manifest.toml
    └── config.cbor    # if <name>.toml carried runtime config
```

The `build/` directory is what the host's auto-load reads at
startup. Production releases ship pre-built artifacts here; dev
workflow produces them via `mitos-build`.

Event types for each module live as a submodule of
`mitos-community-events` (e.g. `mitos_community_events::jpg_store_offer`,
`mitos_community_events::asset_transfer`,
`mitos_community_events::holder_distribution`). Consumers
(`cnft.dev-workers` companions) depend on that single crate rather
than per-module events crates.
