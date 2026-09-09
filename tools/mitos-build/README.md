# mitos-build

Build a wasm-module crate into a deployable artifact directory.

Family A (see the root README). Mirrors `worker-build`'s shape: one focused
tool, producing an artifact ready for `mitos-admin upload-module`.

```bash
mitos-build --manifest community-modules/<name>/mitos.toml
```

The output directory holds the `.wasm` plus the manifest the platform registry
reads at host startup. Modules are **not** baked into the bundle binary — the
bundle links the framework crates and loads artifacts.

- TOML schema, materialisation rules and manifest format:
  [`docs/design/MITOS_BUILD.md`](../../docs/design/MITOS_BUILD.md)
- Delivery sequence and manifest schema in context:
  [`docs/strategy/MITOS_PLATFORM_DEPLOYMENT.md`](../../docs/strategy/MITOS_PLATFORM_DEPLOYMENT.md)
- Writing the module in the first place:
  [`docs/HOWTO_FIRST_MODULE.md`](../../docs/HOWTO_FIRST_MODULE.md)

## Build it, then run it locally

`mitos-run` loads the artifact this produces and drives `init()` against
fixtures — no Dolos snapshot, no production host, full trap backtraces with
debug symbols intact. That loop is much faster than deploying to find out.
