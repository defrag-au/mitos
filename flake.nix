{
  description = "mitos development shell — Cardano indexer framework";

  inputs = {
    defrag-nix.url = "github:defrag-au/defrag-nix";
  };

  outputs =
    { defrag-nix, ... }:
    let
      systems = [
        "aarch64-darwin"
        "x86_64-darwin"
        "x86_64-linux"
        "aarch64-linux"
      ];
      # mitos is a native Rust workspace, but its COMMUNITY MODULES build
      # for `wasm32-wasip2`, so reusing `rust-worker-stack` keeps the
      # toolchain in lock-step with the wider defrag org. The extra tools
      # cost ~nothing in shell startup.
      mkShells =
        system:
        let
          base = defrag-nix.devShells.${system}.rust-worker-stack;
          isDarwin = builtins.match ".*-darwin" system != null;
        in
        {
          # ── wasm32-wasip2 linking on darwin ──────────────────────────
          #
          # `rust-lld` is dynamically linked against `libLLVM.dylib` and its
          # rpath looks in `bin/../lib`, which in this toolchain layout is
          # `lib/rustlib/<target>/lib` — the library actually lives three
          # levels up at `lib/`. dyld therefore fails to load it and LLD
          # dies with SIGABRT, which surfaces from cargo as the unhelpful:
          #
          #   error: linking with `wasm-component-ld` failed
          #   error: failed to invoke LLD: signal: 6 (SIGABRT)
          #
          # Every community module fails to build locally without this —
          # it is NOT module-specific, and it is why `scripts/deploy.sh`
          # builds module wasm ON THE BOX. Linux is unaffected (ELF, and
          # the .so resolves).
          #
          # Derived from `rustc`'s own location rather than hardcoded, so a
          # toolchain bump does not silently re-break it.
          #
          # **The real home for this fix is defrag-nix's `rust-mixed`** —
          # anything in the org that LINKS a wasm32-wasip2 artifact hits
          # it. (A library-only wasip2 build does not link, which is why
          # `cargo build --target wasm32-wasip2 -p <lib>` looks fine.)
          default =
            if isDarwin then
              base.overrideAttrs (old: {
                shellHook = (old.shellHook or "") + ''
                  export DYLD_FALLBACK_LIBRARY_PATH="$(dirname "$(dirname "$(command -v rustc)")")/lib''${DYLD_FALLBACK_LIBRARY_PATH:+:$DYLD_FALLBACK_LIBRARY_PATH}"
                '';
              })
            else
              base;
        };
    in
    {
      devShells = builtins.listToAttrs (
        map (system: {
          name = system;
          value = mkShells system;
        }) systems
      );
    };
}
