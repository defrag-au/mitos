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
      # mitos is a native Rust workspace (no wasm/wrangler in its own
      # build path), but reusing `rust-worker-stack` keeps the toolchain
      # in lock-step with the wider defrag org. The extra tools cost
      # ~nothing in shell startup.
      mkShells =
        system: {
          default = defrag-nix.devShells.${system}.rust-worker-stack;
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
