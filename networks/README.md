# Known-network genesis

Cardano genesis JSON for the networks mitos ships (`mainnet`, `preprod`).
Copied from `infra/services/mitos/{mainnet,preprod}/` so a node does not
need a second repository. The bytes are embedded in `mitos-config` via
`include_bytes!`; a test pins each file's sha256.

Mithril aggregator URLs and keys come from dolos v1.2.0
`src/bin/dolos/init.rs` (`KnownNetwork`). The upstream peer does **not**:
dolos's `From<&KnownNetwork> for PeerConfig` uses Demeter; production and
the infra `dolos.toml` files use the Cardano Foundation backbone, and that
is the peer we derive.
