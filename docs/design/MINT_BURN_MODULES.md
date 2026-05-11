# Mint + burn community modules

Five community modules under `mitos/community-modules/` that
together replace the in-tree `mint-burn-indexer` with finer-
grained event shapes and per-CIP decode. Each emits typed
events via a submodule of `mitos-community-events`.

| Module | Detects | Interest model |
|---|---|---|
| `standard-burn` | `tx.mint` entries with negative quantity for watched policies | Dynamic policies (`update-interest` from companion) |
| `burn-address` | Assets landing at outputs whose address is in the watched set | Dynamic addresses (consumer-declared "burn sinks") |
| `cip-25-mint` | Positive `tx.mint` entries for watched policies whose TX carries label-721 metadata for the asset | Dynamic policies |
| `cip-68-mint` | Paired `_100` reference + `_222`/`_333`/`_444` user-token mints for watched policies; carries decoded reference-datum metadata | Dynamic policies |
| `asset-metadata-update` | CIP-25 metadata refresh on burn TXs (label-721 present on a non-positive mint); CIP-68 reference output respent with new datum | Dynamic policies |

The CIP-25 / CIP-68 split mirrors how the standards actually
work on chain — the decode paths share nothing meaningful, the
event shapes carry different metadata containers, and consumers
typically care about one or the other (or both, but as
independent streams).

## Why five, not one

A `mint-burn` mega-module would conflate detection paths that
have nothing in common at the chain level (negative mint vs.
sent-to-burn-address vs. CIP-25 mint with label-721 vs. CIP-68
paired mint with datum). It would also push the
"what counts as a burn?" decision into the module rather than
the consumer. The community-modules-first preference (see
`docs/strategy/LAYERED_RESPONSIBILITIES.md`) is to keep each
module narrow + composable; consumers subscribe to whatever
combination matches their semantic model.

## Event shapes — typed source of truth

Lives in `crates/mitos-community-events/src/<name>.rs`. Wire
format is CBOR via the existing companion-runtime path; field
shapes are reproduced here as the canonical reference.

### `standard_burn::Burn`

```rust
pub struct Burn {
    pub policy: String,             // 56-char lowercase hex
    pub asset_name_hex: String,
    pub tx_hash: String,            // 64-char hex
    pub quantity_burned: u64,       // positive — quantity_delta negated
}
```

No metadata — burns are destruction events.

### `burn_address::AddressBurn`

```rust
pub struct AddressBurn {
    pub policy: String,
    pub asset_name_hex: String,
    pub tx_hash: String,
    pub output_index: u32,
    pub quantity: u64,
    pub burn_address: String,       // bech32
}
```

One event per (asset, watched-address) pair landing in an output.

### `cip25_mint::Cip25Mint`

```rust
pub struct Cip25Mint {
    pub policy: String,
    pub asset_name_hex: String,
    pub tx_hash: String,
    pub quantity: u64,
    /// JSON-stringified label-721 metadata entry for this asset.
    /// `None` if the TX has label-721 data but decoding the entry
    /// for this asset failed (malformed metadata).
    pub metadata_json: Option<String>,
}
```

### `cip68_mint::{Cip68Mint, Cip68RefInfo, Cip68UserInfo}`

```rust
pub struct Cip68Mint {
    pub policy: String,
    pub tx_hash: String,
    pub reference: Cip68RefInfo,
    pub user: Cip68UserInfo,
}

pub struct Cip68RefInfo {
    pub asset_name_hex: String,     // `_100`-prefix-tagged name
    pub quantity: u64,              // typically 1
    /// Raw datum CBOR — for forensics + consumers that need to
    /// decode application-specific `extra` fields.
    #[serde(with = "serde_bytes")]
    pub datum_cbor: Vec<u8>,
    /// JSON-stringified CIP-68 metadata map (Constructor-0 field 0).
    /// `None` if datum decode failed.
    pub metadata_json: Option<String>,
}

pub struct Cip68UserInfo {
    pub asset_name_hex: String,     // `_222` / `_333` / `_444`-tagged
    pub quantity: u64,              // 1 for NFT, N for FT/RFT
    pub cip67_label: u32,           // 222 / 333 / 444
}
```

One event per `(reference, user)` pair detected in the TX. A TX
that mints two distinct CIP-68 asset families (e.g. a starter-
pack TX) emits two events. A TX that mints 10,000 copies of one
RFT card emits **one event with `user.quantity = 10000`** — the
RFT case is supply-by-quantity, not by repeated mint lines.

### `asset_metadata_update::AssetMetadataUpdate`

```rust
pub enum AssetMetadataUpdate {
    Cip25 {
        policy: String,
        asset_name_hex: String,
        tx_hash: String,
        new_metadata_json: Option<String>,
    },
    Cip68 {
        policy: String,
        reference_asset_name_hex: String,
        tx_hash: String,
        #[serde(with = "serde_bytes")]
        previous_datum_cbor: Vec<u8>,
        #[serde(with = "serde_bytes")]
        new_datum_cbor: Vec<u8>,
        previous_metadata_json: Option<String>,
        new_metadata_json: Option<String>,
    },
}
```

## Decode strategies

### CIP-67 label parsing

Assets following CIP-68 carry a 4-byte prefix on the asset name:
`(00 + label + checksum + 00)` where `label` is a `u16`
(`100` for reference, `222` for NFT, `333` for FT, `444` for
RFT). Pairing is by the **human-name suffix** (asset-name bytes
after the 4-byte prefix). Reference and user assets minted
together carry the same suffix with different label prefixes.

Helper lives in each CIP-68-touching module (`cip_68_mint` and
`asset_metadata_update`) — small enough to duplicate, big enough
that pulling a shared helper crate would impose extra build
overhead per community module. Keep duplicated for v1; promote
to a shared helper if a 3rd module needs it.

### CIP-25 metadata decode

Path: `chain-data::tx-metadata(tx_hash)` → opt CBOR aux-data →
walk to label `721` → walk to the `<policy_hex>` key → walk to
the `<asset_name>` key → serialise the resulting CBOR value to
JSON.

The label-721 block is by-spec a CBOR map keyed by policy hex
strings (the CIP says "policy bytes"; in practice it's the hex
representation). Asset names within are sometimes hex, sometimes
UTF-8 stringified — modules try both for robustness.

JSON-stringification uses `serde_json` via the standard CBOR →
JSON value conversion. Bytes that aren't valid UTF-8 become base64
strings; lists become arrays; maps become objects.

### CIP-68 datum decode

Path: from the reference token's `Produced` event, extract
`output.datum.payload` (raw CBOR PlutusData bytes) →
`pallas_primitives::PlutusData::decode` → expect
`Constr(0, [metadata_map, version_int, extra_any])`.

The `metadata_map` is `PlutusData::Map([(key_bytes, value)])`.
Key bytes are UTF-8 (or near-UTF-8 — e.g. "name", "image",
"description"). Values can be `BoundedBytes`, `Map`, `Array`,
or `Constr` (for image-as-list-of-chunks). Same JSON-conversion
strategy as CIP-25.

If the datum structure doesn't match (Constructor isn't 0, field
count is wrong, etc), `metadata_json = None` but `datum_cbor`
still ships so the consumer can do their own decode for
application-specific shapes.

## Interest models in detail

### Dynamic policies (4 of 5 modules)

`<name>.toml`'s `[interest]` is empty by default. The companion
runtime declares policies via `/api/_interest/<module-id>/subscribe`
with `kind = "policy"`. The platform's `update-interest` path
delivers the new predicate to the running module + invokes
`bootstrap_one_predicate(HoldsPolicy(p))` to hydrate current
state for that policy.

**Bootstrap semantics caveat for mint modules:** bootstrap
synthesises `Produced` events for current UTxOs holding the
watched policy — it does *not* replay historical `Minted`
events. A consumer subscribing late will only receive mint
events from the moment of subscription forward. Historical mint
events must be backfilled out-of-band (e.g. via a data-plane
query or external indexer). Documented per-module in the
module's `<name>.rs` header.

### Dynamic addresses (`burn-address`)

`<name>.toml`'s `[interest].addresses` is empty by default.
Consumer declares burn addresses via
`/api/_interest/<module-id>/subscribe` with `kind = "address"`.
The platform's bootstrap path scans current UTxOs at the
address; the module emits `AddressBurn` for each. Future
arrivals come via live `Produced` events.

**Why no static config?** Burn addresses are
brand/use-case-specific. The `$burnsnek` SNEK burn address
isn't universal; the TCG's "graveyard" address is its own. Each
consumer declares what counts as a burn sink for them.

## Mint detection: `Minted` events vs `read-tx`

Two access paths to `tx.mint`:

1. **`minted-event` variant on the dispatched event stream** —
   one per `(policy, asset_name)` pair in the TX's mint field,
   pre-filtered by the module's `holds-policy` / `holds-asset`
   predicates. Cheap; one host-fn call per TX is avoided.
2. **`chain-data::read-tx(tx_hash) -> tx-record`** — gives the
   full TX rollup including `mint: list<mint-entry>`,
   `aux-data`, all `outputs`, all `inputs.prior-output`. One
   host fn call per TX; module filters internally.

**`standard-burn`** uses (1). Per-policy filtering happens
host-side; the module just emits.

**`cip-25-mint`** uses (1) for the mint detection + a single
`tx-metadata(tx_hash)` call per matching TX for the aux-data.

**`cip-68-mint`** uses (1) for the mint detection. For each
`Minted` event with a CIP-67 prefix, the module needs the
paired event's reference datum — which comes via the same
TX's `Produced` events delivered in the same `handle-events`
call. Module buffers `Minted` + `Produced` for the TX
in-handler, pairs them, emits.

**`asset-metadata-update`** uses (2). The Cip25 variant needs
to check whether the TX's mint entries are net-zero or
negative (the "metadata refresh in a burn TX" hack); cleanest
done by walking the full `tx-record.mint`. The Cip68 variant
needs paired `Consumed` (with `prior-datum`) + `Produced` (with
new datum) on a reference asset; also delivered by `read-tx`.

## `<name>.toml` shape per module

```toml
abi_version = 2

# Empty default interest set — consumers declare what they
# watch via the companion runtime's `/api/_interest/*` endpoints.
[interest]
addresses = []
policies = []

[deps]
mitos-community-events = { path = "../../crates/mitos-community-events" }
# Per-module deps as needed — pallas-primitives for the
# CIP-68 paths, serde_json for metadata serialisation, etc.
```

## Implementation order + lines-of-code estimates

Building in this order so each lands cleanly:

1. **`standard-burn`** — ~150 lines. Establishes the
   `Minted`-event-driven pattern.
2. **`burn-address`** — ~150 lines. Establishes dynamic-address
   interest with no `read-tx` calls.
3. **`cip-25-mint`** — ~300 lines. First module that touches
   `tx-metadata` for the label-721 decode + JSON
   serialisation.
4. **`cip-68-mint`** — ~400 lines. Datum decode, CIP-67 label
   parsing, in-handler buffering for `Minted` + `Produced`
   pairing.
5. **`asset-metadata-update`** — ~300 lines. Shares CIP-25
   metadata-extract helper with `cip-25-mint`, CIP-68 datum
   decode with `cip-68-mint`. `read-tx`-driven.

Total ~1300 lines of community-module code + ~200 lines of
event types across 5 `mitos-community-events` submodules.

## Cross-references

- `docs/strategy/COMMUNITY_MODULES.md` — pattern this builds on
- `docs/strategy/LAYERED_RESPONSIBILITIES.md` — why community
  modules over an in-tree `mint-burn-indexer`
- `docs/HOWTO_CONSUMING_A_COMMUNITY_MODULE.md` — consumer-side
  walkthrough (subscribe targets, channels, recapture)
- `crates/mitos-platform/wit-v2/world.wit` — the WIT surface
  these modules build against
- `community-modules/jpg-co/jpg_co.rs` — reference for the
  module shape (host-fn imports, `Guest` impl, `handle_events`
  dispatch)
- `cnft.dev-workers/types/cardano-assets/` — the shared
  `AssetMetadata` typed decode consumers can layer over
  the `metadata_json` string
