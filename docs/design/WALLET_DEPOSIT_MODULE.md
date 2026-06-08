# Wallet-Deposit Community Module

Watches consumer-declared addresses for **incoming** value — native
assets and (optionally) lovelace — and emits a typed `AddressDeposit`
event per asset landing at a watched address, attributed to the
sending wallet.

It is the inbound counterpart to the existing family: `burn-address`
already watches addresses but emits no sender and skips lovelace;
`asset-transfer` derives a sender but is policy-scoped and excludes
ADA. A deposit watcher is `burn-address`'s address interest + per-output
emission, plus `asset-transfer`'s per-TX sender netting. Almost nothing
is net-new.

## Why this module

The first consumer is **Treasure Island** (game in `augminted-bots`).
Players "bury treasure" by sending assets to a known managed wallet;
the game's Durable Object needs to learn *"asset X from sender S just
landed in managed wallet W"* and position it on the island. See
`augminted-bots/docs/design/treasure-island/10-managed-wallet-and-burying.md`.

Today that inbound path is heavy `cnft.dev-workers` plumbing:

- `captain-hook` ingests blocks (Oura/Blockfrost/Maestro), runs a
  `FilterEngine`, and consults an `address_routing:{network}` KV table
  (watched address → queue + `wallet_id`),
- emits a `WalletIncomingTx` onto a routed queue,
- consumed by the `wallet-inbound` worker, whose pluggable-routing
  (`wallet_processing_rules:{wallet_id}`) is still a TODO,
- which would then have to forward to the game.

Replacing that with a mitos module gets the same wins this repo already
banks for `vesting-tracker` / `holder-distribution`:

- **chain-derived authority** — no third-party indexer in the read path,
- **per-TX live updates** straight to the consumer's companion,
- **no captain-hook config** — no `FilterEngine` rules, no
  `address_routing` KV, no `wallet-inbound` queue consumer, no
  `wallet_processing_rules`. The consumer registers one `AtAddress`
  interest and receives typed events.

captain-hook's responsibilities are being slimmed as mitos grows; this
module moves address-watch ingestion off it for good.

## Scope: one interest predicate, any asset

| Aspect | Decision |
|--------|----------|
| Discovery | `AtAddress(bech32)` — the managed wallet(s). Dynamic, runtime-registered. |
| Assets | Every native asset landing at a watched address. Lovelace optionally (config flag), so ADA prizes can be buried too. |
| Sender | Derived per-TX from the consumed inputs (see below). Provenance only. |
| Direction | Inbound to the watched address. Outbound (the watched address respending) is out of scope — that's `asset-transfer`. |

`AtAddress` already exists (it's what `burn-address` uses), so **no new
WIT interest predicate is required**.

## Event shape

Proposed `crates/mitos-community-events/src/wallet_deposit.rs`:

```rust
use serde::{Deserialize, Serialize};

/// One asset (or lovelace) landing at a watched address within a TX.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddressDeposit {
    /// Watched (recipient) address the value landed at (bech32).
    /// Echoed back so a consumer tracking several wallets can route.
    pub to_address: String,
    /// Net sender (bech32), derived from the TX's consumed inputs.
    /// `None` when indeterminable — cold-start without input
    /// resolution, or a mint-funded output.
    pub from_address: Option<String>,
    /// 64-char lowercase hex tx hash that produced the output.
    pub tx_hash: String,
    /// Output index within `tx_hash`.
    pub output_index: u32,
    /// 56-char hex policy id. Empty string for a lovelace deposit.
    pub policy: String,
    /// Lowercase hex asset name. Empty for lovelace.
    pub asset_name_hex: String,
    /// Quantity — asset units, or lovelace when policy/asset are empty.
    pub quantity: u64,
    /// Absolute slot of the TX's block.
    pub slot: u64,
}
```

Single channel (`0`), one event per `(asset, output)` pair, exactly
like `AddressBurn`. Consumers **dedup on `(tx_hash, output_index,
policy, asset_name_hex)`** — emissions are idempotent so cold-start
re-walks and recapture refills are safe.

`from_address` is **provenance only**. For Treasure Island the payout
recipient is the *digger's* connected wallet, not the burier — so a
`None` sender on the cold-start path costs the consumer nothing.

## Live vs cold-start semantics

**Live (`handle_events`).** The v2 dispatch model delivers *all* events
of any TX that touched a watched address (per the `burn-address`
note), so a single batch carries both the `Produced` output at the
watched address *and* the `Consumed` inputs that funded it:

1. Per `Produced` at a watched address: emit one `AddressDeposit` per
   native asset (and lovelace if enabled). Filter per-output — the
   batch also contains the TX's change/fee outputs, which must not leak
   (same per-output guard `burn-address` applies).
2. Derive `from_address` by netting the `Consumed` inputs in the same
   batch — lift `asset-transfer`'s primary-sender rule (largest
   `|delta|`, ties by address lex sort). For a typical single-wallet
   bury this is just "the one input address."

So the live path needs **no new host-fn** — inputs are already in the
dispatch batch.

**Cold-start.** Newly-watched address → walk `utxos_by_address`
(paged, `WASM_BUDGET_CHUNKING.md`) and emit a deposit per current
unspent `(asset, output)`, mirroring `burn-address::cold_start_address`.
Because the managed wallet's UTxOs are *spent on payout*, the current
unspent set is exactly the **not-yet-paid-out deposits** — the right
recovery set for a fresh or restarted companion. `from_address` is
left `None` on this path (resolving it would require a producing-TX
input lookup; deferred — see Open).

State-kv persistence of the watched-address set + the re-entrant
`rebootstrap` cursor are copied wholesale from `burn-address`.

## What's reused vs net-new

| Piece | Source | Status |
|-------|--------|--------|
| `AtAddress` interest predicate | `burn-address` / data-plane | reuse, unchanged |
| Per-output emit + per-output filter | `burn-address::handle_produced` | copy |
| Cold-start address walk + paged `rebootstrap` | `burn-address` | copy |
| State-kv interest persistence | `burn-address` | copy |
| Per-TX sender netting | `asset-transfer` | lift |
| Lovelace deposits | — | small addition (burn-address skips ADA) |
| `AddressDeposit` event type | — | net-new, ~30 lines |
| Cold-start sender resolution | — | deferred (Open) |

No new WIT interest predicate, no new `chain-data` host-fn for the live
path. The module is `burn-address` with sender attribution and lovelace.

## Companion wiring (Treasure Island)

The Treasure Island DO becomes a `wallet-deposit` companion:

1. Register interest once: `update_interest(Add, [AtAddress(managed_wallet_bech32)])`
   via `/api/_interest/wallet-deposit/subscribe` (`kind = "address"`).
2. Receive `AddressDeposit` events on the companion channel.
3. Feed the existing bury handler (TI design doc, chapter 10):
   idempotent on `(tx_hash, output_index)`, seed a hex from `tx_hash`,
   append a `Buried` placement carrying the asset as payload and
   `from_address` as `buried_by`.

```mermaid
sequenceDiagram
    participant P as Player wallet
    participant M as mitos (wallet-deposit module)
    participant DO as TreasureIslandDO (companion)
    participant C as Connected clients

    P->>M: send asset(s) to managed wallet ("bury")
    M->>M: Produced at watched addr + sender from Consumed
    M->>DO: AddressDeposit { to, from, tx_hash, policy, asset, qty }
    DO->>DO: dedup (tx_hash, output_index); seed hex; append Buried; persist
    DO-->>C: TreasureBuried { by_user, tx_hash }
```

This collapses the captain-hook → `address_routing` KV → `WalletIncomingTx`
→ `wallet-inbound` → forward chain into a single module + companion
subscription. Spam/curation/irreversibility remain **consumer** concerns
(TI's per-wallet caps, denylist, min-UTxO economics) — the module just
reports deposits faithfully.

## Open / deferred

- **Cold-start sender resolution.** Live events get `from_address`
  free; cold-start leaves it `None`. A `chain_data` helper to resolve a
  producing TX's inputs (or threading sender through the bootstrap walk)
  would fill it. Not load-bearing for Treasure Island (sender is
  provenance, not the payout target). Defer.
- **Lovelace dust / spam.** If lovelace deposits are enabled, every
  min-UTxO output at the watched address emits an event. The consumer
  filters (TI's per-wallet cap / min-deposit). Consider an optional
  `min_lovelace` module config to suppress dust at the source.
- **Multi-sender attribution.** Batched/atomic-swap funding picks a
  single primary sender (asset-transfer's rule). Fine for provenance;
  flagged for parity.
- **Naming.** `wallet-deposit` vs `address-deposit` (the latter parallels
  `burn-address`). Maintainer's call; module-id is the hyphenated source
  stem either way.
