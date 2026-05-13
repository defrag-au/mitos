# Vesting-Tracker Community Module

Watches Shield Vest / Shield CrowdLock lock contracts on Cardano
and emits typed lock/unlock events to subscribed consumers.

Companion to `holder-distribution` (the canonical holder ledger
per policy): `vesting-tracker` is the canonical *lock* ledger
per watched lock-contract address or payment credential.

## Why this module

The legacy holder-map flow in `cnft.dev-workers` reconciles
vesting via direct Maestro queries (`services/token-holders/src/
vesting.rs::reconcile_vesting` + `reconcile_crowdlock`). That
flow:

- pages Maestro `/addresses/{addr}/utxos?asset=X` for project
  vests
- pages Maestro `/addresses/cred/{script}/utxos?asset=X` for
  user CrowdLocks
- decodes the Shield datum
- resolves owner PKH → stake key via Maestro
  `/addresses/cred/{pkh}/utxos?count=1` (KV-cached)

Replacing this with a mitos community module gets us:

- chain-derived authority (no third-party indexer in the read
  path)
- per-TX live updates instead of periodic full rebuilds
- consumer-side UTxO-identified rows (precise unlock deletes)

It's the second of two modules whose adoption unblocks
`holder-map` cutting fully off Maestro — see
`cnft.dev-workers/docs/design/HOLDER_MAP_MITOS_CUTOVER_PLAN.md`.

## Scope: one module, two interest predicates

Shield project vests and CrowdLock user vests use **identical
on-chain Plutus datums**:

```text
Constructor 0 [
  Int(unlock_ts_ms),                               // field 0
  List[ Bytes(owner_payment_key_hash_28b) ]        // field 1
]
```

What differs is the discovery mechanism:

| Style | Where to find locks | Interest predicate |
|-------|--------------------|---------------------|
| Shield project vest | A fixed contract address per project | `AtAddress(bech32)` |
| CrowdLock user vest | Any address with this payment script | `AtPaymentCred(cred_28b)` |

Per repo decision D4c (supersedes D4b): **one module**,
`vesting-tracker`, with both interest predicate shapes. Same
datum decoder, same emission shape. The wire carries
`interest_kind` so consumers can scope snapshot replacement to
the right key.

## Event shape

Defined in `crates/mitos-community-events/src/vesting_tracker.rs`.

```rust
pub struct LockRef { tx_hash: String, index: u32 }

pub enum InterestKind { Address, PaymentCred }
pub enum VestStyle { Shield, CrowdLock, Unknown }

pub struct LockEntry {
    utxo_ref: LockRef,
    lock_address: String,
    policy: String,
    asset_name_hex: String,
    amount: u64,
    owner_pkh: String,
    owner_stake_cred_hex: Option<String>,
    unlock_ts_ms: u64,
    vest_style: VestStyle,
    locked_at_tx: String,
}

pub enum VestingEvent {
    Snapshot(VestingSnapshot),
    Locked(VestingLock),
    Unlocked(VestingUnlock),
}
```

### Per-UTxO emission

A single lock UTxO carries one or more non-lovelace assets
(typically one). The module emits one `LockEntry` per
`(utxo, policy, asset_name)` triple. Consumers dedup by all
three.

### Snapshot semantics

`Snapshot` is full-state replacement scoped to one
`(interest_kind, interest_value)` pair. Consumers wipe prior
locks under the same key and re-insert. Emitted on cold-start
registration and after rollbacks.

### Locked / Unlocked semantics

`Locked` per `(produced UTxO, policy, asset_name)` at a watched
address/cred. `Unlocked` identifies by `lock_ref` only — the
consumer deletes the row keyed on the UTxO ref. If the consumer
never saw a corresponding `Locked` (rollback gap, host restart
without snapshot), the delete is a no-op.

### VestStyle: module-authoritative

VestStyle isn't on-chain. The module derives it from the
locking TX's metadata key `674`:

- `msg` contains "Crowd Lock" → `CrowdLock`
- `msg` contains "Shield" → `Shield`
- otherwise → `Unknown`

For cold-start the module fetches metadata per lock UTxO's
producing TX via `chain_data::tx_metadata(oref.tx_hash)`.
For live events `tx_metadata(produced.tx_hash)` does the same.

## Owner stake-cred resolution

The Shield datum carries the owner's *payment* key hash. To
populate the consumer's `holder_vests` (keyed by stake_key),
the module resolves PKH → stake-cred via the new
`chain_data::resolve_stake_for_payment_pkh(pkh)` host-fn.

That host-fn is a thin convenience over
`utxos_by_payment_cred(pkh)`: walks the dolos by-payment-cred
index, picks the first UTxO with a stake part, returns it.
`None` when:

- the PKH has no current UTxOs anywhere
- all UTxOs using the PKH are at enterprise (no-stake) addresses

Consumers should treat `owner_stake_cred_hex: None` as an
unresolved lock and surface it without attributing to a wallet.

## WIT / chain-data delta

`crates/mitos-platform/wit-v2/world.wit` adds:

```wit
interface chain-data {
    use types.{... stake-cred};

    utxos-by-payment-cred: func(cred: list<u8>) -> list<output-ref>;
    resolve-stake-for-payment-pkh: func(pkh: list<u8>) -> option<stake-cred>;
}

interface interest {
    variant interest-predicate {
        at-address(string),
        at-payment-cred(list<u8>),     // new
        at-stake-cred(stake-cred),
        holds-policy(list<u8>),
        holds-asset(asset-id),
        tick-every(u32),
    }
}
```

`crates/mitos-data-plane/src/lib.rs`:

- `ChainDataPlane::utxos_by_payment_cred(&[u8]) -> Vec<OutputRef>`
- `ChainDataPlane::resolve_stake_for_payment_pkh(&[u8]) -> Option<StakeCred>`
  (default impl: walks `utxos_by_payment_cred` + reads first
  output; production impl can override with a single-row index
  lookup later)
- `InterestPredicate::AtPaymentCred([u8; 28])`

Dolos backing: `CardanoIndexExt::utxos_by_payment(cred)` — same
index Maestro uses for `/addresses/cred/{pkh}/utxos`. Cap: 100K
refs (parity with `utxos_by_policy`).

## Bootstrap path

`bootstrap_v2.rs` walks `interest.watched_payment_creds()`
alongside addresses and policies; each cred gets a one-shot
scan via `utxos_by_payment_cred` synthesised into the same
per-TX-grouped dispatch batches the address path uses.

State-kv flag key: `__platform/bootstrap/payment-cred/<hex>`.

## Module behaviour

Source: `community-modules/vesting-tracker/vesting_tracker.rs`.

- Dynamic interest only. Consumers `update_interest(Add,
  [AtAddress(...) | AtPaymentCred(...)])` per registered scope.
- Persists tracked addresses + payment creds via state-kv
  (`tracked-interests` key) so host restarts re-arm the
  filter before the companion reconnects.
- Cold-start per newly-added scope: scan, decode each lock's
  datum, resolve owner stake, look up metadata 674 for
  VestStyle, emit `Snapshot`.
- On `handle_events`: per Produced match → `Locked`;
  per Consumed match → `Unlocked`.

## Consumer pattern (sketch)

```rust
async fn apply_event(&self, ctx: &Ctx, event: VestingEvent) {
    match event {
        VestingEvent::Snapshot(snap) => {
            ctx.exec(
                "DELETE FROM holder_vests WHERE interest_kind = ? AND interest_value = ?",
                vec![interest_kind_to_sql(&snap.interest_kind), snap.interest_value.as_str().into()],
            )?;
            for lock in snap.locks {
                insert_holder_vest(ctx, &lock)?;
            }
        }
        VestingEvent::Locked(VestingLock { lock, .. }) => {
            upsert_holder_vest(ctx, &lock)?;
        }
        VestingEvent::Unlocked(u) => {
            ctx.exec(
                "DELETE FROM holder_vests WHERE lock_tx_hash = ? AND lock_output_index = ?",
                vec![u.lock_ref.tx_hash.as_str().into(), (u.lock_ref.index as i64).into()],
            )?;
        }
    }
}
```

Worker-side schema:

```sql
CREATE TABLE holder_vests (
    stake_key TEXT,                  -- resolved owner stake (or empty for unresolved)
    owner_pkh TEXT NOT NULL,
    lock_tx_hash TEXT NOT NULL,
    lock_output_index INTEGER NOT NULL,
    lock_address TEXT NOT NULL,
    policy TEXT NOT NULL,
    asset_name_hex TEXT NOT NULL,
    amount INTEGER NOT NULL,
    unlock_ts_ms INTEGER NOT NULL,
    vest_style TEXT NOT NULL,
    interest_kind TEXT NOT NULL,
    interest_value TEXT NOT NULL,
    PRIMARY KEY (lock_tx_hash, lock_output_index, policy, asset_name_hex)
);
```

(See cutover plan Phase 4 for the worker-side wiring.)

## Open / deferred

- **Cursor in Snapshot**: currently `cursor_slot = 0` / empty
  hash. Adding a `chain_data::tip()` host-fn (or threading
  cursor through cold-start) would let consumers anchor the
  snapshot to a chain point — useful for chain-reorg recovery.
  Not load-bearing for v1.
- **Pagination past 100K refs**: cold-start suppresses snapshot
  if the cap is hit. CrowdLock contracts at scale (e.g.
  hundreds of thousands of historical locks) would need a
  paginated scan path. Defer.
- **Backfill for new payment-cred interests**: `utxos_by_payment_cred`
  returns *current unspent* UTxOs only. Historical unlocks
  before the consumer subscribed never surface — fine for
  vesting (locks are sticky until claimed) but not for active
  contracts.
