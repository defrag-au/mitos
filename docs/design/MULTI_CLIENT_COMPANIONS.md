# Multi-Client Companions

## The bug

Two workers on different domains (e.g. `hooks.dev.epochify.space` and
`hooks.epochify.space`) that subscribe to the same module with the same
`companion_key` (typically a policy_id) collide on the host. The second
subscribe atomically overwrites the first's persisted record, and all
subsequent emissions get dialed back to **one** of the two URLs. The other
worker is permanently starved — and the host marks every event as `acked`
because the surviving URL keeps responding 200.

Observed in the holder-map cutover: dev's worker had been subscribed for
weeks against `hooks.dev.epochify.space`; prod came up with the same
`MitosCompanion::NAME` and policy IDs, dialing back to
`hooks.epochify.space`. Dev's URL kept winning the URL slot, so every
prod-recapture emission landed on dev (which acked cleanly because the dev
DOs already had the policies) and prod's DOs stayed empty.

## Root cause

The companion store keys persisted records by `(module_id, companion_key)`:

```
<storage>/<module_id>/companions/<companion_key>.cbor
```

The whole `SubscribeRequest` (including `dial_back.url`) is written to that
single file. `drain_one()` scans the directory and produces one emission row
per file, and the dialer dispatches each row to the URL inside that single
persisted `SubscribeRequest`.

The design implicitly assumes **at most one dApp instance per
`companion_key`** — which is wrong as soon as you have a dev environment, a
canary, blue/green, or any kind of fan-out where two workers legitimately
want to consume the same policy stream.

## The fix: per-client companion records

Introduce a third identity component, `client_id`, to disambiguate companion
instances sharing the same `companion_key`. The primary key becomes:

```
(module_id, client_id, companion_key)
```

Two workers subscribing with the same `companion_key` but different
`client_id`s produce **two** companion records. Module emissions fan out to
both. Each gets its own dial-back URL, its own emission stream, its own
cursor.

### Why a separate `client_id` rather than implicitly disambiguating by URL?

We considered hashing the dial-back URL host as an implicit disambiguator.
Rejected because:

- A URL is a transport address, not an identity. A worker that legitimately
  changes its URL (different ingress, different domain after migration)
  would be treated as a new companion and double-deliver until cleanup.
- An explicit `client_id` makes the contract legible: callers know two
  registrations with the same key but different `client_id`s are distinct
  consumers by design.
- Explicit identity gives us a clean handle for admin operations
  (`delete-companion --client-id …`).

`client_id` is opaque to the host. The dApp side picks a value that's
stable for the lifetime of the worker instance. The mitos-companion
runtime defaults it to the host portion of the dial-back URL when the dApp
doesn't supply one — so the common case (each worker on its own ingress
hostname) needs no dApp-side configuration.

## Wire changes

### `SubscribeRequest` gains a required `client_id`

`crates/mitos-protocol/src/subscribe.rs`:

```rust
pub struct SubscribeRequest {
    pub targets: Vec<SubscribeTarget>,
    pub companion_key: String,
    pub client_id: String,            // NEW — required, non-empty
    pub resume_from: Option<ChainPoint>,
    pub interests: Vec<Interest>,
    pub dial_back: Option<DialBackOverride>,
}
```

**No `Option`, no `#[serde(default)]`.** A subscribe request without a
`client_id` field, or with an empty/whitespace-only value, is rejected by
the host with `400 Bad Request: client_id required (non-empty)`. This is
intentional — the bug being fixed is identity ambiguity, and tolerating
"please pick one for me" semantics on the wire just reintroduces the
same class of failure mode where two callers don't realise they're
colliding.

Old clients break. The break is **the intended migration**: every dApp
SDK gets a one-time touch to set `client_id` (or to upgrade to a SDK
version that derives it automatically). The blast radius is bounded
(there's exactly one consumer of mitos-companion at the time of writing,
and it's the cnft.dev-workers monorepo).

### Host-side validation

At `/api/companions/subscribe`:

```rust
if request.client_id.trim().is_empty() {
    return Err(BadRequest("client_id required (non-empty)"));
}
// (Optional) character-set + length sanity:
//   ASCII alnum + `_`, `-`, `.`, max 128 chars. Bech32 stake addresses
//   fit; URL hosts fit; UUIDs fit.
```

The host doesn't synthesise or default `client_id` — that responsibility
sits entirely with the caller. The mitos-companion SDK is what knows
about dial-back URLs and worker identity; surfacing that knowledge to
the host via an automatic fallback would be the wrong layering.

### `MitosCompanion::NAME` semantics unchanged

`NAME` is the *companion type* (the dApp's logical identity); `client_id`
is the *instance*. They're orthogonal — keeping `NAME` intact means the
worker code doesn't change shape.

## Storage layout

```
<storage>/<module_id>/companions/<client_id>/<companion_key>.cbor
```

Two-level directory: the new `client_id` directory sits between the
existing `<module>/companions/` and the `<companion_key>.cbor` leaf.
`client_id` is URL-encoded for filesystem safety (`:` and `/` escape).

### Migration

On host start, a one-time scan rewrites the flat layout into the new one
*and* synthesises a `client_id` for each pre-fix record. The record's own
`SubscribeRequest.dial_back.url` is the source of truth — the URL host
becomes the synthesised `client_id`. Records without a `dial_back.url`
(unreachable, can't have been delivering anyway) are quarantined into
`<storage>/<module>/companions/.unreachable/` for an operator to inspect
and `delete-companion` manually — we don't invent a placeholder
`client_id` for them.

1. For each `<storage>/<module>/companions/*.cbor` (no subdirectory):
2. Decode the `SubscribeRequest`.
3. If `dial_back.url` is `Some(u)`: derive `client_id = host(u)`, set it
   on the request, re-encode, move to
   `<storage>/<module>/companions/<client_id>/<companion_key>.cbor`.
4. If `dial_back.url` is `None`: move to
   `<storage>/<module>/companions/.unreachable/<companion_key>.cbor`,
   warn-log.
5. Record migration in module-level meta
   (`migrated_companions_to_client_id_v1`).

Idempotent: a host that has already migrated finds no flat `*.cbor` files
at the old path and is a no-op. Workers that come back later subscribe
with their actual `client_id` and naturally land in the new layout.

## Emission row schema

`EmissionRecord` carries the composite identity explicitly:

```rust
pub struct EmissionRecord {
    pub id: u64,
    // ... existing fields ...
    pub companion_id: String,          // unchanged: still <companion_key>
    pub client_id: String,             // NEW
    // ...
}
```

Two new fields on the redb encoding. Existing rows (pre-migration) get
`client_id = "legacy"` synthesised during the migration pass; the emissions
table itself doesn't need rewriting since pre-migration rows are all
either acked or nacked.

The dialer's `list_queued_for_companion` becomes
`list_queued_for_client_companion(client_id, companion_key)`.

## Dispatch fan-out

`drain_one()` walks `<storage>/<module>/companions/*/<companion_key>.cbor`
(both directory levels). For every matching `(client_id, companion_key)`
file, write one emission row carrying both fields. The dialer pool keys
its in-memory task map by `(client_id, companion_key)` — two workers'
streams are independent.

URL resolution stays per-task: each `run_companion` task reads its own
`SubscribeRequest.dial_back.url` and substitutes `{op}/{target}/{key}` as
before. `{key}` continues to be the `companion_key`; we don't expose
`client_id` in the URL template (it's a host concern, not a dApp routing
concern).

## Admin operations

- `evict-module <id>`: unchanged shape, recursively removes all
  `client_id` subdirectories.
- `restart-module <id>`: unchanged.
- **New**: `delete-companion --module <id> --client-id <c> --key <k>` —
  surgically remove a single companion record. Useful for cleanup when
  one consumer goes away but others remain.
- `recapture <id>`: targets all `(client_id, companion_key)` pairs under
  the module, sending Recapture/RecaptureReady to each in parallel. The
  v1 `--companion "*"` semantic is preserved; per-`(client_id,key)`
  targeting is a future option.

## Worker-side change

`mitos-companion`'s subscribe builder MUST produce a non-empty
`client_id`. It computes one with this precedence:

1. `MitosCompanion::client_id()` (new trait method) — explicit dApp
   override. Default impl returns `None`.
2. Host portion of `MITOS_REPLICATE_URL` (the obvious source — the URL
   is already worker-unique in any sane deployment).
3. **Panic at runtime startup** if neither yields a value. We refuse to
   ship subscribes without an identity; failing loud at boot is better
   than silently colliding once in flight.

No `cnft.dev-workers`-side code change needed for the holder-map case —
the dial-back URL hostnames already differ between dev and prod
(`hooks.dev.epochify.space` vs `hooks.epochify.space`). The SDK derives
the right value automatically.

## Backwards compatibility

- **Old worker → new host**: subscribe arrives without `client_id`. Host
  returns 400. Worker SDK must be upgraded. **This is the breaking
  change we want** — the alternative is leaving the identity-ambiguity
  hole open for new callers.
- **New worker → old host**: subscribe carries `client_id`; old host
  ignores the unknown field (CBOR is tolerant). Behaves the same as old
  host does today (one-record-per-key, second-write-wins).
- **Existing flat companion files**: on-disk migration runs once on host
  start. Reachable records (with a `dial_back.url`) are migrated;
  unreachable ones are quarantined for operator review.
- **Existing emission rows**: keep their `companion_id`; new rows carry
  both `companion_id` and `client_id`. Dialer matches by composite.

The host-side rev should land + deploy *before* the worker SDK rev to
keep the upgrade window short — the worker can't subscribe until the
host accepts `client_id`-bearing requests.

## Implementation slices

1. **Protocol**: add `client_id` field to `SubscribeRequest`.
2. **Companion store**: two-level directory layout + URL-host fallback.
3. **On-disk migration**: scan old flat files on host start, move to new
   layout.
4. **Emission row**: add `client_id` field, persist both, query by
   composite.
5. **Drain + dispatch**: fan-out per `(client_id, companion_key)`, key
   dialer tasks by composite.
6. **Admin**: extend list outputs and `evict-module` to handle subdirs;
   add `delete-companion`.
7. **mitos-companion runtime**: derive `client_id` from URL host in the
   subscribe builder; optional `Companion::client_id()` override.
8. **Tests**: platform integration test for "two clients, same key" round
   trip; golden run untouched (modules don't change).
9. **Holder-map verify**: redeploy mitos host, observe both dev + prod
   companion records, recapture, confirm both DO populations populate
   independently.

## What this doesn't do

- Doesn't change the `companion_key` semantic — still a free-form
  dApp-chosen string, still routes through `{key}` in the URL template.
- Doesn't introduce client_id-level interest filtering or per-client
  cursor sharing. Each `(client_id, companion_key)` is fully independent.
- Doesn't change the recapture protocol surface. Reason text + companion
  count in the response stay the same shape.
- Doesn't address the broader "are companion identities first-class
  enough" question (e.g. should subscribes carry a signed identity, a TTL,
  etc.). That's a separate conversation.
