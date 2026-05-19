# Recapture — coordinated state rebuild for companion DOs

**Status: v1 implemented + validated in production** (2026-05-11
end-to-end smoke test against `jpg-co`: 1 companion targeted,
5.3s round-trip, bootstrap walked 4444 UTxOs, 109 ghost rows
detected + corrected). Doc preserved as the canonical reference
for the protocol shape, failure modes, and deferred follow-up
work.

**Update (2026-05-19) — dynamic-interest modules.** v1 refilled
companions only via the host-side `run_bootstrap` walk over a
module's *static manifest `[interest]`*. Dynamic-interest modules
(`holder-distribution`, `burn-address`, `vesting-tracker`) declare
an empty manifest `[interest]` by design — their interest is
consumer-registered at runtime via `update-interest` — so
recapture was a silent no-op for them: companions dropped their
projected state on `on_recapture` and were never refilled. Fixed
by the `rebootstrap` module export — see "The `rebootstrap`
export" below.

A protocol + admin endpoint that lets an operator trigger a
clean state rebuild for a community module's subscribed
companions: the host signals each companion to drop its
projected state, waits for the companion to confirm, then
re-runs the module's bootstrap pass so the synthetic events
flow back into a clean target. Replaces the previous manual
two-step (operator runs the worker's `/reset` *and* SSHes to
the box to wipe `kv.redb`) with one coordinated operation.

v1 ships **`companion=*` (all-subscribers) only**. Targeting a
specific companion is forward-compatible in the API but
deferred to a follow-up — the moment a second consumer of any
community module exists, the value of per-companion targeting
becomes load-bearing and we'll build the targeted-emit path
then.

Cross-references:
- `mitos/docs/HOWTO_CONSUMING_A_COMMUNITY_MODULE.md` — operator
  + dApp-author HOWTO; references this doc for the protocol
  details.
- `mitos/docs/strategy/COMMUNITY_MODULES.md` — community
  modules layering this is built for.
- `mitos/docs/design/UNIFIED_SUBSCRIBE.md` — the WS protocol +
  `companions` registry this extends.
- `mitos/docs/design/SUBSCRIPTION_MECHANICS.md` — bootstrap_v2
  + the `[interest]` mechanism the bootstrap pass uses.
- `cnft.dev-workers/docs/JPG_STORE_MIRROR_RELAYERING.md` — the
  migration that surfaced the need.

## Motivation

Recapture used to "just work" because the operator's workflow
was: re-upload the wasm module via `mitos-admin upload-module`,
which incidentally cleared the module's `kv.redb` (bootstrap-done
flags), and then hit the worker's `/_admin/<companion>/reset`
to drop dApp tables + reset cursor. On the follower restart
that followed the re-upload, bootstrap_v2 saw missing flags,
walked unspent UTxOs at the module's watched addresses, and
the resulting synthetic events flowed back into the freshly-
emptied projection.

With community modules + auto-load, that side-channel is gone.
Modules don't get re-uploaded — they're auto-activated on host
startup. `kv.redb` survives auto-load (and should — wiping it
on every redeploy would cause a Created flurry into healthy
subscribers). So the worker's `/reset` alone now leaves the
projection empty *and no events come back to refill it*.

The mechanical answer is still "wipe `kv.redb` + restart", but
it's:

- Unsafe to drive blindly — if the worker hasn't cleaned up
  first, refill INSERTs collide with stale rows; rows for COs
  that were spent during the gap become ghosts.
- Requires SSH access to mitos.
- Couples two operator actions that should be one logical
  operation.

This doc designs the coupled version. The protocol primitive
is general; the v1 endpoint exercises just the all-subscribers
case so we get the operator UX without paying for the more
expensive per-companion plumbing speculatively.

## Today (uncoordinated)

```
operator              worker                       mitos
   │                    │                            │
   │ POST /reset ───────▶                            │
   │                    │ DROP TABLE                 │
   │                    │ cursor → Origin            │
   │                    │                            │
   │ ssh + rm kv.redb ─────────────────────────────▶ │
   │ systemctl restart ───────────────────────────▶  │ bootstrap walks UTxOs
   │                    │                            │ synthetic Produced events
   │                    │ ◀──── apply frames ─────── │
   │                    │ INSERT … ON CONFLICT       │
```

Two halves drive themselves. Order matters but isn't enforced.
Ghost rows possible if `/reset` is skipped.

## Proposed (coordinated)

```
operator                  mitos                            worker
   │                        │                                 │
   │ POST /_admin/modules/<id>/recapture?companion=* ─▶       │
   │                        │ for each companion in           │
   │                        │ <modules_dir>/<id>/companions/ :│
   │                        │  ws ctrl: Recapture ──────────▶ │
   │                        │                                 │ on_recapture hook fires
   │                        │                                 │  drops dApp tables
   │                        │                                 │  cursor → Origin
   │                        │ ◀── ws ctrl: RecaptureReady ─── │
   │                        │ (gather all; timeout per        │
   │                        │  companion)                     │
   │                        │ wipe module's bootstrap-done    │
   │                        │ flags in kv.redb                │
   │                        │ run bootstrap_v2 (no follower   │
   │                        │ restart needed — call directly) │
   │                        │ apply frames × N ─────────────▶ │
   │                        │                                 │ INSERT into empty table
   │                        │ ws ctrl: RecaptureDone ───────▶ │
   │ ◀── 200 OK ──────────  │ { companions: N, events: M,     │
   │                        │   duration_ms: T }              │
```

Ordering invariants:

1. **Mitos waits for every targeted companion to ACK
   `RecaptureReady` before wiping `kv.redb` or starting the
   bootstrap walk.** Without this, fast companions process
   refill events while slow ones are still mid-cleanup.
2. **Cleanup happens through the existing WS, not via the
   worker's HTTPS surface.** No race against parallel operator
   actions on the worker's `/reset` endpoint, no separate
   bearer-token negotiation.
3. **The frame stream stays in order per companion.** Apply
   frames produced after `Recapture` arrive after the
   companion has acked `RecaptureReady` — the worker decodes
   them with empty tables in place.

## Wire protocol

Two new control-frame variants in
`mitos-protocol::wire`:

### `ServerMessage::Recapture` (mitos → companion)

```rust
ServerMessage::Recapture {
    /// The source module being recaptured. Companions
    /// subscribed to multiple community modules (e.g. a worker
    /// consuming both `jpg-co` and `wayup-co`) MUST scope
    /// their cleanup to rows tagged with this module —
    /// blindly dropping the dApp tables would nuke rows from
    /// other subscriptions whose state isn't being refilled.
    /// See "Multi-module companions" below for the schema
    /// contract this implies.
    module: String,
    /// Operator-supplied free-form reason; surfaced in the
    /// companion's `on_recapture` callback for logging. Not
    /// load-bearing for the protocol.
    reason: Option<String>,
}
```

Semantics: "Drop the portion of your projected state that
originated from `module`. Reply with `RecaptureReady` when
done. Apply frames that follow on this companion's WS are
the refill for `module`."

The companion's `mitos-companion` runtime handles this frame
by:

1. Calling the dApp's
   `MitosCompanion::on_recapture(&self, ctx, module, reason)`
   hook. Default impl is a no-op + warning log; community-
   module consumers override and scope cleanup by `module`.
2. Sending `ClientMessage::RecaptureReady`.

**The runtime does NOT reset the companion's cursor.** Earlier
drafts had the runtime auto-reset `mitos_companion_meta` to
`ChainPoint::Origin`, which is safe in the single-target case
but wrong for multi-target companions — rewinding the cursor
affects subscriptions to *other* modules that aren't being
recaptured. The host's bootstrap re-emit produces the same
Apply frames that fill the dApp's tables; the cursor advances
naturally as they arrive. dApps that want explicit cursor
manipulation can do it in `on_recapture` via SQL, but it's
rarely needed.

### `ClientMessage::RecaptureReady` (companion → mitos)

```rust
ClientMessage::RecaptureReady,
```

No payload — the WS conversation is per-companion, so mitos
knows which companion ack'd from the socket alone. Semantics:
"State cleared. Send the refill."

### `ServerMessage::RecaptureDone` (mitos → companion)

```rust
ServerMessage::RecaptureDone {
    /// The cursor that the bootstrap pass advanced to. Helps
    /// the companion runtime checkpoint after the refill
    /// completes. Typically the host's current tip cursor at
    /// the moment bootstrap_v2 finished its UTxO walk.
    cursor: ChainPoint,
    /// How many synthetic events the host emitted for this
    /// companion's view. Useful for logs + operator feedback.
    events_emitted: u64,
}
```

Semantics: "Refill complete. Live chain events resume from
here." The frame is mostly informational — the apply frames
have already done the load-bearing work; this just gives the
companion a clean boundary marker.

## Trait change: `MitosCompanion::on_recapture`

Add to `mitos-companion::traits`:

```rust
/// Drop the portion of the dApp's projected state that
/// originated from `module`, in preparation for a refill from
/// the host's bootstrap pass against that one module. Called
/// by the runtime when the host sends `Recapture`; the
/// runtime then sends `RecaptureReady`.
///
/// **MUST scope cleanup by module** for any companion
/// subscribed to more than one community module. Blindly
/// dropping shared tables takes out rows from other
/// subscriptions that aren't being recaptured. See
/// "Multi-module companions" in RECAPTURE.md.
///
/// Default impl is a no-op + warning log — that's intentional:
/// companions that don't keep meaningful state (e.g. log-only
/// consumers) don't need to do anything. Companions that own
/// SQL tables MUST override.
///
/// `reason` is the free-form operator-supplied label from the
/// admin endpoint, useful for logs.
async fn on_recapture(
    &self,
    ctx: &Ctx,
    module: &str,
    reason: Option<&str>,
) -> Result<()> {
    tracing::warn!(
        module = %module,
        reason = ?reason,
        companion = Self::NAME,
        "Recapture received but on_recapture not implemented; \
         dApp state will be inconsistent after refill"
    );
    Ok(())
}
```

The body for `jpg-store-mirror`'s `JpgCoImpl` differs from
today's `handle_reset` in one important way: it `DELETE`s only
rows whose `source_module` column matches the recapture target,
rather than `DROP TABLE`. Sketch:

```rust
async fn on_recapture(&self, ctx: &Ctx, module: &str, _reason: Option<&str>)
    -> Result<()>
{
    let sql = ctx.storage().sql();
    sql.exec(
        "DELETE FROM collection_offers WHERE source_module = ?",
        vec![SqlStorageValue::from(module)],
    )?;
    Ok(())
}
```

`source_module` is the schema implication covered next.

## Multi-module companions

Recapture is per-module, not per-companion. A worker
subscribed to one community module today (e.g. `jpg-co` only)
can ignore the distinction, but the protocol's design has to
hold up for the Phase 4 `wayup-co` case where one worker
consumes events from two modules into shared dApp tables.

### Schema contract

**Any dApp table populated from a community module's events
MUST carry a `source_module` column** (or equivalent — a
brand enum tagged by module name works too). The column's
value is the module name that produced each row.

For `jpg-store-mirror` post-Phase-4 the `collection_offers`
table grows a column:

```sql
ALTER TABLE collection_offers
  ADD COLUMN source_module TEXT NOT NULL DEFAULT 'jpg-co';
```

The default value handles back-compat for rows inserted
before the column existed (all of which originated from
jpg-co, the only module subscribed at the time). New writes
from each channel set the column explicitly:

```rust
// In JpgCoChannel::apply_event
ctx.exec(
    "INSERT INTO collection_offers (..., source_module) VALUES (..., 'jpg-co') ...",
    /* params */
)?;

// In a future WayupCoChannel::apply_event
ctx.exec(
    "INSERT INTO collection_offers (..., source_module) VALUES (..., 'wayup-co') ...",
    /* params */
)?;
```

### Cleanup contract

`on_recapture(module, reason)` runs scoped DELETEs against
every table that carries `source_module`:

```rust
sql.exec(
    "DELETE FROM collection_offers WHERE source_module = ?",
    vec![SqlStorageValue::from(module)],
)?;
sql.exec(
    "DELETE FROM <other dApp table> WHERE source_module = ?",
    vec![SqlStorageValue::from(module)],
)?;
```

After cleanup the table contains only rows from the
*other* still-subscribed modules. The host's refill repopulates
the recaptured module's rows; live events from other modules
continue uninterrupted on their own WS streams.

### What about cross-module aggregates?

If the dApp maintains computed aggregates that span multiple
modules (e.g. "top CO across both jpg.store and wayup"),
those need to be recomputed after the refill. Two options:

1. **Recompute lazily** — the next query that touches the
   aggregate triggers a SQL recompute. Simplest. Works if
   the aggregate is cheap.
2. **Recompute in `on_recapture` after the per-module
   DELETE** — sets the aggregate to its pre-refill state
   (without the recaptured module's contributions). The
   first batch of refill Apply frames repopulates as they
   arrive.

V1 expectation: dApps choose per-aggregate. The recapture
protocol is agnostic.

### Single-module workers

Workers that only ever subscribe to one community module can
ignore the `source_module` column entirely and let their
`on_recapture` just `DROP TABLE` like the pre-protocol
`handle_reset` does. Same outcome; less ceremony. The
"`source_module` column" requirement only kicks in when a
second community module subscription joins.

This means jpg-store-mirror **doesn't need the schema migration
when recapture v1 ships** — single subscriber today, simple
DROP works. The migration lands as part of Phase 4's
wayup-co work.

## Admin endpoint

```
POST /_admin/modules/{module_id}/recapture
Authorization: Bearer <MITOS_AUTH_TOKEN>
Content-Type: application/json
Body (optional):
  {
    "companion": "*",          // v1 — only "*" accepted
    "reason": "manual rebuild" // optional, opaque label
  }
```

Default body is `{ "companion": "*" }`. Anything other than
`"*"` returns `400 Bad Request` with body
`{"error": "per-companion recapture not yet supported; use companion=*"}`
in v1.

Successful response:

```json
{
  "module": "jpg-co",
  "companions_targeted": 1,
  "events_emitted": 124,
  "duration_ms": 1842
}
```

Failure modes (each `400`/`409`/`504`):

| Code | Condition |
|---|---|
| 400 | `companion` not `"*"` in v1 |
| 400 | Unknown `module_id` |
| 409 | Recapture already in progress for this module |
| 504 | One or more companions failed to ACK `RecaptureReady` within timeout |

When the timeout fires, mitos **does not** wipe `kv.redb` or
re-run bootstrap. The slow companion's state is whatever its
`on_recapture` partially produced; safe-by-default since the
host hasn't fired any refill yet.

## Implementation plan

**All six commits landed and validated.** Section preserved
as the post-hoc breakdown of what landed where + the
reversibility notes that guided rollout. Source-of-truth for
each commit's behaviour is now the code; this is the map back
to the rationale.

Six commits, none destructive in isolation.

### 1. Protocol additions (`mitos-protocol`)

- Add `Recapture { reason }`, `RecaptureDone { cursor,
  events_emitted }` to `ServerMessage`.
- Add `RecaptureReady` to `ClientMessage`.
- Tests: encode/decode round-trip for each new variant.

Forward-compat: existing companions ignore unknown
`ServerMessage` variants? Need to check today's
`mitos-companion` runtime — `serde` should error on unknown
variants by default for adjacently-tagged enums, but I think
the WS protocol uses an internally-tagged enum that surfaces
unknown variants as a parse error. Either way: this is
additive on the host side (host produces new variants) and
consumers built against the new version will handle them
correctly. Old companions never see `Recapture` because the
host won't send it to them — admin endpoint is opt-in.

### 2. Trait hook (`mitos-companion::traits`)

- Add `on_recapture` method with default no-op + warning log.
- Runtime dispatch: on inbound `Recapture` frame, call hook,
  reset metadata-table cursor, send `RecaptureReady`.

### 3. Per-companion control flow (`mitos-platform::host_v2` +
`mitos-platform::dialer`)

- New `host.recapture_module(id, reason)` method:
  - Acquires a per-module recapture mutex (returns 409 if
    already held).
  - Reads `<modules_dir>/<id>/companions/` for the registry.
  - For each companion: spawn a task that sends `Recapture`
    over the existing dial-back WS and awaits
    `RecaptureReady` with a 30s timeout.
  - `try_join_all` — first timeout fails the whole call.
  - On success: wipe bootstrap-done keys in the module's
    `kv.redb` (state-kv helper).
  - Call `bootstrap_v2::run(module_id)` directly — no
    follower restart required since the follower's already
    running and listening on its event channel; bootstrap
    synthesises events that flow through the same dispatch
    path as the initial bootstrap.
  - After bootstrap returns: send `RecaptureDone` to each
    companion with the host's current tip cursor + counted
    events_emitted.
  - Release mutex.

### 4. Admin endpoint (`mitos-platform::admin`)

- `POST /_admin/modules/{module_id}/recapture` route, bearer
  auth, wraps `host.recapture_module(id, reason)`.
- Returns response payload above.
- 400/409/504 mapping per Failure modes table.

### 5. CLI subcommand (`mitos-admin`)

```
mitos-admin recapture <module_id> [--reason "rebuild after schema migration"]
```

- Wraps the admin endpoint. Output is the response JSON
  pretty-printed.

### 6. Worker integration (`cnft.dev-workers/workers/jpg-store-mirror`)

- `on_recapture` on `JpgCoImpl` landed as a single
  `DELETE FROM collection_offers` (chosen over the originally-
  sketched DROP+recreate — DELETE keeps the schema intact, no
  race against incoming refill Apply frames, and the body
  upgrades to `DELETE … WHERE source_module = ?` with one word
  edit when Phase 4 wayup-co lands and the schema gains the
  column).
- No cursor manipulation — the runtime no longer auto-resets
  on recapture, and the bootstrap refill's Apply frames
  naturally advance the cursor.
- Phase 4 (wayup-co) will land the `source_module` schema
  column and the scoped DELETE — covered in the relayering
  plan's Phase 4 work, not here.

Existing `POST /_admin/jpg-co/reset` endpoint stays — useful
as a fallback if the WS connection is broken and the operator
needs to nuke dApp state without mitos cooperation. Doc note
in `do_state.rs` that recapture is the preferred path.

## The `rebootstrap` export — dynamic-interest modules

Recapture's refill has two halves, matching the two bootstrap
models:

- **Event-driven modules** (`jpg-co`, the marketplace family):
  the host walks the module's static manifest `[interest]` UTxOs
  and replays them as synthetic `Produced` events through
  `handle-events`. That is `run_bootstrap`, run inside `start()`.
- **Self-bootstrapping modules** (`holder-distribution`,
  `burn-address`, `vesting-tracker`): the module does its *own*
  cold-start scan (`chain_data::utxos_by_*`) and emits a domain
  `Snapshot`. Their manifest `[interest]` is empty, so
  `run_bootstrap` does nothing for them.

`run_bootstrap` alone therefore cannot refill a self-bootstrapping
module. The fix is a module-side export, symmetric with the
companion-side `on_recapture` hook:

```wit
// wit-v2/world.wit — world mitos-module-v2
export rebootstrap: func() -> result<u64, string>;
```

`HostV2::start(id, rebootstrap: bool)` invokes it after the
`run_bootstrap` pass when `rebootstrap` is `true` — the recapture
flow passes `true`, every other caller `false`. A self-
bootstrapping module's `rebootstrap` re-runs its cold-start over
its *own restored interest*: `init` already rehydrates the
tracked-interest set from `state-kv`, so the module knows what to
re-scan and the host never has to recover or replay the dynamic
interest. Event-driven modules implement `rebootstrap` as a no-op
returning `0`.

The `u64` return is the count of interest predicates the module
re-bootstrapped (one cold-start unit each) — surfaced as the
recapture's `events_emitted` (see open question 4).

**Idempotency.** `rebootstrap` may run repeatedly — recapture is
re-runnable. Each module's cold-start is a fresh chain re-scan +
authoritative re-emit, so repeated runs converge; consumers that
accumulate (e.g. `burn-address`) dedup on event identity, already
a requirement of those modules' delta contracts.

## Failure modes + recovery

| Scenario | Behaviour | Recovery |
|---|---|---|
| Companion times out on `RecaptureReady` | 504 from admin endpoint; no bootstrap fired; companion may be partially cleaned | Operator investigates the companion (logs / WS health), retries the admin call when the companion is responsive. |
| `bootstrap_v2::run` errors mid-walk | Some events emitted, refill incomplete; companions get partial Apply stream then nothing | Operator re-runs recapture; the second pass clears bootstrap-done flags fresh, walks again. Idempotent. |
| Worker disconnects mid-refill | Standard emissions-log behaviour — undelivered Apply frames buffered per existing dispatch; redelivered on reconnect | Worker reconnects, completes refill. No operator action. |
| Two operators race two `recapture` calls **for the same module** | Second call 409s | First call completes; second can be re-run if needed. |
| Two operators race recapture for **different modules** on the same companion | Both proceed concurrently. Each module has its own per-module mutex; the companion's `on_recapture` is called once per module name with the correct scope. | None needed — by design. |
| dApp's `on_recapture` impl doesn't scope by `module` (multi-module worker) | Cleanup is too broad. Rows from non-recaptured modules disappear; their data is gone until those modules are *also* recaptured. | Operator triggers recapture for every other module to refill. dApp bug; fix `on_recapture` to scope by `module`. |
| Host crashes mid-recapture | bootstrap-done flags wiped but bootstrap walk incomplete; on restart, follower re-runs bootstrap (no flags) and re-emits to all subscribers | Same outcome as a successful recapture would have produced. |

## Out of scope (deferred to follow-up)

### Per-companion targeting

`companion=<specific-key>` requires bootstrap_v2 to run for one
companion's subscription only — currently it dispatches events
through the module's broadcast channel, which fans out to every
subscriber. Two paths to enable this:

1. **Targeted emit mode in dispatch.** Add an `emit_to_companion(key, event)`
   variant alongside the existing `emit(event)` broadcast.
   `bootstrap_v2::run_for_companion(module_id, key)` walks
   UTxOs and uses the targeted emit. Other subscribers see
   nothing.
2. **Separate worker process for the bootstrap walk.** Mitos
   spawns a one-shot task that walks UTxOs at the module's
   `[interest]` addresses, runs them through the wasm module's
   `handle_event` outside the broadcast path, and delivers
   directly. More complex; same outcome.

Both approaches need the trait/protocol changes from v1 — only
the dispatch path differs. So v1 lays the runway; v2 wires the
final mile when a second subscriber forces the issue.

### Interest set on the companion side

`mitos_companion_interest` (the dynamic interest table managed
by the runtime) is unclear:

- Reset to empty during recapture? That'd require the dApp to
  re-`/api/_interest/subscribe` for whatever it was watching.
  Forces a clean slate but breaks the "operator runs one
  command and it just works" UX.
- Keep as-is? The wasm module's filter is internal so the
  community module's emissions don't depend on the
  companion's interest set. But the in-tree-indexer path
  (when a companion subscribes to in-tree indexers via
  unified-subscribe) does honour the interest set, so for
  multi-target companions this matters.

V1 decision: **keep interest set as-is.** Recapture is about
dropping the *projected state* (the dApp's SQL view of
chain). The *consumer's filter declaration* shouldn't change
unless the operator says so. Document this in the HOWTO.

### Multi-module recapture

`POST /_admin/modules/_all/recapture` for a host-wide nuke is
plausible but speculative. Skipped — easy to wrap in a script
once individual `recapture` works.

## Open questions

1. **Should `on_recapture` accept a tx-scoped `Ctx` so the
   dApp can run cleanup inside a transaction?** D1 has weak
   transaction guarantees, but CF DOs have per-DO serialised
   execution — a panic mid-`DELETE` would leave a half-state.
   Both the single-module DROP body and the multi-module
   scoped DELETE body are idempotent (`DROP TABLE IF EXISTS`
   / repeated `DELETE WHERE source_module = ?` is a no-op),
   so probably fine, but worth surfacing.

2. **How to handle a companion that's currently in mid-Apply
   when the recapture frame arrives?** Per-WS order says the
   companion finishes the current Apply, then processes
   Recapture. The `module` field on the Recapture frame
   removes one ambiguity: if the in-flight Apply was for a
   different module's stream (multi-target companion), it's
   safely outside the recapture's scope. If it was for the
   same module, `ON CONFLICT DO UPDATE` on the re-INSERT
   handles the duplicate — noisy but correct. Accept the
   noise.

3. **What's the right timeout for `RecaptureReady`?** **30s is
   over-budget for the v1 jpg-store-mirror shape.** First
   production run measured ~1.6s for the worker's
   `DELETE FROM collection_offers` + `RecaptureReady` send
   against a 3582-row table. 30s remains the v1 default to
   give headroom for slower companions / larger tables; can
   tighten to 5s once a second consumer's numbers are in.

4. **Should the recapture response carry a precise
   `events_emitted` count?** **Resolved (2026-05-19).** It now
   carries the `u64` returned by the module's `rebootstrap`
   export — the count of interest predicates re-bootstrapped
   (`0` for event-driven modules, whose refill is the host-side
   `run_bootstrap` walk; that walk's per-address `utxos=N
   batches=M` journal lines remain the finer-grained signal).

5. **How do we enforce the `source_module` schema contract?**
   v1 documents it (this doc + the HOWTO + trait docstring).
   A future refinement: the runtime could surface a helper
   like `Ctx::insert_with_module(table, cols, vals, module)`
   that automatically populates the column, plus a runtime
   `verify_schema(tables)` that warns on tables receiving
   community-module-derived data without a `source_module`
   column. Not needed for v1's single-module worker, but
   surfaces as soon as Phase 4 wayup-co lands.

## What lands in the cnft.dev-workers worker

The relayering plan stays unchanged — recapture is orthogonal
to the four phases. Once the mitos side ships:

- Worker implements `on_recapture` on `JpgCoImpl` (one-line
  hook delegating to the existing `handle_reset` body).
- Worker's HOWTO + relayering docs reference the new endpoint.

No deploy-coupling — the worker's old `/reset` keeps working
while the new endpoint is being shaken out, so we don't have
to ship them atomically.
