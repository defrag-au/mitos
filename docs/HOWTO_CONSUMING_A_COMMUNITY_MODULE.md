# HOWTO: consume a community module from a Cloudflare Worker

You're writing a CF Worker companion that wants events from a
mitos-hosted community module (e.g. `jpg-co`, or — Phase 4 of the
relayering plan — `wayup-co`). This doc walks through the minimal
declaration surface, the boot-time handshake, and the operational
flows (recapture, debugging) for the community-modules-first
shape.

If you're building a *private* per-dApp wasm module that ships in
the worker's own repo, see `HOWTO_FIRST_MODULE.md` — different
shape (worker hosts + uploads its own wasm). This doc is for the
case where the module already exists in `mitos/community-modules/`
and you're the consumer.

Cross-references:
- `strategy/COMMUNITY_MODULES.md` — what community modules are +
  the directory convention
- `strategy/LAYERED_RESPONSIBILITIES.md` — why community modules
  are the default home for chain-recognition logic
- `strategy/MITOS_COMPANION_PATTERN.md` — the companion-DO pairing
  rationale
- `design/UNIFIED_SUBSCRIBE.md` — the dial-back handshake the
  runtime uses
- `HOWTO_FIRST_MODULE.md` — sister doc for building a private
  module from scratch

## What the worker actually expresses

A companion's "needs declaration" is **three names and one
typed event**:

| Declaration | Where | Example |
|---|---|---|
| Companion identity | `MitosCompanion::NAME` | `"jpg-co"` |
| Subscribe target name | derived from `NAME` by default, or override | `Module("jpg-co")` |
| Channel routing name | `MitosChannel::NAME` | `"jpg-co"` |
| Decoded event type | `MitosChannel::Event` | `JpgCoChange` |

Nothing else — no version pin, no contract-address list (those live
in the module's own `<name>.toml`), no schema reference.
Wire-format compatibility is enforced by both sides depending on
the same `mitos_community_events::<name>` submodule for typed
event payloads.

## Minimal companion shape

Reference implementation:
`cnft.dev-workers/workers/jpg-store-mirror/src/do_state.rs`. The
load-bearing parts:

```rust
use async_trait::async_trait;
use mitos_community_events::jpg_co::JpgCoChange;
use mitos_companion::{Ctx, MitosChannel, MitosChannelDyn, MitosCompanion};

const COMPANION_NAME: &str = "jpg-co";

pub struct JpgCoImpl { /* per-DO state */ }

impl MitosCompanion for JpgCoImpl {
    const NAME: &'static str = COMPANION_NAME;
    type Config = ();

    fn channels(&self) -> Vec<Box<dyn MitosChannelDyn>> {
        vec![Box::new(JpgCoChannel { /* … */ })]
    }

    // subscribe_targets() and initial_interests() defaults
    // give us `Module("jpg-co")` + empty interest. No override
    // needed for the single-community-module case.
}

pub struct JpgCoChannel { /* … */ }

#[async_trait(?Send)]
impl MitosChannel for JpgCoChannel {
    const NAME: &'static str = "jpg-co";
    type Event = JpgCoChange;

    async fn apply_event(&self, ctx: &Ctx, event: JpgCoChange)
        -> mitos_companion::Result<()>
    {
        // SQL mutations, broadcast queueing, etc.
        Ok(())
    }
}
```

That's the whole declaration. The defaults in
`mitos_companion::traits` handle everything else:

```rust
// Default impl on the trait.
fn subscribe_targets(&self) -> Vec<SubscribeTarget> {
    vec![SubscribeTarget::Module { name: Self::NAME.to_string() }]
}
fn initial_interests(&self) -> Vec<Interest> {
    Vec::new()
}
```

So a companion named `"jpg-co"` automatically subscribes to a
module named `"jpg-co"`.

## Multiple community modules

When the worker wants events from more than one community module
(e.g. covering both jpg.store and wayup COs), override
`subscribe_targets()` to widen the set:

```rust
impl MitosCompanion for JpgStoreMirror {
    const NAME: &'static str = "jpg-co";   // primary

    fn subscribe_targets(&self) -> Vec<SubscribeTarget> {
        vec![
            SubscribeTarget::Module { name: "jpg-co".into() },
            SubscribeTarget::Module { name: "wayup-co".into() },
        ]
    }

    fn channels(&self) -> Vec<Box<dyn MitosChannelDyn>> {
        vec![
            Box::new(JpgCoChannel { /* … */ }),
            Box::new(WayupCoChannel { /* … */ }),
        ]
    }
}
```

Each target gets its own outbound WS. Mitos substitutes
`{target}` into `MITOS_REPLICATE_URL` (see below) so the WSes
land at distinct paths on the worker
(`/_internal/replicate-jpg-co`, `/_internal/replicate-wayup-co`).
The runtime's WS Hibernation tags each socket with the target
name; inbound frames route to the channel whose `NAME` matches.

## When do you need `initial_interests()`?

Community modules typically filter chain events **internally**
(their `<name>.toml` declares the script addresses or policies
they watch). The `Interest` API is for orthogonal,
consumer-driven filtering — the canonical use today is the
`asset-transfer` community module, which ships with an empty
static interest and lets each consumer scope its subscription
to specific policies via `HoldsPolicy` predicates.

For community modules: leave `initial_interests()` empty unless
the module's docstring explicitly says it honours an `Interest`
filter. Most don't.

## Runtime configuration (`wrangler.toml`)

The worker's runtime needs to know where mitos lives and how
mitos should reach the worker:

```toml
secrets_store_secrets = [
    # Shared bearer token; mitos uses the same value for inbound
    # subscribe POSTs + outbound dial-back authentication.
    { binding = "MITOS_AUTH_TOKEN", secret_name = "MITOS_AUTH_TOKEN" },
]

[vars]
# HTTPS endpoint mitos exposes for the companion's subscribe POST.
MITOS_HOST_URL = "https://mitos.defrag.cc"

# Dial-back URL template. Mitos substitutes:
#   {target} → the subscribe-target's name (e.g. "jpg-co")
#   {key}    → the companion key at dial time
# Single-target companions resolve to a single WS path; multi-target
# companions get one WS per target, each at a distinct path that
# matches a MitosChannel's NAME.
MITOS_REPLICATE_URL = "wss://jpgsm.cnft.dev/_internal/replicate-{target}"

[assets]
run_worker_first = [
    # /_internal/replicate-* is where mitos's dial-back WS lands.
    # /_internal/wake is the runtime's "fresh subscribe" trigger.
    "/_internal/*",
    # /_admin/* covers the dApp's operator-facing endpoints
    # (e.g. /_admin/<companion-name>/reset for state resets).
    "/_admin/*",
]

[durable_objects]
bindings = [
    # Class name is locked once shipped — CF storage is keyed by
    # class. Rename = orphaned state.
    { name = "JPG_CO", class_name = "MitosJpgCoDO" },
]
```

## End-to-end boot sequence

What happens when a fresh DO instance comes up:

1. **DO boots.** The `mitos-companion` runtime takes over and
   reads the companion's persisted registration row from local
   SQL storage (`mitos_companion_registration`).
2. **Subscribe POST.** Runtime sends `POST {MITOS_HOST_URL}/api/companions/subscribe`
   with the companion's `subscribe_targets()` +
   `initial_interests()` payload, authenticated via
   `MITOS_AUTH_TOKEN`.
3. **Mitos accepts.** For each target:
   - `SubscribeTarget::Module { name }` → mitos validates the
     name exists under `<modules_dir>/<name>/`.
   - The companion is persisted under
     `<modules_dir>/<name>/companions/<companion_key>` (per-module
     companion registry).
4. **Dial-back.** `CompanionDialer` opens an outbound WS to the
   substituted `MITOS_REPLICATE_URL`. Each target gets its own
   socket.
5. **Steady state.** As the wasm community module emits events,
   they flow through mitos's per-module broadcast channel →
   outbound WS → the worker's `/_internal/replicate-<target>`
   route → runtime hibernation tags the frame `<target>` → routes
   to the `MitosChannel` whose `NAME` matches → `apply_event`
   decodes the CBOR into the channel's `type Event`.

Watch the journal during a worker deploy and you should see
something like:

```
mitos_platform::companions: subscribe accepted module=jpg-co companion_key=jpg-co
mitos_platform::dialer: companion ws connected module=jpg-co target=wss://jpgsm.cnft.dev/_internal/replicate-jpg-co
```

## Recapture: resetting state for a fresh re-emission

**Recapture** = drop the consumer's projected state, get the
module to re-emit current chain state, repopulate from scratch.
Useful when:

- A schema migration on the dApp side needs a clean re-fill
- The community module's logic changed in a way that means
  previously-stored rows are wrong
- Teething problems with a new module deployment
- Drift detection: ghost rows for since-spent UTxOs the dApp
  missed deleting

Recapture is a **single, host-coordinated operation** as of v1.
The full protocol is in `mitos/docs/design/RECAPTURE.md`; from a
dApp author's perspective:

- The dApp implements `MitosCompanion::on_recapture` to drop
  rows scoped to a given module name.
- The operator runs `mitos-admin recapture <module-id>` (or
  POSTs the equivalent admin endpoint).
- Mitos drives the rest: signals every subscribed companion,
  awaits their cleanup ACK, wipes the module's bootstrap-done
  flags, restarts the follower (which re-walks UTxOs at the
  watched addresses), and signals completion.

### The companion's responsibility — `on_recapture`

Companions that own SQL state MUST implement
`MitosCompanion::on_recapture` to clean up rows that came from
the recaptured module. Single-module companions (most workers
today) can DROP/DELETE everything; multi-module companions MUST
scope by `source_module` per the schema contract in
`docs/design/RECAPTURE.md` "Multi-module companions".

`jpg-store-mirror`'s implementation (single-module today):

```rust
#[async_trait(?Send)]
impl MitosCompanion for JpgCoImpl {
    const NAME: &'static str = "jpg-co";
    type Config = ();

    async fn on_recapture(
        &self,
        ctx: &Ctx,
        module: &str,
        reason: Option<&str>,
    ) -> mitos_companion::Result<()> {
        tracing::info!(module = %module, reason = ?reason, "on_recapture starting");
        ctx.exec("DELETE FROM collection_offers", vec![])?;
        Ok(())
    }
    // ...
}
```

The default impl is a no-op + warning log — companions that
don't keep meaningful state (log-only consumers) can leave it
alone. Companions that own SQL tables MUST override; the
default's warning log is your in-the-act reminder if you
forget.

**Don't reset the companion cursor.** The runtime explicitly
does not reset cursors around `on_recapture` — multi-module
companions can't safely rewind a cursor shared across
subscriptions. The refill Apply frames advance the cursor
naturally as they arrive.

**On error, the runtime does NOT send `RecaptureReady`.** A
partial cleanup followed by refill would seed ghost rows;
the safe failure mode is "timeout, operator investigates."
Return `Err` from `on_recapture` if cleanup fails for any
reason; the admin endpoint will surface a 504 once mitos
times out the per-companion ACK.

### Triggering a recapture (operator side)

From a machine with access to mitos's admin endpoint:

```bash
mitos-admin --token "$MITOS_AUTH_TOKEN" recapture jpg-co \
    --reason "schema migration post-deploy"
```

Or via curl (e.g. on-box where mitos is internal-only):

```bash
ssh root@<mitos-host> 'TOKEN=$(grep ^MITOS_AUTH_TOKEN= /etc/default/mitos-mainnet | cut -d= -f2); \
    curl -sS -X POST \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d "{\"reason\": \"schema migration post-deploy\"}" \
        http://127.0.0.1:8181/_admin/modules/jpg-co/recapture | jq'
```

Expected response shape:

```json
{
  "module": "jpg-co",
  "companions_targeted": 1,
  "events_emitted": 0,
  "duration_ms": 5276
}
```

`events_emitted` is `0` in v1 — the counter is wired but not
populated; companions MUST NOT depend on the value. The
load-bearing signal is the bootstrap walk's journal output
which shows actual UTxO counts dispatched.

Verifying end-to-end via the journal:

```bash
ssh root@<mitos-host> 'journalctl -u mitos-mainnet --since "30 seconds ago" --no-pager \
    | grep -iE "recapture|bootstrap" | head -20'
```

You should see, in order:

1. `recapture: admin endpoint dispatching module=<id>`
2. `recapture: dispatching Recapture frames to subscribed companions … companion_count=N`
3. `RecaptureReady received; unblocking recapture driver` (one per companion)
4. `recapture: all companions ready; wiping bootstrap-done flags`
5. `recapture: bootstrap-done flags cleared flags_cleared=N`
6. `bootstrap: scanning current unspent set at address …` (per watched address)
7. `bootstrap: address complete utxos=N batches=M`
8. `recapture: complete`

Compare pre/post row counts on the dApp side to confirm the
refill landed — a discrepancy is real drift the recapture just
corrected.

### Multi-subscriber considerations (caveat)

The v1 endpoint targets **all** subscribers of a module
(`companion=*` — the only value accepted). With one subscriber
per community module today (jpg-store-mirror → jpg-co), this is
fine. With multiple subscribers, all of them get refilled
simultaneously — disruptive but correct.

Per-companion targeting is forward-compatible in the API
(passing anything other than `"*"` returns 400 with a "deferred"
message) but not yet implemented. The blocker is the dispatch
path — bootstrap currently runs against the module's broadcast
channel which fans out to every subscriber. Targeted emit (one
companion's WS only) needs new infrastructure. See RECAPTURE.md
"Out of scope (deferred to follow-up)" for the design sketch.

### Failure modes + HTTP status mapping

| Status | Body code | Meaning | Recovery |
|---|---|---|---|
| 200 | — | Recapture complete | Verify row counts |
| 400 | `recapture_bad_companion` | Body specified `companion != "*"` | Drop the field or set it to `"*"` |
| 404 | `not_found` | Module not registered on this host | Check `mitos-admin list-modules` |
| 409 | `recapture_in_progress` | Per-module mutex held by an in-flight call | Wait for the first to complete; retry |
| 503 | `recapture_unavailable` | Host's dialer not wired (bundle config issue) | Check `BUNDLE_MODULES_DIR` is set + binary is recent enough |
| 504 | `recapture_timeout` | Companion failed to ACK `RecaptureReady` within timeout, OR `clear_bootstrap_flags` errored | Inspect companion logs; refill **was not** fired so dApp state is whatever `on_recapture` partially produced; retry once the companion is responsive |

A 504 means the refill **didn't** happen — that's by design.
Sending the bootstrap walk against a half-cleaned dApp table
would seed ghost rows for since-spent COs. Better to fail loud
and let the operator retry than to corrupt state silently.

### Fallback: legacy `/reset` on the worker

The worker's pre-recapture `POST /_admin/<companion>/reset`
endpoint (where it exists) is the **fallback** path. It only
cleans the dApp side; mitos doesn't re-emit. Useful when:

- The WS dial-back is broken and you can't reach the worker via
  the recapture flow.
- You want to nuke dApp state without forcing a host-side
  bootstrap walk.

```bash
curl -X POST -H "X-Debug-Token: <DEBUG_TOKEN>" \
    https://<worker-host>/_admin/<companion-name>/reset
```

For routine recapture, prefer the `mitos-admin recapture`
flow — it's coordinated, idempotent, and produces the correct
end-state without ghost rows.

## Troubleshooting

### Worker connects but no events arrive

```bash
# Confirm the module is activated and its follower is running.
ssh root@<mitos-host> 'curl -sS \
    -H "Authorization: Bearer $(grep ^MITOS_AUTH_TOKEN= /etc/default/mitos-mainnet | cut -d= -f2)" \
    http://127.0.0.1:8181/_admin/modules' | jq

# Confirm your companion is registered under the module.
ssh root@<mitos-host> 'ls /var/lib/mitos/modules/<name>/companions/'

# Watch live emissions reaching the dispatcher.
ssh root@<mitos-host> 'journalctl -u mitos-mainnet -f --no-pager | \
    grep -E "emit|module=<name>"'
```

If the module is running and emitting, but no events reach the
worker, suspect the dial-back URL. Re-check `MITOS_REPLICATE_URL`
and make sure `/_internal/replicate-<target>` is listed in the
worker's `run_worker_first` so the path doesn't fall through to
the SPA asset handler.

### Worker subscribe POST fails

```bash
# Test the subscribe endpoint manually with the same auth the
# worker uses. Replace <key>, <target>, etc.
TOKEN=<MITOS_AUTH_TOKEN value>
curl -X POST \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
      "companion_key": "test-companion",
      "targets": [{"kind": "Module", "name": "jpg-co"}],
      "interests": [],
      "dial_back_url": "wss://example.invalid/_internal/replicate-{target}"
    }' \
    https://mitos.defrag.cc/api/companions/subscribe
```

Common causes:
- Wrong bearer token (must match `/etc/default/mitos-mainnet`'s
  `MITOS_AUTH_TOKEN`).
- Module name typo — the `name` in `SubscribeTarget::Module` must
  exactly match a directory under `<modules_dir>/`.
- Module not yet activated (auto-load skipped it because the
  `community-modules/<name>/build/` directory is missing or
  manifests don't validate).

### The Phase 2 acceptance gate (auto-load idempotency)

You can prove auto-load is wired correctly by restarting mitos
twice in a row:

```bash
ssh root@<mitos-host> 'systemctl restart mitos-mainnet'
sleep 12
ssh root@<mitos-host> 'journalctl -u mitos-mainnet --since "20 seconds ago" --no-pager \
    | grep "community module"'
```

First restart after a new build lands: expect
`community module activated module=<name> sha=<new-sha>`.
Second restart with no changes: expect
`community module already active; skipping module=<name> sha=<same-sha>`.
If the first run says "activated" both times, the build artifacts
aren't being read deterministically — check that
`BUNDLE_COMMUNITY_MODULES_DIR` points at the right tree on the
box.
