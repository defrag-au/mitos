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
consumer-driven filtering — e.g. when subscribing to an in-tree
indexer like `marketplace-indexer` that broadcasts everything and
relies on the consumer to filter brand/event-kind.

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
2. **Subscribe POST.** Runtime sends `POST {MITOS_HOST_URL}/subscribe`
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
- During teething problems with a new module deployment

### What "recapture" used to mean (pre-community-modules)

The old workflow: operator re-uploaded the wasm module via
`mitos-admin upload-module`. Re-upload incidentally wiped the
module's `kv.redb` (state-kv, which carries the bootstrap-done
flags). On follower restart the bootstrap pass re-ran, walking
unspent UTxOs at the module's watched addresses and emitting a
synthetic `Produced` event per UTxO. The companion received
those as `Created` and re-INSERTed.

The worker's `POST /_admin/<companion-name>/reset` then took
care of the dApp side: drop the projection tables, reset the
companion cursor to `Origin`.

### What "recapture" means now

Community modules don't get re-uploaded — they're auto-loaded
from `mitos/community-modules/<name>/build/` on startup. The
`kv.redb` survives auto-load (different file from the wasm
artifact). So just hitting the worker's `/reset` empties the
dApp tables but the module won't re-emit anything for the
existing chain history — bootstrap-done flags say "I've already
walked these addresses." Result: empty SQL, only forward events,
historical unspent state lost.

### Procedure (v1: manual ops)

> **Sketchy in multi-tenant scenarios.** If multiple workers
> are subscribed to the same community module, asking the
> module to recapture re-emits to **all** subscribers, not just
> the one that asked. With one subscriber per module this is
> equivalent to per-subscriber replay; with N subscribers it
> blows away N-1 dApps' state.
>
> v1 acceptable workaround: only one subscriber per community
> module today. Hardening (per-subscriber replay) is future
> work — see `design/UNIFIED_SUBSCRIBE.md` notes on cursor
> resume + emissions log.

On the mitos host:

```bash
# 1. Stop mitos so we can mutate module state safely.
ssh root@<mitos-host> 'systemctl stop mitos-mainnet'

# 2. Wipe the module's state-kv (clears bootstrap-done flags).
#    Keep cursor.redb — its presence doesn't suppress bootstrap;
#    only kv.redb's flags do.
ssh root@<mitos-host> 'rm /var/lib/mitos/modules/<name>/kv.redb'

# 3. (Optional) clear the emissions log if it's gotten large
#    and you don't need historical Ack replay. Not required for
#    recapture itself.
# ssh root@<mitos-host> 'rm /var/lib/mitos/modules/<name>/emissions.redb'

# 4. Restart mitos. The follower starts, init() runs, bootstrap
#    walks UTxOs at watched addresses, synthetic Produced events
#    flow through the dispatch path.
ssh root@<mitos-host> 'systemctl start mitos-mainnet'

# 5. Verify bootstrap is re-running. You should see the orchestrator
#    *not* skip addresses this time:
ssh root@<mitos-host> 'journalctl -u mitos-mainnet --since "30 seconds ago" --no-pager \
    | grep -E "bootstrap|module=<name>" | head -20'
# Expect: lines like "bootstrap: scanning address" rather than
# "bootstrap: skipping address; already complete".
```

On the dApp side (worker):

```bash
# 6. Hit the worker's reset endpoint — drops projection tables,
#    resets the companion cursor so it accepts events from
#    Origin.
curl -X POST -H "X-Debug-Token: <DEBUG_TOKEN>" \
    https://<worker-host>/_admin/<companion-name>/reset

# 7. (Implicit) Worker's mitos-companion runtime reconnects.
#    Bootstrap events flow in via the dial-back WS, channel's
#    apply_event INSERTs each.
```

### Future work: an admin endpoint for this

The manual procedure above is fine while we're shaking out
teething problems with one subscriber per module. Once there's a
second consumer of any community module, we need:

1. **Per-subscriber replay**, not host-wide module reset. Likely
   implemented as a "snapshot redirect" on the subscribe path:
   the host walks unspent UTxOs at the module's interest set
   for *just this companion* and delivers synthetic events
   through that one socket without touching other subscribers'
   state.
2. **An admin endpoint** like
   `POST /_admin/modules/{id}/recapture?companion=<key>` to
   trigger the above without SSH.

Until then, document the manual procedure in the dApp's
operations runbook and limit community modules to single
subscribers if the data shape is fragile.

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
    https://mitos.defrag.cc/subscribe
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
