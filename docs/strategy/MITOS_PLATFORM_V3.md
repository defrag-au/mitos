# Mitos Platform v3 — decoupled module lifecycles

**Status: DESIGN ACCEPTED 2026-09-19. Unbuilt.** All open questions are
closed (§9). Supersedes `docs/design/MODULE_VERSIONING.md` and the
"hot-reload is not the goal" non-goal in `MITOS_ISOLATION_ROADMAP.md`.
Extends `MITOS_PLATFORM_V2.md` (dispatch model unchanged),
`MULTI_CLIENT_COMPANIONS.md` (`client_id`), `RECAPTURE.md`,
`MITOS_OBSERVABILITY_API.md`.

This is the strategy document. Execution lives in the work orders
derived from §10; the state machines in §5 are specified as types +
failing tests before any implementation work order is issued.

---

## 1. Why

The original goal: mitos is a **host container**. A Cloudflare worker
deploys a piece of functionality close to the node so chain → worker
communication is as efficient as possible. The worker owns that
functionality; mitos hosts it.

What actually happens: when a module does not meet the needs of its
companion worker, we rebuild ALL ~20 modules and restart the host.

The runtime is not the cause. `POST /_admin/modules/{id}`
(`admin.rs::upload_module`) already validates, atomically activates
`<sha>.wasm`, and calls `ModuleHostV2::replace(id)`, which stops ONE
follower and resumes it from its cursor. Four couplings around that
mechanism force everything into one cadence:

1. **Delivery.** Modules live in `mitos/community-modules/` with path
   deps; the only scripted route to prod is `scripts/deploy.sh` (rsync →
   build host → `mitos-build` every module → restart → boot auto-load).
2. **ABI gate is a text hash.** `manifest::host_wit_sha()` hashes
   `wit-v2/world.wit` byte-for-byte; `start()` refuses any module whose
   sha differs. A comment edit invalidates every module. The WIT changed
   ~9 times May → Sept 2026.
3. **One events crate in every build graph.** Nearly every module, and
   every consuming worker via one git rev, depends on
   `mitos-community-events`.
4. **Identity is the bare `module_id`.** Storage, kv, cursor, emissions,
   companions and `SubscribeTarget::Module` are keyed by it. Two versions
   cannot coexist; `replace()` resumes new code on old state with no
   declared schema.

The test this design must pass, from the roadmap's own words: *restarts
are fine; cross-repo coupled restarts are not.*

## 2. Principles

1. **The worker states which module it needs.** A worker embeds the
   digest of the module it was built against and announces it on
   subscribe. mitos converges. Deploy is `wrangler deploy` and nothing
   else.
2. **Every client runs exactly the module it was built against.** No
   worker is ever broken by another worker's module upgrade.
3. **Public artifact, authenticated pointer.** The wasm is public; what
   is protected is the authority to say which digest runs.
4. **Module state is a cache of the chain.** Rebuild is always available;
   migration is an optimisation. Never mutate live state in place.
5. **Subscribe creates; its absence destroys.** Instances are refcounted
   and ejected; nothing relies on an operator remembering.
6. **Unverifiable ≠ fine.** Unknown action, unverifiable ABI, digest
   mismatch → refuse, loudly. (Inherited from the `wit_sha` gate.)
7. **Named decisions, not booleans.** Every policy below is an enum.
8. **Chain data needs no isolation; modules do.** Two hosts (mainnet,
   preprod). Isolation is per client/namespace inside the host.

## 3. Model

| Noun | Identity | Notes |
|---|---|---|
| **Artifact** | `wasm_sha256` | Immutable bytes in a host-wide content-addressed store `<modules>/_cas/<sha>.wasm`. Public. |
| **Instance** | `(module_id, wasm_sha256, config_sha256)` → `instance_digest` | One follower + cursor + kv + watched index + emissions store. NOT tied to a namespace. |
| **Namespace** | config-defined name (`prod`, `dev`) | Owns a token, an allowed `client_id` set, and a policy profile (resources, eject, revision). |
| **Client** | `(namespace, client_id)` | `client_id` is the worker hostname today (`ownership.cnft.dev`, `jpgsm.cnft.dev`, `hooks.epochify.space`). A worker deployment — atomic across all its DOs. |
| **Client row** | `(namespace, client_id, module_id)` → `{ instance_digest, revision }` | THE unit of versioning. One row per client per module. |
| **Subscription** | `(module_id, client_id, companion_key)` | A companion DO. Hangs off its client row; never carries its own digest. |
| **Revision** | monotonic `u32` per module lineage | Drives the upgrade-action ledger only. Not identity. |

Consequences:

- Two clients announcing the same `(wasm, config)` share ONE instance —
  one dispatch. This is the steady state (dev == prod for most modules
  most of the time; `jpgsm` and `ownership` are built from one repo rev).
- Two clients announcing different digests get two instances
  (**fork on divergence**), between ANY two clients — dev vs prod, or
  `jpgsm` vs `ownership` mid-deploy. The fork collapses when they
  converge.
- "Cutover" is not a host-level event. It is **one client row moving from
  instance A to instance B**. When the last client leaves A, A is
  `Superseded` and retires.
- An instance's effective policy is the STRICTEST among its attached
  clients' namespaces.

## 4. Decisions

### D1 — Per-module delivery is the default path
Host deploys are for host changes. Nothing about a module change touches
the host binary, the other modules, or `systemctl`.

### D2 — ABI compatibility is semver
One package version, `mitos:platform@2.x`, across all interfaces. The
host links every minor it still supports; wasmtime's component linker
resolves semver-compatible imports. Additive = minor bump, existing
modules keep running. Breaking = major bump, refused. Gate:
`major == host_major && minor <= host_minor`. `abi.wit_sha` stays in the
manifest as a diagnostic only. The interface split is a one-way door and
is designed by hand, not delegated.

### D3 — Per-family wire contracts
`mitos-community-events` splits into per-family event crates (`jpg`,
`wayup`, `dex`, `collection`, `asset`). The emission envelope carries
`schema_version`. A worker pins the contract of the family it consumes.

### D4 — Client move (was "cutover only")
A client row moves A → B:

1. Announce: subscribe carries a `ModuleRef` whose digest ≠ the row's.
2. Resolve B: exists → attach path (step 5). Otherwise create it.
3. Create B in `Staging`: fetch + verify artifact (D8), prepare state
   from PARENT A (D5), run instance-scoped upgrade actions (D6).
4. B follows from its cursor to tip, speaking to nobody
   (`routing::fan_out` → `Skip`). The advisory diff of D7 is optional and
   not needed for correctness.
5. **Quiesce, then commit — with a WATERMARK, not an aligned pause.**
   Stop B, then A (`stop_instance`: finishes the block in flight, flushes
   the cursor). Require `S_B ≤ S_A`; if B got ahead, resume both and retry
   later — never wait with two followers stopped. If the client's queue on
   A is empty (or the plan says `purge-emissions`), commit: write B's
   hand-off watermark "serve this client only AFTER `S_A`", then the row —
   and since delivery routing is DERIVED from rows, that one row write is
   the routing change for every companion of the client. If the queue is
   not empty and the plan says drain, RESUME both and wait. After the
   commit B replays `(S_B, S_A]` silently and delivers everything after;
   A `Skip`s the client. A rollback below the watermark lowers it.
   `mitos_lifecycle::handoff` is this step, with the exactly-once property
   as a test.
6. A's refcount drops. Zero → `Superseded` → D10. If A was the module's
   CURRENT instance and the move emptied it, `current.toml` follows the
   client out (`routing::succession`) — otherwise A could never be reaped.

*Refined while coding (2026-09-19).* The first draft switched "at a block
boundary both have processed", chosen ahead of time, and leaned on an
argument about reorgs crossing that slot. Committing at a quiesced
boundary removes the case instead of arguing about it: a rollback BEFORE
the commit is ordinary processing for both instances, one AFTER it is B's
alone, and there is no "during". A first subscribe (no A) skips catch-up,
pause and drain entirely.

*Refined again (2026-09-20, WO-09).* "Pause BOTH at the same block
boundary" assumed a primitive the host does not have: two followers hold two
independent subscriptions, and nothing can pause one, let alone align two.
The watermark needs only the quiesce `upload` already trusts, and it removes
the HOLD as well — B never has to buffer what it might later need, because
`S_B ≤ S_A` guarantees it has not yet produced anything A did not deliver.

Rules: one Staging catch-up at a time host-wide, `prod` clients queued
ahead of `dev`. A new digest arriving for a client whose staging is still
catching up ABANDONS that staging and restarts — never queues. Max live
instances per module is configured (default 4); beyond it a fork is
REFUSED with a typed error rather than silently costing dispatch.

A move where B already exists and is `Active` skips 3–4's creation work
but still quiesces both and commits behind a watermark.

### D5 — State: compatible / migrate / rebuild
Only 6 of 20 modules use `state-kv`. The manifest declares
`state_schema`; on instance creation the host compares it with parent
A's:

| Case | Action |
|---|---|
| equal | copy A's `kv.redb` + cursor + `watched.redb` |
| bumped, module exports `migrate` | copy, then run `migrate(from)` on the COPY |
| bumped, no `migrate` / returns `Err` / no parent | empty state → bootstrap |

- The copy is a file copy with A's follower paused at a block boundary.
  Measured: largest `kv.redb` is 1.3G (ext4, no reflink) — seconds
  against 20s blocks. No redb savepoints.
- Copy with the `reflink-copy` crate's `reflink_or_copy`, NOT
  `std::fs::copy`: identical behaviour on today's ext4 box, a
  millisecond metadata clone on a reflink filesystem (Appendix B). The
  pause stays either way — `kv.redb`, `cursor.redb` and `watched.redb`
  are three files that must describe the same block.
- `migrate(from: u32) -> result<migrate-step, string>` is re-entrant
  (`{done, migrated}`), same shape as `rebootstrap`; it chains
  `from → … → current` inside the module, forward-only, and completes
  before `init` / `handle-events` ever run.
- Failure or trap = delete the copy. A was never touched.
- `emissions.redb` is never copied (2.1G for `collection-metadata`); B
  starts a fresh store.
- Lazy per-value versioning is not a platform feature.

### D6 — Upgrade actions: typed, keyed by revision, split by scope

```toml
[lifecycle]
revision = 5
state_schema = 2

[[lifecycle.upgrade]]
since = 3
actions = ["recapture"]

[[lifecycle.upgrade]]
since = 5
actions = ["purge-emissions", "reset-watched"]
```

ONE table, in both the module's `.toml` and its manifest
(`mitos_lifecycle::artifact::LifecycleSection`). Not `[module] revision` +
top-level `[[upgrade]]` as first drafted: `mitos-build` ships the module's
whole `.toml` to the module as `config.cbor`, and the config's sha is part of
instance identity — so build metadata left in there would reach the module's
`Config` deserialiser AND make a bare `revision` bump fork a new,
byte-identical instance. `mitos-build` strips `[lifecycle]` exactly as it
strips `[deps]`. Every field defaults to 0 / empty, which is what every pre-v3
artifact IS.

The ledger is the client row's `revision`. On a move from `r_old` the
host takes every `[[lifecycle.upgrade]]` with `since > r_old` and runs the UNION —
because environments skip builds (dev takes 3, 4, 5; prod jumps 2 → 5).

`enum UpgradeAction`, kebab-case serde, unknown ⇒ module refused. Each
action has a scope:

| Action | Scope | Effect |
|---|---|---|
| `reset-watched` | instance | clear B's watched-UTxO index at creation |
| `recapture` | client | the moving client's companions drop projected state and are refilled |
| `resync` | client | module re-emits current state to the moving client; companions do not drop |
| `purge-emissions` | client | discard the client's undelivered emissions on A (default is DRAIN before switch) |

- `actions` is a set; the host runs them in its own fixed order:
  instance-scoped at creation; at switch: `purge | drain` → switch →
  `resync` → `recapture`.
- Instance-scoped actions run once, when B is created. A client attaching
  to an existing B runs only client-scoped actions.
- Same revision, different digest (dev riffing without a bump): move,
  run nothing.
- Revision going backwards: per-namespace `RevisionPolicy` —
  `prod: ExplicitRollbackOnly` (admin op, to a retained instance),
  `dev: Allow` (no actions run).

### D7 — Emission diff is ADVISORY
During catch-up B's held emissions for the moving client are diffed
against A's over the same blocks. The typed summary is written to the
lifecycle journal, exposed via API/MCP, and alerts when non-empty on a
`prod` move. It never blocks: a bugfix changes emissions on purpose.

### D8 — Delivery is PULL by digest

```
AnnouncedModule { module, wasm_sha256, manifest_sha256, revision, config: Option<bytes> }   // config ≤ 64 KiB
```

carried on `SubscribeRequest.announced`. **There is no URL on the wire.** The
host COMPUTES where to fetch from — `https://<client_id>/.well-known/mitos/
<module>/<wasm>.{wasm,manifest.toml}` (`artifact::ArtifactSource`) — because
a client id already IS the worker's public hostname. And it does so only for
a client the OPERATOR named in a namespace's `clients` list
(`Registry::may_announce`): under legacy/open mode a client id is whatever
the caller typed, so the host would be fetching from a hostname of the
caller's choosing. Together those leave no SSRF surface to police — no
allowlist, no redirect rules beyond "none", no IP-literal parsing on a
caller-supplied string. `mitos-build` emits `built-module.json`
(`mitos_lifecycle::artifact::BuiltModule` — the digests); the worker embeds
it and serves the two files at `served_paths()`. A cross-repo consumer
commits a `mitos-modules.lock`. mitos: sha in CAS → nothing; else fetch →
verify BOTH shas → `validate_against_host` → wasmtime validate → store.

- **The manifest is pinned too**, by the digest of its FILE BYTES. The wasm sha
  pins the code, but the manifest carries the upgrade actions and the interest
  set and is fetched from the same public place — an unpinned one could arrive
  with `recapture` quietly removed. Raw bytes rather than a canonical form, so
  the digest never depends on a builder's and a host's TOML serialisers
  agreeing. `build_id` therefore gives every build a new manifest digest, which
  is harmless: the manifest is NOT part of instance identity, so a no-change
  rebuild announces the instance the client is already on.
- **The wasm build is NOT reproducible across machines** (found 2026-09-19):
  `debug = "line-tables-only"` with no `--remap-path-prefix` embeds absolute
  source paths, so one source built in two checkouts yields two wasm shas —
  under instance-per-digest, two instances for ever. A module with more than
  one consumer (`jpg-store-*`: `jpgsm` AND `ownership`) must be BUILT ONCE and
  its `built-module.json` shared. Reproducible builds would lift this and are
  worth their own piece of work.

- The simplest artifact host is the worker itself, serving
  `/.well-known/mitos/<id>/<sha>.wasm` as a static asset (artifacts are
  1.3–1.9 MB). mitos also serves its CAS publicly at
  `GET /modules/<sha>.wasm` so generic modules have a home.
- **Boot never needs the network** and never activates anything new: it
  resumes persisted desired state. A new artifact is always a post-boot
  client move through the ledger. `community-modules/` auto-load is
  retired as a delivery mechanism.
- Fetch hygiene: https only, size cap, timeout, no redirects into private
  ranges.
- No signatures while every tenant is us; signatures + per-tenant tokens
  the moment that changes.
- Self-healing: the Apply frame carries `instance_digest`; a companion
  built against another rejects it with its own `ModuleRef`, which IS a
  re-subscribe.
- `upload-module` remains as the operator/debug path.

### D9 — Namespaces; two hosts
Hosts: `mitos-mainnet`, `mitos-preprod`. A third mainnet instance was
measured and rejected (Appendix A): its cost is a second dolos, and chain
data needs no isolation.

A namespace is config: `{ token, allowed client_ids, profile }`. The
token → namespace mapping is the security boundary. Profile:

| | `prod` | `dev` |
|---|---|---|
| `ResourceProfile` | current budgets | lower fuel, `max_memory_bytes` always set, backfill concurrency 1, capped fallback-provider share, followers on a small dedicated tokio runtime |
| `EjectPolicy` | `AlertOnly` for `Abandoned` | `Automatic { grace: 10m, retain: 24h }` |
| `RevisionPolicy` | `ExplicitRollbackOnly` | `Allow` |
| retain after supersede | 72h | 24h |

- An instance attached to both is governed by `NamespaceProfile::protect`
  — NOT "the strictest of each", which is wrong for half the fields.
  Lifecycle fields take the more CAUTIOUS value (a prod-shared instance is
  not reaped on dev's ten-minute timer); resource fields take the more
  GENEROUS one (nor is it starved by dev's fuel ceiling). The governing
  profile is a HIGH-WATER MARK over every client that ever attached, never
  relaxed on detach: with zero clients there is no namespace left to ask.
- Admin ops are client-scoped where possible (`recapture`, `resync`,
  emissions replay target the caller's own clients). Instance-wide
  destructive ops (`evict`) need authority over EVERY attached namespace.
- Alert: a module with >1 live digest among `prod` clients for >24h
  (divergence is meant to be transient in prod).
- Given up knowingly: host-binary staging. `mitos-preprod` is that stage
  — host releases promote preprod → mainnet — so preprod must run a
  permanent baseline module set (today its modules dir is 310M vs
  mainnet's 6.8G). Catches crashes and ABI breaks, not mainnet-scale perf.

### D10 — Instance lifecycle

```
Staging ──► Active ──► Draining ──► Stopped ──► Reaped
   │                      ▲  │
   └─(abandoned)──────────┘  └─(re-attach within grace)──► Active
```

| Transition | Trigger |
|---|---|
| `Staging → Active` | first client switched onto it |
| `Staging → Reaped` | staging abandoned / failed / fetch refused |
| `Active → Draining` | refcount hits zero, cause recorded |
| `Draining → Stopped` | grace elapsed: follower stopped, redb handles closed, `Component` dropped; state RETAINED |
| `Stopped → Active` | a client re-attaches inside `retain` — resumes from cursor |
| `Stopped → Reaped` | `retain` elapsed: evict's teardown order (stop → dialer unregister → close handles → remove dir), emissions purged, CAS refcount dropped |

`enum ZeroRefCause { Superseded, Abandoned }`:

- `Superseded` — every client MOVED to another instance. Auto-retires in
  every namespace (otherwise each prod deploy leaves an alerting orphan);
  retained per profile for rollback.
- `Abandoned` — clients vanished (unsubscribe, lease expiry). `dev`
  ejects automatically; `prod` alerts and waits for an operator.

Desired state is PERSISTED per instance
(`enum DesiredState { Running, Stopped, Pinned }`). This is also the fix
for today's bug where `DELETE /_admin/modules/{id}` only stops the
follower and `auto_resume` resurrects it on the next restart. `Pinned`
= intentionally subscriber-less, neither alerts nor ejects.

**Client lease**, renewed by any authenticated subscribe or acked
delivery. A sleeping DO still answers a dial, so silence ≠ death.
`enum DialFailureClass { Gone /* 404, 410 */, Unavailable /* 5xx, timeout */ }`
— `Gone` gets a short fuse, `Unavailable` a long one. A module that
never emits never dials, so leases need a cheap periodic probe.

**GC:** CAS artifacts are refcounted by instances; keep referenced + N
most-recent unreferenced per module (dev 1, prod 3). `:unsubscribed`
sentinel emissions get an age cap.

### D11 — Reporting: one typed model, three projections
No monitoring stack. Response types move to a shared crate used by the
HTTP handlers, `mitos-admin`, and the MCP tools.

- **HTTP** — the existing `/_admin/*` surface, extended.
- **MCP** — embedded, `rmcp`, streamable HTTP at `/mcp` on the existing
  axum router, same bearer auth, session scoped by namespace. v1: reads +
  safe verbs; destructive tools need full authority and carry the
  destructive hint. Not a stdio sidecar: a local binary can be missing or
  stale, an endpoint on the host it describes cannot.
- **Alerts** — one `AlertKind` enum POSTed to a Discord webhook directly
  (never via a worker that depends on mitos), de-duplicated and
  rate-limited: module refused, quarantined, move failed, lease expired,
  prod divergence >24h, prod instance `Abandoned`, non-empty prod diff.
- **Metrics** — Alloy → Grafana Cloud already runs on the box but only
  scrapes `host`; add one scrape block for mitos's existing `/metrics`.
  Nothing further.

Must be reportable: namespaces and clients; client rows (digest,
revision); instances with lifecycle + desired state, attached clients,
effective policy; forks per module; move progress (staging lag in
slots); revision ledger + actions run; leases (last contact,
`DialFailureClass`); CAS refcounts; diff summaries.

`why_not_running(module | client)` returns a typed reason (ABI refused /
quarantined / stopped by operator / pinned / zero subscribers / staging /
fork limit). Today that is three greps.

**Durable lifecycle journal.** The event ring is in-memory (512) and lost
on restart — precisely when it is wanted. An append-only redb table of
typed lifecycle decisions with cause, ~30 days. The alert webhook, the
explain tool and the diff summaries all read from it.

### D12 — Modules live beside their consumers (big bang)
Subscribers measured on the mainnet host 2026-09-19:

| Modules | Consumers | Home |
|---|---|---|
| `asset-metadata-update`, `asset-transfer`, `cip-25-mint`, `cip-68-mint`, `collection-holders`, `collection-metadata`, `wayup-store-listing`, `wayup-store-sale` | `ownership` | `cnft.dev-workers` |
| `jpg-store-listing`, `jpg-store-offer`, `jpg-store-sale`, `wayup-store-offer` | `ownership` + `jpgsm` | `cnft.dev-workers` (one repo rev ⇒ one digest) |
| `cswap-dex`, `splash-dex`, `holder-distribution` | `hooks.epochify.space` only | epochify's repo — decide at inventory time; until then they stay in mitos as published generic modules consumed via `mitos-modules.lock` |
| `burn-address`, `credit-address`, `standard-burn`, `vesting-tracker` | none | **RETIRE** — evict before migration; source stays |

- Decode crates (`mitos-marketplace-decode`, `mitos-dex-decode`,
  `mitos-vesting-decode`) move only if modules are their sole consumer —
  check `market-ledger` / `token-ledger` first.
  `mitos-module-kit`, `mitos-protocol`, `mitos-companion` stay in mitos.
- `mitos-build` builds a workspace MEMBER, not a generated standalone
  workspace: a nested workspace loses the parent's `[patch]` table and
  keeps a stale `Cargo.lock`, which in the workers repo silently builds
  modules against the pinned rev while everything else uses the patch.

## 5. State machines — BUILT: `crates/mitos-lifecycle`

**Status 2026-09-19: done, 52 tests, clippy clean under `-D warnings`.**
The plan was "types + failing tests, implementer makes them pass". What
got built instead is better for the handoff: the machines are **pure
reducers** — `step(state, event, ctx) → Transition { next, effects }` — in
a crate with no tokio, wasmtime or redb (deps: `serde`, `thiserror`). Every
lifecycle DECISION is already made and tested there. What remains for the
host is to translate the world into events and EXECUTE the effects, which
is mechanical. The same shape as `tx-cart-core`.

The three rules of the pattern (crate docs): no clock — time is passed in;
every effect idempotent, with `recover()` returning what to re-issue after
a crash; named decisions, never booleans.

Both big machines carry an EXHAUSTIVE exploration test: every event
sequence to a fixed depth, the host's view reconstructed ONLY from effects,
invariants checked after every step. Mutation-checked — re-introducing a
real bug (`Start` on a pinned client-less instance; a pause leaked on
abandon) fails exploration with no scenario written for it.

| Machine | Module | Invariants covered |
|---|---|---|
| Client move | `client_move` | commit only with both followers paused; pause/resume and catch-up slot always balanced; row moves ONLY on `SwitchCommitted`; attached set ≡ current instance; stale progress rejected; recovery never trusts in-memory state |
| Instance lifecycle | `instance` | effects ≡ phase on whether the follower runs; never `Active` with zero clients unless pinned; operator stop survives restart AND a subscribe (the v2 resurrect bug); `AlertOnly` never times out; `Superseded` retires quietly even in prod |
| State preparation | `state_prep` | copy / migrate / rebuild; backwards schema ⇒ rebuild; failed migrate ⇒ discard copy |
| Upgrade actions | `upgrade` | union over skipped revisions; once per client; recapture subsumes resync; unknown action ⇒ whole manifest refused |
| Lease | `lease` | `Gone` short fuse / `Unavailable` long; a `503` restarts the short fuse but flapping still expires; one probe per interval |

Of the invariants listed below, the ones this crate CANNOT cover — they
are properties of the host executing effects — become host integration
tests: parent state byte-identical after a failed move; boot with no
network; the `dev`-token boundary.

Original brief, kept for reference — these are not handed to an
implementer as prose:

1. **Client move** (D4) — incl. abandon-and-restart, attach-to-existing,
   crash at every step, reorg across the switch point.
2. **Instance lifecycle** (D10) — incl. re-attach during `Draining` /
   `Stopped`, `Superseded` vs `Abandoned`, strictest-policy resolution.
3. **State preparation** (D5) — copy / migrate / rebuild; trap mid-migrate.
4. **Upgrade-action resolution** (D6) — union over skipped revisions,
   scope split, fixed ordering, unknown ⇒ refuse.
5. **Lease** (D10) — `Gone` vs `Unavailable`, probe for silent modules.

Invariants (property tests):

- A client never receives emissions from two instances for the same block.
- A `dev`-token request can never change a row, instance or emission
  belonging to a `prod` client.
- Parent state is byte-identical before and after a failed move.
- Boot with no network reaches the same running set as before shutdown.
- An instance with refcount 0 is never `Active` past its grace.
- An action declared at revision `r` runs exactly once for every client
  that crosses `r`, regardless of which builds it skipped.

## 6. Security notes
Token → namespace is the boundary; review it (`/security-review`) once
built. Empty `WasiCtx` (no fs, no network), fuel, epoch deadline, memory
limiter and supervisor quarantine are unchanged. Residual risk accepted:
a dev module cannot crash the host through wasm but can exercise a buggy
host path, and disk IO is shared.

## 7. Not delegated
Anything on the box; prod promotion; git history; the WIT interface split
(D2); the five specs in §5; the security review.

## 8. Standing regression oracles
The 69/69 goldens (rebuild BOTH the `.wasm` and `mitos-run` — a stale
`mitos-run` passes silently), and, once built, the emission diff.

## 9. Closed questions

| Question | Decision |
|---|---|
| Two versions live indefinitely, or cutover only? | Instance per digest: any two clients may diverge; cutover is the one-client case. Prod divergence >24h alerts. |
| Shared-module conflicts (`jpg-store-*` has 2 prod consumers) | Same — each client gets the digest it was built against. No ownership concept. |
| Migration model | Rebuild by default, `migrate` on a copy as an optimisation. |
| Upgrade config shape | `actions = [...]` typed list keyed by `[[upgrade]] since`, not a bool. |
| Push or pull | Pull by digest; pointer on the subscribe. |
| Dev isolation | Namespaces inside two hosts; third instance rejected. |
| Fork strategy | Build fork-on-divergence properly (not always-fork). |
| Snapshot consistency | File copy with follower paused at a block boundary. |
| Serialise cutovers | Yes, host-wide, prod first. |
| Held staging emissions | Diffed; advisory, never blocking. |
| Auto-load | Boot resumes persisted state only; never activates new artifacts. |
| WIT versioning granularity | One package version. |
| Event crate granularity | Per family. |
| Namespace set | Config-defined; start `prod` + `dev`. |
| Rollback | Explicit admin op in prod; superseded state retained 72h / 24h. |
| Alert transport | Discord webhook direct. |
| Zero-subscriber modules today | Retire the four. |
| Monitoring stack | None. Scrape the existing `/metrics` with the existing Alloy. |

## 10. Build order

**Pre-flight** (all three repos clean): re-comment every engaged
`[patch]` block as a unit; push mitos and bump the pinned rev in workers;
ship any pending module-behaviour deploy through the OLD path so the
first colocated deploy changes location only; commit or park uncommitted
trees; evict the four retired modules.

**Box hardening — v3 is built on the EXISTING box** (decided 2026-09-19;
RS 4000 G12, 31 GiB, no swap, ext4 — Appendix B is deferred, not
cancelled). Nothing in the design needs new hardware, but memory becomes
the binding constraint, so before Wave 2 lands:

- Add swap: ~8G swapfile on NVMe, `vm.swappiness=10` (or zram). Today
  the only defence against the 17.5G restart peak is the OOM killer.
- `OOMScoreAdjust=-500` on `mitos-mainnet`; `MemoryHigh` on the ledger
  units (they rebuild from the snapshot; mitos state is what we protect).
- Remount `/` `noatime` — `kswapd0` is visibly busy and the ledgers mmap
  a 217G snapshot.
- W1e (the restart-peak investigation) moves to the FRONT of Wave 1.
- The `dev` `ResourceProfile` (`max_memory_bytes` always set) and the
  max-live-instances cap ship WITH instance-per-digest, never after:
  forks are what grow memory in v3.

**Wave 1 — parallel, independent:**
- W1a D1 per-module deploy + persisted `DesiredState` (fixes resurrect-on-restart).
- W1b D2 semver ABI gate (interface split designed by hand first).
- W1c `mitos-build` builds a workspace member; emits `ModuleRef` const / lock.
- W1d D11 shared admin types crate + MCP endpoint + lifecycle journal + alert webhook.
- W1e Investigation: the unexplained 17.5G restart peak (steady 8.9G; `auto_resume` is already sequential).

**The bang** — lands together after W1b + W1c: D12 colocation + D3
per-family event crates.

**Wave 2 — strictly serial:**
1. CAS artifact store + instance re-keying (`module_id` → `instance_digest`). Most invasive; alone.
2. Namespaces: tokens, client rows, profiles.
3. D5 state preparation + D6 action resolution.
4. D4 client move + D7 diff.
5. D10 lifecycle, leases, GC.
6. D8 pull by digest on subscribe; retire auto-load as delivery.

---

## Appendix A — Capacity (measured 2026-09-19)

12 cores, 31 GiB, NO swap, ext4, 465G free of 1007G.

| | now | + a third mainnet instance (rejected) |
|---|---|---|
| Disk | 501G used | +~170G (145G dolos: archive 93 / index 39 / wal 11 / state 4; 7G module state; ~16G src+target) |
| RAM steady | 17G used, 14G page cache | ~26G used, ~5G page cache |
| RAM peak | mainnet 8.9G steady, **17.5G restart peak**; preprod peak 7.1G; ledgers ~8.5G | two mainnet peaks ≈ 45–50G — does not fit |
| CPU | mainnet averages 3.1 cores | ~8 of 12 busy |
| IO pressure | 2–3% (5 min), already the contended resource | worse as page cache shrinks |

Independent of this design: no unit sets `MemoryMax` / `OOMScoreAdjust`.
Give `mitos-mainnet` `OOMScoreAdjust=-500`.

Per-module state: `collection-metadata` kv 1.3G / emissions 2.1G /
`last-trap.toml` 18M; `holder-distribution` kv 4M / emissions 233M. ~20
retained `<sha>.wasm` per module, 1.3–1.9 MB each, never collected.

## Appendix B — Next box: filesystem

> **DEFERRED 2026-09-19.** v3 is being built on the existing x86 box; see
> "Box hardening" in §10. Kept because the benchmark and the filesystem
> reasoning are already paid for. Note the ARM VPS 8000 (~€400/yr) would
> REPLACE the current box rather than add to it.

Today: one 1T virtual disk, everything on a single ext4 root (`vda4`,
`relatime`), Debian 13, kernel 6.12. ext4 cannot reflink and cannot be
converted in place — this is a provisioning-time decision.

**Use XFS for the data volume.** Reflink is default-on, normal writes go
in place (only shared extents are copied on write), it is strong on large
files and parallel IO, and it is in-kernel. Rejected: **btrfs** —
copy-on-write under redb (a CoW B-tree) and fjall (an LSM) pays CoW
twice; the `nodatacow` workaround gives up checksums. **ZFS** — best
feature set, but the ARC competes for RAM on a box where RAM is the
scarce resource, and it is an out-of-tree module on Debian; revisit only
at 64G+.

Layout: small ext4 root (~50G); `/opt` + `/var/lib/mitos` on XFS
(`-m reflink=1`, `noatime`), so a full data disk cannot wedge the OS.
XFS cannot shrink — size deliberately.

What reflink buys, in order of value:

1. Instant rollback points before a risky host deploy
   (`cp -a --reflink=always data data.pre-<change>` with the service
   stopped: 145G in about a second, zero extra space until divergence).
2. Millisecond state copies for instance forks (D5).
3. Throwaway clones of a full mainnet data dir for reproducing bugs. RAM,
   not disk, remains the limit on running them.

**Sizing (netcup, prices 2026-09-19).** Preference is ARM:
**VPS 8000 ARM G11** — 18 vCore, 64 GB, 2 TB NVMe, €33.60/mo. 64 GB is
the number that removes the restart-peak and page-cache problems in
Appendix A; the 6000 (48 GB, €27.07) saves €6.50 to give up the RAM that
matters.

**Benchmark gate before ordering.** The current box is an EPYC 9645
(Zen 5) with 0.00% steal; netcup ARM is Neoverse-N1 on shared vCores.
mitos is per-thread bound (one follower task per module), so extra cores
do not speed up a bootstrap scan, a staging catch-up or a recapture —
and v3 forks on every dev rebuild, so that wall time is the iteration
loop. Time on the existing VPS 3000 ARM G11 vs the current box:
`cargo build --release -p mitos`, `mitos-run` over the 69 goldens, and
steal from `/proc/stat`. ≤2x on goldens: take ARM. ≥3x or visible steal:
take netcup's x86 line at 64 GB, or a Hetzner auction dedicated box
(64 GB, 2×1 TB NVMe, ~€35–50) — the only competitive alternative found.

**Result, measured 2026-09-19 — GATE PASSED, take ARM.** Same source,
same `-j8`, clean builds. x86 = RS 4000 G12 (EPYC 9645, dedicated), run
at lowest CPU/IO priority beside live mitos; ARM = VPS 3000 ARM G11,
idle.

| | x86 | ARM | ratio |
|---|---|---|---|
| host release build | 687.8s | 755.6s | 1.10x (understated — x86 was deprioritised) |
| all modules → wasm (mostly serial) | 108.1s | 199.0s | 1.84x |
| 69 goldens, avg of 3 | 6.8s | 11.2s | 1.64x |
| goldens passing | 69/69 | 69/69 | |
| steal since boot | 0.00% | 0.07% | |

Plan on ~1.6–1.8x wall time for bootstrap scans, catch-ups and
recaptures. 69/69 on aarch64 with no code changes answers the
correctness half of the port.

ARM porting: builds already happen on the box; wasmtime aarch64 is tier
1; redb/fjall are pure Rust. `/usr/local/bin/mithril-client` and
`/usr/local/bin/dolos` are prebuilt binaries needing aarch64 builds —
confirm mithril-client first. Run the test suite once on ARM to flush out
any x86-only fast path.

Ranking of IO levers for this workload: RAM / page cache first, the
backing storage tier second (ask the provider — `ROTA=1` on a virtual
disk says nothing), filesystem third.

## Appendix C — Gaps found in the v2 code

- `DELETE /_admin/modules/{id}` stops only; manifest stays;
  `auto_resume` starts every dir with a manifest ⇒ resurrected on restart.
- `evict` has the right teardown order but is manual and refuses while
  companions are registered.
- No companion liveness: dead companions dialled at a capped 60s backoff
  forever; `Queued` and `:unsubscribed` emissions never purged.
- Old artifacts never collected.
- `ResourceBudget` is global; `max_memory_bytes` optional.
- One shared `MITOS_AUTH_TOKEN`.
- mitos `/metrics` exists but Alloy scrapes only `host`.
- Four modules run with zero subscribers.
