# HOWTO: observe a running mitos host (without SSH)

**Start here when you need to answer "is mitos healthy / is a module
backed up / did anything trap / why is a consumer stale?"** Almost
every such question is answerable over a token-gated HTTP API on
`https://mitos.defrag.cc` and the `mitos-admin` CLI — no `ssh` /
`journalctl` required. SSH is reserved for deploy (build-on-box),
secret rotation, and deep forensics (see the last section).

This is the entry-point guide. Two siblings go deeper on specific
failure shapes:

- `HOWTO_DEBUG_TRAPS.md` — a module *crashes* (wasm trap); reproduce
  locally with `mitos-run`.
- `HOWTO_DEBUGGING_DEPLOYED_MODULES.md` — a module is alive but a
  consumer reports *missing events* (silent drop / drift).

Design background: `design/MITOS_OBSERVABILITY_API.md` (the spec these
endpoints implement), `design/RECAPTURE.md` (the rebuild nuke).

---

## Prerequisites — get the token into your env (this is the whole point)

The historical friction was `ssh … grep MITOS_AUTH_TOKEN
/etc/default/mitos-mainnet` just to call the *already-existing* HTTP
API. Don't. Put the token in your shell once:

```bash
export MITOS_URL=https://mitos.defrag.cc        # mitos-admin defaults to localhost
export MITOS_AUTH_TOKEN=<full-access token>     # from your secret store / direnv
```

`mitos-admin` reads both from the env (`--mitos` / `--token` override).
With those set, every command below runs with no SSH.

**Auth scopes** (details in [Auth](#auth-scopes)):

| Token | Grants |
|---|---|
| `MITOS_AUTH_TOKEN` (full) | every endpoint, any method |
| `MITOS_READONLY_TOKEN` (read-only) | `GET` (observe) endpoints only — safe for dashboards/agents |
| *(none)* | `GET /metrics` and `GET /health` are open |

---

## 30-second triage

```bash
mitos-admin status
```

One call: version + `build_sha` (what's actually deployed), uptime,
chain tip, and a per-module line. A backed-up or recapturing module is
flagged inline:

```
version:        0.0.1
build:          b97bc51bf586-dirty
uptime:         2h13m
tip:            slot 187941104 6b75212275dc…
modules:        15
  collection-holders            3 companion(s)
  jpg-store-listing             0 companion(s)  [BACKLOG 11644q/0p]
  jpg-store-offer               1 companion(s)
  holder-distribution           4 companion(s)  [last trap 9d ago]
```

`[BACKLOG Nq/Mp]` = N queued + M pending emissions. A non-zero,
*non-draining* queue is the headline "this module is backed up" signal.

---

## The endpoint surface

`mitos-admin` wraps these, but they're plain HTTP and self-describing —
`GET /_admin` returns the live index:

```bash
curl -sH "Authorization: Bearer $MITOS_AUTH_TOKEN" $MITOS_URL/_admin | jq .
```

| Method | Path | Scope | What |
|---|---|---|---|
| GET | `/_admin` | read-only | This index. |
| GET | `/_admin/status` | read-only | Whole-host health (the 30-sec triage). |
| GET | `/_admin/events` | read-only | Recent ops-events ring (recapture/trap). |
| GET | `/_admin/modules` | read-only | List modules. |
| GET | `/_admin/modules/{id}` | read-only | Module manifest summary. |
| GET | `/_admin/modules/{id}/companions` | read-only | Per-companion interest, cursor, counts, drain age. |
| GET | `/_admin/modules/{id}/last-trap` | read-only | Last trap fixture (TOML) for replay. |
| GET | `/_admin/modules/{id}/emissions` | read-only | Emissions log (`?status=&companion=&limit=&after_id=`). |
| POST | `/_admin/modules/{id}/recapture` | full | Coordinated state-rebuild (`{"companion":"*","reason":…}`). |
| POST | `/_admin/modules/{id}/restart` · `/evict` | full | Re-instantiate / retire. |
| DELETE | `/_admin/modules/{id}/emissions` | full | Purge emissions (explicit `?status=`). |
| GET | `/metrics` | **open** | Prometheus exposition. |
| GET | `/health` | **open** | Liveness + uptime. |

---

## CLI reference (`mitos-admin`)

**Observe (read-only):**

```bash
mitos-admin status                 # whole-host health
mitos-admin tail [--follow]        # recent ops events; --module / --kind to filter
mitos-admin list-modules
mitos-admin get-module <id>        # summary + per-companion table
mitos-admin emissions --module <id> [--status all|queued|… --companion <key>]
mitos-admin health                 # open /health
```

**Act (full token):**

```bash
mitos-admin recapture <id> --reason "<why>"      # rebuild a module's state from chain
mitos-admin restart-module <id>
mitos-admin evict-module <id> [--force]
mitos-admin delete-companion --module <id> --client-id <c> --key <k>
mitos-admin emissions-replay --module <id> <emission-id>
mitos-admin emissions-purge --module <id> --status <list>
```

**Deploy** is build-on-box and stays SSH-driven — see
[When to still SSH](#when-to-still-ssh).

---

## Diagnostic playbooks

Ordered cheap-signal-first. Each starts from a symptom.

### "Is the host healthy?"
```bash
mitos-admin status
```
`uptime` growing across calls = stable; if it resets, the host is
restarting — fix that first. `build` tells you exactly which commit is
running (the deploy stamps the working-tree `git describe`).

### "Is a module backed up / a lane stalled?"
This is the case we want to catch early. Two views:

```bash
mitos-admin status                 # which modules carry a [BACKLOG]
mitos-admin get-module <id>        # per-companion breakdown
```

In `get-module`, the stall fingerprint is **`queued > 0` with a large
`drained … ago`** (last successful dial) and an old oldest-queued age.
A healthy companion shows `q=0` and a recent drain. Example of a
genuinely stalled vs healthy lane:

```
  jpg-co/jpgsm.cnft.dev  [all policies]  cursor slot …  q=0 p=0 a=4840 …  drained 2m ago     # healthy
  :unsubscribed/         [all policies]  …              q=11644 …         never drained      # orphaned
```

For graphs/alerts, scrape `/metrics` — the decisive gauges are
`mitos_companion_oldest_queued_age_seconds` and
`mitos_companion_last_drain_age_seconds` (see [Metrics](#metrics--dashboards)).

### "Did that recapture finish / did anything trap?"
```bash
mitos-admin tail                   # last events
mitos-admin tail --follow --kind trap          # watch for traps
mitos-admin tail --module <id>                 # one module's history
```
Events are typed: `recapture_started` → `recapture_completed`
(with `companions_targeted`, `duration_ms`), and `trap` (with the host
error). The ring is **in-memory** — it resets on host restart and keeps
the most recent ~512 events. For cumulative counts across restarts use
the `mitos_events_total` counter in `/metrics`.

### "A consumer's data looks stale / drifted"
A consumer (e.g. a worker DB) is missing rows that should be there.
Full triage is in `HOWTO_DEBUGGING_DEPLOYED_MODULES.md`; the short form:

```bash
mitos-admin status                              # is the lane backed up?
mitos-admin emissions --module <id> --status all --companion <key>   # acked vs stuck?
```
If emissions are all `acked` but the consumer is still missing data,
the drop is downstream (consumer-side / dispatcher). If the projection
has genuinely drifted, rebuild from chain:

```bash
mitos-admin recapture <id> --reason "drift recovery"
mitos-admin tail --module <id>                  # confirm recapture_completed
```
Recapture wipes + rebuilds the module's state from current-state UTxOs;
only live state re-materialises, so stale/zombie rows drop. See
`design/RECAPTURE.md`.

### "A module trapped"
```bash
mitos-admin tail --kind trap                                  # what + when
curl -sH "Authorization: Bearer $MITOS_AUTH_TOKEN" \
  $MITOS_URL/_admin/modules/<id>/last-trap > last-trap.toml   # pull the fixture
mitos-run --fixture last-trap.toml                            # replay locally
```
Then follow `HOWTO_DEBUG_TRAPS.md`.

### "Orphaned emissions piling up (`:unsubscribed`)"
A module emitting with no subscriber parks emissions under the
`:unsubscribed` sentinel companion; since compaction only ages
`acked`/`pending`, `queued` grows unbounded. Confirm then purge:

```bash
mitos-admin get-module <id>                     # companions: 0, but status shows backlog
mitos-admin emissions-purge --module <id> --status queued
```

---

## Metrics & dashboards

`GET /metrics` (open, no token) is Prometheus text exposition. Point a
scrape at it. Highest-value series:

| Metric | Use |
|---|---|
| `mitos_companion_oldest_queued_age_seconds{module,companion,client}` | **Stall alert.** Oldest undelivered emission; a high value = a lane that stopped draining. |
| `mitos_companion_last_drain_age_seconds{…}` | Time since last successful dial. |
| `mitos_module_emissions{module,status}` | Backlog (`queued`+`pending`) and lifecycle totals per module. |
| `mitos_recapture_in_progress{module}` | 1 while a recapture runs (alert if stuck high). |
| `mitos_module_last_trap_age_seconds{module}` | Recent trap detector. |
| `mitos_events_total{module,kind}` | Cumulative recaptures/traps (rate alerts). |
| `mitos_chain_tip_slot`, `mitos_uptime_seconds`, `mitos_build_info` | Liveness / position / what's deployed. |

Suggested alerts: `oldest_queued_age_seconds > 600` (lane stalled),
`recapture_in_progress == 1 for > 10m` (stuck recapture),
`increase(mitos_events_total{kind="trap"}[15m]) > 0` (new trap).

> Cost note: `/metrics` and the JSON status/companions endpoints scan
> each module's emissions store on every request. Fine at current sizes
> (scrape every 30–60s); if scrape latency grows, add caching or move to
> atomic counters on the emit/drain path.

---

## Auth scopes

- **Full** (`MITOS_AUTH_TOKEN`): every endpoint, any method.
- **Read-only** (`MITOS_READONLY_TOKEN`): `GET` endpoints only — observe,
  can't recapture/evict/delete/purge. Give this to dashboards + agents.
  **Activate** by setting it in `/etc/default/mitos-mainnet` on the box +
  restarting; unset = only the full token works.
- **Open**: `GET /metrics` and `GET /health` need no token (no secrets).

Mechanics: requests with the full token pass unconditionally; the
read-only token passes only on `GET` (every read endpoint is a GET,
every mutation a POST/DELETE).

---

## When to still SSH

The events feed + endpoints cover the operational subset, not full
structured logging. Reach for the box when:

- **Deploying** — `scripts/deploy.sh` (rsync → build-on-box → restart).
  This is intentionally SSH; see its `--help`.
- **Rotating secrets** — `/etc/default/mitos-mainnet` (incl. setting
  `MITOS_READONLY_TOKEN`).
- **Deep forensics** — `journalctl -u mitos-mainnet` for log lines the
  events ring doesn't capture, or when the host is crash-looping and the
  HTTP server isn't up.

---

## Worked example (this surface, in anger)

A consumer's collection-offer mirror went stale. Pre-observability that
meant SSH + `journalctl | grep`. With this surface:

```bash
mitos-admin status                       # jpg-store-offer lane backed up
mitos-admin get-module jpg-store-offer   # companion queued>0, drained long ago → stalled lane
mitos-admin recapture jpg-store-offer --reason "drift recovery"
mitos-admin tail --module jpg-store-offer
#   recapture_started   …
#   recapture_completed companions_targeted=1 duration_ms=1940
```

The mirror's offer table rebuilt from chain (zombies dropped), verified
by `recapture_completed` in the tail — no SSH at any step.

---

## Not here yet (deferred)

- `archive_horizon_slot` in `/_admin/status` is a `null` placeholder
  (dolos owns the boundary; wiring pending). It's the field that
  explains empty CIP-68 metadata for assets below the horizon.
- Events ring covers `recapture_*` + `trap`; `rebootstrap_completed` and
  `companion_subscribed/evicted` are planned additions.
- Recapture/bootstrap `last_result` (`utxos_ingested`, `completed|trapped`)
  — only the in-progress flag exists today.
