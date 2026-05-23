# Mitos observability + task API (reduce SSH in operator/agent flows)

**Status: spec / future sprint.** Drafted 2026-05-23 after a long
collection-ownership debugging session leaned heavily on `ssh` +
`journalctl` to answer questions the host could expose directly. Goal:
make the common operator/agent loops (status, diagnosis, recovery)
runnable against token-gated HTTP endpoints on `mitos.defrag.cc` with
**LLM-decodable JSON**, so SSH is reserved for genuine host ops
(deploy/build, secret rotation).

Not scoped into the collection-modules work; captured here for a future
sprint.

## The client is `mitos-admin` (wrangler-for-mitos)

The consumer of these endpoints is the existing **`mitos-admin`** CLI
(`tools/mitos-admin`) — think of it as `wrangler` for the mitos host.
It already: reads the bearer token from the **`MITOS_AUTH_TOKEN`** env
var (`#[arg(long, env = "MITOS_AUTH_TOKEN")]`), takes a configurable
base URL (`--mitos`, default the prod host), and wraps the admin API
with subcommands: `list-modules`, `get-module`, `list-companions`,
`emissions` / `emissions-replay`, `recapture`, `restart-module`,
`delete-module` / `prune-modules`, `upload-module`, `deploy`,
`rollback`, `health`.

**This is the right home for status discovery + log tailing — not
`ncli`.** `ncli` is the dApp/notification-config tool; mitos host
operations belong in `mitos-admin`. The sprint adds the missing
*status* + *tail* subcommands to `mitos-admin` and the host endpoints
that back them.

## Why

During the collection-ownership / mitos work, almost every diagnostic
step shelled into the box:

- `journalctl -u mitos-mainnet | grep …` for: which companions are
  subscribed + draining, `rebootstrap complete` (utxos_ingested), trap
  / `out-of-fuel`, recapture coordination (`N ready, M timed out`),
  per-policy emission drains (`rows=…`).
- `grep MITOS_AUTH_TOKEN /etc/default/mitos-mainnet` just to obtain the
  bearer token — then used it against the *already-existing* admin API.
- `ls /opt/mitos/.../companions/…` to see registered companions.
- `date -u`, `systemctl is-active` for liveness/time.

Two problems: (1) the **auth token requires SSH to read**, so even the
HTTP admin API isn't reachable without shelling in first; (2) several
**status facts only exist in the journal**, forcing log-grep instead of
a structured query.

## What already exists (and is underused)

The admin router (`crates/mitos-platform/src/admin.rs`) already serves,
gated by `Authorization: Bearer <MITOS_AUTH_TOKEN>`, reachable at
`https://mitos.defrag.cc`:

- `GET  /_admin/modules` — list (id, sha256, size, abi, trap_strategy).
- `GET  /_admin/modules/{id}` — same fields for one module.
- `GET  /_admin/modules/{id}/emissions` — `{rows,total,counts}` over the
  emissions store (queue depth + per-status counts). **This is the
  per-module queue view I was grepping `rows=…` for.**
- `GET  /_admin/modules/{id}/companions/{client_id}/{companion_key}` —
  one companion's registration.
- `GET  /_admin/modules/{id}/last-trap` — last trap context.
- `POST /_admin/modules/{id}/recapture` — `{"companion":"*"}`.
- `POST /_admin/modules/{id}/evict` / `/restart`.
- `POST /_admin/modules/{id}/emissions/{emission_id}/replay`.
- `GET  /_admin/blocks/{slot}` / `/by-tx/{tx_hash}` / `GET /health`.

**Discoverability is itself a gap** — this surface isn't documented in
one place, so flows reach for `journalctl` instead. Step 0 of the sprint
is just: write these down (and have agents prefer them over SSH).

## Gaps to close

### 1. Auth without SSH (highest leverage)

Reading the bearer token via `ssh … grep MITOS_AUTH_TOKEN
/etc/default/mitos-mainnet` is the thing that forces SSH into
otherwise-HTTP flows. The capability is already there —
`mitos-admin` reads `MITOS_AUTH_TOKEN` from the env — so the fix is
**operational, not code**: the operator's (and agent's) local shell
should have `MITOS_AUTH_TOKEN` set (shell profile / secret store /
direnv), rotated in lockstep with the box's `/etc/default`. Optional
convenience: a `mitos-admin login` that stores the token in a config
file (`~/.config/mitos-admin/`) so it isn't re-pasted. (Optionally also:
a separate read-only token scope for status endpoints vs the
read-write task token.) Once the token is local, every `mitos-admin`
call — status, tail, recapture — runs with no SSH.

### 2. A consolidated `GET /_admin/status`

One call an agent can poll for whole-host health. JSON, stable keys:

```json
{
  "version": "…", "build_sha": "…", "uptime_secs": 1234,
  "tip": { "slot": 0, "hash": "…" },
  "archive_horizon_slot": 0,          // why datum_by_hash fails for old assets
  "modules": [{ "id": "collection-holders", "companions": 3,
                "bootstrap_in_progress": false, "last_trap_secs_ago": null }]
}
```

`archive_horizon_slot` would have directly answered the
collection-metadata trait gap (hash-only datums below the horizon →
empty metadata; see `reference_mitos_archive_horizon`). Surfaced as
`mitos-admin status`.

### 3. Companion + bootstrap status (replace the journal greps)

Enrich `GET /_admin/modules/{id}` (or a `…/companions` list) with, per
companion: `client_id`, `companion_key`, `watched_policies` (the
projected interest), `resume_cursor` (slot), emission counts
(`queued/pending/acked/nacked`), `last_drain_secs_ago`. And a
`bootstrap`/`recapture` block: `in_progress` (bool), `last_result`
(`utxos_ingested`, `duration_ms`, `completed|trapped`), so "is a
recapture still running / did it complete" is a field, not a log grep.

### 4. A recent-events feed: `GET /_admin/events`

The structured equivalent of tailing the journal for operationally
interesting lines — a bounded ring of typed events as JSON:
`recapture_started/completed/timed_out`, `rebootstrap_completed`,
`trap` (`out_of_fuel`/`cabi_realloc` + module), `companion_subscribed/
evicted`. Each with `ts`, `module`, and typed fields. Replaces ~all the
`journalctl | grep` diagnosis. Surfaced as `mitos-admin tail`
(`--follow` to stream, `--module`/`--kind` filters) — the
"`wrangler tail` for mitos".

## LLM-decodable principles

- JSON, stable key names, decision-oriented fields (counts, booleans
  like `bootstrap_in_progress`, `*_secs_ago` deltas rather than raw
  timestamps the agent must diff).
- Bounded responses (paginate emissions/events) so an agent never
  pulls megabytes.
- Read-only endpoints must be side-effect free + cheap (safe to poll).
- Document the surface in one file + expose it from `GET /_admin`
  (self-describing index) so discovery doesn't require reading source.

## Task endpoints (write) — mostly already there

`recapture` / `evict` / `restart` / `replay` exist. Candidates to add as
the need arises: per-companion `reset`/`resubscribe`, `set-interest`.
The deploy itself (rsync + cargo build on the box + `systemctl restart`)
is inherently SSH and stays so — but a `POST /_admin/reload-module/{id}`
(rebuild-free hot-swap of an already-built wasm) could remove the
restart-for-module-only case.

## Phasing

1. **Docs + auth** — document the existing `mitos-admin` surface;
   ensure `MITOS_AUTH_TOKEN` is in the operator/agent local env (opt:
   `mitos-admin login`) so the API is reachable without SSH. (Biggest
   immediate win, little code.)
2. **`GET /_admin/status`** + `mitos-admin status` — version/tip/horizon/
   module summary.
3. **Companion + bootstrap status** on the module detail endpoint +
   richer `mitos-admin get-module` output.
4. **`GET /_admin/events`** ring buffer + `mitos-admin tail` (retires
   journal-grep flows).
5. (opt) read-only token scope; `reload-module`.

## Non-goals

- Replacing `journalctl` for deep forensics — the events feed covers the
  operational subset, not full structured logging.
- Replacing the deploy SSH path (build-on-box is intentional).
- A UI — this is an API for `mitos-admin` + agents; any dashboard is
  separate.
