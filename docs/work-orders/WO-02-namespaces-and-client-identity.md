# WO-02 — Namespaces: a token resolves to a principal

**Design refs:** v3 D9 (Namespaces), §6 (Security notes).
**Lifecycle crate:** `mitos_lifecycle::access` — read the module docs and ALL of
its tests; `the_whole_decision_table` is the specification for this order.
**Depends on:** WO-01 (only for the dependency on `mitos-lifecycle`).
**Blocks:** WO-03 (instances need a `ClientKey`, which needs a namespace).

## Why

v2 has ONE shared `MITOS_AUTH_TOKEN`, checked by THREE separate copies of the
same middleware:

| Copy | Guards |
|---|---|
| `crates/mitos-core/src/auth.rs` | `/api/tx/submit`, the nested `/minibf` |
| `crates/mitos-platform/src/admin.rs` (`require_auth`, `authorize`) | `/_admin/*` |
| `crates/mitos-platform/src/companions.rs` (`require_auth`) | `/api/companions/*` |

Every deployed worker holds that token — so every worker holds the token that
evicts modules. And two client-facing routes trust what they are told:

- `POST /api/companions/subscribe` takes `client_id` from the request body.
  Any token holder can subscribe AS `ownership.cnft.dev`.
- `POST /api/companions/{companion_key}/interest` names NO client at all. It
  applies to every registration under that `companion_key` — and `dev` and
  `prod` workers SHARE companion keys (the policy-id DO name). A dev worker
  can rewrite production's interest set today.

None of it mattered while every token holder was us. v3 lets a subscribe create
an instance and move a client, so the boundary has to exist first.

## The decision is already made — you are wiring it

`mitos_lifecycle::access::Registry::authorize(&Principal, Attempt) ->
Result<Granted, Denied>` IS the policy. This order does three things:

1. Turn a bearer token into a `Principal`.
2. At each route, build the right `Attempt` and call `authorize`.
3. Turn `Granted` / `Denied` into HTTP, and into one operational event.

If you catch yourself writing `if namespace == …` or `if method == GET` to
decide whether something is allowed, stop — that logic is in `authorize`, and a
second copy in the host is how the two drift.

## Files

- `crates/mitos-platform/src/auth.rs` — NEW. The single authenticator.
- `crates/mitos-platform/src/lib.rs` — `pub mod auth;`
- `crates/mitos-platform/src/admin.rs` — delete its `AuthScopes`,
  `require_auth`, `authorize`, `constant_time_eq` and their tests' duplicates;
  use `auth`. KEEP `AuthToken` as a thin compat constructor if removing it
  would touch more than the files listed here (see "Compatibility").
- `crates/mitos-platform/src/companions.rs` — delete its `require_auth`; add
  the two `authorize` calls; one new error variant each on `SubscribeError`
  and `InterestMutationError`.
- `crates/mitos-platform/src/events.rs` — one new `EventKind`.
- `crates/mitos-core/src/auth.rs` — becomes a re-export / thin adapter over
  `mitos_platform::auth`. Its public names must keep resolving: the bundle
  binary uses `mitos_core::require_auth` for `/minibf`.
- `crates/mitos-core/src/tx.rs` — `Attempt::SubmitTx`.
- `crates/mitos-core/src/bundle.rs` — load the registry once; pass the
  authenticator to every router.
- `services/mitos/namespaces.example.toml` — does NOT exist in this repo (it
  is in `infra`). Instead add `docs/work-orders/WO-02-namespaces.example.toml`.

## The work

### 1. The registry file

`MITOS_NAMESPACES_FILE` names a TOML file. **Unset ⇒ legacy mode**
(`Registry::legacy(Namespace "prod", <a prod-shaped profile>)`), and the host
behaves exactly as v2. Every existing test runs in legacy mode and must pass
UNCHANGED.

```toml
operator_token_as_client = "allow-with-warning"   # or "refuse"

[[namespace]]
name = "prod"
token_env = "MITOS_TOKEN_PROD"        # the NAME of an env var — never a token
clients = ["ownership.cnft.dev", "jpgsm.cnft.dev", "hooks.epochify.space"]

[namespace.profile]
tier = "production"
revision = "explicit-rollback-only"
retain_superseded_secs = 259200
[namespace.profile.eject]
kind = "alert-only"
[namespace.profile.resources]
fuel_per_call = 100000000
backfill_concurrency = 4
```

The secret never enters the file: systemd's `EnvironmentFile` carries it, as
it does `MITOS_AUTH_TOKEN` today. Deserialise into a host-side struct (it has
`token_env`, which the pure crate must never see), then build
`access::NamespaceDef { clients: ClientSet::Only(..) }` and call
`Registry::configured`.

`NamespaceProfile` already derives `Deserialize` — parse it and STORE it. Do
NOT apply it: nothing in this order changes a `ResourceBudget`, a timer or a
follower. It is carried so WO-03 can read it.

**The host REFUSES TO START** (typed error, non-zero exit, a message naming
the offending entry) when the file is set and:

- it does not parse, or `Registry::configured` returns a `RegistryError`;
- a `token_env` is unset or empty;
- a namespace token is shorter than 32 bytes;
- any two of {operator full, operator read-only, every namespace token} are
  equal — a token must resolve to exactly one `Principal`;
- `MITOS_AUTH_TOKEN` is unset. **Open mode does not combine with
  namespaces**: a host that declares a security boundary and then admits
  every request unauthenticated is a misconfiguration, not a posture.

### 2. `auth.rs` — one authenticator

- `Authenticator::resolve(&self, bearer: Option<&str>) -> Resolution` where
  `enum Resolution { Principal(Principal), OpenMode, Unauthenticated }`.
  - Compare the presented token against EVERY configured token with the
    existing constant-time compare, and do not return early on a match —
    which token matched must not be observable from timing.
  - `OpenMode` exists only in legacy mode with no `MITOS_AUTH_TOKEN`, and is
    treated downstream as `Principal::Operator(OperatorScope::Full)`. That is
    today's behaviour and the test suite depends on it.
- One axum middleware, used by all three routers, that resolves the principal
  and inserts it into request extensions. `Unauthenticated` ⇒ **401** here.
- **401 means "who are you"; 403 means "not you".** No token / unknown token
  ⇒ 401 from the middleware. `Denied` ⇒ 403 from the handler, with the
  `Denied` Display text as the body. Keep them distinct: a worker holding a
  stale token and a worker holding the WRONG NAMESPACE's token are different
  incidents.
- Construct it from explicit values (tests) or from env (the binary). No test
  may depend on process-global env.

### 3. Call sites — build an `Attempt`, call `authorize`

| Route | `Attempt` |
|---|---|
| every `/_admin/*` EXCEPT the next row | `Admin(Read)` for `GET`, `Admin(Mutate)` otherwise. Decide it ONCE, in a middleware layer on the admin router, not per handler. |
| `POST /_admin/modules/{id}/recapture` | `RecaptureModule` — NOT `Admin(Mutate)`. A worker calls this (see the census). |
| `/metrics`, `/health` | unchanged — open |
| `POST /api/companions/subscribe` | `ActAs(&client_id)`, AFTER `validate_client_id` and BEFORE anything is persisted or handed to the dialer |
| `POST /api/companions/{key}/interest` | see below |
| `POST /api/tx/submit` | `SubmitTx` |
| `/minibf/*` | `ReadChain` — NOT `Admin(Read)`. A worker that is not a subscriber calls this (see the census). |

**The census — every holder of a mitos token, 2026-09-19.** The two non-`Admin`
rows above exist because of it. It was taken AFTER the first draft of this
order, which mapped `/minibf` to `Admin(Read)` and would have 403'd
`wallet-viewer` the day its secret was rotated. If you add a route, or find a
caller this table does not list, STOP and report — a route mapped to the wrong
`Attempt` is an outage at rollout, not at compile time.

| Holder (in `cnft.dev-workers`) | Secret | Calls | `Attempt` | After rotation holds |
|---|---|---|---|---|
| `collection-ownership`, `minting-engine`, `jpg-store-mirror` (companions) | `MITOS_AUTH_TOKEN` | subscribe, interest, `/health` | `ActAs` | its namespace token |
| `projects/holder-map` | `MITOS_AUTH_TOKEN` | subscribe **and** a PRODUCTION cron `POST /_admin/modules/holder-distribution/recapture` | `ActAs`, `RecaptureModule` | the `prod` namespace token — one secret serves both |
| `projects/wallet-viewer` | `MITOS_AUTH_TOKEN` | `/minibf/accounts/…` only — NOT a subscriber, so in no namespace's client list | `ReadChain` | a namespace token |
| `services/tx-submit` — a LIBRARY, linked into `abandonware-marketplace`, `actions`, `defrackit`, `script-depot`, `trade-desk`, `wallet-operations`; it reads the secret from THEIR env, so each of those six binds `MITOS_AUTH_TOKEN` in `wrangler.toml` with no Rust reference of its own | `MITOS_AUTH_TOKEN` | `/api/tx/submit` | `SubmitTx` | a namespace token |
| `workers/mitos-watch` | `MITOS_READONLY_TOKEN` | `/_admin/status`, `/metrics` | `Admin(Read)` | unchanged |
| `tools/notification-config` (`ncli`) | `MITOS_AUTH_TOKEN` | `/_admin/status`, `/_admin/utxos` | `Admin(Read)` | unchanged — an OPERATOR tool, keeps the operator token |

(`jpg-store-mirror`'s `/_admin/asks/*` and `/_admin/reset` are its OWN worker
routes, not mitos's.)

How the census was taken, so it can be re-taken: `rg -l
'MITOS_AUTH_TOKEN|MITOS_AUTH_TOKEN_ENV|MITOS_HOST_URL' -g '*.rs' -g '*.toml'`
across all 27 repos under `~/code/defrag` — only `mitos` and
`cnft.dev-workers` match. Search the `.toml` files too: a first pass over
`.rs` alone found 8 holders and MISSED the six `tx-submit` consumers, which
hold the secret purely through a `wrangler.toml` binding.

`RecaptureModule` is granted to a namespace principal only when no namespace is
more protected than its own, because recapture is whole-module today and
disturbs every client of the module. That rule lives in `authorize`; you map
the route and nothing more.

**Interest mutation.** The handler loads every registration under the
`companion_key`. Call `authorize(principal, ActAs(&record_client_id))` for
EACH, and apply the mutation only to the ones granted. Then:

- some granted ⇒ proceed with those, and those only;
- registrations exist but NONE granted ⇒ **403**;
- no registrations at all ⇒ the existing 404, unchanged.

The ungranted records must be byte-identical afterwards. That is the whole
point of the order.

**`Granted::OperatorTokenUsedAsClient`.** Log at `warn` with the client id,
and record a new `EventKind::OperatorTokenUsedAsClient { client_id }` in the
`EventRing`. That event is how the operator finds out which workers still hold
the old token before flipping to `refuse` — it must be queryable from
`GET /_admin/events`, not only greppable from the journal. Debounce it per
client (once per 10 minutes is plenty; ~91 DOs re-subscribe at once).

### 4. Registrations that no namespace owns

Under a configured registry, a registration already on disk may belong to a
client no namespace lists (`Registry::client_key` ⇒ `None`). Do NOT delete it
and do NOT stop dialling it — this order removes nothing. At boot, log ONE
`warn` listing such client ids, and expose them as `unowned_clients` on
`GET /_admin/status`. Their next subscribe gets a 403 (`UnknownClient`), which
is the signal to add them to a namespace.

### 5. `/_admin/status`

Add, as TYPED structs (no `serde_json::json!`):

```text
namespaces: [{ name, tier, clients: <count>, operator_token_as_client }]
auth_mode:  "legacy" | "namespaces"
unowned_clients: [..]
```

Never serialise a token, a `token_env` VALUE, or anything derived from one.

## Compatibility

- Legacy mode is byte-for-byte v2. The proof is that no existing test changes.
- `admin_router` / `admin_router_with_host` / `companion_router` currently take
  an `AuthToken`. Prefer changing them to take the authenticator; if that
  ripples past the files listed above, keep `AuthToken` as a constructor that
  builds a legacy authenticator and say so under "Deviations".
- The wire does not change. `mitos-protocol` and `mitos-companion` are NOT
  touched: a worker adopts its namespace by having the VALUE of its existing
  `MITOS_AUTH_TOKEN` secret replaced. No worker code changes, no redeploy.

## Out of scope — do not do these

- Applying `NamespaceProfile` to anything (budgets, timers, runtimes).
- Namespace-scoped admin or MCP access. Namespace tokens get NO admin in this
  order; D11 revisits it.
- Per-client recapture. Client rows. `ModuleRef`. Anything in `client_move`.
- Moving or renaming `companions/<client_id>/` — the namespace is DERIVABLE
  from the client id (a client belongs to at most one), which is exactly why
  it does not need to be in the path.
- Token rotation tooling, token hashing at rest, rate limiting.

## Tests that encode the old behaviour

`admin.rs` has four unit tests of its private `authorize` fn
(`full_token_authorizes_any_method`, `readonly_token_authorizes_only_get`,
`wrong_or_missing_token_rejected`, `readonly_unset_means_only_full_works`).
That function is deleted. PORT each to the new authenticator with the same
intent and the same assertions — they must all still hold. Do not drop one.

Every other existing test passes unchanged.

## Acceptance

Through the REAL routers (`tower::ServiceExt::oneshot`), not by calling
`authorize` directly — the pure crate already tests that. What is being proven
here is that every route actually asks.

1. `nix develop -c cargo test -p mitos-platform -p mitos-core` — all pass.
2. New tests, configured registry with `prod` + `dev`:
   - `a_dev_token_cannot_subscribe_as_a_prod_client` — 403, AND assert no file
     was written under `companions/` and the dialer was not called.
   - `a_dev_token_subscribes_as_its_own_client` — 200, record written.
   - `a_dev_token_cannot_rewrite_a_prod_clients_interest` — register the SAME
     `companion_key` under a prod and a dev client; mutate with the dev token;
     assert the dev record changed and the prod record's BYTES did not.
   - `interest_mutation_with_nothing_granted_is_403_not_404`
   - `a_namespace_token_gets_403_on_every_admin_route` — iterate the router's
     routes; do not hand-pick three. Include `POST …/evict`.
   - `an_unknown_token_is_401_and_a_wrong_namespace_is_403`
   - `the_operator_token_as_a_client_is_granted_and_recorded` — assert the
     `OperatorTokenUsedAsClient` event is in the ring, and that 91 subscribes
     from one client produce ONE event, not 91.
   - `refuse_mode_turns_that_into_403`
   - `a_namespace_token_may_submit_a_tx_and_a_readonly_token_may_not`
   - `minibf_accepts_any_authenticated_principal_and_no_other` — prod token,
     dev token, read-only token, full token ⇒ 200; no token / unknown ⇒ 401.
     This is `wallet-viewer` continuing to work after rotation.
   - `a_prod_token_may_recapture_and_a_dev_token_may_not` — through the real
     route. Assert the dev request is 403 AND that the host's recapture was
     never invoked. This is `holder-map`'s cron continuing to work.
   - `recapture_is_not_reachable_as_admin_mutate` — the recapture route must go
     through `RecaptureModule`; assert a prod NAMESPACE token reaches it (it
     would be 403 under `Admin(Mutate)`) while `POST …/evict` with the same
     token is still 403.
   - `unowned_registrations_are_reported_and_left_alone`
3. Startup refusals, one test each: unparseable file; client in two
   namespaces; `token_env` unset; short token; two equal tokens; namespaces
   with `MITOS_AUTH_TOKEN` unset.
4. `status_never_leaks_a_token` — serialise the status response for a
   configured host and assert no configured token value is a substring of it.
5. `rg -n 'fn constant_time_eq|fn require_auth' --type rust crates` — exactly
   ONE definition of each remains. (Confirm the pattern matches before you
   trust an empty result.)
6. clippy `-D warnings` exit 0; `cargo check --workspace --all-targets` exit 0.

## Deploy note (for the operator — NOT part of this order)

1. Deploy with NO `MITOS_NAMESPACES_FILE`. Nothing changes; confirm that.
2. Generate one token per namespace (`openssl rand -hex 32`). Add them to the
   unit's `EnvironmentFile`, write `namespaces.toml` from the subscriber census
   (prod: `ownership.cnft.dev`, `jpgsm.cnft.dev`, `hooks.epochify.space`; dev:
   `ownership.dev.cnft.dev`, `hooks.dev.epochify.space`), set
   `MITOS_NAMESPACES_FILE`, restart. Mode is `allow-with-warning`, so every
   worker keeps working on the old token.
3. Give each holder in the census table its NAMESPACE token. **This is NOT
   `wrangler secret put` per environment.** The workers bind the secret from a
   Cloudflare SECRETS STORE, and the top-level block and `[env.dev]` both bind
   the SAME `secret_name` from the SAME store:

   ```toml
   { store_id = "da9b…", binding = "MITOS_AUTH_TOKEN", secret_name = "MITOS_AUTH_TOKEN" }
   ```

   So `dev` and `prod` workers share ONE secret value today, and no
   per-environment put can separate them. Instead: add two secrets to the
   store (`MITOS_TOKEN_PROD`, `MITOS_TOKEN_DEV`), and in each worker's
   `wrangler.toml` change only `secret_name` — production block → `_PROD`,
   `[env.dev]` → `_DEV`. Keep `binding = "MITOS_AUTH_TOKEN"`: the binding is
   the name the CODE reads, so no Rust changes and `mitos-companion` is
   untouched. Then redeploy each worker.

   That is ~14 `wrangler.toml` files in `cnft.dev-workers` — a config change
   in ANOTHER repo, to be made by the operator, not under this order.

   Six of those holders (the `tx-submit` consumers) and `wallet-viewer`
   subscribe to nothing, so they will NEVER appear in the events below —
   rotate them from the census table, not from the log. For the companions,
   watch `GET /_admin/events`: a client drops out of
   `operator_token_used_as_client` once it is rotated.
   `ncli` and `mitos-watch` keep what they have.
4. When the events stop, set `operator_token_as_client = "refuse"` and
   restart. Then rotate the operator token itself: every worker has held it.
   That rotation is also the BACKSTOP for the seven non-subscribers — `refuse`
   only covers `ActAs`, so a `tx-submit` consumer still on the operator token
   keeps working silently until the token changes, and then fails with a 401,
   loudly. Do it in a window where a failed tx submit will be noticed.
5. Run `/security-review` over this change before step 2 reaches mainnet.
