# mitos-admin

Thin CLI over a running bundle's HTTP admin surface.

Family A (see the root README): talks to a live bundle, not to chunk files.

## Commands

```bash
mitos-admin health                    # uptime, indexer list
mitos-admin status
mitos-admin tail                      # follow the bundle's event stream

# Modules
mitos-admin list-modules
mitos-admin get-module <id>
mitos-admin upload-module <artifact>  # a `mitos-build` artifact directory
mitos-admin restart-module <id>
mitos-admin delete-module <id>
mitos-admin evict-module <id>
mitos-admin deploy <artifact>         # upload + restart in one step

# Coordinated state rebuild — see docs/design/RECAPTURE.md
mitos-admin recapture <module>

# Companions
mitos-admin delete-companion <...>

# The emission log
mitos-admin emissions
mitos-admin emissions-replay <...>
mitos-admin emissions-purge <...>
```

## ⚠️ `recapture` is not a restart

It signals every subscribed companion to **drop projected state**, then re-runs
the module's bootstrap into a clean target. A companion that has not
implemented `on_recapture` will keep its old rows and quietly diverge. Read
`docs/design/RECAPTURE.md` and `docs/design/WASM_BUDGET_CHUNKING.md` (the
re-entrant `rebootstrap` export) before running it against production.

The legacy `list` / `add` / `remove` subcommands retired with the outbound
`Replicator` subscription model and its `/_admin/subscriptions` routes.
