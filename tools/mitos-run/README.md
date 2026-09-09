# mitos-run

Local module test runner — **the fast loop**.

Family A (see the root README).

Loads a built `mitos-build` artifact and drives `init()` against a
fixture-driven data plane, surfacing logs, emissions and **full trap
backtraces with debug symbols intact**.

The point: none of that requires the production mitos host or a Dolos
snapshot. Deploying a module to find out why it traps is a slow way to learn
something this answers in seconds.

```bash
mitos-run --artifact <dir-from-mitos-build> --fixtures <dir>
```

Recipes and fixture shapes:
[`docs/HOWTO_TESTING_COMMUNITY_MODULES.md`](../../docs/HOWTO_TESTING_COMMUNITY_MODULES.md).
When a trap survives this and only reproduces in production, see
[`docs/HOWTO_DEBUG_TRAPS.md`](../../docs/HOWTO_DEBUG_TRAPS.md) and
[`docs/HOWTO_DEBUGGING_DEPLOYED_MODULES.md`](../../docs/HOWTO_DEBUGGING_DEPLOYED_MODULES.md).

## ⚠️ What a fixture run cannot tell you

It exercises `init()` against inputs you chose. It does not exercise budget
exhaustion on a real policy, re-entrant `rebootstrap` chunking, or the
dispatcher's residual pass — all of which are where large-collection modules
actually fail. Green here means the decode logic is right, not that the module
survives production.
