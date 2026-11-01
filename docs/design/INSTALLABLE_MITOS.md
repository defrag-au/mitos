# Installable mitos — one artifact, one file, one command, nobody watching

**Status: PROPOSED** (2026-09-21). Nothing here is built. It sets the target
and the order of work.

**Execution plan: `infra/docs/work-orders/PLAN-I-installable-mitos.md`** — one
plan run end to end, driven from the `infra` repo (the operator's seat), with
its report beside it. §6 here is the WHAT; that plan is the HOW, with every
decision pre-made.

Cross-references:
- `docs/design/FOOTPRINT_AND_MINIMUM_SPEC.md` — what a box needs to RUN mitos.
  This doc is what it takes to GET mitos onto a box. They share a goal
  (anyone can run it) and one hard dependency, called out in §2.
- `docs/work-orders/WO-F5-mitos-bootstraps-itself.md` — already written; it is
  step I3 here.
- `infra/docs/work-orders/WO-F1-arm-mainnet-box.md` — the rebuild of our ARM
  box, and where this was learned. Most of its `provision.sh` is what this
  doc replaces.
- `docs/strategy/MITOS_PLATFORM_V3.md` D8 (pull by digest), D11 (reporting).

## 1. Goal

Someone with a fresh Debian VPS and no knowledge of us gets a mitos node at
chain tip by pasting one block of cloud-init and walking away. We can do the
same on their behalf through a provider's API. The acceptance test is
literal: **rebuild the ARM box with a user-data script, never open an SSH
session, and find it at tip.**

## 2. Why today's install is attended — it is structural, not untidy

The WO-F1 provisioning got a box built, by script, and still needed a person
throughout. Tidying the scripts will not change that, because of what they
are scripts FOR:

| Today | Why it cannot be unattended |
|---|---|
| **Source is rsynced from a laptop and compiled on the box** (`scripts/deploy.sh`). There are no release binaries — CI has no release job. | Needs our checkout, a laptop, SSH, `rustup`, `build-essential`, tens of GB of `target/`, and a release build's worth of time. (UNMEASURED — record wall time and peak RSS of `cargo build --release -p mitos` on the ARM box; it is one `/usr/bin/time -v` away.) |
| The build almost certainly needs more RAM than the run. | **Then the minimum spec is set by `rustc`, not by mitos.** A workspace that embeds dolos and wasmtime is not a small link. If that measurement comes in above the footprint ladder's lowest passing rung, every GB removed from the runtime below it is unreachable for anyone who must compile — and this becomes the one hard dependency between the two docs. |
| Orchestrated over SSH from an operator's machine, across two repos, as a sequence of imperative steps separated by waits of hours. | The state machine lives in a person's head. If they close the laptop, it stops. |
| Configuration is an env file per unit written by hand, plus a `dolos.toml`, plus four genesis JSON files copied per network. | Nothing declares what the node IS. |
| Modules are built and uploaded from a laptop that has the wasm toolchain. | And module builds are not reproducible across machines, so a third party CANNOT build the ones we run. |
| First run needs two other projects' binaries and downloads ~217 G twice (WO-F5). | — |
| Partitioning is netcup-specific. | The most fragile step is the least portable one. |

## 3. The target

```
release artifact  +  mitos.toml  +  `mitos init && systemctl start mitos`
```

### 3.1 Release artifacts — I1

CI builds, for `x86_64-unknown-linux-gnu` AND `aarch64-unknown-linux-gnu`:
`mitos` and the tools a full node runs (`tx-index`, the ledgers). One tarball
per target, a `SHA256SUMS`, and a detached signature (minisign — one small
static verifier, no GPG keyring to explain). Published as GitHub Releases and
mirrored to R2. glibc target matching Debian 13; not musl (the allocator work
in the footprint doc is about glibc).

This deletes from every box: `rustup`, `build-essential`, the source tree,
`target/`. And it UNCOUPLES the minimum spec from the compiler.

A `.deb` is the natural second step on the OS we standardised on (users,
units, upgrades and removal for free). Tarball first: it works everywhere and
the installer below needs it anyway.

### 3.2 One declarative file — I2

`/etc/mitos/mitos.toml` says what the node IS:

```toml
network  = "mainnet"            # genesis + Mithril constants come from the binary
profile  = "full"               # lean | standard | full — footprint §7
data_dir = "/var/lib/mitos"
listen   = "127.0.0.1:8181"

[modules]                        # pulled by digest, never built — §3.4
catalogue = "https://modules.mitos.…/catalogue.toml"
install   = ["collection-holders", "collection-metadata"]

[caches]                         # WO-F2's knobs, as config rather than env
kv_mb = 256
```

- **Genesis files and the Mithril constants are embedded in the binary** for
  the networks we know. Four JSON files and a hex key per network, copied by
  hand between repos today, are exactly the kind of thing that is wrong on
  somebody's box and never on ours. A path override stays for private nets.
- The embedded-dolos config is DERIVED from this file, not authored beside it.
- `deny_unknown_fields`, as every authored file in v3 is.
- Env vars remain as overrides (systemd drop-ins, secrets), not as the source
  of truth. Secrets stay out of the TOML: the auth token is generated by
  `mitos init` into a root-only file the TOML points at.

### 3.3 The binary installs and explains itself — I2, I3

- `mitos init` — creates the user and directories, writes a default
  `mitos.toml` if there is none, generates the auth token, and installs
  systemd units FROM TEMPLATES EMBEDDED IN THE BINARY. No second repo is
  needed to run mitos. Idempotent.
- `mitos doctor` — preflight (arch, glibc, cgroup v2, free disk against the
  chosen profile, RAM against the published minimum, clock, ports) and
  postflight (tip lag, module health). Every refusal names the number it
  wanted and the number it found. `--wait` blocks until at tip, with progress
  — this is what makes an unattended install OBSERVABLE.
- `mitos bootstrap` — WO-F5.
- `mitos` with no subcommand still means "run".

### 3.4 Modules come from a catalogue — I4

v3 already decided modules are pulled by digest (D8), and WO-05 built the
verification. Today the only source is the announcing worker's own hostname.
A standalone operator needs a second source: a signed catalogue — module id →
wasm digest, manifest digest, revision, URL — that `mitos init` resolves and
pulls through the SAME `ArtifactPuller` and the same seven checks. We publish
ours; anyone can publish theirs. This is also what makes "the modules we run"
something a third party can have at all.

### 3.5 Unattended means the state machine is in systemd, not in a person — I3

The install is hours long and must survive a reboot with nobody there. So it
is units, not a script somebody watches:

```
mitos-bootstrap.service   Type=oneshot, RemainAfterExit=yes
                          ConditionPathExists=!/var/lib/mitos/.bootstrapped
                          ExecStart=mitos bootstrap        (resumable — WO-F5)
mitos.service             Requires= / After=mitos-bootstrap.service
```

Boot → bootstrap runs (or resumes, or is skipped) → mitos starts. A power
cut at hour six costs the time since the last import checkpoint, and nobody
has to know it happened.

The whole install is then one cloud-init block — portable to any provider
that takes user-data, netcup's `customScript` included:

```sh
#!/bin/sh
set -eu
curl -fsSLO https://get.mitos.…/mitos-$(uname -m).tar.gz   # + SHA256SUMS + .minisig
# verify, unpack to /usr/local/bin
mitos init --profile full --network mainnet
systemctl enable --now mitos-bootstrap mitos
```

WO-F1 banned `customScript` because a first-boot script that fails is
invisible. That was the right call for a 500-line bash provisioner and the
wrong conclusion in general: the fix is to make the script FOUR LINES and put
everything that can fail inside a binary that records what it is doing —
`mitos doctor` reads the same state file, and so can a remote caller (§5).

### 3.6 Partitioning stops being required

The XFS `reflink=1` volume (v3 Appendix B) makes an instance COPY nearly free.
It is an optimisation for a host that forks instances, not a prerequisite for
running one: `reflink_or_copy` already degrades to a plain copy. A
single-disk VPS installs onto its root filesystem and works. The partitioned
layout becomes an optional profile for boxes we build, and the most
provider-specific, most destructive step leaves the default path entirely.

## 4. Efficient — time to tip

The slow part of a new node is not the download, it is the IMPORT: hours of
CPU turning Mithril's immutable chunks into a dolos data dir, then building
the tx-index over them. Every node repeats identical work to reach an
identical result. (`docs/design/MITHRIL_PHONEBOOK.md` is the sketch that
most of that data dir is a node-shaped extra: the live UTxO book is ~4 G,
the rest is a rewrite of a log we already hold.)

**Publish the result.** After its weekly Mithril refresh, a node we run
publishes a data snapshot — the embedded-dolos data dir plus `tx-index`'s
`base.idx` and segments — keyed by the immutable chunk it corresponds to,
checksummed and signed. `mitos bootstrap --from-snapshot` restores it and
then follows from there: about an hour of download instead of most of a day.

The trust difference is stated, not hidden:

| Path | Trusts | Time |
|---|---|---|
| Mithril import (default) | the Cardano stake distribution, via Mithril's certificate chain | most of a day |
| Data snapshot | the snapshot's publisher | ~an hour |

Anyone running our BINARY already trusts us at least that much, and for a
node we provision on someone's behalf the snapshot is the obvious default.
A node can also verify later: `mitos doctor --verify-against-mithril` walks
the chunks in the background and compares.

## 5. "We do it for them"

Nothing new is needed underneath — it is the same four-line user-data, sent
through a provider's API instead of pasted into a console.

- **Provider adapters.** `infra/tools/netcup` is the first: a typed client
  that can already install an image with user-data and poll the task. Hetzner
  and the rest are the same shape. The adapter's whole job is "create or
  reinstall a server with this user-data and these SSH keys".
- **Observation without SSH.** The installer's state file is served by the
  node's own admin API once it is up, and before that a provisioning run can
  poll the provider's task plus an optional heartbeat the node POSTs to a URL
  given in its config. v3's D11 (one typed reporting model, MCP) is what a
  fleet view is built from; this doc adds nothing to it.
- **Build self-serve FIRST.** Managed provisioning is a wrapper around an
  install that already works unattended. Built the other way round it becomes
  a private pile of scripts only we can run — which is what we have.

## 6. Order of work

| # | What | Depends on |
|---|---|---|
| I1 | Release CI: both targets, checksums, signature, R2 mirror. Our own `deploy.sh` switches to installing the artifact — we must eat this first | — |
| I2 | `mitos.toml` (embedded genesis + Mithril constants, derived dolos config); `mitos init`, `mitos doctor`; units embedded in the binary | — |
| I3 | `mitos bootstrap` (**WO-F5**, written) + the oneshot→service chain + the four-line user-data | I2 |
| I4 | Signed module catalogue; `init` pulls through WO-05's `ArtifactPuller` | I2 |
| I5 | Data snapshot: publish after the weekly refresh; `bootstrap --from-snapshot` | I3 |
| I6 | Provider adapters + heartbeat; the managed path | I1–I3 |

**The acceptance test for I1–I3 is the ARM box:** `netcup rebuild` with the
user-data, no SSH, at tip. It was always going to be rebuilt more than once;
this is what it is rebuilt FOR. WO-F3's measurements then run on a box that
was installed the way a stranger's would be, which is the only honest place
to measure a published minimum spec.

## 7. What happens to WO-F1's work

- `tools/netcup` — kept; it is provider adapter number one.
- `provision.sh` — the hardening baseline survives as an OPTIONAL step for
  boxes we own. The toolchain, the dolos binary, the `mithril-client` CLI,
  the bind mounts and the env-file generation are all replaced by I1–I3.
- The slices and unit hardening move into the units `mitos init` installs,
  so every install gets them, not only ours.

## 8. Non-goals

- Containers / Kubernetes. A single static-ish binary under systemd is
  simpler to reason about, cheaper on a small VPS, and what the footprint
  measurements are taken against. A container image can be built FROM the
  release artifact later at no design cost.
- Windows, macOS, non-systemd Linux as install targets.
- Billing, multi-tenant control plane, customer accounts. §5 is provisioning,
  not a product surface.
- Replacing Mithril as the default trust root.
