#!/usr/bin/env bash
# Bring up the local mitos + minting-engine acceptance stack.
#
#   direnv exec . deploy/local/up.sh          # from the mitos root
#
# Builds the worker (and its module artifact), cross-builds the mitos host for
# the container's architecture, starts a Cloudflare quick tunnel for the worker,
# renders the configs both sides need, then runs `container-compose up` in the
# foreground. Ctrl-C stops both services and the tunnel.
#
# The host source is this checkout. The consumer is not: minting-engine lives in
# cnft.dev-workers, so the script finds a sibling checkout there by default and
# takes WORKER_SRC for any other layout.
#
# Why a tunnel at all: the host's artifact fetch is HTTPS-only by design (mitos
# D8), and a `wrangler dev` on localhost cannot be fetched. The quick tunnel
# gives the worker a real hostname with a valid certificate; the host derives
# the fetch URL from the worker's client id, which is that hostname.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
MITOS_SRC="$(cd "${DIR}/../.." && pwd)"
WORKER_SRC="${WORKER_SRC:-$(cd "${MITOS_SRC}/../cnft.dev-workers" 2>/dev/null && pwd || true)}"
WORKER_DIR="${WORKER_SRC}/workers/minting-engine"
STATE="${DIR}/.tmp"
GENERATED="${DIR}/generated"

log() { printf '▸ %s\n' "$*" >&2; }
die() {
    printf '✗ %s\n' "$*" >&2
    exit 1
}

TUNNEL_PID=""
cleanup() {
    if [ -n "${TUNNEL_PID}" ] && kill -0 "${TUNNEL_PID}" 2>/dev/null; then
        log "stopping cloudflared"
        kill "${TUNNEL_PID}" 2>/dev/null || true
        wait "${TUNNEL_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

[ -f "${MITOS_SRC}/bundles/default/Cargo.toml" ] \
    || die "${MITOS_SRC} is not the mitos checkout; run this from the repo (deploy/local/up.sh)"
if [ -z "${WORKER_SRC}" ] || [ ! -f "${WORKER_DIR}/wrangler.toml" ]; then
    die "no minting-engine under a cnft.dev-workers checkout; set WORKER_SRC=/path/to/cnft.dev-workers"
fi
command -v container-compose >/dev/null || die "container-compose is not on PATH — run this inside the devshell"
command -v cloudflared >/dev/null || die "cloudflared is not on PATH"
STATUS_TEXT="$(container system status 2>/dev/null || true)"
if ! printf '%s\n' "${STATUS_TEXT}" | grep -qE '^status[[:space:]]+running'; then
    die "the container runtime is not running; run: container system start"
fi
DNS_TEXT="$(container system dns list 2>/dev/null || true)"
if ! printf '%s\n' "${DNS_TEXT}" | grep -qx 'test'; then
    log "⚠️  DNS domain 'test' is not registered; the worker will not resolve mitos-lab-mitos.test"
    log "    run once: sudo container system dns create test   (then restart the runtime)"
fi

mkdir -p "${STATE}" "${GENERATED}" "${DIR}/mitos-image" "${STATE}/worker"

# Two tokens, and they must differ: the host refuses a file where the operator
# token and a namespace token are the same value, because a bearer token has to
# resolve to exactly one principal. The worker authenticates with the namespace
# token; the operator token is for the host's admin API and for `curl` here.
if [ ! -f "${DIR}/.env" ]; then
    log "generating ${DIR}/.env with fresh operator + namespace tokens"
    {
        printf 'MITOS_AUTH_TOKEN=%s\n' "$(openssl rand -hex 32)"
        printf 'MITOS_TOKEN_DEV=%s\n' "$(openssl rand -hex 32)"
    } > "${DIR}/.env"
    chmod 600 "${DIR}/.env"
fi
# shellcheck disable=SC1091
set -a
. "${DIR}/.env"
set +a
[ -n "${MITOS_AUTH_TOKEN:-}" ] || die "MITOS_AUTH_TOKEN is empty in ${DIR}/.env"
if [ -z "${MITOS_TOKEN_DEV:-}" ]; then
    MITOS_TOKEN_DEV="$(openssl rand -hex 32)"
    printf 'MITOS_TOKEN_DEV=%s\n' "${MITOS_TOKEN_DEV}" >> "${DIR}/.env"
fi

# 1. Worker: rebuilds the module artifact if its source is newer, then the
#    worker bundle. `wrangler dev` in the container bundles the staged
#    build/ output; its [build] command is a no-op there.
#
#    MITOS_SRC is exported because scripts/build-worker.sh rebuilds the
#    worker's own mitos module and would otherwise take its own sibling
#    default rather than this checkout.
log "building minting-engine (module + worker bundle)"
(cd "${WORKER_DIR}" && MITOS_SRC="${MITOS_SRC}" "${WORKER_SRC}/scripts/build-worker.sh")

# 2. Host: a Linux binary for the container. The musl cross-build is the same
#    route scripts/pack-release.sh uses; incremental after the first run.
log "cross-building mitos for aarch64-unknown-linux-musl (first run is slow)"
(
    cd "${MITOS_SRC}"
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER="${MITOS_SRC}/scripts/zigcc-link-aarch64-linux-musl" \
        cargo zigbuild --release --target aarch64-unknown-linux-musl -p mitos
)
cp "${MITOS_SRC}/target/aarch64-unknown-linux-musl/release/mitos" "${DIR}/mitos-image/mitos"
chmod 0755 "${DIR}/mitos-image/mitos"
if command -v llvm-strip >/dev/null 2>&1; then
    llvm-strip "${DIR}/mitos-image/mitos" || true
fi

# 3. Quick tunnel → the worker's published port. The hostname is the worker's
#    client id and the host's fetch origin.
log "starting cloudflared quick tunnel → 127.0.0.1:18787"
TUNNEL_LOG="${STATE}/cloudflared.log"
: > "${TUNNEL_LOG}"
cloudflared tunnel --no-autoupdate --url http://127.0.0.1:18787 \
    --logfile "${TUNNEL_LOG}" --loglevel info &
TUNNEL_PID=$!

TUNNEL_HOST=""
for _ in $(seq 1 120); do
    TUNNEL_HOST="$(sed -nE 's#.*(https://[a-z0-9-]+\.trycloudflare\.com).*#\1#p' "${TUNNEL_LOG}" | sed -n '1p')"
    [ -n "${TUNNEL_HOST}" ] && break
    kill -0 "${TUNNEL_PID}" 2>/dev/null || die "cloudflared exited; see ${TUNNEL_LOG}"
    sleep 0.5
done
[ -n "${TUNNEL_HOST}" ] || die "cloudflared printed no tunnel URL; see ${TUNNEL_LOG}"
TUNNEL_HOST="${TUNNEL_HOST#https://}"
log "tunnel: https://${TUNNEL_HOST}"

# 4. Host config. `network = preprod` derives Dolos from the embedded genesis;
#    `data_dir` is the named volume. The fallback is off — this stack is about
#    delivery, not resolution.
#
#    The worker subscribes to three modules, and the host refuses a subscribe
#    for a module it has no artifact for. Stage the three from the mitos tree so
#    the host has them at boot; `cip-25-mint` is also the PARENT instance the
#    worker's announced build moves away from.
stage_module() {
    local name="$1"
    local stem="${name//-/_}"
    local src="${MITOS_SRC}/community-modules/${name}"
    local dst="${GENERATED}/community-modules/${name}"
    [ -d "${src}/target/mitos/${name}" ] \
        || die "${name} has no built artifact; run: mitos-build --module community-modules/${name}/${stem}.rs"
    rm -rf "${dst}"
    mkdir -p "${dst}/target/mitos"
    cp "${src}/${stem}.rs" "${dst}/${stem}.rs"
    if [ -f "${src}/${stem}.toml" ]; then
        cp "${src}/${stem}.toml" "${dst}/${stem}.toml"
    fi
    cp -R "${src}/target/mitos/${name}" "${dst}/target/mitos/${name}"
}
rm -rf "${GENERATED}/community-modules"
stage_module cip-25-mint
stage_module cip-68-mint
stage_module credit-address

cat > "${GENERATED}/mitos.toml" <<EOF
network = "preprod"
profile = "full"
data_dir = "/var/lib/mitos"
listen = "0.0.0.0:8080"

[fallback]
provider = "none"
EOF

# 5. The namespace is what lets the worker announce at all: `may_announce`
#    refuses every client an operator has not listed. Legacy mode (no file)
#    refuses every announcement.
cat > "${GENERATED}/namespaces.toml" <<EOF
operator_token_as_client = "allow-with-warning"

[[namespace]]
name = "dev"
token_env = "MITOS_TOKEN_DEV"
clients = ["${TUNNEL_HOST}"]

[namespace.profile]
tier = "development"
revision = "allow"
retain_superseded_secs = 86400

[namespace.profile.eject]
kind = "automatic"
grace_secs = 600
retain_secs = 86400

[namespace.profile.resources]
fuel_per_call = 25000000
backfill_concurrency = 1
EOF

# 6. Worker dev env. `client_id()` defaults to the host of the dial-back URL,
#    so this hostname is what the host authenticates the worker as and fetches
#    from. The worker reaches mitos by container DNS.
#
#    The dial-back deliberately pins `policy_id` instead of using `{key}`: in a
#    deployed Worker the DO name is the policy, but `wrangler dev`'s DO runtime
#    reports no name from inside the object, so the companion falls back to its
#    unique id and `{key}` would select a DO that never subscribed. One policy is
#    all this lab watches, so pinning it is exact — override it with `POLICY=` and
#    pass the same value to `onboard.sh`.
POLICY="${POLICY:-e6ba9c0ff27be029442c32533c6efd956a60d15ecb976acbb64c4de0}"
rm -rf "${STATE}/worker"
mkdir -p "${STATE}/worker"
# The local config: the worker's own, minus two tables that only make sense
# in a deployed Worker:
#   - [build] — the worker is built on the host; `wrangler dev` would run it
#     inside the container, where the Rust toolchain does not exist.
#   - [[env.dev.secrets_store_secrets]] — local mode exposes the binding but
#     has no value for it, and worker_utils::secrets::get_secret treats
#     "binding present, value unreadable" as an error instead of falling back
#     to `.dev.vars`. Removing the binding makes `.dev.vars` the source.
awk '
    /^\[build\]$/ { skip = 1; next }
    /^\[\[env\.dev\.secrets_store_secrets\]\]$/ { skip = 1; next }
    /^\[/ { skip = 0 }
    !skip { print }
' "${WORKER_DIR}/wrangler.toml" > "${STATE}/worker/wrangler.toml"
cp -R "${WORKER_DIR}/build" "${STATE}/worker/build"
cat > "${STATE}/worker/.dev.vars" <<EOF
MITOS_HOST_URL=http://mitos-lab-mitos.test:8080
MITOS_REPLICATE_URL=https://${TUNNEL_HOST}/_internal/{op}-{target}?policy_id=${POLICY}
MITOS_AUTH_TOKEN=${MITOS_TOKEN_DEV}
INTERNAL_API_KEY=${MITOS_AUTH_TOKEN}
DEBUG_TOKEN=${MITOS_AUTH_TOKEN}
EOF

# The companion's onboard route registers the policy interest, then reads a
# deposit address from D1, then wakes the DO. A fresh local D1 has no tables,
# so seed the one column that read touches. `deposit_address` NULL means the
# credit-address watch is skipped; the policy interest is what we are testing.
( cd "${STATE}/worker" && wrangler d1 execute CLIENT_DB --local --env dev --command \
    "CREATE TABLE IF NOT EXISTS collections (policy_id TEXT PRIMARY KEY, deposit_address TEXT)" >/dev/null )
( cd "${STATE}/worker" && wrangler d1 execute CLIENT_DB --local --env dev --command \
    "INSERT OR IGNORE INTO collections (policy_id, deposit_address) VALUES ('${POLICY}', NULL)" >/dev/null )

log "starting stack — Ctrl-C stops both services and the tunnel"
container-compose -f "${DIR}/stack.toml" up
