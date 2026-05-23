#!/usr/bin/env bash
# scripts/deploy.sh — Deploy mitos host to a remote box.
#
# Codifies the manual flow from
# `infra/docs/mitos-operations.md` §"Deploy / upgrade":
# rsync source → cargo build on box → systemctl restart → health check.
#
# Usage:
#   MITOS_HOST=root@1.2.3.4 ./scripts/deploy.sh           # full deploy
#   MITOS_HOST=root@1.2.3.4 ./scripts/deploy.sh verify    # health check only
#   MITOS_HOST=root@1.2.3.4 ./scripts/deploy.sh restart   # restart + verify
#   MITOS_HOST=root@1.2.3.4 ./scripts/deploy.sh --dry-run # show plan, run nothing
#
# Required env:
#   MITOS_HOST          SSH target (e.g. root@159.195.57.187)
#
# Optional env (defaults match infra/docs/mitos-operations.md):
#   MITOS_SRC_REMOTE    Remote source dir       (default /opt/mitos/src)
#   MITOS_SERVICE       systemd unit name       (default mitos-mainnet)
#   MITOS_HEALTH_PORT   /health port on the box (default 8181)
#   MITOS_BUILD_PROFILE Cargo profile           (default release)
#
# What this script DOES NOT do:
# - Module reupload. After a WIT-changing deploy, existing wasm modules
#   (e.g. cnft.dev-workers/workers/collections-mitos/modules/ownership.rs)
#   may need rebuild + reupload via `mitos-build` + `mitos-admin upload-module`
#   from a laptop with the wasm32-wasip2 target available. That's a
#   per-consumer concern and lives in the consumer's repo, not here.
# - Subscription registration. `mitos-admin add --indexer ...` is also
#   per-consumer; don't bake it into a host-deploy script.
# - Module diagnostics post-deploy. Use `mitos-admin list-modules`
#   against the deployed host yourself if you want to see which modules
#   came back online cleanly.

set -euo pipefail

# ----------------------------------------------------------------------
# Resolve paths + env
# ----------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MITOS_SRC_LOCAL="$(cd "$SCRIPT_DIR/.." && pwd)"

# ----------------------------------------------------------------------
# Subcommand + flag parsing (must happen before env-var checks so
# --help works without MITOS_HOST set)
# ----------------------------------------------------------------------

CMD="deploy"
DRY_RUN=0

for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=1 ;;
        deploy|verify|restart) CMD="$arg" ;;
        -h|--help)
            sed -n '1,40p' "${BASH_SOURCE[0]}" | sed 's/^# \?//' >&2
            exit 0
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            echo "Usage: $0 [deploy|verify|restart] [--dry-run]" >&2
            exit 2
            ;;
    esac
done

# Required env (after arg parsing so --help doesn't trip it).
: "${MITOS_HOST:?MITOS_HOST must be set (e.g. root@159.195.57.187)}"

MITOS_SRC_REMOTE="${MITOS_SRC_REMOTE:-/opt/mitos/src}"
MITOS_SERVICE="${MITOS_SERVICE:-mitos-mainnet}"
MITOS_HEALTH_PORT="${MITOS_HEALTH_PORT:-8181}"
MITOS_BUILD_PROFILE="${MITOS_BUILD_PROFILE:-release}"

# ----------------------------------------------------------------------
# Logging helpers
# ----------------------------------------------------------------------

log()  { printf "\033[1;36m▸\033[0m %s\n" "$*" >&2; }
warn() { printf "\033[1;33m! %s\033[0m\n" "$*" >&2; }
err()  { printf "\033[1;31m✗ %s\033[0m\n" "$*" >&2; }
run()  {
    if [[ $DRY_RUN -eq 1 ]]; then
        printf "  [dry-run] %s\n" "$*" >&2
    else
        eval "$@"
    fi
}

# ----------------------------------------------------------------------
# Steps
# ----------------------------------------------------------------------

show_intent() {
    cat >&2 <<EOF

mitos host deploy
  command:        $CMD$( [[ $DRY_RUN -eq 1 ]] && printf " (dry-run)" )
  source (local): $MITOS_SRC_LOCAL
  target (host):  $MITOS_HOST:$MITOS_SRC_REMOTE
  service:        $MITOS_SERVICE
  health port:    $MITOS_HEALTH_PORT
  profile:        $MITOS_BUILD_PROFILE

EOF
}

dirty_tree_warning() {
    if [[ -n "$(cd "$MITOS_SRC_LOCAL" && git status --porcelain 2>/dev/null)" ]]; then
        warn "Working tree has uncommitted changes (will be deployed):"
        (cd "$MITOS_SRC_LOCAL" && git status --short | sed 's/^/  /')
        echo
    fi
}

step_rsync() {
    log "1/5 rsync $MITOS_SRC_LOCAL/ → $MITOS_HOST:$MITOS_SRC_REMOTE/"
    run "rsync -avz --delete \
        --exclude='target/' \
        --exclude='.git/' \
        --exclude='node_modules/' \
        '$MITOS_SRC_LOCAL/' \
        '$MITOS_HOST:$MITOS_SRC_REMOTE/'"
}

step_build() {
    # The rsync excludes .git/, so on-box `git` can't resolve a build
    # SHA. Compute it from the LOCAL working tree (the source of truth
    # for what's being deployed) and inject it as MITOS_BUILD_SHA — the
    # platform build.rs prefers this over on-box git. `--dirty` flags
    # an uncommitted deploy honestly. Surfaced via `GET /_admin/status`.
    local build_sha
    build_sha=$(cd "$MITOS_SRC_LOCAL" && git describe --always --dirty --abbrev=12 2>/dev/null || echo unknown)
    log "2/5 cargo build --profile $MITOS_BUILD_PROFILE -p mitos -p mitos-build (on box; ~5min cold, ~30s incremental; build=$build_sha)"
    run "ssh '$MITOS_HOST' 'cd $MITOS_SRC_REMOTE && MITOS_BUILD_SHA=$build_sha cargo build --profile $MITOS_BUILD_PROFILE -p mitos -p mitos-build'"
}

step_build_community_modules() {
    log "3/5 build community-module wasm artifacts (on box)"
    # Walk every community-modules/<name>/ that has a <name>.rs and
    # run `mitos-build --module <name>.rs`. Artifacts land at
    # community-modules/<name>/target/mitos/<name>/, exactly where
    # the host's community-module auto-load reads from.
    #
    # Per-module resilience:
    #   - source missing → skip silently
    #   - existing wasm newer than every .rs + .toml file in the
    #     module dir → skip (idempotent cache)
    #   - mitos-build failure → log, increment FAILED, keep going.
    #     auto-load gets whatever artifacts succeeded.
    #
    # Single-quoted heredoc on purpose — the loop body runs on the
    # remote box, so $name etc. must NOT expand locally.
    run "ssh '$MITOS_HOST' '
        cd $MITOS_SRC_REMOTE
        BUILT=0
        FRESH=0
        SKIPPED=0
        FAILED=0
        for d in community-modules/*/; do
            name=\$(basename \"\$d\")
            stem=\$(echo \"\$name\" | tr - _)
            src=\"\${d}\${stem}.rs\"
            if [ ! -f \"\$src\" ]; then
                SKIPPED=\$((SKIPPED+1))
                continue
            fi
            wasm=\"\${d}target/mitos/\${name}/\${name}.wasm\"
            if [ -f \"\$wasm\" ] && [ -z \"\$(find \"\$d\" -maxdepth 1 -name \"*.rs\" -newer \"\$wasm\" -print -quit)\" ] && [ -z \"\$(find \"\$d\" -maxdepth 1 -name \"*.toml\" -newer \"\$wasm\" -print -quit)\" ]; then
                FRESH=\$((FRESH+1))
                continue
            fi
            echo \"  building \$name\"
            if ./target/$MITOS_BUILD_PROFILE/mitos-build --module \"\$src\" >/tmp/mitos-build-\$name.log 2>&1; then
                BUILT=\$((BUILT+1))
            else
                FAILED=\$((FAILED+1))
                echo \"    \$name: BUILD FAILED — see /tmp/mitos-build-\$name.log on the host\"
                echo \"    auto-load will skip this module; other modules will continue\"
            fi
        done
        echo \"  community modules: built=\$BUILT  fresh=\$FRESH  skipped=\$SKIPPED  failed=\$FAILED\"
    '"
}

step_restart() {
    log "4/5 systemctl restart $MITOS_SERVICE (SIGTERM, 90s graceful timeout, dolos WAL checkpointed)"
    run "ssh '$MITOS_HOST' 'systemctl restart $MITOS_SERVICE'"
}

step_verify() {
    log "5/5 health check"
    if [[ $DRY_RUN -eq 1 ]]; then
        printf "  [dry-run] ssh %s 'systemctl is-active %s && curl http://127.0.0.1:%s/health'\n" \
            "$MITOS_HOST" "$MITOS_SERVICE" "$MITOS_HEALTH_PORT" >&2
        return 0
    fi

    local active
    if ! active=$(ssh "$MITOS_HOST" "systemctl is-active $MITOS_SERVICE" 2>&1); then
        err "Service is not active: $active"
        warn "Recent journal:"
        ssh "$MITOS_HOST" "journalctl -u $MITOS_SERVICE -n 30 --no-pager" >&2 || true
        return 1
    fi
    log "  service: $active"

    # Poll the health endpoint — the HTTP server binds a few seconds
    # after `systemctl restart` returns (dolos WAL recovery + indexer
    # bootstrap happen first). Real-world bind time on the prod host
    # is ~12s post-restart; allow a generous 30s window.
    local health=""
    local attempt=0
    while (( attempt < 15 )); do
        if health=$(ssh "$MITOS_HOST" "curl -sS --max-time 5 --connect-timeout 2 http://127.0.0.1:$MITOS_HEALTH_PORT/health" 2>&1) \
            && [[ -n "$health" ]]; then
            break
        fi
        attempt=$(( attempt + 1 ))
        sleep 2
    done

    if [[ -z "$health" ]] || ! printf "%s" "$health" | grep -q '"status"'; then
        err "Health endpoint did not respond after 30s: ${health:-(empty)}"
        warn "Recent journal:"
        ssh "$MITOS_HOST" "journalctl -u $MITOS_SERVICE -n 30 --no-pager" >&2 || true
        return 1
    fi

    # Pretty-print if jq is available remotely; otherwise raw.
    if ! printf "%s" "$health" | ssh "$MITOS_HOST" "command -v jq >/dev/null 2>&1 && jq ." 2>/dev/null; then
        printf "  %s\n" "$health" >&2
    fi
}

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

START=$(date +%s)
show_intent

case "$CMD" in
    deploy)
        dirty_tree_warning
        step_rsync
        step_build
        step_build_community_modules
        step_restart
        step_verify
        ;;
    verify)
        step_verify
        ;;
    restart)
        step_restart
        step_verify
        ;;
esac

ELAPSED=$(( $(date +%s) - START ))
log "Done in ${ELAPSED}s."
