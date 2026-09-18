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
    # There is deliberately NO freshness check here. Cargo is the
    # freshness authority, and it already tracks every input:
    #
    #   <name>.rs           -> generated src/lib.rs        (tracked)
    #   <name>.toml [deps]  -> generated Cargo.toml        (tracked)
    #   crates/mitos-*      -> path deps of that Cargo.toml (tracked)
    #   wit-v2/*.wit        -> include_str! into mitos-build, which
    #                          writes the generated wit/ that
    #                          wit_bindgen::generate! tracks (tracked,
    #                          which is why step 2 rebuilds mitos-build
    #                          BEFORE this step runs)
    #
    # An earlier version of this step compared the wasm mtime against
    # the module dir plus the WIT. It re-derived that graph in bash and
    # got one edge wrong — the dependency crates — so a change confined
    # to e.g. mitos-community-events left every module reported "fresh"
    # and shipped stale wasm under a successful deploy. Do not
    # reintroduce a gate here; add the input to the crate instead and
    # cargo will find it.
    #
    # A no-op mitos-build is ~0.6s (cargo itself ~0.3s), so re-running
    # all ~20 unconditionally costs ~12s. That is the whole price.
    #
    # Per-module resilience:
    #   - source missing → skip silently
    #   - mitos-build failure → log, increment FAILED, keep going.
    #     auto-load gets whatever artifacts succeeded.
    #
    # Single-quoted heredoc on purpose — the loop body runs on the
    # remote box, so $name etc. must NOT expand locally.
    #
    # NOTE no apostrophes and no line continuations in the remote block:
    # it is handed to ssh inside a single-quoted string, so either one
    # ends the string early and the remote shell dies with
    # "syntax error: unexpected end of file".
    run "ssh '$MITOS_HOST' '
        cd $MITOS_SRC_REMOTE
        BUILT=0
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
            if ./target/$MITOS_BUILD_PROFILE/mitos-build --module \"\$src\" >/tmp/mitos-build-\$name.log 2>&1; then
                BUILT=\$((BUILT+1))
            else
                FAILED=\$((FAILED+1))
                echo \"    \$name: BUILD FAILED — see /tmp/mitos-build-\$name.log on the host\"
                echo \"    auto-load will skip this module; other modules will continue\"
            fi
        done
        echo \"  community modules: ok=\$BUILT  skipped=\$SKIPPED  failed=\$FAILED\"
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

    # Poll the health endpoint — the HTTP server binds some seconds
    # after `systemctl restart` returns (dolos WAL recovery + indexer
    # bootstrap happen first). Quiescent bind time is ~12s, but a
    # mainnet restart while the box is busy has been measured well
    # past 30s: the old window reported a false failure (and dumped
    # the journal) on a deploy that had in fact succeeded. 90s.
    local health=""
    local attempt=0
    while (( attempt < 45 )); do
        if health=$(ssh "$MITOS_HOST" "curl -sS --max-time 5 --connect-timeout 2 http://127.0.0.1:$MITOS_HEALTH_PORT/health" 2>&1) \
            && [[ -n "$health" ]]; then
            break
        fi
        attempt=$(( attempt + 1 ))
        sleep 2
    done

    if [[ -z "$health" ]] || ! printf "%s" "$health" | grep -q '"status"'; then
        err "Health endpoint did not respond after 90s: ${health:-(empty)}"
        warn "Recent journal:"
        ssh "$MITOS_HOST" "journalctl -u $MITOS_SERVICE -n 30 --no-pager" >&2 || true
        return 1
    fi

    # Pretty-print if jq is available remotely; otherwise raw.
    if ! printf "%s" "$health" | ssh "$MITOS_HOST" "command -v jq >/dev/null 2>&1 && jq ." 2>/dev/null; then
        printf "  %s\n" "$health" >&2
    fi

    step_verify_modules
}

# Read the host's own community-module activation report and fail the
# deploy if anything was refused.
#
# Deliberately NOT a bash-side comparison of artifact shas against
# what the host loaded: the host already validates every module
# (manifest vs wasm bytes, ABI major, wit world, wit revision) and
# reports the result. Re-deriving that here would repeat the mistake
# that made this check necessary — see the note in
# step_build_community_modules. Read the authority; don't reimplement it.
#
# Scoped to the CURRENT boot via ActiveEnterTimestamp so a summary
# from a previous run can't be mistaken for this one's.
step_verify_modules() {
    local since line refused
    since=$(ssh "$MITOS_HOST" "systemctl show -p ActiveEnterTimestamp --value $MITOS_SERVICE" 2>/dev/null) || since=""
    if [[ -n "$since" ]]; then
        line=$(ssh "$MITOS_HOST" "journalctl -u $MITOS_SERVICE --since '$since' --no-pager | grep 'community-modules auto-load complete' | tail -1" 2>&1)
    else
        line=$(ssh "$MITOS_HOST" "journalctl -u $MITOS_SERVICE -n 2000 --no-pager | grep 'community-modules auto-load complete' | tail -1" 2>&1)
    fi

    if [[ -z "$line" ]]; then
        warn "  no community-module auto-load summary this boot (auto-load disabled?)"
        return 0
    fi
    printf "  %s\n" "$line" >&2

    refused=$(printf "%s" "$line" | grep -o 'refused_count=[0-9]*' | cut -d= -f2)
    if [[ -z "$refused" ]]; then
        warn "  host predates refused_count reporting — rebuild it to get this check"
        return 0
    fi
    if [[ "$refused" != "0" ]]; then
        err "$refused community module(s) REFUSED — they are NOT running"
        warn "Refusals from this boot:"
        ssh "$MITOS_HOST" "journalctl -u $MITOS_SERVICE --since '$since' --no-pager | grep 'community module REFUSED' " >&2 || true
        return 1
    fi
    log "  community modules: no refusals"
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
