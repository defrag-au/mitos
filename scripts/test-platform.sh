#!/usr/bin/env bash
# scripts/test-platform.sh — build the test-indexer wasm fixture
# then run the mitos-platform integration tests.
#
# The integration tests under `crates/mitos-platform/tests/`
# (`lifecycle_v2`, `dispatch_v2`) need a real wasm component
# to drive lifecycle + dispatch. They skip cleanly when the
# artifact isn't built, so this script is the recommended path
# to actually exercise them in CI / local dev.
#
# Usage:
#   scripts/test-platform.sh                # build fixture + run tests
#   scripts/test-platform.sh --no-build     # skip fixture build (fast iter)
#
# Why this lives in mitos but the build runs from cnft.dev-workers:
# the wasm32-wasip2 target lives in cnft.dev-workers' nix flake
# (via fenix). Mitos's flake doesn't carry it. Until both flakes
# converge, fixture builds borrow the cnft.dev-workers shell.
#
# Override env:
#   CNFT_DEV_WORKERS    Path to the cnft.dev-workers checkout
#                       (default: ../cnft.dev-workers relative
#                       to this script's parent dir).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MITOS_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CNFT_DEV_WORKERS="${CNFT_DEV_WORKERS:-${MITOS_ROOT}/../cnft.dev-workers}"

NO_BUILD=0
for arg in "$@"; do
    case "$arg" in
        --no-build) NO_BUILD=1 ;;
        -h|--help)
            sed -n '1,28p' "${BASH_SOURCE[0]}" | sed 's/^# \?//'
            exit 0
            ;;
        *) echo "unknown arg: $arg" >&2; exit 2 ;;
    esac
done

if [ ! -d "${CNFT_DEV_WORKERS}" ]; then
    echo "✗ cnft.dev-workers not found at ${CNFT_DEV_WORKERS}" >&2
    echo "  Set CNFT_DEV_WORKERS=/path/to/cnft.dev-workers to override." >&2
    exit 1
fi

if [ "${NO_BUILD}" -eq 0 ]; then
    echo "▸ building test-indexer fixture via cnft.dev-workers' wasip2 toolchain"
    cd "${MITOS_ROOT}"
    nix develop "${CNFT_DEV_WORKERS}" -c cargo run --release -p mitos-build --quiet \
        -- --module modules/test_indexer.rs
    echo
fi

echo "▸ running mitos-platform integration tests"
cd "${MITOS_ROOT}"
nix develop -c cargo test -p mitos-platform
