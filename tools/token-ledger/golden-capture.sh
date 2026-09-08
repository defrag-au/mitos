#!/usr/bin/env bash
#
# golden-capture.sh — pin token-ledger's COMPOSED output, so a refactor can be
# proved neutral instead of eyeballed.
#
# WHY THIS EXISTS. `token-ledger` has 85 unit tests and they are good ones, but
# every number a reader actually sees — the supply cascade, spot, the honesty
# ratio, the holder table — is produced by composing `cohort`, `pools`, the
# store and the wire projections. Change one of those subtly and NOTHING FAILS.
# The number is simply different, and stays different. That is precisely the
# failure mode this repo has been bitten by before (a cohort column that never
# appeared, a reserve read the wrong way round, a `stats`/`inspect` pair where
# only one surface got a decimals fix).
#
# So: freeze real ledgers, record what the tool says about them today, and diff.
#
# WHY FROZEN COPIES AND NOT THE LIVE FILES. `token-ledger-serve` re-walks the
# per-token dbs under /opt/token-ledger/auto, and opening a ledger can migrate
# it. A baseline that moves is not a baseline. `$ROOT/db` and `$ROOT/export`
# are COPIES, made once; nothing in this script writes to the live tree.
#
# WHAT IS DELIBERATELY NOT COVERED. The archive paths (`archive`, `graph`,
# `rollup`, `bundle`). They are actively being walked, so their output moves by
# design — and they do not touch `cohort` or `pools`, which is what the current
# refactor moves. Add them here only alongside a frozen archive copy.
#
#   ./golden-capture.sh capture   # record the baseline into $ROOT/expected
#   ./golden-capture.sh check     # re-run into $ROOT/out and diff; non-zero on drift
#
# Run it ON cardano-infra. Inputs live at $ROOT (455 MB), outputs are a few KB
# and are the thing worth keeping in the repo.

set -uo pipefail

BIN=${BIN:-/opt/mitos/src/target/release/token-ledger}
ROOT=${ROOT:-/opt/token-ledger/golden}
TOP=${TOP:-25}

# Every surface below goes through `cohort`/`pools`; stderr carries tracing
# (timestamps) so only stdout is captured, and the log level is pinned so an
# added `info!` cannot masquerade as a regression.
export RUST_LOG=${RUST_LOG:-error}

mode=${1:-check}
case "$mode" in
  capture) dest="$ROOT/expected" ;;
  check)   dest="$ROOT/out" ;;
  *) echo "usage: $0 [capture|check]" >&2; exit 2 ;;
esac

[ -x "$BIN" ] || { echo "no token-ledger binary at $BIN" >&2; exit 2; }
[ -d "$ROOT/db" ] || { echo "no frozen inputs at $ROOT/db" >&2; exit 2; }

rm -rf "$dest"; mkdir -p "$dest"

# A failing command is RECORDED, not fatal: a regression that breaks one token
# should show up as a diff on that token, not as a script that stopped early
# and silently skipped the rest.
run() {
  local out=$1; shift
  if ! "$@" > "$dest/$out" 2>/dev/null; then
    echo "COMMAND FAILED: $*" > "$dest/$out"
  fi
}

# `classify` FIRST, and it is not optional. Cohorts are STORED on the party
# row, re-derived from the address rather than decided by the walk — that is
# what makes reclassification a one-second re-derivation instead of a re-walk.
# The consequence for this harness is that `stats` would otherwise report
# cohorts assigned by whatever code walked the ledger months ago, and a change
# to classification would show NO drift while being completely live in
# production. Running it here means the baseline always reflects current code.
TOKENS=${TOKENS:-/opt/mitos/src/tools/token-ledger/tokens.toml}

for db in "$ROOT"/db/*.db; do
  [ -e "$db" ] || continue
  name=$(basename "$db" .db)
  "$BIN" classify --db "$db" --tokens "$TOKENS" >/dev/null 2>&1 || true
  run "stats.$name.txt" "$BIN" stats --db "$db" --top "$TOP"
  run "probe.$name.txt" "$BIN" probe --db "$db"
done

# `inspect` reads the artifacts ALONE, with no database, through the same
# projections a frontend uses — so capturing it beside `stats` pins two
# independent code paths over the same ledger. They agreeing is the check a
# byte-level round-trip cannot make.
for card in "$ROOT"/export/*.card.json; do
  [ -e "$card" ] || continue
  token=$(basename "$card" .card.json)
  run "inspect.$token.txt" "$BIN" inspect --dir "$ROOT/export" --token "$token"
done

n=$(find "$dest" -type f | wc -l | tr -d ' ')
if [ "$mode" = capture ]; then
  echo "captured $n files into $dest"
  exit 0
fi

if [ ! -d "$ROOT/expected" ]; then
  echo "no baseline at $ROOT/expected — run '$0 capture' first" >&2
  exit 2
fi

if diff -ru "$ROOT/expected" "$dest"; then
  echo "golden: $n files, no drift"
else
  echo >&2
  echo "golden: DRIFT — the composed output changed. If that was intended," >&2
  echo "re-run with 'capture' and review the diff above before accepting it." >&2
  exit 1
fi
