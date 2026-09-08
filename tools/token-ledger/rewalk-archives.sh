#!/usr/bin/env bash
#
# rewalk-archives.sh — rebuild every policy archive with the observation tier.
#
# WHY A REBUILD AND NOT A TOP-UP. Observations are written by the pass that
# walks a range, and every archive on the box is already `complete` — so no
# further pass is due and none would ever be written. The re-walk IS the
# backfill. Restarting `token-ledger-serve` alone does nothing for a complete
# archive.
#
# WHY IT BUILDS ALONGSIDE. These archives are live: flow-explorer reads them and
# they publish to R2. Walking into a SEPARATE root means a failure halfway
# leaves the old archive untouched, and the swap is one `mv` after everything
# has been verified. Never walk over the only copy of something a reader is
# using.
#
# ⚠️ THE DAEMON MUST BE RESTARTED WITH THE SWAP, NOT AFTER. `Manifest`
# deserialises with serde's default of IGNORING unknown fields, so the running
# (pre-2026-09-08) binary would read a new manifest happily and then SILENTLY
# DROP `profile` the next time it rewrote one. Stop it, swap, start it.
#
#   ./rewalk-archives.sh            # walk every policy into $NEW
#   ./rewalk-archives.sh <policy>   # just one, for a rehearsal
#
# Run on cardano-infra. Each policy's floor comes from its EXISTING manifest —
# a first mint already established by a completed walk is not worth
# rediscovering, and getting it wrong makes a partial walk claim completeness.

set -uo pipefail

BIN=${BIN:-/opt/mitos/src/target/release/token-ledger}
OLD=${OLD:-/opt/token-ledger/archive}
NEW=${NEW:-/opt/token-ledger/archive-v2}
DATA=${DATA:-/opt/market-ledger/snapshot-full/db}
export RUST_LOG=${RUST_LOG:-error}

[ -x "$BIN" ] || { echo "no binary at $BIN" >&2; exit 2; }
mkdir -p "$NEW"

# `first_mint_slot` out of the existing manifest. Empty when it never recorded
# one — the walk then runs to genesis, which is correct but slow, so it is
# reported rather than silently accepted.
floor_of() {
  python3 - "$1" <<'PY'
import json, sys
try:
    m = json.load(open(sys.argv[1]))
    print(m.get("first_mint_slot") or "")
except Exception:
    print("")
PY
}

# IMMUTABLE rows only. An existing archive also carries a `volatile` tip pass —
# the stretch past the immutable snapshot, read from wallet-sieve's chain-tail
# spool — and this re-walk has no `--tail-db`, so it legitimately stops at the
# immutable ceiling. Counting the tail would make every policy report as having
# LOST rows: the rehearsal was "4 fewer", which was exactly `tip-0521`.
#
# Nothing is lost by that. The tail carries no state between refreshes and is
# re-derived whole every time, so the daemon rebuilds it after the swap.
rows_of() {
  python3 - "$1" <<'PY'
import json, sys
try:
    m = json.load(open(sys.argv[1]))
    r = (m.get("rollup") or {}).get("rows", 0)
    r += sum((p.get("movements") or {}).get("rows", 0)
             for p in m.get("passes", [])
             if not p.get("rolled_up") and p.get("kind") == "immutable")
    print(r)
except Exception:
    print(0)
PY
}

targets=()
if [ $# -gt 0 ]; then
  targets=("$1")
else
  for d in "$OLD"/*/; do
    p=$(basename "$d")
    [ -f "$OLD/$p/manifest.json" ] && targets+=("$p")
  done
fi

# One policy at a time measured 1,361 s for 387,820 rows, which extrapolates to
# ~8 hours over the 8.04M rows on the box. A walk is single-threaded and
# CPU-bound at ~0.8 of a core, and the scheduler this box already runs uses four
# walk workers by design — so four is the concurrency the machine is built for,
# not a number invented here. The archives are all `complete`, so the daemon's
# own walk workers have nothing queued and are not competing.
JOBS=${JOBS:-4}

walk_one() {
  local p=$1
  local floor old_rows new_rows obs_kb class complete status s e
  floor=$(floor_of "$OLD/$p/manifest.json")
  old_rows=$(rows_of "$OLD/$p/manifest.json")
  rm -rf "${NEW:?}/$p"

  local args=(reverse --archive-dir "$NEW" --token "$p" --data-dir "$DATA")
  if [ -n "$floor" ]; then
    args+=(--to-slot "$floor" --first-mint "$floor")
  fi

  s=$(date +%s)
  if ! timeout 14400 "$BIN" "${args[@]}" >/dev/null 2>&1; then
    printf '%-14s %8s %9s %9s %8s %-11s %s\n' \
      "${p:0:12}" "$(( $(date +%s) - s ))" "$old_rows" - - - "WALK FAILED"
    return
  fi
  e=$(date +%s)

  new_rows=$(rows_of "$NEW/$p/manifest.json")
  obs_kb=$(du -ck "$NEW"/"$p"/pass-*/observations.parquet 2>/dev/null | tail -1 | cut -f1)
  read -r class complete <<<"$(python3 - "$NEW/$p/manifest.json" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
pr = m.get("profile") or {}
f, s = pr.get("fungible_units", 0), pr.get("single_units", 0)
print(("mixed" if f and s else "fungible" if f else "collection" if s else "unknown"),
      m.get("completeness"))
PY
)"

  status="ok"
  [ "${new_rows:-0}" -lt "${old_rows:-0}" ] && status="⚠ FEWER ROWS THAN BEFORE"
  [ "$complete" != "complete" ] && status="⚠ $complete"

  printf '%-14s %8s %9s %9s %8s %-11s %s\n' \
    "${p:0:12}" "$((e-s))" "$old_rows" "${new_rows:-0}" "${obs_kb:-0}" "$class" "$status"
}

printf '%-14s %8s %9s %9s %8s %-11s %s\n' POLICY SECS ROWS_OLD ROWS_NEW OBS_KB CLASS STATUS
for p in "${targets[@]}"; do
  # Bounded fan-out. `wait -n` returns as soon as ANY job finishes, so a slow
  # policy never idles the other three.
  while [ "$(jobs -rp | wc -l)" -ge "$JOBS" ]; do wait -n; done
  walk_one "$p" &
done
wait
exit 0
