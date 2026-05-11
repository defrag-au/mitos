#!/usr/bin/env bash
# scripts/run-golden-tests.sh — assertion-driven re-run of every
# community-module golden scenario against its checked-in
# expected.json.
#
# Convention (per scenario):
#   community-modules/<name>/tests/fixtures/<scenario>/
#     fixture.toml          interest + utxo + tx_metadata fixture
#     expected.json         expected JSON array of decoded emissions
#     <slot>.block.cbor     one or more blocks; sorted by filename
#                           (= chain order when slot-named)
#     README.md             (optional) scenario provenance
#
# Exit 0 on all-pass; non-zero if any scenario diffs or errors.
# Set `UPDATE_GOLDEN=1` to overwrite expected files with the
# actual emissions (review the diff before committing).
#
# Usage:
#   ./scripts/run-golden-tests.sh                    # assert
#   UPDATE_GOLDEN=1 ./scripts/run-golden-tests.sh    # regenerate

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MITOS_RUN="$ROOT/target/release/mitos-run"

if [[ ! -x "$MITOS_RUN" ]]; then
    echo "missing $MITOS_RUN — run 'cargo build --release --bin mitos-run' first" >&2
    exit 2
fi

declare -i PASSED=0
declare -i FAILED=0
declare -i SKIPPED=0
FAILED_SCENARIOS=()

for module_dir in "$ROOT"/community-modules/*/; do
    name=$(basename "$module_dir")
    artifact="$module_dir/target/mitos/$name"
    if [[ ! -f "$artifact/$name.wasm" ]]; then
        continue
    fi

    [[ -d "$module_dir/tests/fixtures" ]] || continue

    for scenario_dir in "$module_dir/tests/fixtures"/*/; do
        scenario=$(basename "$scenario_dir")
        fixture="$scenario_dir/fixture.toml"
        expected="$scenario_dir/expected.json"

        if [[ ! -f "$fixture" ]]; then
            echo "skip $name/$scenario: no fixture.toml"
            SKIPPED+=1
            continue
        fi

        blocks=()
        while IFS= read -r b; do blocks+=("$b"); done < <(find "$scenario_dir" -maxdepth 1 -name '*.block.cbor' | sort)
        if [[ ${#blocks[@]} -eq 0 ]]; then
            echo "skip $name/$scenario: no *.block.cbor files"
            SKIPPED+=1
            continue
        fi

        cmd=("$MITOS_RUN" --artifact "$artifact" --fixture "$fixture" --expected "$expected")
        for b in "${blocks[@]}"; do cmd+=(--block "$b"); done

        if RUST_LOG=warn "${cmd[@]}" >/tmp/golden-$name-$scenario.log 2>&1; then
            echo "✓ $name/$scenario"
            PASSED+=1
        else
            echo "✗ $name/$scenario — see /tmp/golden-$name-$scenario.log"
            FAILED+=1
            FAILED_SCENARIOS+=("$name/$scenario")
        fi
    done
done

echo "---"
echo "passed=$PASSED failed=$FAILED skipped=$SKIPPED"
if (( FAILED > 0 )); then
    echo "FAILED:" >&2
    for s in "${FAILED_SCENARIOS[@]}"; do echo "  $s" >&2; done
    exit 1
fi
exit 0
