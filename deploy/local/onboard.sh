#!/usr/bin/env bash
# Wake the mint-confirmation companion so its runtime subscribes to the host
# and announces the module. Run once after up.sh prints that the stack is up.
#
#   ./onboard.sh
#   ./onboard.sh <policy-id>
#
# The default policy is the Perp5158 policy from the cip-25-mint golden fixture
# (community-modules/cip-25-mint/tests/fixtures/perp-policy). up.sh seeds a
# `collections` row for it with a NULL deposit address, which is enough for the
# policy interest and skips the credit-address watch. Pass the same `POLICY=`
# value here that up.sh was run with — the dial-back pins it.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
set -a
. "${DIR}/.env"
set +a

POLICY="${1:-${POLICY:-e6ba9c0ff27be029442c32533c6efd956a60d15ecb976acbb64c4de0}}"

curl -sS -X POST \
    -H "X-Debug-Token: ${MITOS_AUTH_TOKEN}" \
    "http://127.0.0.1:18787/collections/${POLICY}/companion/onboard"
printf '\n'
