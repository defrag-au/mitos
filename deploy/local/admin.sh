#!/usr/bin/env bash
# Query the local mitos host's admin API without putting the token on a
# command line or in the transcript.
#
#   ./admin.sh                          # /_admin/status
#   ./admin.sh /_admin/modules/cip-25-mint/rows
#   ./admin.sh /_admin/modules/cip-25-mint/instances
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
set -a
. "${DIR}/.env"
set +a

curl -sS -H "Authorization: Bearer ${MITOS_AUTH_TOKEN}" \
    "http://127.0.0.1:8080${1:-/_admin/status}"
printf '\n'
