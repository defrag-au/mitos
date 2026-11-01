#!/usr/bin/env bash
# publish-release.sh — copy dist/ onto R2 when credentials exist.
#
# Env (same names as token-ledger): R2_ENDPOINT, R2_ACCESS_KEY_ID,
# R2_SECRET_ACCESS_KEY, R2_BUCKET. Optional R2_PREFIX (default mitos-releases).
#
# Absent credentials: print SKIPPED and exit 0. That is a complete local
# result; GitHub Releases / a scp of the tarball still install.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST="${DIST:-$ROOT/dist}"
PREFIX="${R2_PREFIX:-mitos-releases}"

if [[ -z "${R2_ENDPOINT:-}" || -z "${R2_ACCESS_KEY_ID:-}" || -z "${R2_SECRET_ACCESS_KEY:-}" || -z "${R2_BUCKET:-}" ]]; then
    echo "SKIPPED: R2 credentials not set (need R2_ENDPOINT R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_BUCKET)"
    echo "         install locally with: scripts/install-release.sh dist/mitos-*.tar.gz"
    exit 0
fi

if ! command -v rclone >/dev/null 2>&1; then
    echo "rclone is required to publish to R2" >&2
    exit 1
fi

export RCLONE_CONFIG_R2_TYPE=s3
export RCLONE_CONFIG_R2_PROVIDER=Cloudflare
export RCLONE_CONFIG_R2_ENDPOINT="$R2_ENDPOINT"
export RCLONE_CONFIG_R2_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
export RCLONE_CONFIG_R2_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"
export RCLONE_CONFIG_R2_NO_CHECK_BUCKET=true
export RCLONE_CONFIG_R2_ACL=

shopt -s nullglob
files=("$DIST"/mitos-*.tar.gz "$DIST"/SHA256SUMS)
if [[ ${#files[@]} -eq 0 ]]; then
    echo "nothing in $DIST to publish" >&2
    exit 1
fi

for f in "${files[@]}"; do
    [[ -f "$f" ]] || continue
    name="$(basename "$f")"
    echo "put $name -> R2:$R2_BUCKET/$PREFIX/$name"
    rclone copyto "$f" "R2:${R2_BUCKET}/${PREFIX}/${name}" \
        --s3-no-check-bucket --s3-disable-http2
done
