#!/usr/bin/env bash
# Push one policy's archive to R2 — the same rclone-over-env-vars pattern
# `push-artifacts.sh` uses for the token artifacts, pointed at the POLICY
# archive a reverse pass writes (`<archive-dir>/<policy_hex>/`).
#
#   push-archive.sh <policy_hex> [archive-dir]
#
# Order matters and is the whole point of this being a script rather than an
# `rclone sync`:
#
#   1. the Parquet files and pending sidecar are COPIED (never deleted here),
#      with a year-long immutable cache header — they never change once
#      written, and a reader that has one cached keeps it;
#   2. the manifest is copied LAST, uncached, so a reader never sees a
#      manifest naming an object that is not there yet;
#   3. only then are objects the manifest no longer names PRUNED — a rollup's
#      predecessors — so a reader mid-flight on the previous manifest still
#      finds its files.
#
# Keys are `policy-archive/<policy_hex>/<relative path>`, the same relative
# paths the manifest carries, so a reader joins prefix + manifest entry and
# nothing else.
set -euo pipefail

POLICY="${1:?usage: push-archive.sh <policy_hex> [archive-dir]}"
ARCHIVE_DIR="${2:-/opt/token-ledger/archive}"
ENV_FILE="${TOKEN_LEDGER_ENV:-/etc/default/token-ledger}"
[[ -r "$ENV_FILE" ]] || { echo "no credentials at $ENV_FILE" >&2; exit 1; }
set -a; source "$ENV_FILE"; set +a
: "${R2_ENDPOINT:?}" "${R2_ACCESS_KEY_ID:?}" "${R2_SECRET_ACCESS_KEY:?}" "${R2_BUCKET:?}"

DIR="$ARCHIVE_DIR/$POLICY"
[[ -r "$DIR/manifest.json" ]] || { echo "no archive at $DIR" >&2; exit 1; }

export RCLONE_CONFIG_R2_TYPE=s3
export RCLONE_CONFIG_R2_PROVIDER=Cloudflare
export RCLONE_CONFIG_R2_ENDPOINT="$R2_ENDPOINT"
export RCLONE_CONFIG_R2_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
export RCLONE_CONFIG_R2_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"
export RCLONE_CONFIG_R2_NO_CHECK_BUCKET=true
export RCLONE_CONFIG_R2_ACL=
# R2 answers a version-qualified HEAD with 501; the upload itself succeeded.
export RCLONE_CONFIG_R2_NO_HEAD=true

PREFIX="R2:${R2_BUCKET}/policy-archive/${POLICY}"

# 1. Everything but the manifest: immutable, cached for a year. The bundle
#    is KV's, not R2's.
rclone copy "$DIR" "$PREFIX" \
    --exclude 'manifest.json' --exclude 'bundle.bin' --exclude '*.tmp' \
    --header-upload "Cache-Control: public, max-age=31536000, immutable" \
    --transfers 8 --quiet

# 2. The manifest, last, never cached.
rclone copyto "$DIR/manifest.json" "$PREFIX/manifest.json" \
    --header-upload "Content-Type: application/json" \
    --header-upload "Cache-Control: no-cache" \
    --quiet

# 3. The bundle — manifest plus every footer — to KV, one entry per policy
#    per namespace (dev and prod read the same bucket, so they get the same
#    bundle). AFTER the files it names are in R2, BEFORE the prune. Optional:
#    without credentials the archive is still published, and a Worker opens
#    it the slow way.
#    The account is the one the R2 credentials already name; the token is a
#    separate API token scoped to Workers KV Storage: Edit.
CF_ACCOUNT_ID="${CF_ACCOUNT_ID:-${R2_ACCOUNT_ID:-}}"
if [[ -n "${CF_ACCOUNT_ID:-}" && -n "${CF_KV_TOKEN:-}" && -n "${KV_NAMESPACE_IDS:-}" && -r "$DIR/bundle.bin" ]]; then
    UPDATED=$(grep -o '"updated_unix": *[0-9]*' "$DIR/manifest.json" | grep -o '[0-9]*$' || echo 0)
    for NS in $KV_NAMESPACE_IDS; do
        curl -sS -f -o /dev/null -X PUT \
            "https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/storage/kv/namespaces/${NS}/values/${POLICY}" \
            -H "Authorization: Bearer ${CF_KV_TOKEN}" \
            -F "value=@${DIR}/bundle.bin;type=application/octet-stream" \
            -F "metadata={\"updated_unix\":${UPDATED}}" \
            || echo "bundle push to KV namespace ${NS} failed" >&2
    done
else
    echo "bundle not pushed to KV (needs CF_KV_TOKEN, KV_NAMESPACE_IDS and $DIR/bundle.bin)" >&2
fi

# 4. Prune what the manifest no longer names.
rclone sync "$DIR" "$PREFIX" --exclude 'manifest.json' --exclude 'bundle.bin' --exclude '*.tmp' \
    --quiet

echo "pushed $POLICY: $(find "$DIR" -type f | wc -l) files -> $PREFIX"
