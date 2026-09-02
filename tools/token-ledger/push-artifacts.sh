#!/usr/bin/env bash
#
# Push a token's exported artifacts to R2.
#
# Usage:  push-artifacts.sh <token> [export-dir]
#
# ## The key is the token's IDENTITY, not our name for it
#
#     token-ledger/<policy_hex>.<asset_name_hex>/<slot>/spine.bin.gz
#
# `wrt` is a nickname from our own tokens.toml — a DISPLAY label. A consumer
# arriving with a token in hand has its policy id and asset name from chain, and
# keying on the nickname would force them to first discover what we happened to
# call it. The on-chain unit needs no lookup.
#
# `<policy>.<name>` rather than `<policy>/<name>` for two reasons: it is the
# same unit form `chain-ledger::tokens` already keys on, and an asset name may
# legally be EMPTY — which nests to a double slash that CDNs are entitled to
# normalise away, but flattens to a harmless trailing dot. Prefix-listing on
# `token-ledger/<policy>` still finds every asset under a policy either way.
#
# Both the unit and the slot come from the ledger's own `meta`/`cursor` rows
# rather than from arguments, so a key cannot disagree with the data it names.
#
# ## Why the key carries the slot
#
# Artifacts are derived from a walk that ended at a known slot, so keying them
# by that slot makes every object IMMUTABLE. Immutable objects get a one-year
# `immutable` cache header, which means the edge serves them without ever
# revalidating and R2 is hit once per PoP rather than once per visitor.
#
# A new export writes a fresh prefix and only then flips `latest`. Cutover is
# therefore atomic: a client mid-scrub keeps reading the slot it pinned instead
# of having files change underneath it, which is what would happen if we
# overwrote a stable path.
#
# ## Why the bodies are gzipped here
#
# Stored pre-compressed with `Content-Encoding: gzip`, so browsers decompress
# transparently and we ship the gzipped size (spine: 277 KB rather than 330 KB).
# Files stay RAW on disk — `inspect` reads them directly and must not be made to
# care about transport encoding.
#
# ## Credentials
#
# From /etc/default/token-ledger (mode 0600, not in any repo). rclone is
# configured entirely through RCLONE_CONFIG_* environment variables so this
# script leaves nothing on disk to leak or drift.

set -euo pipefail

TOKEN="${1:?usage: push-artifacts.sh <token> [export-dir]}"
EXPORT_DIR="${2:-/opt/token-ledger/export}"
ENV_FILE="${TOKEN_LEDGER_ENV:-/etc/default/token-ledger}"

[[ -r "$ENV_FILE" ]] || { echo "no credentials at $ENV_FILE" >&2; exit 1; }
# shellcheck disable=SC1090
set -a; source "$ENV_FILE"; set +a

: "${R2_ENDPOINT:?}" "${R2_ACCESS_KEY_ID:?}" "${R2_SECRET_ACCESS_KEY:?}" "${R2_BUCKET:?}"

DB="${TOKEN_LEDGER_DB:-/opt/token-ledger/${TOKEN}.db}"
[[ -r "$DB" ]] || { echo "no ledger at $DB" >&2; exit 1; }

# The walk's high-water slot IS the version. Taken from the ledger rather than
# passed in, so the key can never disagree with the data it names.
SLOT="$(sqlite3 "$DB" "SELECT slot FROM cursor WHERE k = 'walk';")"
[[ -n "$SLOT" ]] || { echo "ledger $DB has no walk cursor — run \`walk\` first" >&2; exit 1; }

# The on-chain unit, likewise from the ledger's own record of what it holds.
# `lower(hex(...))` because sqlite's hex() shouts and every other surface here
# — tokens.toml, chain-ledger, Koios — uses lowercase hex.
UNIT="$(sqlite3 "$DB" \
    "SELECT lower(hex(policy)) || '.' || lower(hex(asset_name)) FROM meta WHERE k = 'asset';")"
[[ "$UNIT" == *.* ]] || {
    echo "ledger $DB has no asset meta — re-run \`walk\` to stamp it" >&2; exit 1; }

export RCLONE_CONFIG_R2_TYPE=s3
export RCLONE_CONFIG_R2_PROVIDER=Cloudflare
export RCLONE_CONFIG_R2_ENDPOINT="$R2_ENDPOINT"
export RCLONE_CONFIG_R2_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
export RCLONE_CONFIG_R2_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"
export RCLONE_CONFIG_R2_NO_CHECK_BUCKET=true
export RCLONE_CONFIG_R2_ACL=
# **R2 does not implement version-qualified HEAD, and rclone verifies with one.**
#
# Traced rather than guessed, because the symptom is misleading: every object
# logged `NotImplemented: 501 ... Attempt 1/5 failed ... Attempt 2/5 succeeded`,
# which reads like a failed upload that had to be repeated. It is not. The
# sequence is:
#
#   PUT  /<key>                 -> 200 OK, R2 returns X-Amz-Version-Id
#   HEAD /<key>?versionId=...   -> 501 Not Implemented
#
# The upload SUCCEEDED on the first attempt; only rclone's integrity re-check
# failed, and the retry merely HEADs without the version id, finds the object
# already correct, and skips. So this cost noise, not bandwidth.
#
# `no_head` drops that re-check. We keep a stronger guarantee anyway: `export`
# already decodes every artifact back from disk before this script sees it, and
# a truncated body fails the consumer's `validate()` loudly rather than
# rendering wrong.
export RCLONE_CONFIG_R2_NO_HEAD=true

PREFIX="R2:${R2_BUCKET}/${UNIT}/${SLOT}"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

shopt -s nullglob
FOUND=0
for src in "$EXPORT_DIR/$TOKEN".*.bin; do
    FOUND=1
    # Drop the `<token>.` prefix the export writes: the path already names the
    # token, so `wrt/…/wrt.spine.bin` said it twice. Uniform artifact names also
    # mean a reader builds `<base>/<unit>/<slot>/spine.bin.gz` without knowing
    # anything token-specific.
    name="$(basename "$src")"; name="${name#"$TOKEN."}"
    raw=$(stat -c%s "$src")

    # Compress only where it pays. `txids` is a wall of 32-byte hashes and is
    # incompressible BY CONSTRUCTION — gzipping it produced a file 6 KB LARGER
    # than the input, so we would spend CPU to serve more bytes. Decided per
    # file from the measured result rather than by rule, since which artifacts
    # compress depends on the token.
    gzip -9 -c "$src" > "$STAGE/${name}.gz"
    gz=$(stat -c%s "$STAGE/${name}.gz")
    # An absent `Content-Encoding` IS the identity encoding, so the raw case
    # sets no header at all. rclone only accepts headers S3 actually defines
    # and rejects anything invented, which is the right behaviour — the `.gz`
    # suffix on the key already tells a reader which case it is looking at.
    hdrs=(--header-upload "Content-Type: application/octet-stream"
          --header-upload "Cache-Control: public, max-age=31536000, immutable")
    if (( gz < raw )); then
        body="$STAGE/${name}.gz"; key="${name}.gz"
        hdrs+=(--header-upload "Content-Encoding: gzip")
        printf '  %-28s %8d KB -> %6d KB gz\n' "$name" $((raw / 1024)) $((gz / 1024))
    else
        body="$src"; key="$name"
        printf '  %-28s %8d KB    stored raw (gzip made it bigger)\n' "$name" $((raw / 1024))
    fi

    rclone copyto "$body" "${PREFIX}/${key}" "${hdrs[@]}" \
        --s3-chunk-size 16M --transfers 4 --retries 5 -q
done
[[ "$FOUND" == 1 ]] || { echo "no artifacts matching $EXPORT_DIR/$TOKEN.*.bin" >&2; exit 1; }

# The pointer goes last, and only if every artifact above landed. Written first
# it would advertise a version that does not fully exist yet — a client would
# resolve `latest`, fetch a prefix mid-upload, and get a short file rather than
# an error. `set -e` plus this ordering is the whole atomicity story.
#
# Short TTL because this one object is the only mutable thing in the layout;
# everything it points at is cached forever.
printf '%s\n' "$SLOT" > "$STAGE/latest"
rclone copyto "$STAGE/latest" "R2:${R2_BUCKET}/${UNIT}/latest" \
    --header-upload "Content-Type: text/plain" \
    --header-upload "Cache-Control: public, max-age=60" \
    --retries 5 -q

# The catalogue card, LAST of all.
#
# R2 has no queryable index and a public bucket cannot be listed, so a
# listing surface needs one small object per token it can fetch by key.
# This is that object — and it sits at `<unit>/card.json`, NOT under the
# slot, because a catalogue must not have to resolve `latest` before it can
# show a row.
#
# Written after `latest` for the same reason `latest` is written after the
# artifacts: the card names a slot, and advertising a slot whose files are
# still uploading is exactly the failure the ordering exists to prevent.
CARD="$EXPORT_DIR/$TOKEN.card.json"
if [[ -r "$CARD" ]]; then
    rclone copyto "$CARD" "R2:${R2_BUCKET}/${UNIT}/card.json" \
        --header-upload "Content-Type: application/json" \
        --header-upload "Cache-Control: public, max-age=60" \
        --retries 5 -q
    printf '  %-28s catalogue card\n' "card.json"
else
    # Not fatal: an older export predates the card and its artifacts are
    # still perfectly usable by anything that knows the unit.
    echo "  no card.json — token will not appear in the catalogue" >&2
fi

echo "pushed ${TOKEN} (${UNIT}) @ slot ${SLOT}"
