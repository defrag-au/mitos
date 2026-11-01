#!/usr/bin/env bash
# install-release.sh — verify a packed tarball and install it.
#
# Usage:
#   install-release.sh <tarball> [--root <dir>] [--prefix /opt/mitos]
#
# Layout:
#   <prefix>/releases/<version>/bin/mitos
#   <prefix>/current -> releases/<version>     (atomic rename of a symlink)
#
# ExecStart compatibility (PLAN-I: do not edit unit files):
#   <prefix>/src/target/release/mitos -> ../current/bin/mitos
#   /usr/local/bin/mitos              -> <prefix>/current/bin/mitos
#   (the /usr/local/bin link is skipped when --root is set)
#
# Needs coreutils + tar. minisign is optional: if SHA256SUMS.minisig exists
# beside the tarball, minisign -V is required.

set -euo pipefail

TARBALL=""
ROOT=""
PREFIX="/opt/mitos"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --root) ROOT="${2:?}"; shift 2 ;;
        --prefix) PREFIX="${2:?}"; shift 2 ;;
        -h|--help)
            sed -n '1,20p' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *)
            if [[ -z "$TARBALL" ]]; then
                TARBALL="$1"
                shift
            else
                echo "unexpected argument: $1" >&2
                exit 2
            fi
            ;;
    esac
done

: "${TARBALL:?usage: install-release.sh <tarball> [--root DIR] [--prefix /opt/mitos]}"

if [[ ! -f "$TARBALL" ]]; then
    echo "no tarball at $TARBALL" >&2
    exit 1
fi

rooted() {
    local p="$1"
    if [[ -z "$ROOT" ]]; then
        printf '%s\n' "$p"
    else
        printf '%s/%s\n' "$ROOT" "${p#/}"
    fi
}

DIR="$(cd "$(dirname "$TARBALL")" && pwd)"
BASE="$(basename "$TARBALL")"
SUMS="$DIR/SHA256SUMS"

if [[ -f "$SUMS" ]]; then
    (cd "$DIR" && sha256sum -c SHA256SUMS --ignore-missing 2>/dev/null) \
        || (cd "$DIR" && shasum -a 256 -c SHA256SUMS)
else
    echo "WARNING: no SHA256SUMS beside $BASE — installing unverified" >&2
fi

if [[ -f "$SUMS.minisig" ]]; then
    if ! command -v minisign >/dev/null 2>&1; then
        echo "SHA256SUMS.minisig present but minisign is not installed" >&2
        exit 1
    fi
    PUB="${MINISIGN_PUB:-$(dirname "$0")/../release/minisign.pub}"
    minisign -Vm "$SUMS" -p "$PUB"
fi

VERSION="$(basename "$BASE" .tar.gz)"
VERSION="${VERSION#mitos-}"

PREFIX_R="$(rooted "$PREFIX")"
REL_DIR="$PREFIX_R/releases/$VERSION"
TMP="$PREFIX_R/releases/.incoming-$$"
CURRENT="$PREFIX_R/current"
CURRENT_TMP="$PREFIX_R/current.new"

rm -rf "$TMP"
mkdir -p "$TMP" "$PREFIX_R/releases"
tar -C "$TMP" -xzf "$TARBALL"
# tarball contains mitos-<ver>-<triple>/bin/mitos
INNER="$(find "$TMP" -mindepth 1 -maxdepth 1 -type d | head -1)"
if [[ -z "$INNER" || ! -x "$INNER/bin/mitos" ]]; then
    echo "tarball did not contain bin/mitos" >&2
    rm -rf "$TMP"
    exit 1
fi

rm -rf "$REL_DIR"
mv "$INNER" "$REL_DIR"
rm -rf "$TMP"

ln -sfn "releases/$VERSION" "$CURRENT_TMP"
mv -f "$CURRENT_TMP" "$CURRENT"

# Old unit ExecStart=/opt/mitos/src/target/release/mitos
UNIT_BIN="$(rooted /opt/mitos/src/target/release/mitos)"
mkdir -p "$(dirname "$UNIT_BIN")"
ln -sfn "$CURRENT/bin/mitos" "$UNIT_BIN"

if [[ -z "$ROOT" ]]; then
    mkdir -p /usr/local/bin
    ln -sfn "$CURRENT/bin/mitos" /usr/local/bin/mitos
fi

echo "installed $VERSION"
echo "  current: $CURRENT -> releases/$VERSION"
echo "  binary:  $CURRENT/bin/mitos"
