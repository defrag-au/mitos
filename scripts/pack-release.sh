#!/usr/bin/env bash
# pack-release.sh — cross-compile a host tarball the way CI will.
#
# Default target is aarch64-unknown-linux-musl: that is the std sitting in
# rust-worker-stack, and cargo-zigbuild is how we already ship ARM Linux
# binaries (shiku, ipfs-relay). PLAN-I's gnu/glibc-2.35 tarball is the
# GitHub Actions shape; this script is the laptop stand-in.
#
# Usage:
#   nix develop -c ./scripts/pack-release.sh
#   TARGET=aarch64-unknown-linux-gnu.2.35 ./scripts/pack-release.sh
#
# Writes dist/mitos-<version>-<target>.tar.gz and dist/SHA256SUMS.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TARGET="${TARGET:-aarch64-unknown-linux-musl}"
PROFILE="${PROFILE:-release}"
VERSION="${VERSION:-$(git describe --always --dirty --abbrev=12 2>/dev/null || echo unknown)}"
# Target dir uses the rustc triple (no glibc suffix).
TRIPLE="${TARGET%%.*}"
OUT_DIR="${OUT_DIR:-$ROOT/dist}"
STAGE="$OUT_DIR/mitos-${VERSION}-${TRIPLE}"

log() { printf "▸ %s\n" "$*" >&2; }

# rustc always passes -Wl,--fix-cortex-a53-843419 on aarch64-linux; zig 0.16
# rejects it. cargo-zigbuild 0.22.3's zigcc wrapper does not strip it.
# Override only the rustc LINKER (not CC) so C crates still use zigbuild's
# wrapper and we do not double-link musl crt1.o.
if [[ "$TRIPLE" == "aarch64-unknown-linux-musl" ]]; then
    export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER="$ROOT/scripts/zigcc-link-aarch64-linux-musl"
fi
log "zigbuild -p mitos --profile $PROFILE --target $TARGET (version $VERSION)"
cargo zigbuild --profile "$PROFILE" --target "$TARGET" -p mitos

BIN="$ROOT/target/${TRIPLE}/${PROFILE}/mitos"
if [[ ! -x "$BIN" ]]; then
    echo "expected binary at $BIN" >&2
    exit 1
fi

if command -v llvm-strip >/dev/null 2>&1; then
    llvm-strip "$BIN" || true
elif command -v strip >/dev/null 2>&1; then
    strip "$BIN" || true
fi

rm -rf "$STAGE"
mkdir -p "$STAGE/bin"
cp "$BIN" "$STAGE/bin/mitos"
chmod 0755 "$STAGE/bin/mitos"
"$STAGE/bin/mitos" --version >/dev/null 2>&1 || true

TARBALL="$OUT_DIR/mitos-${VERSION}-${TRIPLE}.tar.gz"
mkdir -p "$OUT_DIR"
tar -C "$OUT_DIR" -czf "$TARBALL" "$(basename "$STAGE")"
(
    cd "$OUT_DIR"
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$(basename "$TARBALL")" >SHA256SUMS
    else
        shasum -a 256 "$(basename "$TARBALL")" >SHA256SUMS
    fi
)

log "wrote $TARBALL"
cat "$OUT_DIR/SHA256SUMS" >&2
