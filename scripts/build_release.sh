#!/usr/bin/env bash
# Cross-compile and package a v0.25.3 release for all 5 platforms.
#
# Output: releases/v$VERSION/
#   <binary>-v$VERSION-<friendly-target>.{tar.gz,zip}
#   <binary>-<linux-x86_64|linux-arm64|macos-x86_64|macos-arm64|windows-x86_64>.{tar.gz,zip}  (versionless aliases)
#   oxidb-wasm-v$VERSION.tar.gz + oxidb-wasm.tar.gz
#   SHA256SUMS.txt

set -euo pipefail

VERSION="${VERSION:-0.25.3}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.."; pwd)"
TARGET_DIR="${ROOT}/target-local"
OUT_DIR="${ROOT}/releases/v${VERSION}"
mkdir -p "$OUT_DIR"

echo "═══════════════════════════════════════════════════════════════════════"
echo "  Building OxiDB v${VERSION}"
echo "  out: ${OUT_DIR}"
echo "═══════════════════════════════════════════════════════════════════════"

cd "$ROOT"

# ─── targets ────────────────────────────────────────────────────────
declare -a TARGETS=(
  "aarch64-apple-darwin       darwin-arm64    macos-arm64"
  "x86_64-apple-darwin        darwin-amd64    macos-x86_64"
  "x86_64-unknown-linux-musl  linux-amd64     linux-x86_64"
  "aarch64-unknown-linux-musl linux-arm64     linux-arm64"
  "x86_64-pc-windows-gnu      windows-amd64   windows-x86_64"
)

# Each binary spec: <package>:<binary-name>:<feature-flags>
declare -a BINS=(
  "oxidb-server:oxidb-server:--features cluster"
  "oxidb-cli:oxidb:"
  "oxipool:oxipool:"
)

# ─── 1. cross-compile ───────────────────────────────────────────────
for spec in "${TARGETS[@]}"; do
  read -r RTGT FTGT _ <<<"$spec"
  echo ""
  echo "── target: $RTGT ($FTGT) ─────────────────────"
  for bin in "${BINS[@]}"; do
    IFS=':' read -r PKG BIN FEATS <<<"$bin"
    echo "  ▸ ${PKG}"
    cargo build --release --target "$RTGT" --package "$PKG" $FEATS 2>&1 | grep -E "error|warning: unused|Compiling ${PKG} |Finished" | tail -5
  done
done

# ─── 2. WASM ────────────────────────────────────────────────────────
echo ""
echo "── WASM (wasm-pack) ──────────────────────────"
cd "${ROOT}/oxidb-wasm"
wasm-pack build --target web --release --out-dir pkg 2>&1 | tail -3
cd "$ROOT"

# ─── 3. package ─────────────────────────────────────────────────────
echo ""
echo "── packaging ──────────────────────────────────"
cd "$OUT_DIR"
rm -f ./*.tar.gz ./*.zip ./SHA256SUMS.txt

for spec in "${TARGETS[@]}"; do
  read -r RTGT FTGT VLESS <<<"$spec"
  for bin in "${BINS[@]}"; do
    IFS=':' read -r PKG BIN _ <<<"$bin"
    SRC="${TARGET_DIR}/${RTGT}/release/${BIN}"
    if [[ "$RTGT" == *"windows"* ]]; then
      SRC="${SRC}.exe"
      ARCHIVE="${BIN}-v${VERSION}-${FTGT}.zip"
      ( cd "$(dirname "$SRC")" && zip -j -q "${OUT_DIR}/${ARCHIVE}" "${BIN}.exe" )
    else
      ARCHIVE="${BIN}-v${VERSION}-${FTGT}.tar.gz"
      tar czf "${OUT_DIR}/${ARCHIVE}" -C "$(dirname "$SRC")" "$(basename "$SRC")"
    fi
    printf "  %-50s %8d bytes\n" "$ARCHIVE" "$(stat -f '%z' "${OUT_DIR}/${ARCHIVE}")"
  done
done

# ─── 4. WASM archives ───────────────────────────────────────────────
echo ""
echo "── WASM archives ──────────────────────────────"
WASM_PKG="${ROOT}/oxidb-wasm/pkg"
WASM_ARCHIVE="oxidb-wasm-v${VERSION}.tar.gz"
tar czf "${OUT_DIR}/${WASM_ARCHIVE}" \
  -C "${WASM_PKG}" \
  oxidb_wasm_bg.wasm oxidb_wasm.js oxidb_wasm.d.ts oxidb_wasm_bg.wasm.d.ts package.json
cp "${OUT_DIR}/${WASM_ARCHIVE}" "${OUT_DIR}/oxidb-wasm.tar.gz"
printf "  %-50s %8d bytes\n" "$WASM_ARCHIVE" "$(stat -f '%z' "${OUT_DIR}/${WASM_ARCHIVE}")"

# ─── 5. versionless aliases (for README "latest" links) ─────────────
echo ""
echo "── versionless aliases ────────────────────────"
for spec in "${TARGETS[@]}"; do
  read -r RTGT FTGT VLESS <<<"$spec"
  for bin in "${BINS[@]}"; do
    IFS=':' read -r PKG BIN _ <<<"$bin"
    if [[ "$RTGT" == *"windows"* ]]; then
      SRC="${BIN}-v${VERSION}-${FTGT}.zip"
      DST="${BIN}-${VLESS}.zip"
    else
      SRC="${BIN}-v${VERSION}-${FTGT}.tar.gz"
      DST="${BIN}-${VLESS}.tar.gz"
    fi
    cp "${OUT_DIR}/${SRC}" "${OUT_DIR}/${DST}"
    printf "  %-44s → %s\n" "$SRC" "$DST"
  done
done

# ─── 6. SHA256SUMS ──────────────────────────────────────────────────
echo ""
echo "── SHA256SUMS ─────────────────────────────────"
cd "$OUT_DIR"
shasum -a 256 ./*.tar.gz ./*.zip > SHA256SUMS.txt
wc -l SHA256SUMS.txt

echo ""
echo "═══════════════════════════════════════════════════════════════════════"
echo "  done — ${OUT_DIR}"
echo "  next:  gh release create v${VERSION} ${OUT_DIR}/*"
echo "═══════════════════════════════════════════════════════════════════════"
