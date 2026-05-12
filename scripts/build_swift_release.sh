#!/usr/bin/env bash
#
# Build the OxiDB embedded XCFramework with universal slices.
#
# Output: release/OxiDBEmbedded.xcframework.zip plus its SwiftPM checksum.
#
# Slices (3):
#   - macos-universal  (arm64 + x86_64 via lipo)
#   - ios-arm64        (device)
#   - ios-simulator    (arm64 + x86_64 via lipo)
#
# Why universal slices: SwiftPM/Xcode pick a slice per-platform, not
# per-architecture, so an Intel iOS simulator running on an Intel Mac
# needs the x86_64 simulator architecture inside the *simulator* slice
# — it can't fall back to the device arm64 slice. Same story for
# Intel Macs and the macOS slice.
#
# Deployment targets are set explicitly so the linker doesn't warn
# "object file built for newer macOS than being linked":
#   macOS 13.0   (matches Package.swift `.macOS(.v13)`)
#   iOS   16.0   (matches Package.swift `.iOS(.v16)`; overridable via env)

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RELEASE_DIR="$ROOT_DIR/release"
HEADERS_DIR="$ROOT_DIR/oxidb-embedded-ffi/include"

MAC_TARGET_DIR="$ROOT_DIR/.build-rust-macos"
IOS_TARGET_DIR="$ROOT_DIR/.build-rust-ios"
IOS_SIM_TARGET_DIR="$ROOT_DIR/.build-rust-ios-sim"

IOS_DEPLOY="${IPHONEOS_DEPLOYMENT_TARGET:-16.0}"
MAC_DEPLOY="${MACOSX_DEPLOYMENT_TARGET:-13.0}"

LIB_NAME="liboxidb_embedded_ffi.a"

mkdir -p "$RELEASE_DIR"

# ---------------------------------------------------------------------------
# 1) macOS — build both architectures, lipo into a universal .a
# ---------------------------------------------------------------------------

echo "▸ macOS arm64 …"
MACOSX_DEPLOYMENT_TARGET="$MAC_DEPLOY" \
CARGO_TARGET_DIR="$MAC_TARGET_DIR" \
  cargo build --release -p oxidb-embedded-ffi --target aarch64-apple-darwin

echo "▸ macOS x86_64 …"
MACOSX_DEPLOYMENT_TARGET="$MAC_DEPLOY" \
CARGO_TARGET_DIR="$MAC_TARGET_DIR" \
  cargo build --release -p oxidb-embedded-ffi --target x86_64-apple-darwin

MAC_UNI_DIR="$RELEASE_DIR/.mac-universal"
mkdir -p "$MAC_UNI_DIR"
echo "▸ lipo: macOS universal (arm64 + x86_64)"
lipo -create \
  "$MAC_TARGET_DIR/aarch64-apple-darwin/release/$LIB_NAME" \
  "$MAC_TARGET_DIR/x86_64-apple-darwin/release/$LIB_NAME" \
  -output "$MAC_UNI_DIR/$LIB_NAME"

# ---------------------------------------------------------------------------
# 2) iOS device — arm64 only (no x86_64 device target exists)
# ---------------------------------------------------------------------------

echo "▸ iOS device arm64 …"
SDKROOT="$(xcrun --sdk iphoneos --show-sdk-path)" \
IPHONEOS_DEPLOYMENT_TARGET="$IOS_DEPLOY" \
CARGO_TARGET_DIR="$IOS_TARGET_DIR" \
  cargo build --release -p oxidb-embedded-ffi --target aarch64-apple-ios

# ---------------------------------------------------------------------------
# 3) iOS simulator — build both architectures, lipo into a universal .a
# ---------------------------------------------------------------------------

echo "▸ iOS simulator arm64 …"
SDKROOT="$(xcrun --sdk iphonesimulator --show-sdk-path)" \
IPHONEOS_DEPLOYMENT_TARGET="$IOS_DEPLOY" \
CARGO_TARGET_DIR="$IOS_SIM_TARGET_DIR" \
  cargo build --release -p oxidb-embedded-ffi --target aarch64-apple-ios-sim

echo "▸ iOS simulator x86_64 …"
SDKROOT="$(xcrun --sdk iphonesimulator --show-sdk-path)" \
IPHONEOS_DEPLOYMENT_TARGET="$IOS_DEPLOY" \
CARGO_TARGET_DIR="$IOS_SIM_TARGET_DIR" \
  cargo build --release -p oxidb-embedded-ffi --target x86_64-apple-ios

IOS_SIM_UNI_DIR="$RELEASE_DIR/.ios-sim-universal"
mkdir -p "$IOS_SIM_UNI_DIR"
echo "▸ lipo: iOS simulator universal (arm64 + x86_64)"
lipo -create \
  "$IOS_SIM_TARGET_DIR/aarch64-apple-ios-sim/release/$LIB_NAME" \
  "$IOS_SIM_TARGET_DIR/x86_64-apple-ios/release/$LIB_NAME" \
  -output "$IOS_SIM_UNI_DIR/$LIB_NAME"

# ---------------------------------------------------------------------------
# 4) Assemble the XCFramework
# ---------------------------------------------------------------------------

rm -rf "$RELEASE_DIR/OxiDBEmbedded.xcframework"
rm -f "$RELEASE_DIR/OxiDBEmbedded.xcframework.zip"

echo "▸ xcodebuild -create-xcframework …"
xcodebuild -create-xcframework \
  -library "$IOS_TARGET_DIR/aarch64-apple-ios/release/$LIB_NAME" \
  -headers "$HEADERS_DIR" \
  -library "$IOS_SIM_UNI_DIR/$LIB_NAME" \
  -headers "$HEADERS_DIR" \
  -library "$MAC_UNI_DIR/$LIB_NAME" \
  -headers "$HEADERS_DIR" \
  -output "$RELEASE_DIR/OxiDBEmbedded.xcframework"

# Clean up intermediates — only the final .xcframework + zip should remain.
rm -rf "$MAC_UNI_DIR" "$IOS_SIM_UNI_DIR"

echo "▸ Zipping XCFramework …"
ditto -c -k --sequesterRsrc --keepParent \
  "$RELEASE_DIR/OxiDBEmbedded.xcframework" \
  "$RELEASE_DIR/OxiDBEmbedded.xcframework.zip"

echo
echo "SwiftPM checksum:"
swift package compute-checksum "$RELEASE_DIR/OxiDBEmbedded.xcframework.zip"
