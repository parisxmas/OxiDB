#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RELEASE_DIR="$ROOT_DIR/release"
HEADERS_DIR="$ROOT_DIR/oxidb-embedded-ffi/include"
MAC_TARGET_DIR="$ROOT_DIR/.build-rust-macos"
IOS_TARGET_DIR="$ROOT_DIR/.build-rust-ios16"
IOS_SIM_TARGET_DIR="$ROOT_DIR/.build-rust-ios-sim16"
DEPLOYMENT_TARGET="${IPHONEOS_DEPLOYMENT_TARGET:-16.0}"

mkdir -p "$RELEASE_DIR"

echo "Building macOS arm64 static library..."
CARGO_TARGET_DIR="$MAC_TARGET_DIR" \
  cargo build --release -p oxidb-embedded-ffi --target aarch64-apple-darwin

echo "Building iOS arm64 static library..."
SDKROOT="$(xcrun --sdk iphoneos --show-sdk-path)" \
IPHONEOS_DEPLOYMENT_TARGET="$DEPLOYMENT_TARGET" \
CARGO_TARGET_DIR="$IOS_TARGET_DIR" \
  cargo build --release -p oxidb-embedded-ffi --target aarch64-apple-ios

echo "Building iOS simulator arm64 static library..."
SDKROOT="$(xcrun --sdk iphonesimulator --show-sdk-path)" \
IPHONEOS_DEPLOYMENT_TARGET="$DEPLOYMENT_TARGET" \
CARGO_TARGET_DIR="$IOS_SIM_TARGET_DIR" \
  cargo build --release -p oxidb-embedded-ffi --target aarch64-apple-ios-sim

rm -rf "$RELEASE_DIR/OxiDBEmbedded.xcframework"
rm -f "$RELEASE_DIR/OxiDBEmbedded.xcframework.zip"

echo "Creating XCFramework..."
xcodebuild -create-xcframework \
  -library "$IOS_TARGET_DIR/aarch64-apple-ios/release/liboxidb_embedded_ffi.a" \
  -headers "$HEADERS_DIR" \
  -library "$IOS_SIM_TARGET_DIR/aarch64-apple-ios-sim/release/liboxidb_embedded_ffi.a" \
  -headers "$HEADERS_DIR" \
  -library "$MAC_TARGET_DIR/aarch64-apple-darwin/release/liboxidb_embedded_ffi.a" \
  -headers "$HEADERS_DIR" \
  -output "$RELEASE_DIR/OxiDBEmbedded.xcframework"

echo "Zipping XCFramework..."
ditto -c -k --sequesterRsrc --keepParent \
  "$RELEASE_DIR/OxiDBEmbedded.xcframework" \
  "$RELEASE_DIR/OxiDBEmbedded.xcframework.zip"

echo "SwiftPM checksum:"
swift package compute-checksum "$RELEASE_DIR/OxiDBEmbedded.xcframework.zip"
