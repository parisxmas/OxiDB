#!/bin/bash
# Build the native OxiDB library into this app's jniLibs (lite, mobile profile).
set -e
cd "$(dirname "$0")/../../.."
cargo ndk -t arm64-v8a -t x86_64 \
    -o clients/oxidb-embedded-dart/example/android/app/src/main/jniLibs \
    build --profile mobile -p oxidb-embedded-ffi --no-default-features
