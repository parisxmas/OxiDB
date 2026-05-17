#!/bin/bash -eu
# OSS-Fuzz build script for OxiDB.
#
# Invoked inside the OSS-Fuzz build container (see Dockerfile). The
# contract is documented at:
#   https://google.github.io/oss-fuzz/getting-started/new-project-guide/rust-lang/#buildsh
#
# Mirrored to projects/oxidb/build.sh in the OSS-Fuzz upstream repo
# when submitting. See infra/oss-fuzz/README.md for the playbook.
#
# Contract:
#   $SRC          source root (we cloned into $SRC/oxidb in Dockerfile)
#   $OUT          output dir — every fuzz binary must end up here
#   $SANITIZER    sanitizer name (address / undefined / memory)
#   $CFLAGS       sanitizer-aware compile flags (cargo-fuzz handles
#                 these automatically when invoked via `cargo fuzz`)

# The fuzz crate is excluded from the main workspace (libfuzzer-sys
# needs nightly + sanitizer-friendly codegen). Run cargo-fuzz from
# inside fuzz/ rather than the workspace root.
cd "$SRC/oxidb/fuzz"

# Build every target. cargo-fuzz wires up the sanitizer flags from
# OSS-Fuzz's env vars automatically.
cargo +nightly fuzz build -O --debug-assertions

# Copy each resulting binary to $OUT under its target name. OSS-Fuzz
# discovers fuzz binaries by enumerating $OUT.
FUZZ_TARGET_BIN_DIR="target/aarch64-unknown-linux-gnu/release"
if [ ! -d "$FUZZ_TARGET_BIN_DIR" ]; then
  # OSS-Fuzz build hosts are usually x86_64; the path the cargo-fuzz
  # build emits to depends on $CARGO_BUILD_TARGET / host triple. Fall
  # back to the standard x86_64 location.
  FUZZ_TARGET_BIN_DIR="target/x86_64-unknown-linux-gnu/release"
fi
if [ ! -d "$FUZZ_TARGET_BIN_DIR" ]; then
  # Last resort: the un-triple'd location.
  FUZZ_TARGET_BIN_DIR="target/release"
fi

for target in wire_deserialize wire_oxiwire wire_resp wire_pg \
              oxiwire_roundtrip resp_roundtrip msgpack_roundtrip; do
  if [ -x "$FUZZ_TARGET_BIN_DIR/$target" ]; then
    cp "$FUZZ_TARGET_BIN_DIR/$target" "$OUT/$target"
    echo "[oss-fuzz] copied $target → \$OUT/$target"
  else
    echo "[oss-fuzz] WARNING: $target not built at $FUZZ_TARGET_BIN_DIR" >&2
  fi
done
