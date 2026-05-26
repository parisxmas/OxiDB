#!/usr/bin/env bash
set -euo pipefail

# ─── NuGet Pack Script for OxiDB .NET Packages ───────────────────────────────
#
# Builds native libraries for each platform and creates .nupkg files.
#
# Usage:
#   ./pack.sh                    # Build for current platform only + pack
#   ./pack.sh --all-platforms    # Cross-compile for all platforms + pack
#   ./pack.sh --pack-only        # Skip native build, just dotnet pack
#
# Prerequisites:
#   - Rust toolchain (rustup, cargo)
#   - .NET SDK 10.0+
#   - For cross-compilation: appropriate Rust targets installed
#       rustup target add aarch64-apple-darwin x86_64-apple-darwin \
#                         x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu \
#                         x86_64-pc-windows-gnu
#
# Output:
#   .nupkg files in dotnet/artifacts/
# ──────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
EMBEDDED_DIR="$SCRIPT_DIR/OxiDb.Client.Embedded"
RUNTIMES_DIR="$EMBEDDED_DIR/runtimes"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"
CONFIGURATION="Release"

# ─── Helpers ──────────────────────────────────────────────────────────────────

info()  { echo "==> $*"; }
error() { echo "ERROR: $*" >&2; exit 1; }

build_native() {
    local target="$1"
    local rid="$2"
    local lib_name="$3"

    info "Building oxidb-embedded-ffi for $target → $rid"
    cargo build --release -p oxidb-embedded-ffi --target "$target" \
        --manifest-path "$ROOT_DIR/Cargo.toml"

    local src="$ROOT_DIR/target/$target/release/$lib_name"
    local dst="$RUNTIMES_DIR/$rid/native/$lib_name"

    if [ ! -f "$src" ]; then
        echo "  WARNING: $src not found, skipping $rid"
        return 1
    fi

    mkdir -p "$(dirname "$dst")"
    cp "$src" "$dst"
    info "  Copied → $dst ($(du -h "$src" | cut -f1))"
}

build_native_current() {
    info "Building oxidb-embedded-ffi for current platform"
    cargo build --release -p oxidb-embedded-ffi --manifest-path "$ROOT_DIR/Cargo.toml"

    local os arch rid lib_name src

    os="$(uname -s)"
    arch="$(uname -m)"

    case "$os" in
        Darwin)
            lib_name="liboxidb_embedded_ffi.dylib"
            case "$arch" in
                arm64) rid="osx-arm64" ;;
                x86_64) rid="osx-x64" ;;
                *) error "Unsupported macOS arch: $arch" ;;
            esac
            ;;
        Linux)
            lib_name="liboxidb_embedded_ffi.so"
            case "$arch" in
                aarch64) rid="linux-arm64" ;;
                x86_64) rid="linux-x64" ;;
                *) error "Unsupported Linux arch: $arch" ;;
            esac
            ;;
        MINGW*|MSYS*|CYGWIN*)
            lib_name="oxidb_embedded_ffi.dll"
            rid="win-x64"
            ;;
        *) error "Unsupported OS: $os" ;;
    esac

    src="$ROOT_DIR/target/release/$lib_name"
    if [ ! -f "$src" ]; then
        error "Native library not found at $src"
    fi

    mkdir -p "$RUNTIMES_DIR/$rid/native"
    cp "$src" "$RUNTIMES_DIR/$rid/native/$lib_name"
    info "Copied → runtimes/$rid/native/$lib_name ($(du -h "$src" | cut -f1))"
}

build_native_all() {
    # macOS
    build_native "aarch64-apple-darwin"       "osx-arm64"   "liboxidb_embedded_ffi.dylib" || true
    build_native "x86_64-apple-darwin"        "osx-x64"     "liboxidb_embedded_ffi.dylib" || true

    # Linux
    build_native "x86_64-unknown-linux-gnu"   "linux-x64"   "liboxidb_embedded_ffi.so"    || true
    build_native "aarch64-unknown-linux-gnu"  "linux-arm64"  "liboxidb_embedded_ffi.so"    || true

    # Windows
    build_native "x86_64-pc-windows-gnu"      "win-x64"     "oxidb_embedded_ffi.dll"      || true
}

dotnet_pack() {
    info "Packing NuGet packages (Configuration=$CONFIGURATION)"
    mkdir -p "$ARTIFACTS_DIR"

    dotnet pack "$SCRIPT_DIR/OxiDb.Client.Tcp/OxiDb.Client.Tcp.csproj" \
        -c "$CONFIGURATION" -o "$ARTIFACTS_DIR" --no-restore

    dotnet pack "$SCRIPT_DIR/OxiDb.Client.Embedded/OxiDb.Client.Embedded.csproj" \
        -c "$CONFIGURATION" -o "$ARTIFACTS_DIR" --no-restore

    dotnet pack "$SCRIPT_DIR/OxiDb.EntityFrameworkCore/OxiDb.EntityFrameworkCore.csproj" \
        -c "$CONFIGURATION" -o "$ARTIFACTS_DIR" --no-restore

    dotnet pack "$SCRIPT_DIR/OxiDb.Linq/OxiDb.Linq.csproj" \
        -c "$CONFIGURATION" -o "$ARTIFACTS_DIR" --no-restore

    info "Packages:"
    ls -lh "$ARTIFACTS_DIR"/*.nupkg 2>/dev/null || echo "  (none found)"
    ls -lh "$ARTIFACTS_DIR"/*.snupkg 2>/dev/null || true
}

# ─── Main ─────────────────────────────────────────────────────────────────────

MODE="${1:-}"

# Always restore first
info "Restoring .NET dependencies"
dotnet restore "$SCRIPT_DIR/OxiDb.sln"

case "$MODE" in
    --all-platforms)
        build_native_all
        dotnet_pack
        ;;
    --pack-only)
        dotnet_pack
        ;;
    *)
        build_native_current
        dotnet_pack
        ;;
esac

info "Done! Packages are in dotnet/artifacts/"
echo ""
echo "To publish to NuGet:"
echo "  dotnet nuget push $ARTIFACTS_DIR/*.nupkg \\"
echo "    --api-key YOUR_API_KEY \\"
echo "    --source https://api.nuget.org/v3/index.json"
