#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ANDROID_DIR="$SCRIPT_DIR"
JNILIB_DIR="$ANDROID_DIR/oxidb/src/main/jniLibs"

# NDK — find latest installed
NDK_DIR="$(ls -d "$HOME/Library/Android/sdk/ndk"/*/ 2>/dev/null | sort -V | tail -1)"
if [ -z "$NDK_DIR" ]; then
    echo "ERROR: Android NDK not found. Install via Android Studio → SDK Manager."
    exit 1
fi

# Use NDK with source.properties for cargo-ndk compatibility
for d in $(ls -d "$HOME/Library/Android/sdk/ndk"/*/ 2>/dev/null | sort -V); do
    if [ -f "$d/source.properties" ]; then
        NDK_DIR="$d"
    fi
done
export ANDROID_NDK_HOME="$NDK_DIR"
echo "Using NDK: $ANDROID_NDK_HOME"

# Step 1: Build Rust native libraries for all ABIs.
# `mobile` profile = release + opt-level=z (measured ~20% smaller).
# OXIDB_LITE=1 drops the SQL engine and the PDF/DOCX/XLSX text extractors
# (documents on those types are stored but not text-indexed; `sql` commands
# answer an explicit error).
FEATURES="android"
if [ "${OXIDB_LITE:-0}" = "1" ]; then
    FEATURE_ARGS="--no-default-features --features $FEATURES"
    echo "Building LITE native libraries (no SQL engine, no doc-format extraction)..."
else
    FEATURE_ARGS="--features $FEATURES"
    echo "Building native libraries..."
fi
cd "$REPO_ROOT"
cargo ndk -t arm64-v8a -t armeabi-v7a -t x86_64 \
    -o "$JNILIB_DIR" \
    build --profile mobile -p oxidb-embedded-ffi $FEATURE_ARGS

echo ""
echo "Native libraries:"
find "$JNILIB_DIR" -name "*.so" -exec ls -lh {} \;

# Step 2: Build AAR (if Gradle wrapper exists)
if [ -f "$ANDROID_DIR/gradlew" ]; then
    echo ""
    echo "Building AAR..."
    cd "$ANDROID_DIR"
    ./gradlew :oxidb:assembleRelease
    echo ""
    echo "AAR output:"
    find "$ANDROID_DIR" -name "*.aar" -exec ls -lh {} \;
else
    echo ""
    echo "Skipping AAR build (no gradlew). Run 'gradle wrapper' in $ANDROID_DIR first."
    echo "Or copy jniLibs + OxiDb.java directly into your Android project."
fi

echo ""
echo "Done! Android library ready."
echo ""
echo "Usage in your Android project:"
echo "  1. Copy jniLibs/ to app/src/main/jniLibs/"
echo "  2. Copy OxiDb.java to your source tree"
echo "  3. Use:"
echo '     OxiDb db = new OxiDb(context.getFilesDir() + "/oxidb_data");'
echo '     db.insert("users", new JSONObject().put("name", "Alice"));'
echo '     JSONArray results = db.find("users", new JSONObject());'
echo '     db.close();'
