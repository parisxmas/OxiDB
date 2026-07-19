#!/bin/bash
# Run the MongoDB CRUD specification tests against OxiDB.
# The spec files (mongodb/specifications) are CC BY-NC-SA licensed, so they
# are cloned to a cache directory rather than vendored into this repo.
set -e
cd "$(dirname "$0")/.."
CACHE="${MONGO_SPECS_DIR:-$HOME/.cache/mongodb-specifications}"
if [ ! -d "$CACHE/source/crud/tests/unified" ]; then
    git clone --depth 1 https://github.com/mongodb/specifications.git "$CACHE"
fi
MONGO_SPECS_DIR="$CACHE" cargo test --release --test mongo_spec_crud -- --ignored --nocapture "$@"
