#!/bin/bash
set -e

if ! command -v wasm-pack &> /dev/null; then
    echo "Installing wasm-pack..."
    curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh
fi

echo "Building WASM..."
wasm-pack build --target web --release

echo ""
echo "Build complete! To run:"
echo "  python3 -m http.server 8080"
echo "  open http://localhost:8080"
echo ""
echo "Make sure the Go dashboard is running on :3000 for WebSocket data."
