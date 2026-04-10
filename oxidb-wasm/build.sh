#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "Building OxiDB WebAssembly module..."
wasm-pack build --target web --release --out-dir pkg

SIZE=$(du -sh pkg/oxidb_wasm_bg.wasm | cut -f1)
echo ""
echo "Build complete! WASM binary: $SIZE"
echo "Output: oxidb-wasm/pkg/"
echo ""
echo "To test locally:"
echo "  cd oxidb-wasm"
echo "  python3 -m http.server 8080"
echo "  open http://localhost:8080/example/"
