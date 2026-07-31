# OxiDB Geo Globe

131,831 populated places in an OxiDB document database compiled to WebAssembly,
rendered as a Three.js point-cloud globe. Click the globe to run a real
`$near` query (nearest cities, ranked by the engine); drag the slider to run
`$geoWithin` with a spherical cap. Every query executes in the browser tab —
there is no server.

Route mode snaps two clicks to the road graph with `$near` and runs the
engine's `$shortestPath` (Dijkstra) — the whole route is one aggregation
in the tab.

Live: https://oxidb.baltavista.com/demo/geo/

## Run locally

    # build the engine (once): from oxidb-wasm/
    wasm-pack build --target web --release --out-dir ../examples/geo-globe-demo/pkg
    python3 -m http.server 8777
    # open http://127.0.0.1:8777/

## Files

- `pkg/` — `oxidb-wasm` built with `wasm-pack build --target web`
- `cities.json` — 131,831 populated places from GeoNames cities1000
  (population ≥ 1,000; metro-thinned so districts merge into their city;
  CC BY 4.0, https://www.geonames.org/)
- `roads.json` / `nodes.json` — routable graph from Natural Earth 10m roads
  (public domain): planar-noded via shapely, dangling ends bridged ≤10 km,
  ferries kept as sea links; built by `tools/build-roads.py`
- `three.module.js`, `OrbitControls.js` — vendored three.js 0.166.1 (MIT)
