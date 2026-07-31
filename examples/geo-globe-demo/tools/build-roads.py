#!/usr/bin/env python3
"""Build a routable edge collection from Natural Earth 10m roads.

Natural Earth ships geometry, not topology: junctions are (mostly) shared
vertices, not declared nodes. This script makes a graph out of it:

  1. every line's vertices are keyed on a rounded grid (~110 m), which both
     identifies shared-vertex junctions and snaps near-miss endpoints;
  2. a vertex used by more than one line — or any line endpoint — becomes a
     node; lines are split into edges between consecutive nodes;
  3. each edge gets its haversine length and a simplified point list for
     drawing.

Output (next to the demo): roads.json  [{a, b, km, pts, t}]
                           nodes.json  [[lon, lat], ...]  (id = index)

Ferry routes are kept (marked t:"ferry") — without them islands are honest
but useless islands. Run with the geojson path as the only argument.
"""
import json
import math
import sys
from collections import Counter, defaultdict

SNAP = 2  # decimals: ~1.1 km — NE junctions rarely share exact vertices
KEEP_TYPES = {
    "Major Highway", "Beltway", "Bypass", "Secondary Highway", "Road",
    "Unknown",  # 25k features; dropping them shreds Africa/Asia connectivity
    "Ferry Route", "Ferry, seasonal",
}
EARTH_KM = 6371.0088

# Fixed links newer than Natural Earth's digitization. NE has NO crossing of
# the İzmit Gulf (its two Marmara "ferries" are Black Sea international
# routes), so Istanbul→Bursa routed 300 km around the gulf. Each polyline's
# join vertices land in the same SNAP cell as an existing road vertex, which
# is exactly the near-miss-junction mechanism the grid pass implements.
CURATED = [
    # O-5 / Osmangazi Bridge (2016): D-100 at Dilovası → bridge → Orhangazi
    # → Bursa ring. Joins: (29.5275,40.7874), (29.3031,40.4796), (29.0872,40.2728).
    [(29.528, 40.787), (29.513, 40.757), (29.512, 40.712), (29.45, 40.63),
     (29.36, 40.52), (29.303, 40.48), (29.19, 40.40), (29.10, 40.31),
     (29.087, 40.273)],
]


def hav(a, b):
    dla = math.radians(b[1] - a[1])
    dlo = math.radians(b[0] - a[0])
    h = (math.sin(dla / 2) ** 2
         + math.cos(math.radians(a[1])) * math.cos(math.radians(b[1]))
         * math.sin(dlo / 2) ** 2)
    return 2 * EARTH_KM * math.asin(math.sqrt(h))


def main(path):
    gj = json.load(open(path))
    raw_lines = []  # (coords, is_ferry)
    for f in gj["features"]:
        p = f["properties"]
        if p.get("type") not in KEEP_TYPES:
            continue
        ferry = p.get("featurecla") == "Ferry"
        g = f["geometry"]
        parts = [g["coordinates"]] if g["type"] == "LineString" else g["coordinates"]
        for coords in parts:
            if len(coords) >= 2:
                raw_lines.append(([(round(x, 4), round(y, 4)) for x, y, *_ in coords], ferry))
    for coords in CURATED:
        raw_lines.append(([(round(x, 4), round(y, 4)) for x, y in coords], False))

    # TRUE planar noding: Natural Earth junctions are crossings, not shared
    # vertices — union the whole network so every intersection becomes a
    # vertex on both lines. (Ferries union'd separately keep their flag;
    # they only meet roads at ports, i.e. line endpoints.)
    from shapely.geometry import LineString, MultiLineString
    from shapely.ops import unary_union

    def noded(group):
        if not group:
            return []
        u = unary_union([LineString(c) for c in group])
        geoms = u.geoms if isinstance(u, MultiLineString) else [u]
        return [[(round(x, 4), round(y, 4)) for x, y in g.coords] for g in geoms]

    lines = [(c, False) for c in noded([c for c, f in raw_lines if not f])]
    lines += [(c, True) for c in noded([c for c, f in raw_lines if f])]

    # Pass 1: how many lines touch each snapped vertex.
    def key(pt):
        return (round(pt[0], SNAP), round(pt[1], SNAP))

    uses = Counter()
    for coords, _ in lines:
        seen_in_line = set()
        for pt in coords:
            k = key(pt)
            if k not in seen_in_line:  # a line revisiting a vertex counts once
                uses[k] += 1
                seen_in_line.add(k)

    # Pass 2: nodes = junctions (shared vertices) + every line endpoint.
    node_id = {}
    nodes = []

    def node_for(pt):
        k = key(pt)
        if k not in node_id:
            node_id[k] = len(nodes)
            nodes.append([k[0], k[1]])
        return node_id[k]

    edges = []
    for coords, ferry in lines:
        cut = [0]
        for i in range(1, len(coords) - 1):
            if uses[key(coords[i])] > 1:
                cut.append(i)
        cut.append(len(coords) - 1)
        for c0, c1 in zip(cut, cut[1:]):
            seg = coords[c0:c1 + 1]
            a, b = node_for(seg[0]), node_for(seg[-1])
            if a == b:
                continue  # a loop too tight for the snap grid
            km = sum(hav(seg[i], seg[i + 1]) for i in range(len(seg) - 1))
            # Drawing detail: every 3rd vertex plus the endpoints.
            pts = [seg[0]] + seg[1:-1][::3] + [seg[-1]]
            e = {"a": a, "b": b, "km": round(km, 1),
                 "pts": [[p[0], p[1]] for p in pts]}
            if ferry:
                e["t"] = "ferry"
            edges.append(e)

    # Gap bridging: separately digitized lines often END a few km apart
    # without touching. Every degree-1 node (a dangling end) gets one bridge
    # edge to the nearest node of a DIFFERENT component within BRIDGE_KM —
    # targeted at digitization gaps, small enough not to leap seas (ferries
    # are the legitimate sea links).
    BRIDGE_KM = 10.0
    degree = Counter()
    for e in edges:
        degree[e["a"]] += 1
        degree[e["b"]] += 1

    parent = list(range(len(nodes)))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for e in edges:
        union(e["a"], e["b"])

    cell_deg = 0.15  # ~16 km cells
    grid_nodes = defaultdict(list)
    for i, (lon, lat) in enumerate(nodes):
        grid_nodes[(int(lat // cell_deg), int(lon // cell_deg))].append(i)

    bridges = 0
    for i, (lon, lat) in enumerate(nodes):
        if degree[i] != 1:
            continue
        ci, cj = int(lat // cell_deg), int(lon // cell_deg)
        best = None
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                for j in grid_nodes.get((ci + di, cj + dj), ()):
                    if j == i or find(j) == find(i):
                        continue
                    d = hav(nodes[i], nodes[j])
                    if d <= BRIDGE_KM and (best is None or d < best[0]):
                        best = (d, j)
        if best:
            d, j = best
            edges.append({"a": i, "b": j, "km": round(max(d, 0.1), 1),
                          "pts": [nodes[i], nodes[j]], "t": "bridge"})
            union(i, j)
            bridges += 1
    print(f"bridges added: {bridges}")

    # Connectivity report — honesty about the network.
    comp = Counter(find(i) for i in range(len(nodes)))
    biggest = comp.most_common(1)[0][1] if comp else 0

    out_dir = __file__.rsplit("/", 2)[0]
    json.dump(edges, open(f"{out_dir}/roads.json", "w"), separators=(",", ":"))
    json.dump(nodes, open(f"{out_dir}/nodes.json", "w"), separators=(",", ":"))
    print(f"nodes: {len(nodes)}  edges: {len(edges)}  "
          f"total: {sum(e['km'] for e in edges):,.0f} km")
    print(f"components: {len(comp)}  biggest: {biggest} nodes "
          f"({100 * biggest / max(1, len(nodes)):.1f}%)")


if __name__ == "__main__":
    main(sys.argv[1])
