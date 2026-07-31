// OxiDB Geo Globe — 10k cities in an OxiDB document database compiled to
// WebAssembly, rendered as a Three.js point cloud. Click the globe: a real
// `$near` query ranks the nearest cities; the slider runs `$geoWithin`
// with a spherical cap. No server anywhere — the database is in this tab.
import * as THREE from "three";
import { OrbitControls } from "./OrbitControls.js";
import init, * as oxidb from "./pkg/oxidb_wasm.js?v=r1";

const R = 1; // globe radius in scene units
const EARTH_KM = 6371.0088;

// ── boot: engine + data + index ─────────────────────────────────────────────
const bootmsg = document.getElementById("bootmsg");
bootmsg.textContent = "loading engine…";
await init();
oxidb.init();

// Data loading with a real progress bar and optional browser persistence.
// The five data files total ~19 MB decompressed; with consent they live in
// the Cache API (versioned) and later visits never touch the network — the
// database itself rebuilds locally in about a second, so caching the FILES
// (byte-identical, no staleness logic) beats persisting the DB image.
const DATA_CACHE = "geo-globe-data-v2";
// Old data versions must not linger (19 MB each). A returning "yes" visitor
// misses the new empty cache, refetches, and the consent block silently
// refills it — no re-ask, the consent covers the data, not one version.
if ("caches" in window)
  caches.keys().then((ks) =>
    ks.forEach((k) => {
      if (k.startsWith("geo-globe-data-") && k !== DATA_CACHE) caches.delete(k);
    })
  );
const FILE_BYTES = {
  "./cities.json": 8559352,
  "./roads.json": 9719480,
  "./nodes.json": 867519,
  "./borders.json?v=2": 155031,
  "./land.json": 76677,
  // ?v=2: the file gained ISO codes; the query string gives it a fresh
  // cache key so a stored v2 cache refetches just this file (the stale
  // un-versioned entry is 8 KB of harmless dead weight).
  "./countries.json?v=2": 9945,
};
const FETCH_SHARE = 0.8; // downloads own 80% of the bar; build steps the rest
const bytesTotal = Object.values(FILE_BYTES).reduce((a, b) => a + b, 0);
let bytesDone = 0;
let stagePct = 0;
const bar = (msg) => {
  if (msg) bootmsg.textContent = msg;
  const pct = Math.min(100, (bytesDone / bytesTotal) * FETCH_SHARE * 100 + stagePct);
  document.getElementById("bootfill").style.width = `${pct}%`;
  document.getElementById("bootpct").textContent = `${Math.round(pct)}%`;
};
const stage = (pts, msg) => {
  stagePct += pts;
  bar(msg);
};
const persistChoice = localStorage.getItem("geoPersist"); // "yes" | "no" | null
const rawBuffers = new Map(); // kept until the consent decision
let servedFromCache = false;
async function loadData(url, label) {
  bar(`loading ${label}…`);
  let resp = null;
  let fromCache = false;
  if (persistChoice === "yes" && "caches" in window) {
    resp = await (await caches.open(DATA_CACHE)).match(url);
    if (resp) servedFromCache = fromCache = true;
  }
  if (!resp) resp = await fetch(url);
  const reader = resp.body.getReader();
  const chunks = [];
  let got = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    got += value.length;
    bytesDone += value.length;
    bar();
  }
  // Size drift between the baked estimate and reality: settle the account.
  bytesDone += (FILE_BYTES[url] ?? got) - got;
  bar();
  const buf = new Uint8Array(got);
  let o = 0;
  for (const c of chunks) {
    buf.set(c, o);
    o += c.length;
  }
  // Keep every network-fetched body: either the user hasn't decided yet
  // (kept until the consent dialog), or they said "yes" but the cache
  // missed (evicted / version bump) and it must be silently refilled.
  if (!fromCache) rawBuffers.set(url, buf);
  return JSON.parse(new TextDecoder().decode(buf));
}

const cities = await loadData("./cities.json", "cities");

// Insert as documents: {name, country, pop, loc: [lon, lat]}.
stage(2, "inserting cities…");
const t0 = performance.now();
const docs = cities.map((c) => ({ n: c.n, c: c.c, p: c.p, loc: [c.lon, c.lat] }));
const BATCH = 2000;
for (let i = 0; i < docs.length; i += BATCH) {
  oxidb.insert_many("cities", JSON.stringify(docs.slice(i, i + BATCH)));
}
const insertMs = performance.now() - t0;

stage(5, "building geohash index…");
const t1 = performance.now();
oxidb.create_geo_index("cities", "loc");
const indexMs = performance.now() - t1;

const roadEdges = await loadData("./roads.json", "road network");
const roadNodes = await loadData("./nodes.json", "road nodes");
stage(3, "building road graph…");
// The engine gets LEAN edge documents (a, b, km, i); the drawing detail
// (pts) stays in JS, looked up by the edge's `i` when a route comes back.
const t2 = performance.now();
const NB = 5000;
for (let i = 0; i < roadNodes.length; i += NB) {
  oxidb.insert_many(
    "nodes",
    JSON.stringify(roadNodes.slice(i, i + NB).map((p, k) => ({ i: i + k, loc: p })))
  );
}
oxidb.create_geo_index("nodes", "loc");
for (let i = 0; i < roadEdges.length; i += NB) {
  oxidb.insert_many(
    "roads",
    JSON.stringify(
      roadEdges.slice(i, i + NB).map((e, k) => {
        const d = { i: i + k, a: e.a, b: e.b, km: e.km };
        if (e.t) d.t = e.t;
        return d;
      })
    )
  );
}
oxidb.create_index("roads", "a");
oxidb.create_index("roads", "b");
const roadsMs = performance.now() - t2;

document.getElementById("sub").textContent =
  `${cities.length.toLocaleString()} cities in a document database compiled to ` +
  `WebAssembly — every query below runs in this tab. No server.`;
document.getElementById("docCount").textContent =
  `${cities.length.toLocaleString()} (${insertMs.toFixed(0)} ms)`;
document.getElementById("indexMs").textContent =
  `${indexMs.toFixed(0)} ms · roads ${roadsMs.toFixed(0)} ms ` +
  `(${roadEdges.length.toLocaleString()} edges)`;

// ── scene ───────────────────────────────────────────────────────────────────
const canvas = document.getElementById("scene");
const renderer = new THREE.WebGLRenderer({ canvas, antialias: true });
renderer.setPixelRatio(Math.min(devicePixelRatio, 2));
const scene = new THREE.Scene();
scene.background = new THREE.Color(0x060a12);
const camera = new THREE.PerspectiveCamera(40, 1, 0.01, 50);
// Start looking at the initial query point (Istanbul), slightly tilted north.
{
  const la = THREE.MathUtils.degToRad(41.0), lo = THREE.MathUtils.degToRad(28.98);
  camera.position
    .set(Math.cos(la) * Math.cos(lo), Math.sin(la) + 0.25, -Math.cos(la) * Math.sin(lo))
    .normalize()
    .multiplyScalar(2.8);
}
const controls = new OrbitControls(camera, canvas);
controls.enableDamping = true;
controls.dampingFactor = 0.06;
controls.minDistance = 1.05; // ~320 km up — small countries fill the screen
controls.maxDistance = 6;
controls.enablePan = false;
controls.autoRotate = false; // the panel checkbox drives this
controls.autoRotateSpeed = 0.5;

function toXYZ(lon, lat, r = R) {
  const la = THREE.MathUtils.degToRad(lat);
  const lo = THREE.MathUtils.degToRad(lon);
  return new THREE.Vector3(
    r * Math.cos(la) * Math.cos(lo),
    r * Math.sin(la),
    -r * Math.cos(la) * Math.sin(lo)
  );
}
function toLonLat(v) {
  const n = v.clone().normalize();
  return {
    lat: THREE.MathUtils.radToDeg(Math.asin(n.y)),
    lon: THREE.MathUtils.radToDeg(Math.atan2(-n.z, n.x)),
  };
}

// Globe body: an equirectangular land/water texture painted on a 2D canvas
// from Natural Earth land polygons — Canvas2D's evenodd fill handles the
// concave coastlines and lake holes that would otherwise need
// sphere-surface triangulation. Standard equirect pixel mapping
// ((lon+180)/360, (90-lat)/180) lines up with SphereGeometry's default UVs
// and with the city-point projection.
const landPolys = await loadData("./land.json", "land polygons");
const globeTexture = (() => {
  const W = 2048, H = 1024;
  const cv = document.createElement("canvas");
  cv.width = W; cv.height = H;
  const ctx = cv.getContext("2d");
  ctx.fillStyle = "#081527"; // water
  ctx.fillRect(0, 0, W, H);
  ctx.fillStyle = "#182c47"; // land
  // One fill per polygon: evenodd handles that polygon's lake holes, while
  // separate polygons paint over each other instead of XOR-ing to water.
  for (const rings of landPolys) {
    ctx.beginPath();
    for (const ring of rings) {
      ring.forEach(([lon, lat], i) => {
        const x = ((lon + 180) / 360) * W;
        const y = ((90 - lat) / 180) * H;
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      });
      ctx.closePath();
    }
    ctx.fill("evenodd");
  }
  const tex = new THREE.CanvasTexture(cv);
  tex.colorSpace = THREE.SRGBColorSpace;
  tex.anisotropy = 4;
  return tex;
})();
const globe = new THREE.Mesh(
  new THREE.SphereGeometry(R * 0.995, 64, 64),
  new THREE.MeshBasicMaterial({ map: globeTexture })
);
scene.add(globe);

const grat = new THREE.Group();
const gratMat = new THREE.LineBasicMaterial({ color: 0x16233c, transparent: true, opacity: 0.7 });
for (let lat = -60; lat <= 60; lat += 30) {
  const pts = [];
  for (let lon = 0; lon <= 360; lon += 4) pts.push(toXYZ(lon, lat, R * 0.997));
  grat.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints(pts), gratMat));
}
for (let lon = 0; lon < 360; lon += 30) {
  const pts = [];
  for (let lat = -90; lat <= 90; lat += 4) pts.push(toXYZ(lon, lat, R * 0.997));
  grat.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints(pts), gratMat));
}
scene.add(grat);

// Country borders (Natural Earth 110m admin-0, compacted to rings). Each
// ring carries a color class from a build-time greedy graph coloring
// (adjacency = shared boundary vertices; 5 classes cover the map), so
// neighbouring countries always contrast. Long edges are subdivided so a
// segment hugs the sphere instead of chording through it.
{
  const rings = await loadData("./borders.json?v=2", "country borders");
  const PALETTE = [0x7fb2e5, 0xe08a8a, 0x8fc98f, 0xe5c97a, 0xc09ad6].map(
    (c) => new THREE.Color(c)
  );
  const verts = [];
  const cols = [];
  for (const [k, ring] of rings) {
    const col = PALETTE[k % PALETTE.length];
    for (let i = 1; i < ring.length; i++) {
      const [lon0, lat0] = ring[i - 1];
      const [lon1, lat1] = ring[i];
      let dlon = lon1 - lon0;
      if (Math.abs(dlon) > 180) continue; // antimeridian jump — skip the seam
      const steps = Math.max(1, Math.ceil(Math.max(Math.abs(dlon), Math.abs(lat1 - lat0)) / 2));
      let prev = toXYZ(lon0, lat0, R * 0.999);
      for (let s = 1; s <= steps; s++) {
        const t = s / steps;
        const cur = toXYZ(lon0 + dlon * t, lat0 + (lat1 - lat0) * t, R * 0.999);
        verts.push(prev.x, prev.y, prev.z, cur.x, cur.y, cur.z);
        cols.push(col.r, col.g, col.b, col.r, col.g, col.b);
        prev = cur;
      }
    }
  }
  const g = new THREE.BufferGeometry();
  g.setAttribute("position", new THREE.BufferAttribute(new Float32Array(verts), 3));
  g.setAttribute("color", new THREE.BufferAttribute(new Float32Array(cols), 3));
  scene.add(
    new THREE.LineSegments(
      g,
      new THREE.LineBasicMaterial({ vertexColors: true, transparent: true, opacity: 0.85 })
    )
  );
}

// Country name labels (NE admin-0 NAME at LABEL_X/Y) — built lazily on the
// first toggle. Sprites sit just above the surface with depthTest on, so
// the opaque globe hides the far-side names for free. When zoomed out only
// the major countries (LABELRANK ≤ 4) show; zooming in reveals the rest.
const countryData = await loadData("./countries.json?v=2", "country names");
let countryLabels = null;
let labelsHaveFlags = null;
const flagOf = (cc) =>
  String.fromCodePoint(...[...cc].map((ch) => 0x1f1e6 + ch.charCodeAt(0) - 65));
function ensureCountryLabels() {
  const wantFlags = document.getElementById("countryflags").checked;
  if (countryLabels && labelsHaveFlags === wantFlags) return;
  if (countryLabels) {
    scene.remove(countryLabels);
    for (const s of countryLabels.children) {
      s.material.map.dispose();
      s.material.dispose();
    }
  }
  labelsHaveFlags = wantFlags;
  countryLabels = new THREE.Group();
  for (const c of countryData) {
    const big = c.r <= 3;
    const fs = big ? 34 : 26;
    const font = `600 ${fs}px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif`;
    const text = wantFlags && c.c ? `${flagOf(c.c)} ${c.n}` : c.n;
    const cv = document.createElement("canvas");
    let ctx = cv.getContext("2d");
    ctx.font = font;
    const w = Math.ceil(ctx.measureText(text).width) + 12;
    const h = fs + 14;
    cv.width = w;
    cv.height = h;
    ctx = cv.getContext("2d");
    ctx.font = font;
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.shadowColor = "rgba(4, 8, 16, 0.9)";
    ctx.shadowBlur = 6;
    ctx.fillStyle = big ? "#c8d8f0" : "#96abc9";
    ctx.fillText(text, w / 2, h / 2);
    const tex = new THREE.CanvasTexture(cv);
    tex.anisotropy = 4;
    // sizeAttenuation off = constant screen size, like a map label — zooming
    // changes what fits around a name, never the name itself.
    const s = new THREE.Sprite(
      new THREE.SpriteMaterial({
        map: tex,
        transparent: true,
        depthWrite: false,
        sizeAttenuation: false,
      })
    );
    const scale = big ? 0.00035 : 0.00029;
    s.scale.set(w * scale, h * scale, 1);
    s.position.copy(toXYZ(c.lon, c.lat, R * 1.005));
    s.userData.r = c.r;
    countryLabels.add(s);
  }
  scene.add(countryLabels);
}
document.getElementById("countrynames").addEventListener("change", (e) => {
  if (e.target.checked) ensureCountryLabels();
  if (countryLabels) countryLabels.visible = e.target.checked;
});
document.getElementById("countryflags").addEventListener("change", () => {
  // Only rebuild when the labels are on screen; an off-screen toggle is
  // picked up the next time the names checkbox builds them.
  if (document.getElementById("countrynames").checked) {
    ensureCountryLabels();
    countryLabels.visible = true;
  }
});

// Starfield.
{
  const n = 1200;
  const pos = new Float32Array(n * 3);
  for (let i = 0; i < n; i++) {
    const v = new THREE.Vector3().randomDirection().multiplyScalar(20 + Math.random() * 15);
    pos.set([v.x, v.y, v.z], i * 3);
  }
  const g = new THREE.BufferGeometry();
  g.setAttribute("position", new THREE.BufferAttribute(pos, 3));
  scene.add(new THREE.Points(g, new THREE.PointsMaterial({ color: 0x2a3854, size: 0.035 })));
}

// City point cloud with per-vertex color + size (shader material so the
// query results can restyle individual cities cheaply).
const COL_BASE = new THREE.Color(0x46a8d0);
const COL_WITHIN = new THREE.Color(0xffb74d);
const COL_NEAR = new THREE.Color(0xffffff);
const positions = new Float32Array(cities.length * 3);
const colors = new Float32Array(cities.length * 3);
const sizes = new Float32Array(cities.length);
const indexByKey = new Map(); // "lon,lat" -> point index
cities.forEach((c, i) => {
  const v = toXYZ(c.lon, c.lat);
  positions.set([v.x, v.y, v.z], i * 3);
  COL_BASE.toArray(colors, i * 3);
  // Population-weighted; the dataset floor is now 1k (and GeoNames
  // sometimes records 0) — clamp both the log argument and the size.
  sizes[i] = Math.max(0.6, 1.4 + Math.min(2.2, Math.log10(Math.max(c.p, 1000)) - 4.5));
  indexByKey.set(`${c.lon},${c.lat}`, i);
});
const cityGeo = new THREE.BufferGeometry();
cityGeo.setAttribute("position", new THREE.BufferAttribute(positions, 3));
cityGeo.setAttribute("color", new THREE.BufferAttribute(colors, 3));
cityGeo.setAttribute("psize", new THREE.BufferAttribute(sizes, 1));
const cityMat = new THREE.ShaderMaterial({
  transparent: true,
  depthWrite: false,
  vertexShader: `
    attribute float psize; varying vec3 vc;
    void main() {
      vc = color;
      vec4 mv = modelViewMatrix * vec4(position, 1.0);
      gl_PointSize = min(psize * (8.0 / -mv.z), psize * 5.0) * ${Math.min(devicePixelRatio, 2).toFixed(1)};
      gl_Position = projectionMatrix * mv;
    }`,
  fragmentShader: `
    varying vec3 vc;
    void main() {
      float d = length(gl_PointCoord - 0.5);
      if (d > 0.5) discard;
      gl_FragColor = vec4(vc, smoothstep(0.5, 0.18, d));
    }`,
  vertexColors: true,
});
const cityPoints = new THREE.Points(cityGeo, cityMat);
scene.add(cityPoints);

// Pick marker + radius ring.
const marker = new THREE.Mesh(
  new THREE.SphereGeometry(0.007, 16, 16),
  new THREE.MeshBasicMaterial({ color: 0xff5252 })
);
marker.visible = false;
scene.add(marker);
const ringMat = new THREE.LineBasicMaterial({ color: 0xffb74d, transparent: true, opacity: 0.85 });
let ring = null;

// Blink halo: clicking a city in the list pulses this at its point.
const blinkMat = new THREE.MeshBasicMaterial({
  color: 0x4dd0e1,
  transparent: true,
  blending: THREE.AdditiveBlending,
  depthWrite: false,
});
const blink = new THREE.Mesh(new THREE.SphereGeometry(0.011, 16, 16), blinkMat);
blink.visible = false;
scene.add(blink);
// Route mode state: A/B endpoint markers and the drawn route.
const routeGroup = new THREE.Group();
scene.add(routeGroup);
const mkEnd = (color) => {
  const m = new THREE.Mesh(
    new THREE.SphereGeometry(0.007, 16, 16),
    new THREE.MeshBasicMaterial({ color })
  );
  m.visible = false;
  scene.add(m);
  return m;
};
const endA = mkEnd(0x66bb6a);
const endB = mkEnd(0xff5252);
function clearRoute() {
  routeGroup.clear();
  endA.visible = false;
  endB.visible = false;
}

function drawPolyline(pts, color, dashed = false) {
  const verts = [];
  for (let i = 1; i < pts.length; i++) {
    const [lon0, lat0] = pts[i - 1];
    const [lon1, lat1] = pts[i];
    if (Math.abs(lon1 - lon0) > 180) continue;
    const steps = Math.max(1, Math.ceil(Math.max(Math.abs(lon1 - lon0), Math.abs(lat1 - lat0)) / 1));
    let prev = toXYZ(lon0, lat0, R * 1.001);
    for (let s = 1; s <= steps; s++) {
      const t = s / steps;
      const cur = toXYZ(lon0 + (lon1 - lon0) * t, lat0 + (lat1 - lat0) * t, R * 1.001);
      verts.push(prev.x, prev.y, prev.z, cur.x, cur.y, cur.z);
      prev = cur;
    }
  }
  const g = new THREE.BufferGeometry();
  g.setAttribute("position", new THREE.BufferAttribute(new Float32Array(verts), 3));
  const mat = dashed
    ? new THREE.LineDashedMaterial({ color, dashSize: 0.02, gapSize: 0.015 })
    : new THREE.LineBasicMaterial({ color, linewidth: 2 });
  const line = new THREE.LineSegments(g, mat);
  if (dashed) line.computeLineDistances();
  routeGroup.add(line);
}

async function routeBetween(a, b) {
  // Snap both ends to the road graph with $near (nearest node document).
  const snap = (p) => {
    const q = { loc: { $near: { $geometry: { type: "Point", coordinates: [p.lon, p.lat] }, $maxDistance: 1_000_000 } } };
    const r = JSON.parse(oxidb.find("nodes", JSON.stringify(q)));
    return r.length ? r[0].i : null;
  };
  const src = snap(a);
  const dst = snap(b);
  if (src === null || dst === null) {
    return { ok: false, msg: "no road within 1000 km" };
  }
  // The whole route is ONE aggregation: match the source node document,
  // then $shortestPath over the edge collection.
  const pipeline = [
    { $match: { i: src } },
    { $shortestPath: {
        from: "roads",
        source: "$i", target: dst,
        edgeFrom: "a", edgeTo: "b",
        weight: "km", undirected: true,
        as: "route", costField: "totalKm",
        maxCost: 30000,
    } },
  ];
  let out;
  try {
    out = JSON.parse(oxidb.aggregate("nodes", JSON.stringify(pipeline)));
  } catch (e) {
    const msg = `error: ${String(e).slice(0, 80)}`;
    return { ok: false, msg };
  }
  const doc = out[0] ?? {};
  if (!doc.route || !doc.route.length) {
    // Honest failure: the network is real-world disconnected in places.
    drawPolyline([[a.lon, a.lat], [b.lon, b.lat]], 0x7d8fac, true);
    const msg = doc.totalKm === 0 ? "same node" : "no route (disconnected network)";
    return { ok: false, msg };
  }
  let ferries = 0;
  for (const e of doc.route) {
    const pts = roadEdges[e.i]?.pts;
    if (!pts) continue;
    const isFerry = e.t === "ferry" || e.t === "bridge";
    if (isFerry) ferries++;
    drawPolyline(pts, isFerry ? 0x4dd0e1 : 0xffe082, isFerry);
  }
  const stat =
    `${Math.round(doc.totalKm).toLocaleString()} km · ${doc.route.length} segments` +
    (ferries ? ` · ${ferries} ferry/bridge` : "");
  return { ok: true, msg: stat };
}

let blinkStart = 0;
function blinkAt(lon, lat) {
  blink.position.copy(toXYZ(lon, lat, R * 1.004));
  blinkStart = performance.now();
  blink.visible = true;
}

function drawRing(lon, lat, km) {
  if (ring) scene.remove(ring);
  // Circle of angular radius km/EARTH_KM around the point, on the sphere.
  const ang = km / EARTH_KM;
  const center = toXYZ(lon, lat).normalize();
  const any = Math.abs(center.y) < 0.99 ? new THREE.Vector3(0, 1, 0) : new THREE.Vector3(1, 0, 0);
  const u = new THREE.Vector3().crossVectors(center, any).normalize();
  const v = new THREE.Vector3().crossVectors(center, u).normalize();
  const pts = [];
  for (let i = 0; i <= 128; i++) {
    const t = (i / 128) * Math.PI * 2;
    const p = center
      .clone()
      .multiplyScalar(Math.cos(ang))
      .addScaledVector(u, Math.sin(ang) * Math.cos(t))
      .addScaledVector(v, Math.sin(ang) * Math.sin(t))
      .multiplyScalar(R * 1.002);
    pts.push(p);
  }
  ring = new THREE.Line(new THREE.BufferGeometry().setFromPoints(pts), ringMat);
  scene.add(ring);
}

// ── queries ────────────────────────────────────────────────────────────────
const flag = (cc) =>
  cc.replace(/./g, (ch) => String.fromCodePoint(0x1f1e6 + ch.charCodeAt(0) - 65));
let picked = { lon: 28.9784, lat: 41.0082 }; // start on Istanbul
let lastNear = [];

// Slider position 1..100 → 1k..3M on a log scale; 0 = no filter.
function minPopValue() {
  const v = Number(document.getElementById("minpop").value);
  if (v === 0) return 0;
  const raw = 10 ** (3.0 + (v / 100) * 3.48);
  // Friendly steps; the default notch v=49 lands exactly on 50,000.
  const step = raw >= 25000 ? 5000 : raw >= 5000 ? 1000 : 100;
  return Math.round(raw / step) * step;
}
const fmtPop = (n) =>
  n >= 1_000_000 ? `${(n / 1_000_000).toFixed(1)}M` : `${Math.round(n / 1000)}k`;

function runQueries() {
  const { lon, lat } = picked;
  const radiusKm = Number(document.getElementById("radius").value);
  const minPop = minPopValue();
  document.getElementById("minPopLabel").textContent = minPop ? fmtPop(minPop) : "any";

  // $near — the cities inside the slider's radius, ranked by the engine.
  // Same radius as the $geoWithin below: the ring, the count and the list
  // tell one story (and two different operators agree on the answer).
  // Geo composes with ordinary predicates: one query, both conditions,
  // every candidate verified against the whole thing.
  const nearQ = {
    loc: { $near: { $geometry: { type: "Point", coordinates: [lon, lat] }, $maxDistance: radiusKm * 1000 } },
  };
  if (minPop) nearQ.p = { $gte: minPop };
  let t = performance.now();
  const near = JSON.parse(oxidb.find("cities", JSON.stringify(nearQ)));
  const nearMs = performance.now() - t;

  // $geoWithin — spherical cap of the slider's radius.
  const withinQ = {
    loc: { $geoWithin: { $centerSphere: [[lon, lat], radiusKm / EARTH_KM] } },
  };
  if (minPop) withinQ.p = { $gte: minPop };
  t = performance.now();
  const within = JSON.parse(oxidb.find("cities", JSON.stringify(withinQ)));
  const withinMs = performance.now() - t;

  // Panel.
  document.getElementById("nearQ").innerHTML =
    `{loc: {<b>$near</b>: [${lon.toFixed(2)}, ${lat.toFixed(2)}]}}`;
  document.getElementById("nearMs").textContent = `${nearMs.toFixed(1)} ms`;
  document.getElementById("nearCount").textContent = near.length.toLocaleString();
  document.getElementById("withinMs").textContent = `${withinMs.toFixed(1)} ms`;
  document.getElementById("withinCount").textContent = `${within.length} cities`;
  const hav = (a, b) => {
    const dla = THREE.MathUtils.degToRad(b[1] - a[1]);
    const dlo = THREE.MathUtils.degToRad(b[0] - a[0]);
    const h =
      Math.sin(dla / 2) ** 2 +
      Math.cos(THREE.MathUtils.degToRad(a[1])) *
        Math.cos(THREE.MathUtils.degToRad(b[1])) *
        Math.sin(dlo / 2) ** 2;
    return 2 * EARTH_KM * Math.asin(Math.sqrt(h));
  };
  const listEl = document.getElementById("nearList");
  listEl.scrollTop = 0;
  lastNear = near;
  listEl.innerHTML = near
    .map((d, i) => {
      const km = hav([lon, lat], d.loc);
      return `<li data-i="${i}"><span class="name">${flag(d.c)} ${d.n}</span><span class="pop">${
        d.p > 0 ? fmtPop(d.p) : ""
      }</span><span class="km">${
        km < 10 ? km.toFixed(1) : Math.round(km).toLocaleString()
      } km</span></li>`;
    })
    .join("");

  // Recolor the point cloud: base → within (warm) → near top-10 (white).
  const colorAttr = cityGeo.getAttribute("color");
  for (let i = 0; i < cities.length; i++) COL_BASE.toArray(colorAttr.array, i * 3);
  for (const d of within) {
    const i = indexByKey.get(`${d.loc[0]},${d.loc[1]}`);
    if (i !== undefined) COL_WITHIN.toArray(colorAttr.array, i * 3);
  }
  for (const d of near.slice(0, 10)) {
    const i = indexByKey.get(`${d.loc[0]},${d.loc[1]}`);
    if (i !== undefined) COL_NEAR.toArray(colorAttr.array, i * 3);
  }
  colorAttr.needsUpdate = true;

  marker.position.copy(toXYZ(lon, lat, R * 1.005));
  marker.visible = true;
  drawRing(lon, lat, radiusKm);
}

// ── directions panel: Google-Maps-style from/to over the city list ─────────
{
  const picked = { from: null, to: null };
  // Case- and diacritic-folded matching: "istanbul", "İSTANBUL" and
  // "ıstanbul" (Turkish keyboard) all find "Istanbul"; "kadikoy" finds
  // "Kadıköy". NFD strips combining marks; ı folds to i explicitly
  // (it has no combining mark to strip).
  const fold = (t) =>
    t
      .toLowerCase()
      .replaceAll("ı", "i")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "");
  const foldedNames = cities.map((c) => fold(c.n));
  const wire = (inputId, sugId, slot) => {
    const input = document.getElementById(inputId);
    const sug = document.getElementById(sugId);
    const hide = () => (sug.style.display = "none");
    input.addEventListener("input", () => {
      picked[slot] = null;
      const q = fold(input.value.trim());
      if (q.length < 2) return hide();
      // Prefix matches first, then substring — biggest cities on top.
      const starts = [];
      const contains = [];
      for (let i = 0; i < cities.length; i++) {
        const n = foldedNames[i];
        if (n.startsWith(q)) starts.push(cities[i]);
        else if (n.includes(q)) contains.push(cities[i]);
        if (starts.length > 400) break;
      }
      const byPop = (a, b) => b.p - a.p;
      const top = [...starts.sort(byPop), ...contains.sort(byPop)].slice(0, 8);
      if (!top.length) return hide();
      sug.innerHTML = top
        .map(
          (c, i) =>
            `<div data-i="${i}"><span>${flag(c.c)} ${c.n}</span><span class="cc">${fmtPop(Math.max(c.p, 1000))}</span></div>`
        )
        .join("");
      sug.style.display = "block";
      [...sug.children].forEach((el, i) => {
        el.addEventListener("mousedown", (e) => {
          e.preventDefault();
          picked[slot] = top[i];
          input.value = top[i].n;
          hide();
          maybeRoute();
        });
      });
    });
    input.addEventListener("blur", () => setTimeout(hide, 150));
  };
  const maybeRoute = () => {
    const { from, to } = picked;
    if (!from || !to) return;
    clearRoute();
    const a = { lon: from.lon, lat: from.lat };
    const b = { lon: to.lon, lat: to.lat };
    endA.position.copy(toXYZ(a.lon, a.lat, R * 1.004));
    endB.position.copy(toXYZ(b.lon, b.lat, R * 1.004));
    endA.visible = endB.visible = true;
    document.getElementById("dirStat").textContent = "routing…";
    routeBetween(a, b).then((r) => {
      document.getElementById("dirStat").textContent = r ? r.msg : "?";
      document.getElementById("dirStat").style.color = r?.ok ? "" : "var(--warm)";
    });
  };
  wire("fromCity", "fromSug", "from");
  wire("toCity", "toSug", "to");
}

// ── interaction ────────────────────────────────────────────────────────────
const ray = new THREE.Raycaster();
const ndc = new THREE.Vector2();
let downAt = null;
canvas.addEventListener("pointerdown", (e) => (downAt = [e.clientX, e.clientY]));
canvas.addEventListener("pointerup", (e) => {
  if (!downAt) return;
  const moved = Math.hypot(e.clientX - downAt[0], e.clientY - downAt[1]);
  downAt = null;
  if (moved > 4) return; // it was a drag
  ndc.set((e.clientX / innerWidth) * 2 - 1, -(e.clientY / innerHeight) * 2 + 1);
  ray.setFromCamera(ndc, camera);
  const hit = ray.intersectObject(globe, false)[0];
  if (!hit) return;
  const p = toLonLat(hit.point);
  picked = p;
  runQueries();
});
document.getElementById("nearList").addEventListener("click", (e) => {
  const li = e.target.closest("li[data-i]");
  if (!li) return;
  const d = lastNear[Number(li.dataset.i)];
  if (d) blinkAt(d.loc[0], d.loc[1]);
});
document.getElementById("autorotate").addEventListener("change", (e) => {
  controls.autoRotate = e.target.checked;
});
document.getElementById("radius").addEventListener("input", (e) => {
  document.getElementById("radiusKm").textContent = e.target.value;
  runQueries();
});
document.getElementById("minpop").addEventListener("input", runQueries);

// ── loop ───────────────────────────────────────────────────────────────────
function resize() {
  renderer.setSize(innerWidth, innerHeight, false);
  camera.aspect = innerWidth / innerHeight;
  camera.updateProjectionMatrix();
}
addEventListener("resize", resize);
resize();
renderer.setAnimationLoop(() => {
  // Drag speed proportional to altitude, or the last zoom levels are
  // untouchably twitchy (a fixed angular speed sweeps a whole small
  // country per pixel of drag when the camera is 300 km up).
  controls.rotateSpeed = THREE.MathUtils.clamp((camera.position.length() - 1) * 0.85, 0.05, 1);
  controls.update();
  if (countryLabels?.visible) {
    const showAll = camera.position.length() < 1.9;
    for (const s of countryLabels.children) s.visible = showAll || s.userData.r <= 4;
  }
  // Keep the pick marker a steady on-screen size: scale with camera
  // distance (capped so it never balloons when zoomed far out).
  if (marker.visible) {
    const d = camera.position.distanceTo(marker.position);
    marker.scale.setScalar(Math.min(1, d / 1.8));
  }
  for (const m of [endA, endB]) {
    if (m.visible) {
      const d = camera.position.distanceTo(m.position);
      m.scale.setScalar(Math.min(1, d / 1.8));
    }
  }
  if (blink.visible) {
    // Exactly two pulses: |sin| gives one 0→1→0 hump per 450 ms.
    const t = performance.now() - blinkStart;
    if (t > 900) {
      blink.visible = false;
    } else {
      const pulse = Math.abs(Math.sin((t * Math.PI) / 450));
      const d = camera.position.distanceTo(blink.position);
      blink.scale.setScalar((0.6 + 1.2 * pulse) * Math.min(1, d / 1.8));
      blinkMat.opacity = 0.15 + 0.8 * pulse;
    }
  }
  renderer.render(scene, camera);
});

stage(10, "ready");
runQueries();
document.getElementById("boot").style.opacity = "0";
setTimeout(() => document.getElementById("boot").remove(), 600);

// Persistence consent: asked once, after the first successful load. "Store"
// writes the exact downloaded bytes into the Cache API — nothing to go
// stale, next visits read them locally and skip the network.
(async () => {
  const clearRow = document.getElementById("clearStore");
  const showClear = () => (clearRow.style.display = "");
  document.getElementById("clearStoreLink").addEventListener("click", async (e) => {
    e.preventDefault();
    await caches.delete(DATA_CACHE);
    localStorage.removeItem("geoPersist");
    clearRow.style.display = "none";
  });
  const store = async () => {
    const cache = await caches.open(DATA_CACHE);
    for (const [url, buf] of rawBuffers) {
      await cache.put(
        url,
        new Response(buf, { headers: { "Content-Type": "application/json" } })
      );
    }
    rawBuffers.clear();
    localStorage.setItem("geoPersist", "yes");
    showClear();
  };
  if (persistChoice === "yes") {
    // Consent was already given. Anything the cache missed this visit
    // (evicted, or a data-version bump) was fetched and kept in
    // rawBuffers — refill silently instead of asking again.
    if ("caches" in window) await store();
    return;
  }
  if (persistChoice === "no" || !("caches" in window)) return;
  const ask = document.getElementById("persistAsk");
  ask.style.display = "";
  document.getElementById("persistYes").addEventListener("click", async () => {
    ask.style.display = "none";
    await store();
  });
  document.getElementById("persistNo").addEventListener("click", () => {
    ask.style.display = "none";
    rawBuffers.clear();
    localStorage.setItem("geoPersist", "no");
  });
})();
