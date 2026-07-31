// OxiDB Geo Globe — 10k cities in an OxiDB document database compiled to
// WebAssembly, rendered as a Three.js point cloud. Click the globe: a real
// `$near` query ranks the nearest cities; the slider runs `$geoWithin`
// with a spherical cap. No server anywhere — the database is in this tab.
import * as THREE from "three";
import { OrbitControls } from "./OrbitControls.js";
import init, * as oxidb from "./pkg/oxidb_wasm.js";

const R = 1; // globe radius in scene units
const EARTH_KM = 6371.0088;

// ── boot: engine + data + index ─────────────────────────────────────────────
const bootmsg = document.getElementById("bootmsg");
bootmsg.textContent = "loading engine…";
await init();
oxidb.init();

bootmsg.textContent = "loading 10,000 cities…";
const cities = await (await fetch("./cities.json")).json();

// Insert as documents: {name, country, pop, loc: [lon, lat]}.
const t0 = performance.now();
const docs = cities.map((c) => ({ n: c.n, c: c.c, p: c.p, loc: [c.lon, c.lat] }));
const BATCH = 2000;
for (let i = 0; i < docs.length; i += BATCH) {
  oxidb.insert_many("cities", JSON.stringify(docs.slice(i, i + BATCH)));
}
const insertMs = performance.now() - t0;

bootmsg.textContent = "building geohash index…";
const t1 = performance.now();
oxidb.create_geo_index("cities", "loc");
const indexMs = performance.now() - t1;

document.getElementById("docCount").textContent =
  `${cities.length.toLocaleString()} (${insertMs.toFixed(0)} ms)`;
document.getElementById("indexMs").textContent = `${indexMs.toFixed(0)} ms`;

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
controls.minDistance = 1.3;
controls.maxDistance = 6;
controls.enablePan = false;
controls.autoRotate = true;
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
const landPolys = await (await fetch("./land.json")).json();
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

// Country borders (Natural Earth 110m admin-0, compacted to rings).
// Long edges are subdivided so a segment hugs the sphere instead of
// chording through it.
{
  const rings = await (await fetch("./borders.json")).json();
  const verts = [];
  for (const ring of rings) {
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
        prev = cur;
      }
    }
  }
  const g = new THREE.BufferGeometry();
  g.setAttribute("position", new THREE.BufferAttribute(new Float32Array(verts), 3));
  scene.add(
    new THREE.LineSegments(
      g,
      new THREE.LineBasicMaterial({ color: 0x28425f, transparent: true, opacity: 0.9 })
    )
  );
}

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
  sizes[i] = 1.4 + Math.min(2.2, Math.log10(c.p) - 4.5); // population-weighted
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
      gl_PointSize = psize * (8.0 / -mv.z) * ${Math.min(devicePixelRatio, 2).toFixed(1)};
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
  new THREE.SphereGeometry(0.012, 16, 16),
  new THREE.MeshBasicMaterial({ color: 0xff5252 })
);
marker.visible = false;
scene.add(marker);
const ringMat = new THREE.LineBasicMaterial({ color: 0xffb74d, transparent: true, opacity: 0.85 });
let ring = null;

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

function runQueries() {
  const { lon, lat } = picked;
  const radiusKm = Number(document.getElementById("radius").value);

  // $near — nearest cities, ranked by the engine (capped at 3000 km so the
  // index cover, not a whole-planet scan, serves it).
  const nearQ = {
    loc: { $near: { $geometry: { type: "Point", coordinates: [lon, lat] }, $maxDistance: 3_000_000 } },
  };
  let t = performance.now();
  const near = JSON.parse(oxidb.find("cities", JSON.stringify(nearQ)));
  const nearMs = performance.now() - t;

  // $geoWithin — spherical cap of the slider's radius.
  const withinQ = {
    loc: { $geoWithin: { $centerSphere: [[lon, lat], radiusKm / EARTH_KM] } },
  };
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
  listEl.innerHTML = near
    .map((d) => {
      const km = hav([lon, lat], d.loc);
      return `<li><span class="name">${flag(d.c)} ${d.n}</span><span class="km">${
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
  controls.autoRotate = false;
  picked = toLonLat(hit.point);
  runQueries();
});
document.getElementById("radius").addEventListener("input", (e) => {
  document.getElementById("radiusKm").textContent = e.target.value;
  runQueries();
});

// ── loop ───────────────────────────────────────────────────────────────────
function resize() {
  renderer.setSize(innerWidth, innerHeight, false);
  camera.aspect = innerWidth / innerHeight;
  camera.updateProjectionMatrix();
}
addEventListener("resize", resize);
resize();
renderer.setAnimationLoop(() => {
  controls.update();
  renderer.render(scene, camera);
});

runQueries();
document.getElementById("boot").style.opacity = "0";
setTimeout(() => document.getElementById("boot").remove(), 600);
