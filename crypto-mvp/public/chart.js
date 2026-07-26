// chart.js — raw-WebGL line chart of BTCUSDT trades streamed from OxiDB.
//   Loads last N trades via REST, then subscribes to the WS feed for updates.

"use strict";

const REST_URL  = "http://127.0.0.1:9080";
const WS_URL    = "ws://127.0.0.1:9082/ws";
const COLLECTION = "trades";
const MAX_POINTS = 1000;
const SYMBOL     = "BTCUSDT";

// ─── Rolling buffer ────────────────────────────────────────────────────────
// Two parallel Float32Array slots keep a fixed-size ring of (ts, price).
const tsBuf    = new Float64Array(MAX_POINTS);
const priceBuf = new Float32Array(MAX_POINTS);
let count = 0;     // logical count (0..MAX_POINTS)

function pushTrade(ts, price) {
  if (count < MAX_POINTS) {
    tsBuf[count]    = ts;
    priceBuf[count] = price;
    count++;
  } else {
    tsBuf.copyWithin(0, 1, MAX_POINTS);
    priceBuf.copyWithin(0, 1, MAX_POINTS);
    tsBuf[MAX_POINTS - 1]    = ts;
    priceBuf[MAX_POINTS - 1] = price;
  }
}

// ─── WebGL setup ───────────────────────────────────────────────────────────
const canvas = document.getElementById("chart");
const gl = canvas.getContext("webgl", { antialias: true, alpha: false });
if (!gl) throw new Error("WebGL not supported");

const VS = `
attribute vec2 a_xy;
uniform vec4 u_bounds;        // [minX, maxX, minY, maxY]
void main() {
  float x = (a_xy.x - u_bounds.x) / max(u_bounds.y - u_bounds.x, 1e-6);
  float y = (a_xy.y - u_bounds.z) / max(u_bounds.w - u_bounds.z, 1e-6);
  // Pad y by 6% so the line never hugs the edges
  y = 0.07 + y * 0.86;
  gl_Position = vec4(x * 2.0 - 1.0, y * 2.0 - 1.0, 0.0, 1.0);
}`;
const FS = `
precision mediump float;
uniform vec3 u_color;
void main() { gl_FragColor = vec4(u_color, 1.0); }
`;

function compile(src, type) {
  const sh = gl.createShader(type);
  gl.shaderSource(sh, src);
  gl.compileShader(sh);
  if (!gl.getShaderParameter(sh, gl.COMPILE_STATUS)) {
    throw new Error("shader: " + gl.getShaderInfoLog(sh));
  }
  return sh;
}
const prog = gl.createProgram();
gl.attachShader(prog, compile(VS, gl.VERTEX_SHADER));
gl.attachShader(prog, compile(FS, gl.FRAGMENT_SHADER));
gl.linkProgram(prog);
if (!gl.getProgramParameter(prog, gl.LINK_STATUS)) {
  throw new Error("link: " + gl.getProgramInfoLog(prog));
}
gl.useProgram(prog);

const a_xy     = gl.getAttribLocation(prog,  "a_xy");
const u_bounds = gl.getUniformLocation(prog, "u_bounds");
const u_color  = gl.getUniformLocation(prog, "u_color");

const buf = gl.createBuffer();
gl.bindBuffer(gl.ARRAY_BUFFER, buf);
gl.bufferData(gl.ARRAY_BUFFER, MAX_POINTS * 2 * 4, gl.DYNAMIC_DRAW);
gl.enableVertexAttribArray(a_xy);
gl.vertexAttribPointer(a_xy, 2, gl.FLOAT, false, 0, 0);

const xy = new Float32Array(MAX_POINTS * 2);

function resize() {
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth  * dpr;
  const h = canvas.clientHeight * dpr;
  if (canvas.width !== w || canvas.height !== h) {
    canvas.width = w; canvas.height = h;
    gl.viewport(0, 0, w, h);
  }
}
window.addEventListener("resize", () => { resize(); render(); });

// ─── Stats DOM ─────────────────────────────────────────────────────────────
const $state    = document.getElementById("state");
const $statusEl = document.getElementById("status");
const $last     = document.getElementById("last");
const $min      = document.getElementById("min");
const $max      = document.getElementById("max");
const $count    = document.getElementById("count");
const $span     = document.getElementById("span");
const $crosshair = document.getElementById("crosshair");

function fmtMoney(n) {
  return "$" + n.toLocaleString(undefined, {
    minimumFractionDigits: 2, maximumFractionDigits: 2,
  });
}
function fmtSpan(ms) {
  if (ms < 60_000) return (ms / 1000).toFixed(0) + "s";
  if (ms < 3_600_000) return (ms / 60_000).toFixed(1) + "m";
  return (ms / 3_600_000).toFixed(1) + "h";
}

// ─── Render ────────────────────────────────────────────────────────────────
let rafQueued = false;
function queueRender() {
  if (rafQueued) return;
  rafQueued = true;
  requestAnimationFrame(() => { rafQueued = false; render(); });
}

function render() {
  resize();
  gl.clearColor(0.04, 0.047, 0.063, 1.0);
  gl.clear(gl.COLOR_BUFFER_BIT);
  if (count < 2) return;

  // X is the point INDEX, not the timestamp — millisecond timestamps blow
  // past Float32's 24-bit mantissa (~16 M) and collapse all X values onto
  // the same coordinate. Tooltip still uses the real timestamp.
  let minY = priceBuf[0], maxY = priceBuf[0];
  for (let i = 1; i < count; i++) {
    const p = priceBuf[i];
    if (p < minY) minY = p;
    if (p > maxY) maxY = p;
  }
  if (maxY === minY) { maxY = minY + 1; }
  const minX = 0, maxX = count - 1;

  // Pack into vertex buffer using indexed X.
  for (let i = 0; i < count; i++) {
    xy[i * 2]     = i;
    xy[i * 2 + 1] = priceBuf[i];
  }

  gl.bindBuffer(gl.ARRAY_BUFFER, buf);
  gl.bufferSubData(gl.ARRAY_BUFFER, 0, xy.subarray(0, count * 2));
  gl.uniform4f(u_bounds, minX, maxX, minY, maxY);

  // Decide line color from the trend (last vs first).
  const up = priceBuf[count - 1] >= priceBuf[0];
  if (up) gl.uniform3f(u_color, 0.51, 0.66, 0.54);   // green
  else    gl.uniform3f(u_color, 0.85, 0.42, 0.42);   // red

  gl.drawArrays(gl.LINE_STRIP, 0, count);

  // Stats
  $last.textContent  = fmtMoney(priceBuf[count - 1]);
  $min.textContent   = fmtMoney(minY);
  $max.textContent   = fmtMoney(maxY);
  $count.textContent = count;
  $span.textContent  = fmtSpan(tsBuf[count - 1] - tsBuf[0]);
}

// ─── Crosshair tooltip ────────────────────────────────────────────────────
canvas.addEventListener("mousemove", (e) => {
  if (count < 2) return;
  const rect = canvas.getBoundingClientRect();
  const x = (e.clientX - rect.left) / rect.width;
  const idx = Math.min(count - 1, Math.max(0, Math.floor(x * count)));
  const ts    = tsBuf[idx];
  const price = priceBuf[idx];
  $crosshair.style.display = "block";
  $crosshair.style.left = (e.clientX + 12) + "px";
  $crosshair.style.top  = (e.clientY + 12) + "px";
  $crosshair.innerHTML  =
    `<b>${fmtMoney(price)}</b><br>` +
    new Date(ts).toLocaleTimeString();
});
canvas.addEventListener("mouseleave", () => { $crosshair.style.display = "none"; });

// ─── Status helpers ───────────────────────────────────────────────────────
function setStatus(text, live) {
  $state.textContent = text;
  $statusEl.classList.toggle("live", !!live);
}

// ─── Boot — uses the OxiDB SDK loaded via /oxidb.js ───────────────────────
//   Polling-based realtime: 250 ms incremental REST fetch keyed off the
//   highest `_id` we've seen. Cross-connection WebSocket subscriptions
//   currently drop events on this server build, so we sidestep them.
(async () => {
  document.title = `${SYMBOL} · OxiDB`;
  setStatus("loading…", false);

  const db = new OxiDB(REST_URL);
  const trades = db.collection(COLLECTION);

  // Initial snapshot — last MAX_POINTS, ascending for the chart.
  const initial = await trades.find({}, { sort: { ts: -1 }, limit: MAX_POINTS });
  initial.reverse();
  for (const d of initial) pushTrade(d.ts, d.price);
  let lastId = initial.length ? initial[initial.length - 1]._id : 0;
  queueRender();

  setStatus("live", true);

  // Polling loop — fetch only docs with _id > lastId.
  setInterval(async () => {
    try {
      const fresh = await trades.find(
        { _id: { $gt: lastId } },
        { sort: { _id: 1 }, limit: 500 },
      );
      if (fresh.length === 0) return;
      for (const d of fresh) {
        pushTrade(d.ts, d.price);
        if (d._id > lastId) lastId = d._id;
      }
      queueRender();
    } catch (e) {
      console.warn("poll error:", e.message);
    }
  }, 250);
})().catch((e) => {
  console.error(e);
  setStatus("error: " + e.message, false);
});
