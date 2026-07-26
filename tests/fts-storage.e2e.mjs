// Full-text search over stored file contents, via REST.
//
// The blob FTS index holds text extracted from uploaded files (HTML, XML, JSON,
// PDF, DOCX, XLSX, OCR'd images). It was wire-only; this covers the endpoint that
// makes it reachable from a project — including that a bucket's read rule governs
// searching it, not only downloading from it.
//
//   OXIDB_REST_URL=http://127.0.0.1:8091 \
//   OXIDB_JWT_SECRET=<the server's secret> \
//   [OXIDB_RESTART_CMD=<restart the server, same data dir>] \
//   node tests/fts-storage.e2e.mjs

import { createHmac } from "node:crypto";

const BASE = process.env.OXIDB_REST_URL || "http://127.0.0.1:8091";
const SECRET = process.env.OXIDB_JWT_SECRET;
if (!SECRET) {
  console.error("set OXIDB_JWT_SECRET (the server's REST JWT secret)");
  process.exit(2);
}

let pass = 0;
const fails = [];
const ok = (c, m) => {
  if (c) { pass++; console.log("  ok   " + m); }
  else { fails.push(m); console.error("  FAIL " + m); }
};

const b64 = (b) => Buffer.from(b).toString("base64url");
function token(sub, role) {
  const now = Math.floor(Date.now() / 1000);
  const h = b64(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const p = b64(JSON.stringify({ sub, role, iat: now, exp: now + 900 }));
  return `${h}.${p}.${b64(createHmac("sha256", SECRET).update(`${h}.${p}`).digest())}`;
}
const ADMIN = token("admin@test", "admin");
const ANON = token("anon", "read");

async function call(method, path, { body, tok = ADMIN, raw, type } = {}) {
  const headers = { Authorization: `Bearer ${tok}` };
  headers["Content-Type"] = type ?? "application/json";
  const r = await fetch(`${BASE}${path}`, {
    method,
    headers,
    body: raw !== undefined ? raw : body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await r.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  return { status: r.status, data };
}

const OPEN = `ftsfiles${process.pid}`;
const SHUT = `ftsshut${process.pid}`;
const search = (bucket, query, opts = {}, tok = ANON) =>
  call("POST", "/api/storage/_search", { body: { bucket, query, ...opts }, tok });

console.log("# full-text search over stored files");

// ── upload a corpus ─────────────────────────────────────────────────────────
// "quota" twice in one file, once in another: ranking has to tell them apart.
await call("PUT", `/api/storage/${OPEN}/handbook.txt`, {
  raw: "the quota chapter explains a quota in detail", type: "text/plain",
});
await call("PUT", `/api/storage/${OPEN}/notes.txt`, {
  raw: "a passing mention of quota here", type: "text/plain",
});
await call("PUT", `/api/storage/${OPEN}/unrelated.txt`, {
  raw: "avocados, entirely", type: "text/plain",
});
await call("PUT", `/api/storage/${SHUT}/private.txt`, {
  raw: "confidential quota planning", type: "text/plain",
});

// Indexing is a background worker, so give it a moment before asking.
let rows = [];
for (let i = 0; i < 40; i++) {
  const r = await search(OPEN, "quota", { limit: 10 });
  rows = Array.isArray(r.data) ? r.data : [];
  if (rows.length >= 2) break;
  await new Promise((res) => setTimeout(res, 250));
}
ok(rows.length === 2, `only the matching files come back (${rows.length} of 3)`);
ok(rows[0]?.key === "handbook.txt", "ranked: the file mentioning it twice is first");
ok(!rows.some((h) => h.key === "unrelated.txt"), "the unrelated file is absent");
ok(rows.every((h) => h.bucket === OPEN), "hits carry their bucket");

const miss = await search(OPEN, "kangaroo");
ok(Array.isArray(miss.data) && miss.data.length === 0, "a term nobody wrote returns nothing");

// ── the shape of the request ────────────────────────────────────────────────
const noBucket = await call("POST", "/api/storage/_search", { body: { query: "quota" }, tok: ANON });
ok(noBucket.status === 400, `a search with no bucket is refused (${noBucket.status})`);
const noQuery = await search(OPEN, undefined);
ok(noQuery.status === 400, `a search with no query is refused (${noQuery.status})`);

// ── highlights ──────────────────────────────────────────────────────────────
const hl = await search(OPEN, "quota", { limit: 3, highlight: true });
const snippets = JSON.stringify(hl.data ?? {});
ok(hl.status === 200 && snippets.includes("<mark>"), "highlighted snippets mark the term");

// ── a bucket closed by a rule cannot be searched either ─────────────────────
const beforeRule = await search(SHUT, "quota");
ok((beforeRule.data ?? []).length === 1, "the second bucket is searchable to begin with");
await call("POST", `/api/rules/${SHUT}`, { body: { read: "false" } });
const afterRule = await search(SHUT, "quota");
ok(afterRule.status === 403, `read:false refuses the search, not just the download (${afterRule.status})`);
const asAdmin = await search(SHUT, "quota", {}, ADMIN);
ok((asAdmin.data ?? []).length === 1, "the service key still sees it, as it bypasses rules");

// ── survives a restart (the index is persisted, unlike collection ones were) ─
if (process.env.OXIDB_RESTART_CMD) {
  const { execSync } = await import("node:child_process");
  execSync(process.env.OXIDB_RESTART_CMD, { stdio: "ignore" });
  for (let i = 0; i < 60; i++) {
    try { if ((await fetch(`${BASE}/api/ping`)).ok) break; } catch {}
    await new Promise((r) => setTimeout(r, 500));
  }
  const after = await search(OPEN, "quota", { limit: 10 });
  const rows2 = Array.isArray(after.data) ? after.data : [];
  ok(after.status === 200 && rows2.length === 2, `still searchable after a restart (${rows2.length} hits)`);
  ok(rows2[0]?.key === "handbook.txt", "and still ranked");
} else {
  console.log("  … set OXIDB_RESTART_CMD to also cover surviving a restart");
}

// ── cleanup ─────────────────────────────────────────────────────────────────
await call("DELETE", `/api/rules/${SHUT}`);
for (const [b, keys] of [[OPEN, ["handbook.txt", "notes.txt", "unrelated.txt"]], [SHUT, ["private.txt"]]]) {
  for (const k of keys) await call("DELETE", `/api/storage/${b}/${k}`);
  await call("DELETE", `/api/storage/${b}`);
}

console.log(fails.length ? `\nFAILED (${fails.length}): ${fails.join("; ")}` : `\n${pass} assertions passed`);
process.exit(fails.length ? 1 : 0);
