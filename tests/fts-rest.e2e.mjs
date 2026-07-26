// Full-text search over the REST surface: ranking, highlights, and every gate.
//
// The engine has had BM25 since the beginning, but only over the wire — this
// covers the REST endpoints that make it reachable from a browser or an OxiBase
// project.
//
//   OXIDB_REST_URL=http://127.0.0.1:8091 \
//   OXIDB_JWT_SECRET=<the server's secret> \
//   node tests/fts-rest.e2e.mjs
//
// The secret is used to mint the three roles this has to distinguish: admin,
// an end-user (authenticated), and the browser-safe anon key (read).

import { createHmac } from "node:crypto";

const BASE = process.env.OXIDB_REST_URL || "http://127.0.0.1:8091";
const SECRET = process.env.OXIDB_JWT_SECRET;
if (!SECRET) {
  console.error("set OXIDB_JWT_SECRET (the server's REST JWT secret)");
  process.exit(2);
}

let pass = 0;
const fails = [];
const ok = (cond, msg) => {
  if (cond) { pass++; console.log("  ok   " + msg); }
  else { fails.push(msg); console.error("  FAIL " + msg); }
};

const b64 = (b) => Buffer.from(b).toString("base64url");
function token(sub, role) {
  const now = Math.floor(Date.now() / 1000);
  const h = b64(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const p = b64(JSON.stringify({ sub, role, iat: now, exp: now + 600 }));
  const sig = b64(createHmac("sha256", SECRET).update(`${h}.${p}`).digest());
  return `${h}.${p}.${sig}`;
}
const ADMIN = token("admin@test", "admin");
const USER = token("ada@test", "authenticated");
const ANON = token("anon", "read");

async function call(method, path, { body, tok = ADMIN } = {}) {
  const r = await fetch(`${BASE}${path}`, {
    method,
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${tok}` },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await r.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  return { status: r.status, data };
}

const COL = `fts_test_${process.pid}`;
const RULED = `fts_ruled_${process.pid}`;
const CLOSED = `fts_closed_${process.pid}`;

console.log("# full-text search over REST");

// ── corpus ──────────────────────────────────────────────────────────────────
// "storage" appears twice in one document and once in another: BM25 must put the
// denser one first, which is the whole point of ranking over substring matching.
await call("POST", `/api/${COL}/documents`, {
  body: { docs: [
    { id: 1, title: "storage engine", body: "the storage engine writes storage pages" },
    { id: 2, title: "query planner", body: "the planner picks an index for storage" },
    { id: 3, title: "unrelated", body: "nothing to see about avocados here" },
  ] },
});
const idx = await call("POST", `/api/${COL}/text_index`, { body: { fields: ["title", "body"] } });
ok(idx.status === 200, `text index created (${idx.status})`);

// ── ranking ─────────────────────────────────────────────────────────────────
const hits = await call("POST", `/api/${COL}/text_search`, { body: { query: "storage", limit: 10 } });
ok(hits.status === 200, `search answered (${hits.status})`);
const rows = Array.isArray(hits.data) ? hits.data : [];
ok(rows.length === 2, `only matching documents come back (${rows.length} of 3)`);
ok(rows[0]?.id === 1, "ranked: the document mentioning it twice is first");
ok(!rows.some((d) => d.id === 3), "the unrelated document is absent");

const none = await call("POST", `/api/${COL}/text_search`, { body: { query: "kangaroo" } });
ok(Array.isArray(none.data) && none.data.length === 0, "a term nobody used returns nothing");

const noQuery = await call("POST", `/api/${COL}/text_search`, { body: {} });
ok(noQuery.status === 400, `a search with no query is refused (${noQuery.status})`);

// ── highlights ──────────────────────────────────────────────────────────────
const hl = await call("POST", `/api/${COL}/text_search`, {
  body: { query: "storage", limit: 5, highlight: { snippet_chars: 40, max_snippets: 2 } },
});
const first = Array.isArray(hl.data) ? hl.data[0] : null;
ok(hl.status === 200 && !!first, `highlighted search answered (${hl.status})`);
ok(JSON.stringify(first ?? {}).includes("storage"), "the snippet carries the matched term");

// ── who may search, who may index ───────────────────────────────────────────
const anonSearch = await call("POST", `/api/${COL}/text_search`, { body: { query: "storage" }, tok: ANON });
ok(anonSearch.status === 200, `the browser key may search an unruled collection (${anonSearch.status})`);

const anonIndex = await call("POST", `/api/${COL}/text_index`, { body: { fields: ["body"] }, tok: ANON });
ok(anonIndex.status === 403, `the browser key may not build an index (${anonIndex.status})`);

// ── a closed collection stays closed ────────────────────────────────────────
await call("POST", `/api/${CLOSED}/documents`, { body: { doc: { body: "secret storage notes" } } });
await call("POST", `/api/${CLOSED}/text_index`, { body: { fields: ["body"] } });
await call("POST", `/api/rules/${CLOSED}`, { body: { read: "false" } });
const closed = await call("POST", `/api/${CLOSED}/text_search`, { body: { query: "storage" }, tok: ANON });
ok(closed.status === 403, `read:false refuses the search outright (${closed.status})`);

// ── a row-level rule filters the matches rather than refusing them ──────────
await call("POST", `/api/${RULED}/documents`, {
  body: { docs: [
    { owner: "ada@test", body: "ada's own storage notes" },
    { owner: "someone@else", body: "somebody else's storage notes" },
  ] },
});
await call("POST", `/api/${RULED}/text_index`, { body: { fields: ["body"] } });
await call("POST", `/api/rules/${RULED}`, { body: { read: "auth.username == doc.owner" } });

const asUser = await call("POST", `/api/${RULED}/text_search`, { body: { query: "storage" }, tok: USER });
const userRows = Array.isArray(asUser.data) ? asUser.data : [];
ok(asUser.status === 200, `a row-level rule still allows the search (${asUser.status})`);
ok(userRows.length === 1, `filtered to the caller's own rows (${userRows.length} of 2)`);
ok(userRows[0]?.owner === "ada@test", "and it is the caller's row");

const asAdmin = await call("POST", `/api/${RULED}/text_search`, { body: { query: "storage" } });
ok((asAdmin.data ?? []).length === 2, "the service key sees both, as it bypasses rules");

// ── the index must survive a restart ────────────────────────────────────────
// Only the definition is persisted; the postings are rebuilt from the documents
// at open. Without that, a text index quietly disappeared on every deploy and
// every search answered "no text index" — invisible until something restarted.
if (process.env.OXIDB_RESTART_CMD) {
  const { execSync } = await import("node:child_process");
  execSync(process.env.OXIDB_RESTART_CMD, { stdio: "ignore" });
  for (let i = 0; i < 40; i++) {
    try {
      const r = await fetch(`${BASE}/api/ping`);
      if (r.ok) break;
    } catch {}
    await new Promise((r) => setTimeout(r, 500));
  }
  const after = await call("POST", `/api/${COL}/text_search`, { body: { query: "storage", limit: 10 } });
  const rows2 = Array.isArray(after.data) ? after.data : [];
  ok(after.status === 200, `search still works after a restart (${after.status})`);
  ok(rows2.length === 2, `the index was rebuilt (${rows2.length} hits)`);
  ok(rows2[0]?.id === 1, "and it still ranks correctly");
} else {
  console.log("  … set OXIDB_RESTART_CMD to also cover surviving a restart");
}

// ── cleanup ─────────────────────────────────────────────────────────────────
for (const c of [COL, RULED, CLOSED]) {
  await call("DELETE", `/api/rules/${c}`);
  await call("DELETE", `/api/collections/${c}`);
}

console.log(fails.length ? `\nFAILED (${fails.length}): ${fails.join("; ")}` : `\n${pass} assertions passed`);
process.exit(fails.length ? 1 : 0);
