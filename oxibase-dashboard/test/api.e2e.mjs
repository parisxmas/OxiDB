// End-to-end check of the OxiBase control-plane API the dashboard consumes.
// Mirrors src/api.ts's calls (signup -> create -> list -> get -> rotate ->
// delete) against a running oxibase. Exits 0 on success, 1 on first failure.
//
//   OXIBASE_URL=http://127.0.0.1:4460 node test/api.e2e.mjs

const BASE = process.env.OXIBASE_URL || "http://127.0.0.1:4460";
let token = null;
let passed = 0;

function ok(cond, msg) {
  if (!cond) {
    console.error(`  ✗ ${msg}`);
    process.exit(1);
  }
  passed++;
  console.log(`  ✓ ${msg}`);
}

async function req(method, path, body, auth = true) {
  const headers = { "Content-Type": "application/json" };
  if (auth && token) headers["Authorization"] = `Bearer ${token}`;
  const res = await fetch(`${BASE}/platform/v1${path}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  const data = text ? JSON.parse(text) : null;
  return { status: res.status, data };
}

const email = `dev${Date.now() % 100000}@example.com`;

console.log("# OxiBase dashboard API contract");
{
  const r = await req("POST", "/signup", { email, password: "password1" }, false);
  ok(r.status === 201 && r.data.token, "signup -> 201 + token");
  token = r.data.token;
}
{
  const r = await req("GET", "/projects");
  ok(r.status === 200 && Array.isArray(r.data), "list projects -> [] (empty)");
  ok(r.data.length === 0, "no projects yet");
}
let ref, anon, service;
{
  const r = await req("POST", "/projects", { name: "dashboard test" });
  ok(r.status === 201 && r.data.ref, "create project -> 201 + ref");
  ok(!!r.data.anon_key && !!r.data.service_role_key, "create returns anon + service_role keys");
  ref = r.data.ref;
  anon = r.data.anon_key;
  service = r.data.service_role_key;
}
{
  const r = await req("GET", "/projects");
  ok(r.data.length === 1 && r.data[0].ref === ref, "list now shows the project");
  ok(!r.data[0].anon_key, "list omits secret keys");
}
{
  const r = await req("GET", `/projects/${ref}`);
  ok(r.status === 200 && r.data.anon_key === anon, "get project re-derives the same anon key");
}
{
  const r = await req("POST", `/projects/${ref}/keys/rotate`);
  ok(r.status === 200 && r.data.anon_key && r.data.anon_key !== anon, "rotate -> new anon key differs");
  ok(r.data.service_role_key !== service, "rotate -> new service_role key differs");
}
{
  const r = await req("DELETE", `/projects/${ref}`);
  ok(r.status === 200, "delete project -> 200");
  const l = await req("GET", "/projects");
  ok(l.data.length === 0, "project gone after delete");
}
// Guard: unauthenticated project list is rejected.
{
  const saved = token;
  token = null;
  const r = await req("GET", "/projects");
  ok(r.status === 401, "no token -> 401");
  token = saved;
}

console.log(`\n✅ ALL ${passed} assertions passed — the dashboard's API works end-to-end.`);
