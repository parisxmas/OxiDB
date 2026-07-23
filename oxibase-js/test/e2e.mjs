// e2e for oxibase-js: provision a throwaway project on a running OxiBase control
// plane, then drive the SDK exactly like you'd use supabase-js.
//
//   OXIBASE_CP_URL   control plane (default http://127.0.0.1:4460)
//   OXIBASE_DATA_URL data plane REST (default http://127.0.0.1:8087)
//
// Requires the SDK to be built first (npm run build runs it for you).

import { createClient } from "../dist/index.js";

const CP = (process.env.OXIBASE_CP_URL || "http://127.0.0.1:4460") + "/platform/v1";
const DATA = process.env.OXIBASE_DATA_URL || "http://127.0.0.1:8087";

let pass = 0;
const ok = (c, m) => {
  if (c) { pass++; console.log("  ✓", m); }
  else { console.error("  ✗ FAIL:", m); process.exit(1); }
};

async function cp(path, { token, body } = {}) {
  const r = await fetch(`${CP}${path}`, {
    method: body ? "POST" : "GET",
    headers: { "Content-Type": "application/json", ...(token ? { Authorization: `Bearer ${token}` } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  return { status: r.status, data: await r.json().catch(() => null) };
}

// Provision a project → ref + service_role key (full access for the test).
const su = await cp("/signup", { body: { email: `sdk${process.pid}@example.com`, password: "hunter2hunter2" } });
ok(su.status === 201 && su.data.token, "control plane: signup");
const token = su.data.token;
const pj = await cp("/projects", { token, body: { name: "sdk-e2e" } });
ok(pj.status === 201 && pj.data.ref, "control plane: create project");
const ref = pj.data.ref;
const key = pj.data.service_role_key;

// ── Use it just like Supabase ───────────────────────────────────────────────
const oxibase = createClient(DATA, key, { ref });

// insert (returns representation by default in postgrest-js when you chain .select)
{
  const { data, error } = await oxibase.from("notes").insert({ body: "first note", done: false }).select();
  ok(!error, `insert: no error (${error?.message ?? ""})`);
  ok(Array.isArray(data) && data[0]?.body === "first note", "insert: returns the row");
}

// select
{
  const { data, error } = await oxibase.from("notes").select("*");
  ok(!error && Array.isArray(data) && data.length === 1, "select: one row");
}

// insert a second, then order + limit
await oxibase.from("notes").insert({ body: "second note", done: false });
{
  const { data, error } = await oxibase.from("notes").select("body").order("body", { ascending: false }).limit(1);
  ok(!error && data?.[0]?.body === "second note", "order desc + limit");
}

// filter with .eq()
{
  const { data, error } = await oxibase.from("notes").select("*").eq("body", "first note");
  ok(!error && data?.length === 1 && data[0].body === "first note", "filter .eq()");
}

// update with .eq()
{
  const { error } = await oxibase.from("notes").update({ done: true }).eq("body", "first note");
  ok(!error, "update .eq()");
  const { data } = await oxibase.from("notes").select("done").eq("body", "first note");
  ok(data?.[0]?.done === true, "update took effect");
}

// delete with .eq()
{
  const { error } = await oxibase.from("notes").delete().eq("body", "second note");
  ok(!error, "delete .eq()");
  const { data } = await oxibase.from("notes").select("*");
  ok(data?.length === 1, "one row after delete");
}

// ── SQL-engine extension ────────────────────────────────────────────────────
{
  await oxibase.sql("CREATE TABLE IF NOT EXISTS metrics (id INTEGER PRIMARY KEY, v INTEGER)");
  await oxibase.sql("INSERT INTO metrics (id, v) VALUES (1, 42)");
  const { results, error } = await oxibase.sql("SELECT v FROM metrics WHERE id = ?", [1]);
  ok(!error, `sql: no error (${error ?? ""})`);
  const last = results?.[results.length - 1];
  ok(last?.rows?.[0]?.[0] === 42, "sql: parameterized SELECT returns 42");
}

// cleanup
await cp(`/projects/${ref}`, { token, body: undefined });
await fetch(`${CP}/projects/${ref}`, { method: "DELETE", headers: { Authorization: `Bearer ${token}` } });

console.log(`\n${pass} passed`);
