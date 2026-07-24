// e2e for oxibase-js: provision a throwaway project on a running OxiBase control
// plane, then drive the SDK end to end.
//
//   OXIBASE_CP_URL         control plane (default http://127.0.0.1:4460)
//   OXIBASE_DATA_URL       data plane REST (default http://127.0.0.1:8087)
//   OXIDB_PLATFORM_SECRET  the control plane's signing secret (required)
//
// Requires the SDK to be built first (npm run build runs it for you).

import { createClient } from "../dist/index.js";
import { devToken } from "../../oxibase/test/lib.mjs";

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
// Developer sign-in is Google-only, so the test mints its own session token
// with the deployment's platform secret instead of signing up.
const SECRET = process.env.OXIDB_PLATFORM_SECRET;
if (!SECRET) {
  console.error("set OXIDB_PLATFORM_SECRET (the control plane's signing secret)");
  process.exit(2);
}
const token = devToken(`sdk${process.pid}@example.com`, SECRET);
const pj = await cp("/projects", { token, body: { name: "sdk-e2e" } });
ok(pj.status === 201 && pj.data.ref, "control plane: create project");
const ref = pj.data.ref;
const key = pj.data.service_role_key;

// ── The query builder ───────────────────────────────────────────────────────
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

// ── Social sign-in surface ──────────────────────────────────────────────────
{
  const authed = createClient(DATA, key, {
    ref,
    authUrl: process.env.OXIBASE_CP_URL || "http://127.0.0.1:4460",
  });

  const s = await authed.auth.getSettings();
  ok(!s.error && s.password === true, `auth.getSettings: reachable (${s.error ?? ""})`);
  ok(Array.isArray(s.providers) && s.providers.length === 0, "auth.getSettings: no providers configured yet");

  // Configure GitHub on the project, then re-read.
  await fetch(`${CP}/projects/${ref}/auth/providers`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
    body: JSON.stringify({
      github: { client_id: "ghid", client_secret: "ghsecret" },
      redirect_urls: ["https://app.example.com/*"],
    }),
  });
  const s2 = await authed.auth.getSettings();
  ok(s2.providers.includes("github"), "auth.getSettings: sees the configured provider");

  // The authorize URL is built without navigating (no browser here).
  const started = authed.auth.signInWithOAuth({
    provider: "github",
    redirectTo: "https://app.example.com/done",
    navigate: false,
  });
  ok(!started.error && started.url.includes(`/projects/${ref}/auth/authorize/github`), "auth.signInWithOAuth: builds the authorize URL");
  ok(started.url.includes(encodeURIComponent("https://app.example.com/done")), "auth.signInWithOAuth: carries redirect_to");

  // Following it lands on GitHub's consent screen.
  const hop = await fetch(started.url, { redirect: "manual" });
  ok(hop.status === 302, "auth.signInWithOAuth: the URL redirects");
  ok(
    (hop.headers.get("location") ?? "").startsWith("https://github.com/login/oauth/authorize"),
    "auth.signInWithOAuth: redirects to GitHub",
  );

  // The callback hands the session over in the fragment; the SDK adopts it.
  const adopted = authed.auth.getSessionFromUrl(
    "https://app.example.com/done#access_token=tok123&refresh_token=ref456&token_type=bearer&expires_in=3600",
  );
  ok(adopted?.token === "tok123" && adopted?.refreshToken === "ref456", "auth.getSessionFromUrl: adopts the session");
  ok(authed.auth.getSession()?.token === "tok123", "auth.getSessionFromUrl: session is now current");
  const failed = authed.auth.getSessionFromUrl("https://app.example.com/done#error=access_denied");
  ok(failed?.error === "access_denied", "auth.getSessionFromUrl: surfaces a declined sign-in");
  ok(authed.auth.getSessionFromUrl("https://app.example.com/done") === null, "auth.getSessionFromUrl: null when there is no fragment");

  // A persisted session can be restored after a reload — without this, every
  // refresh of a real app silently signs the user out.
  authed.auth.signOut();
  ok(authed.auth.getSession() === null, "auth.signOut: back to the anon key");
  authed.auth.setSession({ token: "restored.tok", refreshToken: "restored.ref" });
  const restored = authed.auth.getSession();
  ok(restored?.token === "restored.tok" && restored?.refreshToken === "restored.ref", "auth.setSession: restores a persisted session");
}

// cleanup
await cp(`/projects/${ref}`, { token, body: undefined });
await fetch(`${CP}/projects/${ref}`, { method: "DELETE", headers: { Authorization: `Bearer ${token}` } });

console.log(`\n${pass} passed`);
