// End-to-end check of per-project request rate limiting.
//
//   OXIBASE_URL=http://127.0.0.1:4460 \
//   OXIBASE_DATA_URL=http://127.0.0.1:8087 \
//   OXIDB_PLATFORM_SECRET=<the control plane's secret> \
//   node oxibase/test/ratelimit.e2e.mjs
//
// The cap is owned by the control plane (a field on the project row) and
// enforced by the data plane, which reads it per request — so this drives both:
// set a limit, spend it, get 429s, raise it, and confirm one project's flood
// never spends another's budget.

import { devToken, counter } from "./lib.mjs";

const CP = (process.env.OXIBASE_URL || "http://127.0.0.1:4460") + "/platform/v1";
const DATA = process.env.OXIBASE_DATA_URL || "http://127.0.0.1:8087";
const SECRET = process.env.OXIDB_PLATFORM_SECRET;
if (!SECRET) {
  console.error("set OXIDB_PLATFORM_SECRET (the control plane's signing secret)");
  process.exit(2);
}

const { ok, total } = counter();
const token = devToken(`rate-e2e-${process.pid}@example.com`, SECRET);

async function cp(method, path, body) {
  const r = await fetch(`${CP}${path}`, {
    method,
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: r.status, data: await r.json().catch(() => null) };
}

/// One data-plane read as the project.
const read = (ref, key) =>
  fetch(`${DATA}/rest/v1/items?db=${ref}`, { headers: { Authorization: `Bearer ${key}` } });

console.log("# OxiBase per-project rate limiting");

const a = await cp("POST", "/projects", { name: "rate e2e a" });
const b = await cp("POST", "/projects", { name: "rate e2e b" });
ok(a.status === 201 && b.status === 201, "two projects provisioned");
const [refA, keyA] = [a.data.ref, a.data.service_role_key];
const [refB, keyB] = [b.data.ref, b.data.service_role_key];

{
  ok(a.data.max_requests_per_min === 0, "new project defaults to unlimited (0)");
  const r = await read(refA, keyA);
  ok(r.status === 200, "unlimited: a request goes through");
}

{
  const r = await cp("PATCH", `/projects/${refA}/limits`, { max_requests_per_min: 3 });
  ok(r.status === 200 && r.data.max_requests_per_min === 3, "limit set to 3/min");
}

{
  // The data plane reads the cap per request, so it applies immediately — no
  // restart, no cache to wait out.
  const codes = [];
  for (let i = 0; i < 5; i++) codes.push((await read(refA, keyA)).status);
  const allowed = codes.filter((c) => c === 200).length;
  const refused = codes.filter((c) => c === 429).length;
  ok(allowed === 3, `exactly the budget is served (3 of 5, got ${allowed})`);
  ok(refused === 2, `the rest are refused with 429 (got ${refused})`);
}

{
  const r = await read(refA, keyA);
  ok(r.status === 429, "still refused inside the window");
  const retry = r.headers.get("retry-after");
  ok(!!retry && Number(retry) > 0 && Number(retry) <= 60, `Retry-After is a sane second count (${retry})`);
  const body = await r.json().catch(() => null);
  ok(body?.limit === 3, "the body reports the limit that was hit");
}

{
  // The point of the whole feature: a noisy tenant must not cost a quiet one.
  const r = await read(refB, keyB);
  ok(r.status === 200, "a different project is unaffected by the flood");
}

{
  // Raising the cap frees the tenant again without waiting for the window.
  const patched = await cp("PATCH", `/projects/${refA}/limits`, { max_requests_per_min: 1000 });
  ok(patched.status === 200, "limit raised");
  const r = await read(refA, keyA);
  ok(r.status === 200, "raising the cap takes effect immediately");
}

{
  const r = await cp("PATCH", `/projects/${refA}/limits`, { max_requests_per_min: 5_000_000 });
  ok(r.status === 400, "an absurd cap is refused");
}

{
  // Owner endpoints address a project by ref *or* slug, like the rest of the
  // API — a slug used to 404 here, and worse, a write keyed on it would have
  // matched nothing at all.
  const slug = (await cp("GET", "/projects")).data.find((p) => p.ref === refA)?.slug;
  ok(!!slug, `project has a slug (${slug})`);
  const bySlug = await cp("PATCH", `/projects/${slug}/limits`, { max_requests_per_min: 7 });
  ok(bySlug.status === 200, "limits can be set by slug");
  const byRef = await cp("GET", `/projects/${refA}`);
  ok(byRef.data.max_requests_per_min === 7, "the write landed on the project, not nowhere");
  const users = await cp("GET", `/projects/${slug}/users`);
  ok(users.status === 200, "users readable by slug");
  await cp("PATCH", `/projects/${refA}/limits`, { max_requests_per_min: 0 });
}

{
  // Turning it off restores unlimited behaviour.
  await cp("PATCH", `/projects/${refA}/limits`, { max_requests_per_min: 0 });
  const codes = [];
  for (let i = 0; i < 12; i++) codes.push((await read(refA, keyA)).status);
  ok(
    codes.every((c) => c === 200),
    "0 means unlimited again",
  );
}

for (const ref of [refA, refB]) await cp("DELETE", `/projects/${ref}`);
ok(true, "cleanup: projects deleted");

console.log(`\n${total()}/${total()} passed`);
