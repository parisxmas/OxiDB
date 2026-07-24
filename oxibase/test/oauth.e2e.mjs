// End-to-end check of per-project end-user OAuth against a running OxiBase.
//
//   OXIBASE_URL=http://127.0.0.1:4460 \
//   OXIDB_PLATFORM_SECRET=<the control plane's secret> \
//   node oxibase/test/oauth.e2e.mjs
//
// Everything up to the provider round-trip is covered: configuration, the
// public discovery endpoint, the authorize redirect, the redirect allow-list,
// and state validation on the callback. The code-for-token exchange itself
// needs real provider credentials and is not exercised here.

import { devToken, counter } from "./lib.mjs";

const BASE = (process.env.OXIBASE_URL || "http://127.0.0.1:4460") + "/platform/v1";
const SECRET = process.env.OXIDB_PLATFORM_SECRET;
if (!SECRET) {
  console.error("set OXIDB_PLATFORM_SECRET (the control plane's signing secret)");
  process.exit(2);
}

const { ok, total } = counter();
const token = devToken(`oauth-e2e-${process.pid}@example.com`, SECRET);

async function api(method, path, { body, auth = true, redirect = "manual" } = {}) {
  const res = await fetch(`${BASE}${path}`, {
    method,
    redirect,
    headers: {
      "Content-Type": "application/json",
      ...(auth ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }
  return { status: res.status, data, location: res.headers.get("location") };
}

console.log("# OxiBase end-user OAuth");

// ── provision ───────────────────────────────────────────────────────────────
const created = await api("POST", "/projects", { body: { name: "oauth e2e" } });
ok(created.status === 201 && created.data.ref, "create project");
const ref = created.data.ref;

// ── configuration ───────────────────────────────────────────────────────────
{
  const r = await api("GET", `/projects/${ref}/auth/providers`);
  ok(r.status === 200, "providers: readable by the owner");
  ok(r.data.google.client_id === null && !r.data.google.secret_set, "providers: google unset initially");
  ok(
    r.data.github.callback_url.endsWith(`/projects/${ref}/auth/callback/github`),
    "providers: callback URL is derived and shown",
  );
}
{
  const r = await api("GET", `/projects/${ref}/auth/providers`, { auth: false });
  ok(r.status === 401, "providers: unauthenticated read is refused");
}
{
  const r = await api("PATCH", `/projects/${ref}/auth/providers`, {
    body: {
      google: { client_id: "gid.apps.googleusercontent.com", client_secret: "g-secret" },
      github: { client_id: "ghid", client_secret: "gh-secret" },
      redirect_urls: ["https://app.example.com/callback", "https://app.example.com/auth/*"],
    },
  });
  ok(r.status === 200, "configure: both providers accepted");
  ok(r.data.google.secret_set && r.data.github.secret_set, "configure: secrets recorded");
  const serialized = JSON.stringify(r.data);
  ok(
    !serialized.includes("g-secret") && !serialized.includes("gh-secret"),
    "configure: the response never echoes a client secret",
  );
}
{
  // A wildcard that could widen the host must be refused at configuration time.
  const r = await api("PATCH", `/projects/${ref}/auth/providers`, {
    body: { redirect_urls: ["https://*"] },
  });
  ok(r.status === 400, "configure: host-widening wildcard is rejected");
}

// ── public discovery ────────────────────────────────────────────────────────
{
  const r = await api("GET", `/projects/${ref}/auth/settings`, { auth: false });
  ok(r.status === 200, "settings: public, no auth needed");
  ok(
    r.data.providers.includes("google") && r.data.providers.includes("github"),
    "settings: lists the configured providers",
  );
  ok(r.data.google_client_id === "gid.apps.googleusercontent.com", "settings: exposes the public client id");
  ok(!JSON.stringify(r.data).includes("secret"), "settings: no secret material");
}

// ── authorize ───────────────────────────────────────────────────────────────
let stateBlob;
{
  const r = await api(
    "GET",
    `/projects/${ref}/auth/authorize/github?redirect_to=${encodeURIComponent("https://app.example.com/callback")}`,
    { auth: false },
  );
  ok(r.status === 302, "authorize: redirects to the provider");
  const u = new URL(r.location);
  ok(u.origin + u.pathname === "https://github.com/login/oauth/authorize", "authorize: GitHub consent URL");
  ok(u.searchParams.get("client_id") === "ghid", "authorize: carries the project's client id");
  ok(
    u.searchParams.get("redirect_uri").endsWith(`/projects/${ref}/auth/callback/github`),
    "authorize: redirect_uri is our callback",
  );
  ok(u.searchParams.get("scope").includes("user:email"), "authorize: requests the email scope");
  stateBlob = u.searchParams.get("state");
  ok(!!stateBlob, "authorize: carries a signed state");
}
{
  const r = await api(
    "GET",
    `/projects/${ref}/auth/authorize/github?redirect_to=${encodeURIComponent("https://app.example.com/auth/done?x=1")}`,
    { auth: false },
  );
  ok(r.status === 302, "authorize: wildcard entry matches a deeper path");
}
{
  const r = await api(
    "GET",
    `/projects/${ref}/auth/authorize/github?redirect_to=${encodeURIComponent("https://evil.example.com/steal")}`,
    { auth: false },
  );
  ok(r.status === 403, "authorize: an unlisted redirect_to is refused");
}
{
  const r = await api("GET", `/projects/${ref}/auth/authorize/github`, { auth: false });
  ok(r.status === 400, "authorize: redirect_to is required");
}
{
  const r = await api("GET", `/projects/${ref}/auth/authorize/twitter?redirect_to=x`, { auth: false });
  ok(r.status === 404, "authorize: unknown provider");
}

// ── callback ────────────────────────────────────────────────────────────────
{
  const r = await api("GET", `/projects/${ref}/auth/callback/github?code=abc&state=forged.sig`, {
    auth: false,
  });
  ok(r.status === 400, "callback: a forged state is refused");
}
{
  const r = await api("GET", `/projects/${ref}/auth/callback/github?code=abc`, { auth: false });
  ok(r.status === 400, "callback: a missing state is refused");
}
{
  // A state minted for the *google* flow must not be replayable on github's.
  const r = await api(
    "GET",
    `/projects/${ref}/auth/callback/google?code=abc&state=${encodeURIComponent(stateBlob)}`,
    { auth: false },
  );
  ok(r.status === 400, "callback: a state is bound to its provider");
}
{
  // The provider declining is reported back to the app, on the pre-registered
  // URL, in the fragment — never as a raw error page.
  const r = await api(
    "GET",
    `/projects/${ref}/auth/callback/github?error=access_denied&state=${encodeURIComponent(stateBlob)}`,
    { auth: false },
  );
  ok(r.status === 302, "callback: a declined sign-in redirects back to the app");
  ok(
    r.location.startsWith("https://app.example.com/callback#error=access_denied"),
    "callback: the error lands in the fragment of the allowed URL",
  );
}

{
  // Revoking a redirect URL takes effect for flows already in progress: the
  // state is still validly signed, but the target is no longer allowed.
  await api("PATCH", `/projects/${ref}/auth/providers`, {
    body: { redirect_urls: ["https://other.example.com/callback"] },
  });
  const r = await api(
    "GET",
    `/projects/${ref}/auth/callback/github?error=access_denied&state=${encodeURIComponent(stateBlob)}`,
    { auth: false },
  );
  ok(r.status === 403, "callback: a redirect URL revoked mid-flow is refused");
  // Put it back for the remaining assertions.
  await api("PATCH", `/projects/${ref}/auth/providers`, {
    body: { redirect_urls: ["https://app.example.com/callback", "https://app.example.com/auth/*"] },
  });
}

// ── clearing ────────────────────────────────────────────────────────────────
{
  const r = await api("PATCH", `/projects/${ref}/auth/providers`, { body: { google: null } });
  ok(r.status === 200 && !r.data.google.secret_set, "clear: google removed");
  const s = await api("GET", `/projects/${ref}/auth/settings`, { auth: false });
  ok(!s.data.providers.includes("google"), "clear: settings no longer advertises google");
  ok(s.data.providers.includes("github"), "clear: github untouched");
}
{
  const r = await api(
    "GET",
    `/projects/${ref}/auth/authorize/google?redirect_to=${encodeURIComponent("https://app.example.com/callback")}`,
    { auth: false },
  );
  ok(r.status === 501, "clear: authorize for an unconfigured provider is 501");
}

// ── cleanup ─────────────────────────────────────────────────────────────────
{
  const r = await api("DELETE", `/projects/${ref}`);
  ok(r.status === 200, "cleanup: project deleted");
}

console.log(`\n${total()}/${total()} passed`);
