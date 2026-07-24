// End-to-end check of per-project end-user sign-in against a running OxiBase:
// social OAuth (Google, GitHub) and passwordless magic links.
//
//   OXIBASE_URL=http://127.0.0.1:4460 \
//   OXIDB_PLATFORM_SECRET=<the control plane's secret> \
//   [OXIBASE_MAIL_SINK=<the control plane's mail sink file>] \
//   node oxibase/test/auth.e2e.mjs
//
// For OAuth, everything up to the provider round-trip is covered: configuration,
// the public discovery endpoint, the authorize redirect, the redirect allow-list
// and state validation on the callback. The code-for-token exchange needs real
// provider credentials and is not exercised here. Magic links are covered end to
// end when the control plane runs with a mail sink.

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

console.log("# OxiBase end-user sign-in (OAuth + magic links)");

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

// ── magic links ─────────────────────────────────────────────────────────────
// The full round trip is only checkable when a mail transport is present. With
// OXIBASE_MAIL_SINK the control plane appends each message to a file, so the
// test can read back the link it just triggered.
const SINK = process.env.OXIBASE_MAIL_SINK;
let magicAvailable = false;
{
  const s = await api("GET", `/projects/${ref}/auth/settings`, { auth: false });
  ok(typeof s.data.magic_link === "boolean", "settings: advertises whether magic links work");
  magicAvailable = s.data.magic_link;
  if (SINK) ok(magicAvailable === true, "settings: magic links enabled by the mail transport");
}
{
  const r = await api("POST", `/projects/${ref}/auth/magiclink`, {
    auth: false,
    body: { email: "someone@example.com", redirect_to: "https://evil.example.com/steal" },
  });
  ok(r.status === 403, "magic link: an unlisted redirect_to is refused");
}
{
  const r = await api("POST", `/projects/${ref}/auth/magiclink`, {
    auth: false,
    body: { email: "someone@example.com" },
  });
  ok(r.status === 400, "magic link: redirect_to is required");
}

if (SINK) {
  const { readFileSync, writeFileSync } = await import("node:fs");
  const inbox = () =>
    readFileSync(SINK, "utf8")
      .split("\n")
      .filter(Boolean)
      .map((l) => JSON.parse(l));
  writeFileSync(SINK, "");

  const email = `magic${process.pid}@example.com`;
  const dest = "https://app.example.com/callback";
  const r = await api("POST", `/projects/${ref}/auth/magiclink`, {
    auth: false,
    body: { email, redirect_to: dest },
  });
  ok(r.status === 200, "magic link: request accepted");

  // send_async runs on a thread; give it a moment to land in the sink.
  let mail = [];
  for (let i = 0; i < 40 && mail.length === 0; i++) {
    await new Promise((r) => setTimeout(r, 50));
    try {
      mail = inbox();
    } catch {
      mail = [];
    }
  }
  ok(mail.length === 1 && mail[0].to === email, "magic link: one message, to the right address");
  const link = (mail[0].body.match(/https?:\/\/\S+/) ?? [])[0];
  ok(!!link && link.includes("/auth/magiclink/verify?token="), "magic link: the mail carries the link");

  // Following the link signs the user in and bounces to the app.
  const followed = await fetch(link, { redirect: "manual" });
  ok(followed.status === 302, "magic link: the link redirects");
  const to = followed.headers.get("location") ?? "";
  ok(to.startsWith(`${dest}#access_token=`), "magic link: lands on the app with a session in the fragment");
  const params = new URLSearchParams(to.slice(to.indexOf("#") + 1));
  const access = params.get("access_token");
  ok(!!access && !!params.get("refresh_token"), "magic link: both tokens present");

  // The session is real: it must be a project-signed token for that user.
  const claims = JSON.parse(Buffer.from(access.split(".")[1], "base64url").toString());
  ok(claims.sub === email, "magic link: the access token identifies the user");
  ok(claims.role === "authenticated", "magic link: the token carries the authenticated role");

  // Single use.
  const again = await fetch(link, { redirect: "manual" });
  ok(again.status === 400, "magic link: the link cannot be used twice");

  // The user now exists and is verified — the click proved the address.
  const users = await api("GET", `/projects/${ref}/users`);
  const created = users.data.find((u) => u.email === email);
  ok(!!created && created.verified === true, "magic link: created the user, already verified");

  // The token is not just well-shaped — the data plane accepts it. (Signature
  // verification happens there, against the project's public key.)
  if (process.env.OXIBASE_DATA_URL) {
    const r = await fetch(`${process.env.OXIBASE_DATA_URL}/rest/v1/notes?db=${ref}`, {
      headers: { Authorization: `Bearer ${access}` },
    });
    ok(r.status === 200, `magic link: the data plane accepts the session (${r.status})`);
    const tampered = access.slice(0, -3) + (access.endsWith("aaa") ? "bbb" : "aaa");
    const bad = await fetch(`${process.env.OXIBASE_DATA_URL}/rest/v1/notes?db=${ref}`, {
      headers: { Authorization: `Bearer ${tampered}` },
    });
    ok(bad.status === 401, "magic link: a tampered token is rejected there");
  }

  // Refresh works from a magic-link session like any other.
  const refreshed = await api("POST", `/projects/${ref}/auth/refresh`, {
    auth: false,
    body: { refresh_token: params.get("refresh_token") },
  });
  ok(refreshed.status === 200 && refreshed.data.token, "magic link: the session refreshes");
} else if (!magicAvailable) {
  const r = await api("POST", `/projects/${ref}/auth/magiclink`, {
    auth: false,
    body: { email: "someone@example.com", redirect_to: "https://app.example.com/callback" },
  });
  ok(r.status === 501, "magic link: reports email is not configured (no mail transport)");
  console.log("  … set OXIBASE_MAIL_SINK to exercise the full round trip");
} else {
  // A real mail transport is configured but this run cannot read the mailbox.
  // Requesting a link here would send actual mail to a made-up address, so the
  // send path is left alone.
  console.log("  … live mail transport without a sink — skipping the send (would email for real)");
}

// ── per-user admin routes ───────────────────────────────────────────────────
{
  // Every address contains "@", which encodeURIComponent turns into %40 — so
  // these routes only work if the server decodes the path segment. They were
  // 404-ing for every real email until it did.
  const email = `admin-probe-${process.pid}@example.com`;
  const signup = await api("POST", `/projects/${ref}/auth/signup`, {
    auth: false,
    body: { email, password: "hunter2hunter2" },
  });
  ok(signup.status === 201, "admin routes: user created");

  const verified = await api("POST", `/projects/${ref}/users/${encodeURIComponent(email)}/verify`);
  ok(verified.status === 200, `admin routes: verify by encoded email (${verified.status})`);
  // A 200 is not proof: the write has to have landed on a row. Addressed by
  // slug, these updates used to match nothing and still report success.
  const listed = await api("GET", `/projects/${ref}/users`);
  ok(
    listed.data.find((u) => u.email === email)?.verified === true,
    "admin routes: verify actually changed the user",
  );

  const pw = await api("POST", `/projects/${ref}/users/${encodeURIComponent(email)}/password`, {
    body: { password: "newpassword123" },
  });
  ok(pw.status === 200, "admin routes: set password by encoded email");

  const login = await api("POST", `/projects/${ref}/auth/login`, {
    auth: false,
    body: { email, password: "newpassword123" },
  });
  ok(login.status === 200 && login.data.token, "admin routes: the user can sign in afterwards");

  const gone = await api("DELETE", `/projects/${ref}/users/${encodeURIComponent(email)}`);
  ok(gone.status === 200, "admin routes: delete by encoded email");
  const users = await api("GET", `/projects/${ref}/users`);
  ok(!users.data.some((u) => u.email === email), "admin routes: the user is gone");
}

// ── cleanup ─────────────────────────────────────────────────────────────────
{
  const r = await api("DELETE", `/projects/${ref}`);
  ok(r.status === 200, "cleanup: project deleted");
}

console.log(`\n${total()}/${total()} passed`);
