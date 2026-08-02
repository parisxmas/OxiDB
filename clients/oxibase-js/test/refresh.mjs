// Refresh-token behaviour, against a stub fetch that models the real server:
// `/auth/refresh` is **single-use** — it revokes the presented token and hands
// back a new one (oxibase/src/handlers.rs `end_user_refresh`).
//
// Two things that used to break follow from that single-use rule:
//   1. N requests 401ing at once each POSTed the same refresh token, so one
//      rotated and the rest came back 401 with a perfectly good session.
//   2. Rotation was invisible to the app, so a persisted session kept the
//      token spent by the first refresh and died on the next reload.
//
// No server needed: node test/refresh.mjs (after npm run build).

import { createClient } from "../dist/index.js";

let pass = 0;
const ok = (c, m) => {
  if (c) { pass++; console.log("  ✓", m); }
  else { console.error("  ✗ FAIL:", m); process.exit(1); }
};

const DATA = "http://data.stub";
const CP = "http://cp.stub";
const REF = "proj1";

// A stub OxiBase: `live` access tokens are accepted, refresh tokens are
// single-use, and every rotation counts.
function stub() {
  const s = {
    access: new Set(["A1"]),      // currently valid access tokens
    refresh: new Set(["R0"]),     // unspent refresh tokens
    refreshCalls: 0,
    dataCalls: 0,
    n: 1,
    delayMs: 5,                   // rotation is not instant
  };
  s.fetch = async (input, init = {}) => {
    const url = String(input);
    const json = (status, body) =>
      new Response(JSON.stringify(body), {
        status,
        headers: { "Content-Type": "application/json" },
      });
    if (url.includes("/auth/refresh")) {
      s.refreshCalls++;
      const { refresh_token } = JSON.parse(init.body ?? "{}");
      await new Promise((r) => setTimeout(r, s.delayMs));
      if (!s.refresh.delete(refresh_token)) return json(401, { message: "invalid refresh token" });
      s.n++;
      const access = `A${s.n}`;
      const refresh = `R${s.n}`;
      s.access.add(access);
      s.refresh.add(refresh);
      return json(200, { token: access, refresh_token: refresh });
    }
    s.dataCalls++;
    const bearer = new Headers(init.headers).get("Authorization")?.replace("Bearer ", "");
    if (!s.access.has(bearer)) return json(401, { message: "expired" });
    return json(200, [{ id: 1 }]);
  };
  return s;
}

const client = (s) =>
  createClient(DATA, "anon-key", { ref: REF, authUrl: CP, fetch: (i, o) => s.fetch(i, o) });

// ── 1. A burst of 401s rotates exactly once, and every request survives ──────
{
  const s = stub();
  const ox = client(s);
  ox.auth.setSession({ token: "A0-expired", refreshToken: "R0" });

  const results = await Promise.all(
    Array.from({ length: 8 }, () => ox.from("posts").select("*")),
  );
  ok(results.every((r) => r.error === null), "all 8 concurrent requests succeed");
  ok(s.refreshCalls === 1, `refresh POSTed once, not 8 times (was ${s.refreshCalls})`);
  ok(ox.auth.getSession().token === "A2", "the client is holding the rotated access token");
}

// ── 2. Every rotation is reported, so a persisted session can keep up ────────
{
  const s = stub();
  const ox = client(s);
  const events = [];
  const off = ox.auth.onAuthStateChange((event, session) => events.push([event, session]));

  ox.auth.setSession({ token: "A0-expired", refreshToken: "R0" });
  ok(events.length === 0, "setSession does not echo back to the listener");

  await ox.from("posts").select("*");
  ok(events.length === 1 && events[0][0] === "tokenRefreshed", "a refresh reports tokenRefreshed");
  ok(events[0][1].refreshToken === "R2", "the event carries the *new* refresh token");

  off();
  s.access.clear(); // force another refresh
  await ox.from("posts").select("*");
  ok(events.length === 1, "unsubscribing stops the callbacks");

  ox.auth.signOut();
  ok(ox.auth.getSession() === null, "signOut clears the session");
}

// ── 3. The regression itself: a stale stored token cannot be resumed ─────────
// This is what a persisting app hits without (2) — kept as the reason the
// callback exists, not just a nicety.
{
  const s = stub();
  const first = client(s);
  let saved = { token: "A0-expired", refreshToken: "R0" }; // what login stored
  first.auth.setSession(saved);
  await first.from("posts").select("*"); // rotates R0 → R2 in memory only

  const reloadedStale = client(s);
  reloadedStale.auth.setSession(saved); // the app re-saved nothing
  const stale = await reloadedStale.from("posts").select("*");
  ok(stale.error !== null, "resuming with the spent refresh token fails (the reported bug)");

  // Now the same reload, with the app having persisted from the callback.
  const s2 = stub();
  const live = client(s2);
  live.auth.onAuthStateChange((_e, session) => { if (session) saved = session; });
  saved = { token: "A0-expired", refreshToken: "R0" };
  live.auth.setSession(saved);
  await live.from("posts").select("*");

  const reloadedFresh = client(s2);
  reloadedFresh.auth.setSession(saved);
  s2.access.clear(); // the stored access token has expired too
  const fresh = await reloadedFresh.from("posts").select("*");
  ok(fresh.error === null, "resuming with the persisted rotated token works");
}

console.log(`\n${pass} assertions passed`);
