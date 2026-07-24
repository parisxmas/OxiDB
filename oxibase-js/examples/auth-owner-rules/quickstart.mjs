// Per-project end-user auth + owner-only security rules with oxibase-js.
//
// A project's own users sign up against the project:
// `oxibase.auth.signUp/signInWithPassword` mints a token signed with the
// PROJECT's key, carrying the user's identity. The data plane verifies it with
// the project's public key, and rules see `auth.username` (the email) and
// `auth.role == "authenticated"`.
//
// The board: any signed-in user reads it, but may only CREATE rows they own and
// UPDATE/DELETE their OWN. Ownership is enforced by rules on the server.
//
//   read:   "auth.role == 'authenticated'"  only signed-in users (not the anon key)
//   create: "auth.username == newDoc.owner"  may only create rows they own
//   update: "auth.username == doc.owner"     may only edit their own (per row)
//   delete: "auth.username == doc.owner"     may only delete their own (per row)
//
// Run:
//   cd oxibase-js && npm run build
//   OXIBASE_URL=http://127.0.0.1:8087 \       # data plane
//   OXIBASE_AUTH_URL=http://127.0.0.1:4460 \  # control plane (for .auth)
//   OXIBASE_REF=<ref> \
//   OXIBASE_ANON_KEY=<anon key> \
//   OXIBASE_SERVICE_ROLE_KEY=<service_role key> \  # sets the rules (operator)
//     node examples/auth-owner-rules/quickstart.mjs

import { createClient } from "../../dist/index.js";

const DATA = process.env.OXIBASE_URL || "http://127.0.0.1:8087";
const AUTH = process.env.OXIBASE_AUTH_URL || "http://127.0.0.1:4460";
const REF = process.env.OXIBASE_REF;
const ANON = process.env.OXIBASE_ANON_KEY;
const SERVICE = process.env.OXIBASE_SERVICE_ROLE_KEY;
if (!REF || !ANON || !SERVICE) {
  console.error("Set OXIBASE_REF, OXIBASE_ANON_KEY and OXIBASE_SERVICE_ROLE_KEY.");
  process.exit(1);
}

const TASKS = `board_${process.pid}`;
const log = (t) => console.log(`\n\x1b[1m${t}\x1b[0m`);
let pass = 0;
const ok = (c, m) => (c ? (pass++, console.log("  ✓ " + m)) : (console.error("  ✗ " + m), process.exit(1)));

// Unique per-run emails so re-runs don't collide.
const alicem = `alice+${process.pid}@demo.test`;
const bobm = `bob+${process.pid}@demo.test`;

// A fresh client for an end-user (starts as the anon key; `.auth` swaps in a
// user session). `authUrl` points at the control plane where auth lives.
const newClient = () => createClient(DATA, ANON, { ref: REF, authUrl: AUTH });

async function main() {
  // ── 1. Operator installs owner-only rules (service_role) ────────────────────
  log("1. Operator sets owner-only rules on the task board");
  const rulesRes = await fetch(`${DATA}/api/rules/${TASKS}?db=${REF}`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${SERVICE}` },
    body: JSON.stringify({
      // Row-level read: each user sees ONLY the rows they own — enforced by the
      // server, no client filter needed.
      read: "auth.username == doc.owner",
      create: "auth.username == newDoc.owner",
      update: "auth.username == doc.owner",
      delete: "auth.username == doc.owner",
    }),
  });
  ok(rulesRes.ok, "rules installed (read + write: owner-only, row-level)");

  // ── 2. Two end-users sign up against the PROJECT ────────────────────────────
  log("2. Alice and Bob sign up (project end-users)");
  const alice = newClient();
  const bob = newClient();
  const aSignup = await alice.auth.signUp({ email: alicem, password: "s3cret-alice" });
  const bSignup = await bob.auth.signUp({ email: bobm, password: "s3cret-bob" });
  ok(!aSignup.error && !bSignup.error, "both signed up and hold project-signed sessions");
  ok(!!alice.auth.getSession(), "alice has an active session");

  // ── 3. Create tasks they own; spoofing another owner is rejected ────────────
  log("3. Create tasks (a user may only create rows they own)");
  ok(!(await alice.from(TASKS).insert({ title: "Write report", owner: alicem, done: false })).error,
    "alice creates her own task");
  ok(!(await alice.from(TASKS).insert({ title: "Review PR", owner: alicem, done: false })).error,
    "alice creates a second");
  ok(!!(await alice.from(TASKS).insert({ title: "Sneaky", owner: bobm, done: false })).error,
    "alice CANNOT create a task owned by bob (create rule denies)");
  ok(!(await bob.from(TASKS).insert({ title: "Ship release", owner: bobm, done: false })).error,
    "bob creates his own task");

  // ── 4. Row-level reads: a plain select returns ONLY the caller's own rows ───
  log("4. Read the board — the server filters each row (no client filter)");
  const aTasks = (await alice.from(TASKS).select("title")).data ?? []; // no .eq!
  const bTasks = (await bob.from(TASKS).select("title")).data ?? [];
  ok(aTasks.length === 2, `alice's plain select returns only her 2 tasks`);
  ok(bTasks.length === 1, `bob's plain select returns only his 1 task`);

  // ── 5. Writes are owner-scoped by the rules ─────────────────────────────────
  log("5. Cross-user writes are denied by the rules");
  ok(!(await alice.from(TASKS).update({ done: true }).eq("title", "Write report")).error,
    "alice updates her OWN task");
  ok(!!(await alice.from(TASKS).update({ done: true }).eq("title", "Ship release")).error,
    "alice CANNOT update bob's task");
  ok(!!(await bob.from(TASKS).delete().eq("title", "Review PR")).error,
    "bob CANNOT delete alice's task");
  ok(!(await bob.from(TASKS).delete().eq("title", "Ship release")).error,
    "bob deletes his OWN task");

  // ── 6. The anon key owns nothing, so it sees nothing ────────────────────────
  log("6. The anon key sees no rows (owns none)");
  const anon = createClient(DATA, ANON, { ref: REF }); // never calls .auth
  ok(((await anon.from(TASKS).select("*")).data ?? []).length === 0, "anon key → 0 rows");

  // ── 7. signOut reverts to the anon key ──────────────────────────────────────
  log("7. signOut reverts the session");
  alice.auth.signOut();
  ok(alice.auth.getSession() === null, "alice signed out");
  ok(((await alice.from(TASKS).select("*")).data ?? []).length === 0, "…and now reads as the anon key → 0 rows");

  // ── 8. Cleanup (service_role bypasses rules) ────────────────────────────────
  log("8. Cleanup");
  await fetch(`${DATA}/rest/v1/${TASKS}?owner=in.(${encodeURIComponent(alicem)},${encodeURIComponent(bobm)})`, {
    method: "DELETE",
    headers: { Authorization: `Bearer ${SERVICE}` },
  });
  await fetch(`${DATA}/api/rules/${TASKS}?db=${REF}`, {
    method: "DELETE",
    headers: { Authorization: `Bearer ${SERVICE}` },
  });
  ok(true, "tasks and rules removed");

  console.log(`\n\x1b[32m${pass} checks passed\x1b[0m\n`);
}

main().catch((e) => {
  console.error("\n\x1b[31mFAILED:\x1b[0m", e.message);
  process.exit(1);
});
