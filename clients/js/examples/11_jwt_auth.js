// 11_jwt_auth.js — full auth flow: signup → login → verify → use token.
//   Server must be started with OXIDB_JWT_SECRET set.
const { OxiDB } = require("../index.js");

(async () => {
  const db = new OxiDB("http://127.0.0.1:9080");

  // Pick a unique-per-run username to keep example idempotent.
  const username = "demo_" + Math.random().toString(36).slice(2, 8);
  const password = "s3cret_" + Math.random().toString(36).slice(2, 8);

  // 1. Sign up — server stores argon2 hash, returns a fresh JWT.
  const signup = await db.auth.signup(username, password, "readwrite");
  console.log("signed up:", signup.user);

  // 2. Log out (drop the token), then log back in.
  db.auth.setToken(null);
  const login = await db.auth.login(username, password);
  console.log("logged in:", login.user);

  // 3. Verify — server decodes the token and confirms it.
  const verified = await db.auth.verify();
  console.log("verified:", verified);

  // 4. Use the authenticated client normally — token is auto-attached.
  await db.collection("auth_demo").insert({
    user: username,
    posted_at: new Date().toISOString(),
  });
  const mine = await db.collection("auth_demo").findOne({ user: username });
  console.log("my doc:", mine);
})().catch((e) => { console.error(e); process.exit(1); });
