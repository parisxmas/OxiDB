// 12_security_rules.js — owner-only updates via per-collection ACL.
//   Requires OXIDB_JWT_SECRET; rules see `auth.username` and `doc.owner`.
const { OxiDB } = require("../index.js");

(async () => {
  const admin = new OxiDB("http://127.0.0.1:9080");

  // 0. Bootstrap an admin (only role allowed to setRules).
  const adminUser = "admin_" + Math.random().toString(36).slice(2, 6);
  await admin.auth.signup(adminUser, "pw", "admin");

  // Two normal users with their own JWTs.
  const alice = "alice_" + Math.random().toString(36).slice(2, 6);
  const bob   = "bob_"   + Math.random().toString(36).slice(2, 6);

  const tmp = new OxiDB("http://127.0.0.1:9080");
  await tmp.auth.signup(alice, "pw", "readwrite");
  const aliceToken = tmp.auth.getToken();
  await tmp.auth.signup(bob, "pw", "readwrite");
  const bobToken = tmp.auth.getToken();

  // Lock the `notes` collection — set rules as admin.
  await admin.collection("notes").setRules({
    read:   "true",
    create: "auth != null",
    update: "auth.username == doc.owner",
    delete: "auth.username == doc.owner",
  });

  // Alice posts a note.
  const aliceClient = new OxiDB("http://127.0.0.1:9080", { token: aliceToken });
  await aliceClient.collection("notes").insert({ owner: alice, body: "hello" });
  console.log("alice posted ok");

  // Bob tries to edit Alice's note → should be denied.
  const bobClient = new OxiDB("http://127.0.0.1:9080", { token: bobToken });
  try {
    await bobClient.collection("notes").update(
      { owner: alice }, { $set: { body: "hijacked" } },
    );
    console.log("UNEXPECTED: bob edited alice's note");
  } catch (e) {
    console.log("denied for bob:", e.message);
  }
})().catch((e) => { console.error(e); process.exit(1); });
