// 08_ttl_index.js — sessions auto-expire 1 hour after `created_at`.
const { OxiDB } = require("../index.js");
const crypto = require("node:crypto");

(async () => {
  const db = new OxiDB("http://127.0.0.1:9080");
  const sessions = db.collection("sessions");

  await sessions.delete({});

  // Set up the TTL index once (idempotent on most servers; ignore re-creates).
  try {
    await sessions.createIndex("created_at", {
      type: "ttl", expireAfterSeconds: 3600,
    });
  } catch (e) { /* index may already exist */ }

  for (const user of ["alice", "bob", "carol"]) {
    await sessions.insert({
      user,
      token: crypto.randomBytes(16).toString("hex"),
      ip: "10.0.0." + Math.floor(Math.random() * 255),
      created_at: Date.now(),
    });
  }

  console.log("active:", await sessions.count());
  console.log("indexes:", await sessions.listIndexes());
})().catch((e) => { console.error(e); process.exit(1); });
