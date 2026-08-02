// 01_hello.js — connect, ping, insert, find — smallest possible Node.js example.
const { OxiDB } = require("../index.js");

(async () => {
  const db = new OxiDB("http://127.0.0.1:9080");

  console.log("ping:", await db.ping());

  await db.collection("users").insert({
    name: "Alice", age: 30, email: "alice@example.com",
  });

  const alice = await db.collection("users").findOne({ name: "Alice" });
  console.log("loaded:", alice);
})().catch((e) => { console.error(e); process.exit(1); });
