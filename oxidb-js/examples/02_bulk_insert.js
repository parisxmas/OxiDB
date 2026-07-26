// 02_bulk_insert.js — insertMany 1000 docs in one call, then count.
const { OxiDB } = require("../index.js");

(async () => {
  const db = new OxiDB("http://127.0.0.1:9080");
  const events = db.collection("events");

  await events.delete({});

  const docs = Array.from({ length: 1000 }, (_, i) => ({
    user_id: (i % 100) + 1,
    type: ["login", "click", "purchase", "logout"][i % 4],
    amount: Math.round(Math.random() * 1000) / 100,
    ts: Date.now() - i * 1000,
  }));

  console.time("insertMany 1000");
  await events.insertMany(docs);
  console.timeEnd("insertMany 1000");

  console.log("total:", await events.count());
  console.log("purchases:", await events.count({ type: "purchase" }));
})().catch((e) => { console.error(e); process.exit(1); });
