// 07_indexes.js — create field / unique / composite indexes; list & drop.
const { OxiDB } = require("../index.js");

(async () => {
  const db = new OxiDB("http://127.0.0.1:9080");
  const users = db.collection("users");

  await users.delete({});

  await users.createIndex("email", { unique: true });
  await users.createIndex("country");
  await users.createIndex("country_age", {
    type: "composite", fields: ["country", "age"],
  });

  console.log("indexes:", await users.listIndexes());

  await users.insert({ email: "a@x.com", country: "TR", age: 30 });
  try {
    await users.insert({ email: "a@x.com", country: "US", age: 25 });
  } catch (e) {
    console.log("unique index blocked duplicate email:", e.message);
  }

  await users.dropIndex("country");
  console.log("after drop:", await users.listIndexes());
})().catch((e) => { console.error(e); process.exit(1); });
