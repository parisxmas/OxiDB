// 03_query_operators.js — $gt / $lt / $in / $or / $and / $regex showcase.
const { OxiDB } = require("../index.js");

(async () => {
  const db = new OxiDB("http://127.0.0.1:9080");
  const u = db.collection("users");
  await u.delete({});

  await u.insertMany([
    { name: "Alice",   age: 30, country: "TR", role: "admin" },
    { name: "Bob",     age: 17, country: "US", role: "user" },
    { name: "Carol",   age: 42, country: "TR", role: "admin" },
    { name: "Dan",     age: 25, country: "DE", role: "user" },
    { name: "Eve",     age: 19, country: "US", role: "user" },
  ]);

  console.log("\n$gte 18 + sort by age desc:");
  console.log(await u.find({ age: { $gte: 18 } }, { sort: { age: -1 } }));

  console.log("\n$in TR or DE:");
  console.log(await u.find({ country: { $in: ["TR", "DE"] } }));

  console.log("\n$or admin OR age<20:");
  console.log(await u.find({ $or: [{ role: "admin" }, { age: { $lt: 20 } }] }));

  console.log("\n$and TR + admin:");
  console.log(await u.find({ $and: [{ country: "TR" }, { role: "admin" }] }));

  console.log("\n$regex starts with 'A' (case-insensitive):");
  console.log(await u.find({ name: { $regex: "^a", $options: "i" } }));
})().catch((e) => { console.error(e); process.exit(1); });
