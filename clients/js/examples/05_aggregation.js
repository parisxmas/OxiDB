// 05_aggregation.js — top-N customers by spend.
//   $match → $group → $sort → $limit pipeline.
const { OxiDB } = require("../index.js");

(async () => {
  const db = new OxiDB("http://127.0.0.1:9080");
  const orders = db.collection("orders");

  await orders.delete({});
  await orders.insertMany([
    { customer: "alice", amount: 120.50, status: "paid" },
    { customer: "bob",   amount:  45.00, status: "paid" },
    { customer: "alice", amount: 999.00, status: "paid" },
    { customer: "carol", amount: 320.00, status: "paid" },
    { customer: "dan",   amount:  10.00, status: "refunded" },
    { customer: "alice", amount:  50.00, status: "paid" },
    { customer: "bob",   amount: 175.00, status: "paid" },
  ]);

  const top = await orders.aggregate([
    { $match: { status: "paid" } },
    { $group: {
        _id: "$customer",
        spend: { $sum: "$amount" },
        n:     { $sum: 1 },
    }},
    { $sort:  { spend: -1 } },
    { $limit: 3 },
  ]);

  console.log("Top 3 customers:");
  top.forEach((r, i) =>
    console.log(`  ${i + 1}. ${r._id}: $${r.spend} (${r.n} orders)`));
})().catch((e) => { console.error(e); process.exit(1); });
