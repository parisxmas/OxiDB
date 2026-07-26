// 09_sql_query.js — same data, queried with SQL.
//   GROUP BY + ORDER BY + LIMIT.
const { OxiDB } = require("../index.js");

(async () => {
  const db = new OxiDB("http://127.0.0.1:9080");
  const sales = db.collection("sales");

  await sales.delete({});
  await sales.insertMany([
    { region: "EU", category: "books",       amount:  29.99, month: "2026-01" },
    { region: "EU", category: "electronics", amount: 599.00, month: "2026-01" },
    { region: "US", category: "books",       amount:  19.50, month: "2026-01" },
    { region: "US", category: "electronics", amount: 899.00, month: "2026-02" },
    { region: "EU", category: "books",       amount:  39.00, month: "2026-02" },
    { region: "TR", category: "electronics", amount: 449.00, month: "2026-02" },
  ]);

  const rows = await db.sql(`
    SELECT region, category, COUNT(*) AS n, SUM(amount) AS total
    FROM sales
    WHERE month >= '2026-01'
    GROUP BY region, category
    ORDER BY total DESC
    LIMIT 10
  `);

  console.log("REGION | CATEGORY     | N | TOTAL");
  console.log("------ + ------------ + - + --------");
  for (const r of rows) {
    console.log(`${r.region.padEnd(6)} | ${String(r.category).padEnd(12)} | ${r.n} | ${r.total.toFixed(2)}`);
  }
})().catch((e) => { console.error(e); process.exit(1); });
