// Prove all three engines are reachable and selectable from the SDK.
// Run against a data plane with OXIDB_SQL=1 and OXIDB_TSDB=1 (auth off).
//   OXIBASE_DATA_URL=http://127.0.0.1:18099 node test/engines.mjs
import { createClient } from "../dist/index.js";

const DATA = process.env.OXIBASE_DATA_URL || "http://127.0.0.1:18099";
const oxibase = createClient(DATA, "no-auth"); // auth off → any bearer works, no ref (single db)

let pass = 0;
const ok = (c, m) => { if (c) { pass++; console.log("  ✓", m); } else { console.error("  ✗ FAIL:", m); process.exit(1); } };

// 1) DOCUMENT engine — .from(name) where name is NOT a SQL table
{
  await oxibase.from("people").insert({ name: "ada", role: "eng" });
  const { data, error } = await oxibase.from("people").select("*").eq("name", "ada");
  ok(!error && data?.length === 1 && data[0].role === "eng" && "_id" in data[0],
     "document engine: .from('people') → collection row (has _id)");
}

// 2) SQL engine — create the name as a SQL table, then .from(name) routes to SQL
{
  await oxibase.sql("CREATE TABLE widgets (id INTEGER PRIMARY KEY, label TEXT)");
  await oxibase.sql("INSERT INTO widgets (id, label) VALUES (1, 'gear')");
  const { data, error } = await oxibase.from("widgets").select("*");
  // SQL rows have exactly the declared columns and NO document _id.
  ok(!error && data?.length === 1 && data[0].label === "gear" && !("_id" in data[0]),
     "SQL engine: .from('widgets') → SQL row (id/label, no _id)");
}

// 3) TSDB engine — .schema('tsdb') sends Accept-Profile: tsdb
{
  const ts = 1_700_000_000_000;
  const w = await oxibase.schema("tsdb").from("cpu").insert({ ts, host: "a", usage: 0.5 });
  ok(!w.error, `TSDB write via .schema('tsdb') (${w.error?.message ?? ""})`);
  const { data, error } = await oxibase.schema("tsdb").from("cpu").select("usage").eq("host", "a");
  ok(!error && Array.isArray(data) && data.length >= 1, "TSDB read via .schema('tsdb') returns points");
  ok(data?.[0]?.ts === ts, `TSDB point carries the real ts (got ${data?.[0]?.ts}, want ${ts})`);
  ok(data?.[0]?.value === 0.5, "TSDB point carries the field value");
  console.log("    tsdb row:", JSON.stringify(data?.[0]));
}

// 4) Show how to KNOW what exists on each engine
{
  const cols = await (await fetch(`${DATA}/api/collections`)).json();
  const { results } = await oxibase.sql("SHOW TABLES");
  ok(cols.collections?.includes("people"), "list document collections → includes 'people'");
  ok(results?.[results.length - 1]?.rows?.some((r) => r[0] === "widgets"), "SHOW TABLES → includes 'widgets'");
}

console.log(`\n${pass} passed`);
