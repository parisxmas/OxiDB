// A fuller oxibase-js walkthrough: a tiny task-tracker backend.
//
// It touches every part of the client — the document engine (CRUD, filters,
// ordering, pagination, resource embedding), the SQL engine (`.sql`), the
// time-series engine (`.schema("tsdb")`), and idiomatic error handling.
//
// In a real project you would `npm install oxibase-js` and:
//     import { createClient } from "oxibase-js";
// Here we import the in-repo build so the example runs from the source tree.
//
// Run it (server-side — this uses the service_role key):
//     cd oxibase-js && npm run build
//     OXIBASE_URL=http://127.0.0.1:8087 \
//     OXIBASE_REF=<project ref> \
//     OXIBASE_KEY=<service_role key> \
//       node examples/task-tracker/quickstart.mjs
//
// Requires the data plane running with OXIDB_SQL=1 (for §5) and OXIDB_TSDB=1
// (for §6); those sections skip themselves gracefully if the engine is off.

import { createClient } from "../../dist/index.js";

const URL = process.env.OXIBASE_URL || "http://127.0.0.1:8087";
const REF = process.env.OXIBASE_REF;
const KEY = process.env.OXIBASE_KEY;
if (!REF || !KEY) {
  console.error("Set OXIBASE_REF and OXIBASE_KEY (service_role). See the header comment.");
  process.exit(1);
}

const oxibase = createClient(URL, KEY, { ref: REF });

// A tiny helper: unwrap { data, error } or throw with a clear message.
async function must(promise, label) {
  const { data, error } = await promise;
  if (error) throw new Error(`${label}: ${error.message ?? error}`);
  return data;
}

// Unique suffix so re-runs don't collide (no Date.now in the point below, but
// fine for names). Uses a random-ish tag from the process id + a counter.
const TAG = `qs${process.pid}`;
const log = (title) => console.log(`\n\x1b[1m${title}\x1b[0m`);

async function main() {
  // ── 1. Seed projects (a document collection, auto-created on first insert) ──
  log("1. Create two projects");
  const projects = await must(
    oxibase
      .from(`${TAG}_projects`)
      .insert([
        { name: "Website relaunch", owner: "ada" },
        { name: "Mobile app", owner: "bob" },
      ])
      .select(), // return the inserted rows (with their generated _id)
    "insert projects",
  );
  const [web, mobile] = projects;
  console.log(`  → ${projects.length} projects, ids ${web._id} and ${mobile._id}`);

  // ── 2. Seed tasks, each belonging to a project via `<parent>_id` ────────────
  // The field name that links a child to its parent is `<singular(parent)>_id`.
  // Our parent collection is `${TAG}_projects`; the FK the embed looks for is
  // `${TAG}_project_id`. We set it explicitly here.
  log("2. Add tasks to each project");
  const fk = `${TAG}_project_id`;
  await must(
    oxibase.from(`${TAG}_tasks`).insert([
      { title: "Design mockups", status: "done", priority: 2, [fk]: web._id },
      { title: "Build homepage", status: "doing", priority: 1, [fk]: web._id },
      { title: "SEO audit", status: "todo", priority: 3, [fk]: web._id },
      { title: "Push notifications", status: "todo", priority: 1, [fk]: mobile._id },
      { title: "App store listing", status: "doing", priority: 2, [fk]: mobile._id },
    ]),
    "insert tasks",
  );
  console.log("  → 5 tasks inserted");

  // ── 3. Query: filter + order + paginate ─────────────────────────────────────
  log("3. Open tasks (status != done), highest priority first, page 1 (size 2)");
  const openTasks = await must(
    oxibase
      .from(`${TAG}_tasks`)
      .select("title, status, priority")
      .neq("status", "done")
      .order("priority", { ascending: true })
      .range(0, 1), // rows 0..1 inclusive = first page of 2 (Content-Range paging)
    "select open tasks",
  );
  for (const t of openTasks) console.log(`  • [p${t.priority}] ${t.title} (${t.status})`);

  // ── 4. Resource embedding: tasks with their parent project in one request ───
  log("4. Tasks with embedded project (belongs-to, one round trip)");
  const withProject = await must(
    oxibase
      .from(`${TAG}_tasks`)
      .select(`title, status, ${TAG}_projects(name, owner)`)
      .eq("status", "doing"),
    "select tasks + project",
  );
  for (const t of withProject) {
    const p = t[`${TAG}_projects`];
    console.log(`  • ${t.title} → project "${p?.name}" (owner ${p?.owner})`);
  }

  // ── 5. Update + delete ──────────────────────────────────────────────────────
  log("5. Move 'Build homepage' to done, drop the SEO audit");
  await must(
    oxibase.from(`${TAG}_tasks`).update({ status: "done" }).eq("title", "Build homepage"),
    "update task",
  );
  await must(oxibase.from(`${TAG}_tasks`).delete().eq("title", "SEO audit"), "delete task");
  const remaining = await must(oxibase.from(`${TAG}_tasks`).select("_id"), "count tasks");
  console.log(`  → ${remaining.length} tasks remain`);

  // ── 6. SQL engine: an analytics rollup the document API can't express ────────
  log("6. SQL analytics — task count by status (requires OXIDB_SQL=1)");
  {
    // Build a SQL table from the document data. In a real app the SQL table
    // would be the source of truth; here we mirror a couple of rows to show the
    // API. `.sql` params are always bound (?), never string-concatenated.
    const setup = await oxibase.sql(`CREATE TABLE IF NOT EXISTS ${TAG}_metrics (status TEXT, n INTEGER)`);
    if (setup.error) {
      console.log(`  (skipped: ${setup.error})`);
    } else {
      await oxibase.sql(
        `INSERT INTO ${TAG}_metrics (status, n) VALUES (?, ?), (?, ?), (?, ?)`,
        ["done", 2, "doing", 1, "todo", 1],
      );
      const { results, error } = await oxibase.sql(
        `SELECT status, SUM(n) AS total FROM ${TAG}_metrics GROUP BY status ORDER BY total DESC`,
      );
      if (error) console.log(`  (query failed: ${error})`);
      else {
        const r = results[results.length - 1];
        console.log(`  columns: ${r.columns.join(", ")}`);
        for (const [status, total] of r.rows) console.log(`  • ${status}: ${total}`);
      }
    }
  }

  // ── 7. Time-series engine: activity metrics over time ───────────────────────
  log("7. Time-series — record + read completion events (requires OXIDB_TSDB=1)");
  {
    const ts = 1_700_000_000_000; // fixed timestamp so the example is deterministic
    const w = await oxibase
      .schema("tsdb")
      .from(`${TAG}_activity`)
      .insert({ ts, team: "web", completed: 1 });
    if (w.error) {
      console.log(`  (skipped: ${w.error.message ?? w.error})`);
    } else {
      const { data, error } = await oxibase
        .schema("tsdb")
        .from(`${TAG}_activity`)
        .select("completed")
        .eq("team", "web");
      if (error) console.log(`  (read failed: ${error.message})`);
      else console.log(`  → points:`, JSON.stringify(data));
    }
  }

  // ── 8. Cleanup ──────────────────────────────────────────────────────────────
  log("8. Cleanup");
  await oxibase.from(`${TAG}_tasks`).delete().neq("_id", -1); // delete all
  await oxibase.from(`${TAG}_projects`).delete().neq("_id", -1);
  await oxibase.sql(`DROP TABLE IF EXISTS ${TAG}_metrics`);
  console.log("  → done\n");
}

main().catch((e) => {
  console.error("\n\x1b[31mFAILED:\x1b[0m", e.message);
  process.exit(1);
});
