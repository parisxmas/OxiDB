// OxiBase multi-tenant load benchmark.
//
// Phases:
//   1. provision N tenant projects (OxiWire admin: keypair + project row + DB),
//      with unlimited quotas so seeding isn't capped;
//   2. seed each with COLLECTIONS collections × ROWS documents (batched REST);
//   3. run CONCURRENCY oxibase-js clients querying their own project at once for
//      DURATION seconds, as if each were a separate app;
//   4. print throughput + latency percentiles.
//
// Runs inside the compose network (targets the internal data plane directly, so
// Cloudflare's bot filter is out of the picture). GELF logging on the server
// records every request into OxiDB's _gelf_logs for separate inspection.
//
// Env: PROJECTS, COLLECTIONS, ROWS, BATCH, DURATION, CONCURRENCY, SEED_CONC,
//      WIRE (host:port), REST_BASE (http://host:port), OXIBASE_JS (module path).

import { connectWire, genKeypair, mintJwt, pool, pct } from "./lib.mjs";

const CFG = {
  projects: +(process.env.PROJECTS ?? 20),
  collections: +(process.env.COLLECTIONS ?? 5),
  rows: +(process.env.ROWS ?? 5000),
  batch: +(process.env.BATCH ?? 1000),
  duration: +(process.env.DURATION ?? 30),
  concurrency: +(process.env.CONCURRENCY ?? 20),
  seedConc: +(process.env.SEED_CONC ?? 16),
  wire: process.env.WIRE ?? "data-plane:4444",
  restBase: process.env.REST_BASE ?? "http://data-plane:8087",
  oxibaseJs: process.env.OXIBASE_JS ?? "/repo/oxibase-js/dist/index.js",
};

const now = () => Number(process.hrtime.bigint() / 1000n) / 1000; // ms
const log = (...a) => console.log(`[${new Date().toISOString()}]`, ...a);

function genRef() {
  const a = "abcdefghijklmnopqrstuvwxyz0123456789";
  return Array.from({ length: 16 }, () => a[(Math.random() * a.length) | 0]).join("");
}

async function provision() {
  const [host, port] = CFG.wire.split(":");
  const w = connectWire(host, +port);
  await w.ready;
  const projects = [];
  for (let n = 0; n < CFG.projects; n++) {
    const ref = genRef();
    const slug = `bench-${n}`;
    const { privateKey, pubB64 } = genKeypair();
    await w.call({ cmd: "create_database", name: ref });
    await w.call({
      cmd: "insert",
      db: "oxibase",
      collection: "projects",
      doc: {
        ref,
        slug,
        owner: "bench@example.com",
        pubkey: pubB64,
        // Unlimited quotas for the load test.
        max_collections: 0,
        max_tables: 0,
        max_documents: 0,
        isolation: "shared",
        created_at: Math.floor(Date.now() / 1000),
        bench: true,
      },
    });
    const serviceKey = mintJwt(privateKey, {
      sub: `admin@${ref}`,
      role: "admin",
      ttlSecs: 3 * 3600,
    });
    projects.push({ ref, slug, serviceKey });
  }
  w.end();
  return projects;
}

function makeDoc(i) {
  return {
    i,
    bucket: i % 100,
    name: `item-${i}`,
    val: Math.round(Math.random() * 1e6),
    ok: i % 2 === 0,
    ts: new Date(1700000000000 + i * 1000).toISOString(),
  };
}

async function insertBatch(ref, col, key, docs) {
  const res = await fetch(`${CFG.restBase}/rest/v1/${col}?db=${ref}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      apikey: key,
      authorization: `Bearer ${key}`,
    },
    body: JSON.stringify(docs),
  });
  if (!res.ok) throw new Error(`insert ${col} → ${res.status} ${await res.text()}`);
}

async function seed(projects) {
  // Flatten every (project, collection) into one work list, seed with a pool.
  const tasks = [];
  for (const p of projects) {
    for (let c = 0; c < CFG.collections; c++) tasks.push({ p, col: `c${c}` });
  }
  let doneDocs = 0;
  const totalDocs = tasks.length * CFG.rows;
  await pool(tasks, CFG.seedConc, async ({ p, col }) => {
    for (let off = 0; off < CFG.rows; off += CFG.batch) {
      const docs = [];
      for (let i = off; i < Math.min(off + CFG.batch, CFG.rows); i++) docs.push(makeDoc(i));
      await insertBatch(p.ref, col, p.serviceKey, docs);
      doneDocs += docs.length;
    }
    // Index the queried field so the load phase is index-backed.
    await fetch(`${CFG.restBase}/api/${col}/indexes?db=${p.ref}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${p.serviceKey}`,
      },
      body: JSON.stringify({ field: "bucket" }),
    }).catch(() => {});
  });
  return { doneDocs, totalDocs };
}

async function loadPhase(projects) {
  const { createClient } = await import(CFG.oxibaseJs);
  // One oxibase-js client per project — each runs against its own tenant DB.
  const clients = projects.map((p) =>
    createClient(CFG.restBase, p.serviceKey, { ref: p.ref }),
  );
  const lat = [];
  let ok = 0;
  let err = 0;
  const deadline = now() + CFG.duration * 1000;
  let stop = false;
  setTimeout(() => (stop = true), CFG.duration * 1000);

  async function worker(slot) {
    while (!stop && now() < deadline) {
      const p = slot % projects.length;
      const client = clients[p];
      const col = `c${(Math.random() * CFG.collections) | 0}`;
      const bucket = (Math.random() * 100) | 0;
      const t0 = now();
      const { error } = await client.from(col).select("*").eq("bucket", bucket).limit(50);
      const dt = now() - t0;
      if (error) err++;
      else {
        ok++;
        lat.push(dt);
      }
    }
  }
  await Promise.all(Array.from({ length: CFG.concurrency }, (_, s) => worker(s)));
  lat.sort((a, b) => a - b);
  return { ok, err, lat };
}

async function main() {
  log("config:", JSON.stringify(CFG));

  log("── phase 1: provision", CFG.projects, "projects (OxiWire admin)");
  let t = now();
  const projects = await provision();
  log(`provisioned ${projects.length} projects in ${((now() - t) / 1000).toFixed(1)}s`);

  log(
    "── phase 2: seed",
    CFG.collections,
    "collections ×",
    CFG.rows,
    "rows/collection =",
    (CFG.projects * CFG.collections * CFG.rows).toLocaleString(),
    "docs",
  );
  t = now();
  const { doneDocs } = await seed(projects);
  const seedSecs = (now() - t) / 1000;
  log(
    `seeded ${doneDocs.toLocaleString()} docs in ${seedSecs.toFixed(1)}s` +
      ` (${Math.round(doneDocs / seedSecs).toLocaleString()} docs/s)`,
  );

  log("── phase 3: load —", CFG.concurrency, "concurrent oxibase-js clients for", CFG.duration, "s");
  t = now();
  const { ok, err, lat } = await loadPhase(projects);
  const secs = (now() - t) / 1000;

  log("── results ──────────────────────────────────────");
  log(`queries ok    : ${ok.toLocaleString()}  (errors: ${err})`);
  log(`throughput    : ${Math.round(ok / secs).toLocaleString()} queries/s`);
  log(`latency p50   : ${pct(lat, 50).toFixed(1)} ms`);
  log(`latency p95   : ${pct(lat, 95).toFixed(1)} ms`);
  log(`latency p99   : ${pct(lat, 99).toFixed(1)} ms`);
  log(`latency max   : ${(lat[lat.length - 1] ?? 0).toFixed(1)} ms`);
  log("BENCH_DONE");
}

main().catch((e) => {
  console.error("BENCH_FAILED", e);
  process.exit(1);
});
