# oxibase-js

A **Supabase-compatible** JavaScript/TypeScript client for [OxiBase](../oxibase).

OxiBase's data plane implements the PostgREST wire grammar (ADR-0019), so the
data API *is* the real [`@supabase/postgrest-js`](https://github.com/supabase/postgrest-js)
query builder. `createClient(url, key).from("table")` behaves exactly like
`supabase.from("table")`. On top of that, oxibase-js adds:

- **per-project targeting** — every request carries `?db=<ref>`
- **bearer auth** — the project's `anon` or `service_role` key
- **`.sql()`** — an OxiBase extension for the standalone SQL engine

## Install

```bash
npm install oxibase-js
```

## Use it like Supabase

```ts
import { createClient } from "oxibase-js";

const oxibase = createClient(
  "https://your-oxidb-host",   // data-plane REST origin
  ANON_KEY,                    // from the dashboard: Open a project → API keys
  { ref: "your-project-ref" },
);

// The full PostgREST / Supabase query builder:
const { data, error } = await oxibase
  .from("notes")
  .select("*")
  .eq("done", false)
  .order("created_at", { ascending: false })
  .limit(20);

await oxibase.from("notes").insert({ body: "hello" });
await oxibase.from("notes").update({ done: true }).eq("id", 1);
await oxibase.from("notes").delete().eq("id", 1);

// OxiBase extension — the SQL engine (needs OXIDB_SQL=1 on the server):
const { results } = await oxibase.sql("SELECT count(*) FROM notes WHERE done = ?", [true]);
```

`.from(t)` targets a **document collection** by default; if `t` names a **SQL
table**, the same call is served by the SQL engine (dispatch is automatic and
unambiguous — a collection and a SQL table never share a name).

## Keys & permissions

- **anon key** (role `read`) — browser-safe; read-only over REST.
- **service_role key** (role `admin`) — server-side only; full read/write,
  bypasses rules. Never ship it to a browser in production.

## Example app

[`examples/notes`](examples/notes) — a tiny React app that does full CRUD against
an OxiBase project with this client, exactly the way a Supabase quickstart does.

## Develop

```bash
npm install
npm run build     # tsc -> dist/
npm test          # provisions a throwaway project on a running OxiBase and drives the SDK
```

The test expects a running control plane (`OXIBASE_CP_URL`, default
`http://127.0.0.1:4460`) and data plane (`OXIBASE_DATA_URL`, default
`http://127.0.0.1:8087`, started with `OXIDB_SQL=1`).
