# oxibase-js

The JavaScript/TypeScript client for [OxiBase](../oxibase).

OxiBase's data plane implements the PostgREST wire grammar (ADR-0019), so
`createClient(url, key).from("table")` gives you a full-featured PostgREST
query builder. On top of that, oxibase-js adds:

- **per-project targeting** — every request carries `?db=<ref>`
- **bearer auth** — the project's `anon` or `service_role` key
- **`.sql()`** — the standalone SQL engine
- **`.subscribe()`** — realtime change events over WebSocket
- **`.storage`** — per-project file storage
- **`.auth`** — end-user sign-up / sign-in with sessions

## Install

```bash
npm install oxibase-js
```

## Quick start

```ts
import { createClient } from "oxibase-js";

const oxibase = createClient(
  "https://your-oxidb-host",   // data-plane REST origin
  ANON_KEY,                    // from the dashboard: Open a project → API keys
  { ref: "your-project-ref" },
);

// The full PostgREST query builder:
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

## Which engine? (document / SQL / time-series)

OxiBase has three engines behind one URL. How you call decides which one serves
the request:

| Engine | How you reach it | Example |
| --- | --- | --- |
| **Document** (collections) | `.from(name)` where `name` is **not** a SQL table (the default). Collections are auto-created on first insert. | `oxibase.from("notes").select("*")` |
| **SQL** (tables) | `.from(name)` where `name` **is** a SQL table, or `.sql(...)` directly. | `oxibase.from("orders").select("*")` · `oxibase.sql("SELECT …")` |
| **Time-series** (measurements) | `.schema("tsdb").from(measurement)` — sends `Accept-Profile: tsdb`. | `oxibase.schema("tsdb").from("cpu").select("usage")` |

Notes:

- **Document vs SQL is by name, not a flag.** A collection and a SQL table can
  never share a name, so dispatch is unambiguous — but that also means *you*
  decide by how the object was created: `oxibase.sql("CREATE TABLE orders …")`
  makes `orders` a SQL table (so `.from("orders")` is SQL); anything else is a
  document collection. There is no per-request override for this pair.
- **Time-series is explicit** via the schema profile, because a measurement
  doesn't exist until its first write (so existence can't route it).
- **The SQL and time-series engines are off by default** — start the data plane
  with `OXIDB_SQL=1` / `OXIDB_TSDB=1`.

**How to tell what lives where:**

```ts
// document collections:
const cols = await (await fetch(`${oxibase.url}/api/collections?db=${oxibase.ref}`,
  { headers: { Authorization: `Bearer ${KEY}` } })).json();   // { collections: [...] }
// SQL tables:
const { results } = await oxibase.sql("SHOW TABLES");          // rows of [table, rowCount]
```

In the OxiBase dashboard these are the **Collections** and **SQL Tables** tabs.

## Keys & permissions

- **anon key** (role `read`) — browser-safe; read-only over REST.
- **service_role key** (role `admin`) — server-side only; full read/write,
  bypasses rules. Never ship it to a browser in production.

## End-user auth

Your app's own users sign in against the project. Pass `authUrl` (the control
plane) to `createClient`, then:

```js
// email + password
await oxibase.auth.signUp({ email, password });
await oxibase.auth.signInWithPassword({ email, password });

// social sign-in — configure the provider in the console's Users tab first
const { providers } = await oxibase.auth.getSettings();     // ["google", "github"]
oxibase.auth.signInWithOAuth({ provider: "github", redirectTo: "https://app/callback" });

// on the page the provider sent them back to:
const session = oxibase.auth.getSessionFromUrl();           // adopts + cleans the URL

// or, with a Google ID token you already hold:
await oxibase.auth.signInWithIdToken({ provider: "google", token: credential });

oxibase.auth.getSession();   // { token, refreshToken } | null
oxibase.auth.signOut();      // back to the client's original key
```

Every `.from()` / `.sql()` call then runs as that user, so security rules see
their identity. Access tokens refresh automatically on a 401.

## Example app

[`examples/notes`](examples/notes) — a tiny React app that does full CRUD against
an OxiBase project with this client.

## Develop

```bash
npm install
npm run build     # tsc -> dist/
npm test          # provisions a throwaway project on a running OxiBase and drives the SDK
```

The test expects a running control plane (`OXIBASE_CP_URL`, default
`http://127.0.0.1:4460`) and data plane (`OXIBASE_DATA_URL`, default
`http://127.0.0.1:8087`, started with `OXIDB_SQL=1`).
