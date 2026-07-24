# barisdb — OxiBase example app

A React + Vite app driving **both engines** of one OxiBase project (`barisdb`)
with [`oxibase-js`](../../):

- **`addressses`** — a SQL-engine table (`id` PK, `name`, `surname`, `address`,
  `birthdate`), used through the `oxibase.sql()` extension with parameterized
  statements (`INSERT … VALUES (?, …)`).
- **`deneme`** — a document collection, used through the Supabase-style
  `oxibase.from("deneme").select()/insert()/delete()` builder.

## Run

1. In the OxiBase dashboard, **Open a project → API keys** and copy the project
   `ref` and the **service_role** key.
2. Configure the connection:

   ```bash
   cp .env.example .env
   # edit .env: VITE_OXIBASE_URL, VITE_OXIBASE_REF, VITE_OXIBASE_KEY
   ```

   `.env` is gitignored — never commit real keys.

3. ```bash
   npm install
   npm run dev
   ```

## Auth

This demo uses the **service_role** key (role `admin`) because the SQL engine
over REST is RBAC-gated: the anon key (role `read`) can only `SELECT`, never
`INSERT`/`UPDATE`/`DELETE` — no rule can change that. That makes the
service_role key acceptable **only for a localhost demo**; never ship it in a
real browser bundle.

For a document-only app the Supabase model applies unchanged: use the anon key
plus a security rule on the collection (`setup.mjs` shows how — it installs a
public rule with the service_role key, server-side).

## How it connects

All of it is in [`src/oxibase.ts`](src/oxibase.ts):

```ts
import { createClient } from "oxibase-js";
export const oxibase = createClient(url, key, { ref });
```

`src/App.tsx` then talks to both engines of the same project:

```ts
await oxibase.sql('SELECT * FROM "addressses" ORDER BY "id"');
await oxibase.from("deneme").insert({ name });
```
