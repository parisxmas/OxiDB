# OxiBase Notes — example app

A tiny React + Vite app that does full CRUD (select / insert / update / delete)
against an OxiBase project using [`oxibase-js`](../../) — the same
`createClient().from("notes").select()` API you'd use with Supabase.

## Run

1. In the OxiBase dashboard, **Open a project → API keys** and copy the project
   `ref` and a key.
2. Configure the connection:

   ```bash
   cp .env.example .env
   # edit .env: VITE_OXIBASE_URL, VITE_OXIBASE_REF, VITE_OXIBASE_KEY
   ```

   `.env` is gitignored — never commit real keys.

3. Make sure the OxiDB data plane is running (this demo also uses the SQL engine
   nowhere, so plain `oxidb-server` is enough), then:

   ```bash
   npm install
   npm run dev
   ```

The app stores notes in a document collection (`demo_notes`), auto-created on the
first insert.

## Auth note

This demo uses the **service_role** key so writes work directly from the browser
— fine on localhost, **not** for production. In a real app, keep `service_role`
server-side and use the browser-safe **anon** key together with security rules
(the OxiBase RLS analog).

## How it connects

All of it is in [`src/oxibase.ts`](src/oxibase.ts):

```ts
import { createClient } from "oxibase-js";
export const oxibase = createClient(url, key, { ref });
```

Everything else in `src/App.tsx` is ordinary `oxibase.from("demo_notes")...`
calls — identical to Supabase.
