# OxiBase Notes — example app

A tiny React + Vite app that does full CRUD (select / insert / update / delete)
against an OxiBase project using [`oxibase-js`](../../) — the same
`createClient().from("notes").select()` API you'd use with Supabase.

## Run

1. In the OxiBase dashboard, **Open a project → API keys** and copy the project
   `ref`, the **anon** key, and the **service_role** key.
2. Configure the connection:

   ```bash
   cp .env.example .env
   # edit .env: VITE_OXIBASE_URL, VITE_OXIBASE_REF, VITE_OXIBASE_KEY (anon key)
   ```

   `.env` is gitignored — never commit real keys.

3. Install the security rule that lets the anon key write `demo_notes` (one time,
   with the service_role key — see [Auth](#auth) below):

   ```bash
   OXIBASE_REF=<ref> OXIBASE_SERVICE_ROLE_KEY=<service_role key> node setup.mjs
   ```

4. Make sure the OxiDB data plane is running, then:

   ```bash
   npm install
   npm run dev
   ```

The app stores notes in a document collection (`demo_notes`), auto-created on the
first insert.

## Auth

This demo uses the browser-safe **anon** key (role `read`) — the correct,
Supabase-style model. Two rules of the OxiBase data plane make that safe:

- The anon key is **read-only by default**; anon **writes are denied** unless a
  collection has a security rule that grants them (OxiBase's RLS analog).
- `setup.mjs` installs a public rule on `demo_notes`
  (`create/read/update/delete: "true"`) so this public demo can write. A real app
  would scope it to the row owner, e.g. `update: "auth.uid == doc.user_id"`.

The **service_role** key (role `admin`, bypasses rules) is used only by
`setup.mjs`, server-side — it never reaches the browser bundle.

## How it connects

All of it is in [`src/oxibase.ts`](src/oxibase.ts):

```ts
import { createClient } from "oxibase-js";
export const oxibase = createClient(url, key, { ref });
```

Everything else in `src/App.tsx` is ordinary `oxibase.from("demo_notes")...`
calls — identical to Supabase.
