# OxiBase dashboard

The control-plane web UI for OxiBase — a **static React SPA** (React + TypeScript
+ Vite). It talks to the OxiBase API (`/platform/v1/*`) for developer auth and
project provisioning; per ADR-0021 it is served as static assets (nginx/CDN) and
never enters a Rust binary.

Matches the OxiDB Studio (`oxidb-app`) stack so components (SQL editor, CRUD
grid) can be shared.

## Features

- **Auth** — signup / login against `/platform/v1`, session JWT in `localStorage`.
- **Projects** — list, create (provisions an isolated tenant database), view/copy
  the `anon` + `service_role` keys, rotate keys, delete. **Open** a project to
  edit its data.
A project opens with three tabs — the document and SQL engines are separate
stores with separate objects:

- **Collections** (`DataBrowser`) — the **document engine**. Browse collections,
  view rows in a grid, insert a JSON document, delete a row. Over the PostgREST
  surface (`/rest/v1/{collection}?db=<ref>`).
- **SQL Tables** (`SqlTables`) — the **SQL engine**. Browse tables (`SHOW TABLES`
  with row counts), view each table's rows and its schema (`DESCRIBE`), over
  `/api/sql?db=<ref>`.
- **SQL** (`SqlRunner`) — run ad-hoc DDL/DML/`SELECT` batches against the SQL
  engine, results rendered as a grid.

The SQL tabs require the data plane to run with `OXIDB_SQL=1`.

Data-plane calls use the project's `service_role` key (this is the developer's
own admin console).

## Develop

```bash
npm install
VITE_OXIBASE_URL=http://127.0.0.1:4460 \
VITE_OXIDB_URL=http://127.0.0.1:8087 \
  npm run dev
```

- `VITE_OXIBASE_URL` — the OxiBase control-plane API base (auth + projects).
- `VITE_OXIDB_URL` — the oxidb-server **data-plane** REST base (table/SQL editor).

Leave either empty to call the same origin (when the dashboard is served behind a
proxy that routes `/platform/*` to oxibase and `/rest/v1` + `/api/*` to the data
plane).

## Build & deploy

```bash
npm run build          # -> dist/ (static assets)
```

Serve `dist/` from nginx/CDN under the control-plane host. It is a pure SPA — no
server runtime.

## Verify the API contract

`test/api.e2e.mjs` drives the exact calls the UI makes against a running oxibase:

```bash
OXIBASE_URL=http://127.0.0.1:4460 node test/api.e2e.mjs
```
