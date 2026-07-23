# OxiBase dashboard

The control-plane web UI for OxiBase — a **static React SPA** (React + TypeScript
+ Vite). It talks to the OxiBase API (`/platform/v1/*`) for developer auth and
project provisioning; per ADR-0021 it is served as static assets (nginx/CDN) and
never enters a Rust binary.

Matches the OxiDB Studio (`oxidb-app`) stack so components (Monaco SQL editor,
CRUD grid) can be shared as the table/SQL-editor pages land.

## v1 (this)

- **Auth** — signup / login against `/platform/v1`, session JWT in `localStorage`.
- **Projects** — list, create (provisions an isolated tenant database), view/copy
  the `anon` + `service_role` keys, rotate keys, delete.

Planned next: a **table editor** and **SQL runner** over the selected project's
data plane (`/rest/v1?db=<ref>`) using the zero-dep `oxidb-js` SDK.

## Develop

```bash
npm install
VITE_OXIBASE_URL=http://127.0.0.1:4460 npm run dev   # points at a local oxibase
```

`VITE_OXIBASE_URL` is the OxiBase API base; leave empty to call the same origin
(when the dashboard is served behind the proxy that routes `/platform/*` to
oxibase).

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
