# OxiBase multi-tenant load benchmark

Provisions N isolated tenant projects, seeds each with `COLLECTIONS × ROWS`
documents, then runs `CONCURRENCY` concurrent [`oxibase-js`](../../oxibase-js)
clients querying their own project at once — as if each were a separate app.
While it runs, the data plane and control plane log **every request to GELF**,
which OxiDB ingests into its own `_gelf_logs` collection for later inspection.

It talks to the **internal** data plane (bypassing Cloudflare's bot filter), so
it must run on the compose network.

## Run (on the server, in a node container on the compose network)

```sh
docker run --rm --network oxibase_default \
  -v /root/oxibase:/repo -w /repo/bench/oxibase-load \
  -e PROJECTS=20 -e COLLECTIONS=5 -e ROWS=5000 \
  -e DURATION=30 -e CONCURRENCY=20 \
  node:20-slim node run.mjs
```

`oxibase-js` is imported from `/repo/oxibase-js/dist/index.js` (its `node_modules`
must be present — ship them alongside), overridable via `OXIBASE_JS`.

## Knobs (env)

| var | default | meaning |
|---|---|---|
| `PROJECTS` | 20 | tenant projects to provision |
| `COLLECTIONS` | 5 | collections per project |
| `ROWS` | 5000 | documents per collection |
| `BATCH` | 1000 | insert batch size |
| `DURATION` | 30 | load-phase seconds |
| `CONCURRENCY` | 20 | concurrent query clients |
| `SEED_CONC` | 16 | concurrent seed workers |
| `WIRE` | `data-plane:4444` | OxiWire admin endpoint (provisioning) |
| `REST_BASE` | `http://data-plane:8087` | REST base for seed + query |
| `OXIBASE_JS` | `/repo/oxibase-js/dist/index.js` | oxibase-js module |

Provisioning uses the OxiWire admin path (unauthenticated admin when
`OXIDB_AUTH` is off) because developer signup is Google-only; each project gets a
generated ES256 key and an admin (`service_role`) JWT minted locally.

## Inspecting the GELF logs afterward

Every request lands in the default database's `_gelf_logs` collection (enable
with `OXIDB_GELF_PORT` on the data plane). Query it over the wire / REST to
summarise what happened (counts by path/status, latency).
