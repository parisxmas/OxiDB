# ShopEdge — Sharded + Replicated OxiDB Cluster

A real-world deployment of OxiDB demonstrating **sharding** (oxipool) and
**replication** (Raft, via `--features cluster`) in a two-tier topology.

```
                  ┌────────────────┐
                  │  API (Go HTTP) │  :8080
                  └────────┬───────┘
                           ▼
                  ┌────────────────┐
                  │  pool-router   │  :4445  (top-tier oxipool)
                  │  CRC32 hash →  │
                  │  3 shards      │
                  └─┬──────┬─────┬─┘
       ┌────────────┘      │     └────────────┐
       ▼                   ▼                  ▼
 ┌───────────┐       ┌───────────┐      ┌───────────┐
 │pool-shard-a│      │pool-shard-b│     │pool-shard-c│   :4446 (per-shard pools)
 │ master/   │      │ master/    │     │ master/    │
 │ replicas  │      │ replicas   │     │ replicas   │
 └─┬─┬─┬─────┘      └─┬─┬─┬──────┘     └─┬─┬─┬──────┘
   ▼ ▼ ▼              ▼ ▼ ▼              ▼ ▼ ▼
 ┌──┐┌──┐┌──┐       ┌──┐┌──┐┌──┐       ┌──┐┌──┐┌──┐
 │A0││A1││A2│       │B0││B1││B2│       │C0││C1││C2│  :4444 client / :5000 raft
 └──┘└──┘└──┘       └──┘└──┘└──┘       └──┘└──┘└──┘
   Raft group A       Raft group B       Raft group C
```

## Quick start

```bash
cd ShardReplicaRealWorldTest

# Build images and bring up the cluster (9 dbs + 3 shard pools + router + API)
docker compose up -d --build

# Wait for cluster-init to finish (Raft bootstrap on each shard's leader)
docker compose logs -f cluster-init     # Ctrl+C when you see "cluster-init: done."

# Hit the API
curl -s http://localhost:8080/api/health        | jq
curl -s http://localhost:8080/api/topology      | jq
curl -s http://localhost:8080/api/raft/metrics  | jq

# Seed data (200 customers, 50 products, 1000 orders, 5000 events)
curl -s -X POST http://localhost:8080/api/seed | jq

# Sharded write
curl -s -X POST http://localhost:8080/api/cart \
     -H "content-type: application/json" \
     -d '{"customer_id": 42, "product_id": 1, "qty": 2}' | jq

# Read from replica (sharded by customer_id)
curl -s http://localhost:8080/api/cart/42 | jq

# Checkout (TX-pinned to one master)
curl -s -X POST http://localhost:8080/api/checkout \
     -H "content-type: application/json" \
     -d '{"customer_id": 42}' | jq

# Single-shard read
curl -s http://localhost:8080/api/orders/42 | jq

# Scatter-gather across all 3 shards
curl -s 'http://localhost:8080/api/orders?status=pending' | jq

# End-to-end smoke test (5 assertions)
docker compose --profile smoke run --rm smoke

# Tear down (deletes volumes — all data lost)
docker compose down -v
```

## What's running

| Service | Image | Listening | Purpose |
|---|---|---|---|
| `db-a0` … `db-c2` (9 total) | `shopedge/oxidb-server` | `:4444` (client) · `:5000` (raft) | OxiDB nodes; 3 Raft groups (A, B, C) |
| `pool-shard-a/b/c` (3) | `shopedge/oxipool` | `:4446` | Per-shard master/replica router (writes→master, reads→replicas RR) |
| `pool-router` | `shopedge/oxipool` | `:4445` (host-published) | Top-tier shard router; CRC32(shard_key) → shard |
| `cluster-init` | `shopedge/cluster-init` | — | One-shot: bootstraps each Raft group then exits |
| `api` | `shopedge/api` | `:8080` (host-published) | Go HTTP API in front of the router |
| `smoke` (profile: `smoke`) | `shopedge/smoke` | — | End-to-end validation harness |

Only `pool-router:4445` and `api:8080` are exposed on the host. Everything
else is reachable only inside the `shopedge` docker network.

## Data layout

| Collection | Shard key | Where it lives |
|---|---|---|
| `customers` | none (unsharded) | shard A (writes → A0, reads → A1/A2 RR) |
| `products` | none (unsharded) | shard A |
| `categories` | none (unsharded) | shard A |
| `orders` | `customer_id` | sharded across A · B · C |
| `carts` | `customer_id` | sharded across A · B · C (TX-pinned to master) |
| `events` | `customer_id` | sharded across A · B · C (time-series) |

Configured in `docker-compose.yml` under the `pool-router` service via
`OXIPOOL_SHARD_KEYS=orders:customer_id,carts:customer_id,events:customer_id`.

## API endpoints

```
GET  /                         landing page
GET  /api/health               ping router + every db node
GET  /api/topology             configured topology snapshot
GET  /api/raft/metrics         raft state of all 9 nodes (leader, term, log)
POST /api/seed                 populate cluster with synthetic data
GET  /api/products             catalog browse (unsharded → shard A replicas)
POST /api/cart                 add line {customer_id, product_id, qty}
GET  /api/cart/:customer_id    fetch cart (sharded read)
POST /api/checkout             {customer_id} — creates order, clears cart in TX
GET  /api/orders/:customer_id  order history (sharded read)
GET  /api/orders?status=...    cross-shard query (scatter-gather)
```

## Smoke test

`docker compose --profile smoke run --rm smoke` runs five assertions and
prints a pass/fail table:

1. **Cluster health** — pings the router and all 9 db nodes.
2. **Sharding distribution** — inserts orders with `customer_id` 1..60 via the
   router, then direct-counts on each shard's master. Expects the totals to
   match `CRC32(customer_id) % 3` exactly.
3. **Raft replication** — inserts via the router; queries each replica
   directly; expects the row to appear on all 3 nodes of the target shard.
4. **TX pinning + read-your-writes** — `begin_tx`, insert, find inside the
   TX (must see the uncommitted row), commit. Confirms the connection is
   pinned to a master for the TX duration.
5. **Scatter-gather** — `find` without the shard key returns docs from all
   three shards; total matches what was written.

## Known caveats

### Static-master assumption

`pool-shard-a` is configured with `OXIPOOL_MASTER=db-a0:4444` — a fixed value.
Raft leadership is dynamic, so under normal operation A0 stays the leader and
everything works. **If A0 fails**, the remaining nodes elect a new leader
(say A1), but oxipool keeps trying A0 → writes will fail until A0 returns.

The honest fix is a leader-aware oxipool (subscribe to `raft_metrics` and
update `master` when leadership changes) or a sidecar that rewrites the
config when leadership shifts. This isn't built yet.

### Cluster bootstrap is required exactly once

`cluster-init` runs `raft_init` + `raft_add_learner` + `raft_change_membership`
on each shard's `*0` node on first startup. It's safe to run again — the
errors are logged and ignored. The data volumes (`db-X-data`) persist Raft
state across restarts, so subsequent `docker compose up` calls won't need
re-init.

If you `docker compose down -v` (deletes volumes), the next `up` will
re-init from scratch.

### Volumes

Nine named volumes (`db-a0-data` … `db-c2-data`). For a clean slate:

```bash
docker compose down -v
```

## Files

```
ShardReplicaRealWorldTest/
├── docker-compose.yml
├── README.md                 (this file)
├── api/                      Go HTTP API in front of the router
│   ├── main.go               entry point + http server setup
│   ├── server.go             Server struct, route registration, helpers
│   ├── handlers.go           all endpoint handlers
│   ├── seed.go               POST /api/seed implementation
│   ├── raw.go                tiny TCP client for raft_metrics
│   ├── go.mod
│   └── Dockerfile
├── cluster-init/             one-shot Raft bootstrapper
│   ├── main.go
│   ├── go.mod
│   └── Dockerfile
└── smoke/                    end-to-end validation harness
    ├── main.go
    ├── go.mod
    └── Dockerfile
```

The Dockerfiles use `context: ..` (the repo root) so they can pull in the
existing `Dockerfile`, `Dockerfile.oxipool`, and the `go/oxidb` + `go/oxiwire`
client libraries via local module replaces.
