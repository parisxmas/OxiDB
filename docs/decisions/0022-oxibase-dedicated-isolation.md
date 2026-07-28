# ADR-0022: Dedicated tenant isolation for OxiBase (one process per project)

**Status:** Proposed — 2026-07-28. Nothing is implemented; `"isolation":
"shared"` is a hardcoded literal on the project row today
([`oxibase/src/handlers.rs:250`](../../oxibase/src/handlers.rs)) and there is no
`dedicated` code path anywhere in `oxibase/` or `oxidb-server/`. This ADR
specifies what that field will one day mean.
**Related:** [ADR-0020](0020-oxibase-control-plane.md) (OxiBase design, the
per-project key and secret model), [ADR-0021](0021-oxibase-separate-service.md)
(the control plane as its own service; the `tenant_auth::project_secret` hook
this ADR extends), [ADR-0012](0012-multi-database.md) (`DatabaseManager` and the
Admin-only `create_database`/`drop_database` used for provisioning),
[ADR-0019](0019-postgrest-rest-surface.md) (the `/rest/v1` surface a tenant is
served on).

## Context

Every OxiBase project today is one database inside one shared `oxidb-server`
process. `tenant_auth::resolve_tenant` turns a URL segment (`?db=<ref>`, or the
path slug) into a database name, and everything downstream — PostgREST grammar,
rules/RLS, storage, SQL, TSDB — runs against it in-process. That density is
OxiDB's advantage over per-tenant-Postgres platforms: a few hundred tenants fit
where a competitor runs a few hundred containers.

Shared mode isolates more than it might appear to:

- **Storage** — a project is its own `DatabaseManager` database: own directory
  under `OXIDB_DATA`, own collections, own SQL engine, own TSDB, own buckets.
  Names cannot collide across tenants.
- **Keys** — each project has its own ES256 keypair, the private scalar sealed
  with `OXIDB_SEAL_KEY`. The data plane resolves the verification key *per
  target database*, so a token minted for project A cannot be replayed at B.
- **Rows** — security rules give row-level isolation between an app's own users.
- **Resources** — per-project `max_collections` / `max_tables` /
  `max_documents` / `max_storage_bytes` / `max_requests_per_min`, enforced in
  the data plane at the point the tenant resolves.

What it does not isolate is **the process**. All tenants share one worker pool,
one process-global document cache (0.39.10), and one address space. Two
consequences:

1. **Work, not requests, is the real resource.** `max_requests_per_min` counts
   requests. One tenant running expensive aggregations inside its budget can
   still saturate the pool and raise every other tenant's latency.
2. **Blast radius is the whole server.** This is not hypothetical: on
   2026-07-26 the shared data plane was OOM-killed by a single workload
   (3.4M rows written by a stress test plus self-logging, ~4.6 GB anon), taking
   every tenant on the host with it.

Neither is fixable inside a shared process without building a scheduler and a
per-tenant memory accountant — a large amount of machinery to approximate what
the OS already does between processes. And a paying customer's real question
("can a neighbour affect me, and can you prove separation?") wants a boundary
that can be pointed at, not a quota table.

The question this ADR answers: **what is the smallest change that makes a
project a separate process, without changing its URL, its SDK, or a single line
of the data-plane request path?**

## Decision

Add a second isolation mode. `isolation` on the project row stops being inert
and starts carrying a **location**; the data plane proxies to it at the one
point where a request is already attributed to a tenant.

```
                    ┌──────────────────────────────────────────┐
   client ──────────►  oxidb-server  (shared data plane)       │
   /<slug>/rest/v1  │                                          │
                    │  resolve_tenant(segment) ──► project row │
                    │        │                                 │
                    │        ├── shared     → serve locally    │
                    │        │                (today's path,   │
                    │        │                 byte-for-byte)  │
                    │        └── dedicated  → proxy to node ───┼──┐
                    └──────────────────────────────────────────┘  │
                                                                  ▼
                                          ┌───────────────────────────────────┐
                                          │ oxidb-server (tenant abc123)      │
                                          │  own volume, own ports, own RAM   │
                                          │  same binary, unmodified handlers │
                                          └───────────────────────────────────┘
```

### 1. The project row carries a node

```json
{
  "ref": "abc123", "slug": "runclub",
  "isolation": "dedicated",
  "node": { "wire": "10.0.3.17:4444", "rest": "10.0.3.17:8090", "unit": "oxidb-tnt-abc123" },
  "…": "pubkey / priv_enc / quotas unchanged"
}
```

Shared projects keep `"isolation": "shared"` with **no** `node` key and take
exactly the path they take now. The absence of `node` — not the string — is
what the data plane branches on, so a row written before this ADR needs no
migration.

### 2. Provisioning gains a placement step

`handlers.rs` currently provisions with a single
`state.upstream.create_database(&project_ref)` against one fixed upstream. For a
dedicated project that becomes:

1. `Placement::provision(project_ref) -> NodeAddr` — a one-method trait,
   implemented per environment (Docker/compose first, since that is how the
   live stack already runs; systemd, Nomad or a K8s StatefulSet later). It
   launches an `oxidb-server` with its own data volume, its own ports, and the
   standard env.
2. Wait for readiness. `oxidb_client::Pool` already distinguishes `Connect`
   (retryable) from `Io` (not), so this is a bounded retry loop against a PING,
   not a new mechanism.
3. `create_database(ref)` against **that** node — the identical wire call, a
   different address.
4. Push the project's public key and limits into the node (§3).
5. Write `node` onto the project row.

Rollback already exists (`handlers.rs` drops the database when the row insert
fails); it gains "destroy the node". Deletion destroys the node and its volume.

### 3. The node gets its metadata pushed, not fetched

A dedicated node must verify that project's API keys (`project_secret`) and
enforce its quotas — both of which live in the shared `oxibase.projects` row
that the node, having its own data directory, cannot read.

**Decision: the control plane pushes.** At provision time, and again on
`POST /platform/v1/projects/{ref}/keys/rotate`, the control plane writes the
public key and the limits into the node over the wire (that endpoint already
invalidates `secret_cache`; it gains one more call). The node stores them
locally and verifies against them.

The rejected alternative is a **remote read** — the node's `tenant_auth` reads
the shared metadata database over the network, cached with the short TTL already
present in `project_secret`. It is less code, but it makes every dedicated
tenant depend at runtime on a shared component, which is exactly the property
the customer is paying to remove. With push, a dedicated tenant keeps serving
with the control plane down.

Note the direction this fixes: keys are pushed, never pulled, so a compromised
tenant node cannot enumerate other projects' rows.

### 4. Routing: one hop, decided where the tenant already resolves

The branch belongs in `oxidb-server/src/rest/mod.rs`, immediately after
`resolve_tenant` and **before** the quota and rate-limit checks — the same place
per-project rate limiting was put (0.39.16), and for the same reason: it is the
one point every surface on that listener passes through. If the row has a
`node`, the request is reverse-proxied to that node's REST listener and the
response streamed back; otherwise it is served locally, unchanged.

Everything downstream runs on the dedicated node **unmodified**, because it is
the same binary: rules, RLS, PostgREST grammar for all three engines, storage,
realtime. The tenant's URL, keys and SDK do not change — a project can move
between modes without its application noticing.

The alternative placement is an nginx map from slug to upstream at the edge. It
saves the hop, but couples every provision to a config reload. Do the in-process
proxy first; because the node address is on the project row, an edge map for the
heaviest tenants is derivable from it later without changing anyone's URL.

WebSocket (realtime) needs the same treatment on the WS listener — a proxied
upgrade rather than a proxied request. It is the one surface where "same
branch, different plumbing" applies.

### 5. Upgrading a live tenant, shared → dedicated

This is the operation that makes dedicated a sellable plan change rather than a
provisioning-time choice, and its primitives exist:

1. `POST /api/backup?db=<ref>` already streams a `tar.gz` of one database, and
   the engine's backup is low-lock with a **pinned generation**, so the archive
   is crash-consistent as of a well-defined pin instant while writes continue.
2. Provision the node, restore into it.
3. Flip the row to `dedicated` with the node address.
4. Drop the shared copy.

For v1, take a brief **read-only window** on that one tenant across steps 1–3
rather than building write-replay: the per-project refusal path already exists
in `tenant_auth::rate_limit_hit`, so a `maintenance` flag answering 503 with
`Retry-After` is a few lines. Zero-downtime migration is the same problem PITR
already solves (replay the WAL segments written after the pin) and can come
later, if a customer asks for it.

The reverse move — dedicated → shared, on downgrade — is the same procedure
with the endpoints swapped.

### 6. What stays global

OxiMem's keyspace and S3 buckets are process-global by design (ADR-0012) and do
not become per-tenant under dedicated mode. A dedicated node gets its own
OxiMem and its own S3 namespace by virtue of being its own process, which is
strictly more isolation than shared mode offers; nothing is shared back.

## Options considered

| Option | Verdict |
|---|---|
| **Process per tenant, proxied by the shared plane** (this ADR) | **Chosen.** The data-plane change is one branch at an existing chokepoint; the tenant's URL and SDK are untouched; the OS provides the isolation instead of a scheduler we would have to write. |
| Per-tenant scheduling + memory accounting inside one process | Rejected. Large machinery to approximate process boundaries, and it still cannot bound a single tenant's RSS well enough to survive the OOM case. It would also mean a resource model in the hot path of every request. |
| Edge routing only (nginx map, no proxy in Rust) | Rejected for v1, viable as an optimisation. Saves a hop but ties provisioning to config reloads and leaves the shared plane unable to place tenants dynamically. |
| Node fetches its metadata from the shared store | Rejected (§3). Reintroduces the shared runtime dependency dedicated mode exists to remove. |
| A separate binary for tenant nodes | Rejected. The value of "same binary" is that every surface — rules, three engines, realtime, storage — works on day one with no second implementation to keep in sync. |

## Consequences

**Gained**

- CPU and memory isolation between tenants, enforced by the OS. The gap
  `max_requests_per_min` cannot close (it counts requests, not work) closes.
- Blast radius of one tenant. The 2026-07-26 OOM would have killed one project.
- Per-tenant tuning: `OXIDB_POOL_SIZE`, cache budget, disk-first mode — even a
  pinned OxiDB version for a customer who does not want to move.
- Per-tenant backup, restore and PITR with no effect on neighbours.
- A separation story that can be pointed at: separate process, separate volume,
  separate key, and — if wanted — a separate encryption key at rest.

**Paid**

- An RSS and disk floor per tenant, so density drops from "hundreds per
  process" to "one process each". This is the whole trade: shared is the
  density advantage, dedicated is the thing that gets charged for.
- An orchestrator becomes a component OxiBase owns: health checks, restart
  policy, orphan detection (rows whose node never answers), and volume
  lifecycle on delete.
- One extra network hop per request unless the edge map is added later.
- Two placements to test against for every future data-plane feature. The
  mitigation is that the node runs the same binary, so the surface under test
  is identical — only the routing differs.

**Unchanged**

- Shared-mode requests: the branch is on the presence of `node`, and a shared
  row does not have one. No new work on the existing path.
- Tenant-facing contracts: URL, anon/service_role keys, SDKs, rules, quotas.

## Implementation sketch (for whoever picks this up)

1. `Placement` trait + a Docker implementation; `node` written on the project
   row; provisioning and deletion wired through it.
2. The metadata push endpoint on the data plane (`pubkey` + limits), called at
   provision and on key rotation.
3. The proxy branch in `rest/mod.rs`, plus the WS upgrade equivalent.
4. `maintenance` flag + the backup/restore migration path, both directions.
5. Health checks, orphan sweep, and a `GET /platform/v1/projects/{ref}/node`
   for the dashboard to show where a project lives.

Each step is independently shippable, and steps 1–3 are enough to provision a
*new* project as dedicated; step 4 is what makes it a plan upgrade.
