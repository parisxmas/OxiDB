# Stability Policy

**Status:** Effective once OxiDB 1.0.0 ships.

**Source decisions:** [ADR-0003](decisions/0003-1.0-stability-scope.md) (scope)
and [ADR-0004](decisions/0004-phase-0-answers.md) (per-client tiering +
promotion criteria). This document is the operational version; the ADRs are
the source of truth.

## What the 1.0 stable surface covers

The 1.0 backward-compatibility promise (see [SEMVER.md](SEMVER.md)) applies
exactly to what's listed below. Anything not listed is **experimental** and
may evolve in any 1.x release.

### Engine — document core (stable)

- **Document CRUD**: `insert`, `insert_many`, `find`, `find_one`, `update`,
  `update_one`, `delete`, `delete_one`, `count`
- **JSON query operators**: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`,
  `$in`, `$nin`, `$exists`, `$regex`, `$elemMatch`, `$all`, `$size`, `$not`,
  `$type`, `$mod`, `$and`, `$or`, `$nor`, `$expr`
- **Update operators**: `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`,
  `$rename`, `$currentDate`, `$push`, `$pull`, `$addToSet`, `$pop`
- **Aggregation pipeline stages**: `$match`, `$group`, `$sort`, `$skip`,
  `$limit`, `$project`, `$count`, `$unwind`, `$addFields`, `$lookup`, `$out`,
  `$dateHistogram`
- **Accumulators**: `$sum`, `$avg`, `$min`, `$max`, `$count`, `$first`,
  `$last`, `$push`, `$addToSet`, `$percentile`
- **Indexes**: single-field, unique, and composite
- **Transactions**: OCC with 3-phase commit (current `src/transaction.rs`)
- **Document full-text search**: `text_search` with TF-IDF, default settings
- **Blob storage core**: `create_bucket`, `list_buckets`, `delete_bucket`,
  `put_object`, `get_object`, `head_object`, `delete_object`, `list_objects`
- **Encryption at rest**: AES-GCM via `OXIDB_ENCRYPTION_KEY`

### Wire / protocol (stable)

- **TCP OxiWire** — with the 1.0 `HELLO` handshake (version + feature
  negotiation)
- **TLS**: `OXIDB_TLS_CERT` / `OXIDB_TLS_KEY`
- **SCRAM-SHA-256** authentication (RFC 7677, stored-verifier model)
- **RBAC roles**: `Admin`, `ReadWrite`, `Read`

### On-disk format (stable, with refuse-newer / read-older invariants)

- `.btree` (OXBT-headed)
- `.wal` (OXWA-headed, v1 + v2 records)
- `.fidx` / `.cidx` (OXIX-headed)
- `_tx_commit_log` (OXTX-headed)
- Blob layout: `_blobs/<bucket>/<id>.data` + `<id>.meta` (JSON `format_version`)

Every file type carries a version header. The engine refuses to open a newer
format and reads all currently-deployed format versions. See
[`docs/format/`](format/) for per-file specs and [the migration tool](#migrations)
below.

### Configuration env vars (stable)

`OXIDB_ADDR`, `OXIDB_DATA`, `OXIDB_POOL_SIZE`, `OXIDB_IDLE_TIMEOUT`,
`OXIDB_ENCRYPTION_KEY`, `OXIDB_TLS_CERT`, `OXIDB_TLS_KEY`, `OXIDB_AUTH`,
`OXIDB_AUDIT`, `OXIDB_LAZY_SYNC`, `OXIDB_SYNC_INTERVAL_MS`.

Adding new env vars is additive (MINOR). Changing the default of an existing
one follows the rule in [SEMVER.md §"Optional config defaults"](SEMVER.md).

## Client SDK tiers

### Tier A — covered by the 1.0 backward-compat promise

Each Tier-A client tags a `1.0` package, audits its public surface, marks
experimental APIs as such, and ships an `api/v1.json` snapshot diffed in CI.

- `clients/python/` — TCP client (`oxidb`)
- `python-embedded/` — embedded FFI (`oxidb-embedded`)
- `clients/go/` — `oxidb-go` (OxiWire)
- `clients/julia/OxiDb` — TCP (already document-only and Tables.jl-aligned per
  [ADR-0001](decisions/0001-julia-no-dbinterface.md))
- `julia/OxiDbEmbedded` — embedded FFI
- `clients/dotnet/OxiDb.Client.Tcp`
- `clients/dotnet/OxiDb.Client.Embedded`
- `clients/dotnet/OxiDb.Linq` — typed query syntax over either .NET client
- `clients/js/` — JS/TS (REST + WebSocket)
- `oxidb-java/` — pure-Java OxiWire client (`com.oxidb:oxidb-client`)

### Tier B — shipped but explicitly Experimental

Tier-B clients ship with a `1.0-experimental` (or rolling `0.x`) version
suffix and a README header note stating that breaking changes can occur in
any 1.x minor of the engine. They graduate to Tier A via the promotion
mechanism below.

- `php/` (PHP TCP + FFI bindings)
- `clients/swift/` (iOS C-FFI bindings)

## Experimental subsystems (NOT covered by 1.0)

The following features ship in OxiDB 1.0 binaries but are explicitly outside
the stable surface. They are gated by env vars, feature flags, or
"Experimental" namespaces:

### Engine — opt-in subsystems

- **PITR** (`OXIDB_PITR`) — format and CLI may evolve
- **Raft cluster mode** (`OXIDB_NODE_ID`, `--features cluster`) — needs
  Jepsen-style hardening before commitment
- **FDW / linked collections** — adapters, auth passthrough, write proxy
  surface all in motion
- **OxiScript stored procedures** — DSL likely to evolve
- **Vector search** (`vidx`) — APIs and on-disk format may change
- **Security rules + JWT auth** (Firebase-like layer, `OXIDB_JWT_SECRET`)
- **TTL indexes** — semantics largely stable but eviction cadence + edge
  cases need pinning before promotion

### Alternate / additional wire protocols

- REST HTTP API (`OXIDB_HTTP_PORT`)
- WebSocket (`OXIDB_WS_PORT`)
- OxiMem RESP (`OXIDB_OXIMEM_PORT`)
- MQTT v3.1.1 (`OXIDB_MQTT_PORT`)
- S3 API (`OXIDB_S3_PORT`)
- GELF ingest (`OXIDB_GELF_PORT`, `OXIDB_UDP_PORT`)

Each is individually a candidate for stabilisation in a 1.x minor via the
promotion process below.

### Query/auxiliary features

- Aggregation stages not in the stable list (new stages are experimental
  until promoted)
- Change streams / `watch` (transport semantics may change)
- Procedure scheduler

## Promotion: experimental → stable

<a id="promotion"></a>
A subsystem moves into the stable surface only when **all five** of the
following hold (per [ADR-0004 §1](decisions/0004-phase-0-answers.md)):

1. The subsystem's surface (API + on-disk format + wire shape) has been on
   master for **≥ 6 months without breaking changes**
2. A written spec exists in `docs/` — for on-disk format, wire shape, or API
   as appropriate
3. Line coverage for the subsystem's code is **≥ 70%** in CI, with at least
   one integration test exercising the full request → durable-state →
   readback cycle
4. At least **one external user** is running it in production (a documented
   case study, not a CI fixture)
5. A dedicated **promotion ADR** is merged (`ADR-00NN: promote X to stable
   in 1.Y`), recording the version it lands in and the deprecation window
   for any prior experimental shape

The same five-criterion bar applies to Tier-B clients seeking promotion to
Tier A.

## Migrations

<a id="migrations"></a>
Any on-disk format change ships with a corresponding `oxidb migrate` upgrade
path. The CLI scaffold lives in `oxidb-cli/src/migrate.rs`; see
[ADR-0003 Phase 4](decisions/0003-1.0-stability-scope.md). Format changes
that the migration tool cannot handle automatically are major releases (see
[SEMVER.md §MAJOR](SEMVER.md)).
