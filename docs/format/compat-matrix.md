# Wire-Protocol Compatibility Matrix

**Status:** Effective once OxiDB 1.0.0 ships.

**Source:** [ADR-0003](../decisions/0003-1.0-stability-scope.md) Phase 2 —
"OxiWire `hello` handshake; `/v1/` URL prefix for REST; subprotocol
versioning for WS; compat matrix doc."

This document defines how OxiDB client and server versions interoperate
across each wire protocol. It is the contract that lets a 1.0-tagged
client talk to any 1.x server (forward compat) and a 1.x server accept
any 1.0-tagged client (backward compat) for the full 24-month LTS window
defined in [SECURITY.md](../SECURITY.md).

## TL;DR

| Protocol | Versioning surface | 1.0 baseline | How to discover |
|---|---|---|---|
| OxiWire (TCP) | `wire_versions` array in `HELLO` | `1` | Send `{"cmd":"hello"}` on a fresh connection |
| REST HTTP | URL prefix | `/v1/` | `GET /v1/hello` returns server info |
| WebSocket | `Sec-WebSocket-Protocol` header | `oxidb.v1` | Offer the subprotocol on the upgrade request |
| OxiMem RESP, MQTT, S3, PG-wire, GELF | (experimental, see [STABILITY.md](../STABILITY.md)) | — | — |

## OxiWire (TCP) — `HELLO` handshake

### Negotiation

A client opens the TCP connection and **optionally** sends `HELLO` as its
first message:

```json
{
  "cmd": "hello",
  "client": "oxidb-py/1.0",
  "wire_versions": [1]
}
```

`client` is a free-form identification string (logged on the server for
diagnostics, never enforced). `wire_versions` is the list of OxiWire
versions the client knows how to speak; absent means `[1]` for
backward-compat with pre-1.0 clients.

The server responds:

```json
{
  "ok": true,
  "server": {
    "name": "oxidb-server",
    "version": "0.28.12",
    "wire_version": 1,
    "supported_wire_versions": [1],
    "stable_surface_version": "1.0",
    "features": ["fts", "blobs", "txn", "rbac", "tls", "encryption_at_rest", "audit", "scram_sha_256", "indexes", "aggregation"],
    "experimental_features": ["raft", "pitr", "vector_search", "fdw", "stored_procedures", "ttl_indexes", "change_streams", "rest_http", "websocket", "oximem", "mqtt", "s3", "gelf"],
    "auth_methods": ["scram-sha-256"]
  }
}
```

The server picks the **highest mutually-supported version** from
`wire_versions ∩ supported_wire_versions`. If the intersection is empty,
the server returns `{"ok": false, "error": "no compatible wire version (…)"}`
and the connection is the client's to close.

### Pre-auth, idempotent

`HELLO` is allowed **before** authentication — it carries no data and
cannot read or mutate state. It is also **idempotent** — a client may
send it again at any point in the connection (e.g. after auth completes)
without state changes beyond the new wire-version selection.

### Backward compat

A client that does not send `HELLO` defaults to wire version 1. Pre-1.0
clients continue to work against any 1.x server without modification.

## REST HTTP — `/v1/` URL prefix

The 1.0 REST stable surface is reached at `/v1/<path>`. Example:

```bash
curl http://localhost:7474/v1/api/collections
curl http://localhost:7474/v1/hello   # version-info endpoint
```

During the 1.x deprecation window the legacy bare path `/<path>`
(without the `v1` prefix) continues to route to the same handlers. The
legacy form gets removed in the major version that introduces breaking
changes to the REST shape (see [DEPRECATION.md](../DEPRECATION.md)).

`GET /v1/hello` is the REST equivalent of the OxiWire `HELLO` and returns
the same server-info fields. It is unauthenticated.

When a future 2.0 introduces breaking changes:

- `/v1/api/...` continues to work, frozen at 1.x semantics
- `/v2/api/...` carries the new semantics
- Bare `/api/...` either redirects to `/v2/api/...` or returns 410 Gone,
  per the 2.0 release notes

## WebSocket — `Sec-WebSocket-Protocol` subprotocol

The 1.0 WebSocket stable surface uses RFC 6455 subprotocol negotiation.
Client side:

```http
GET /ws HTTP/1.1
Upgrade: websocket
Sec-WebSocket-Protocol: oxidb.v1
Sec-WebSocket-Key: <random>
…
```

Server echoes the chosen subprotocol:

```http
HTTP/1.1 101 Switching Protocols
Upgrade: websocket
Sec-WebSocket-Protocol: oxidb.v1
Sec-WebSocket-Accept: <derived>
```

If the client does not include `Sec-WebSocket-Protocol`, the server
responds without the header and the connection still completes — the
session has no negotiated subprotocol. This is the backward-compat path
for pre-1.0 clients.

A client that offers multiple subprotocols (e.g.
`Sec-WebSocket-Protocol: oxidb.v1, oxidb.v2`) gets the highest one the
server recognizes. Currently only `oxidb.v1` is recognized.

## Cross-version compatibility matrix

The "X" cells are the cases the 1.0 stability promise covers. Other
combinations may work, but are not contractually supported:

| Client \ Server                     | 1.0 | 1.1 | … | 1.x final | 2.0 |
|-------------------------------------|:---:|:---:|:-:|:---------:|:---:|
| **1.0** (Tier A clients)            | X   | X   | X | X         | X¹  |
| **1.1**                             | X²  | X   | X | X         | X¹  |
| **1.x final**                       | X²  | X²  | … | X         | X¹  |
| **2.0**                             | —   | —   | — | —         | X   |
| **Pre-1.0** (`0.x` rolling)         | best-effort, no guarantee | | | | |

¹ Only via the `oxidb.v1` (WS) / `/v1/api/...` (REST) / `wire_version=1`
(OxiWire) legacy paths. The new 2.0 surfaces are not v1-client compatible.

² Forward-compat: an older client may not exercise new features added in
the newer server. The new features ship as additive surface (see
[SEMVER.md](../SEMVER.md)) so the client's existing calls keep working.

## Tier-A clients covered

Per [STABILITY.md §Tier A](../STABILITY.md), the following clients ship
`api/v1.json` snapshots and are diffed in CI to keep this matrix honest:

- `python/` (TCP), `python-embedded/` (FFI)
- `go/` (OxiWire)
- `julia/OxiDb` (TCP), `julia/OxiDbEmbedded` (FFI)
- `dotnet/OxiDb.Client.Tcp`, `dotnet/OxiDb.Client.Embedded`, `dotnet/OxiDb.Linq`
- `oxidb-js/` (REST + WebSocket)
- `oxidb-java/` (pure-Java OxiWire)

Tier-B clients (`php/`, `swift/`) ship with `1.0-experimental` markers and
are **not** covered by this matrix.

## How this matrix is enforced

1. **Server-side**: `oxidb-server/src/hello.rs` exposes
   `SUPPORTED_WIRE_VERSIONS` and reuses the same constants in REST `/v1/`
   routing and WS subprotocol negotiation. Bumping the stable surface
   requires editing exactly one place per protocol.
2. **Client-side (planned, Phase 3)**: each Tier-A client commits an
   `api/v1.json` snapshot. The CI diff test fails on any change to the
   client's 1.0 public surface unless an `intentional-v1-bump` label is
   set on the PR.
3. **Cross-version (planned, Phase 6 RC)**: docker-compose-driven
   matrix of `{client v0.28, v1.0} × {server v0.28, v1.0}` exercises
   the four corners before 1.0.0 GA.

## When to revisit

- A new wire version (`HELLO` v2, REST `/v2/`, `oxidb.v2` subprotocol) —
  requires its own ADR and updates here to add a row/column.
- Promotion of an experimental protocol (RESP, MQTT, S3, PG-wire, GELF)
  into the 1.x stable surface — requires a promotion ADR per
  [STABILITY.md §Promotion](../STABILITY.md) and matrix update.
- A 2.0 release — replaces the X¹ legacy-path qualifier with the actual
  deprecation/removal terms from the 2.0 release notes.
