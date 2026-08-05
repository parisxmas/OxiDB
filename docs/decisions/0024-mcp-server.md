# ADR-0024: MCP server — a Model Context Protocol front end as a standalone client binary

**Status:** Accepted — v1 (stdio transport, the full read tool set across all
three engines + FTS, flag-gated writes) landed & tested 2026-08-05: 26 unit +
stdio tests offline, and a 26-check live e2e
([`oxidb-mcp/tests/live_e2e.py`](../../oxidb-mcp/tests/live_e2e.py), opt-in)
driving the real binary against a real server —
documents (insert/find-with-truncation/count/aggregate/explain), SQL
(DDL/params/DESCRIBE/read-only refusal), TSDB (downsample + group_by), FTS
refusal-not-empty, write-gating and db-pinning. **Host acceptance done the
same day**: a real headless Claude Code session (`claude -p --mcp-config`)
connected, listed the tools, and answered a five-part cross-engine question
(collections+counts, document `$group`, SQL aggregate, grouped TSDB means,
and an `explain` naming the strategy) with every value matching the seeded
data exactly.
**Supersedes:** —
**Related:** [ADR-0021](0021-oxibase-separate-service.md) (the precedent: a
standalone binary on `oxidb-client`, no engine involvement),
[ADR-0023](0023-postgres-wire-protocol.md) (the other "speak someone else's
protocol" decision, and the source of the verify-against-the-real-client
lesson),
[ADR-0012](0012-multi-database.md) (the `db` parameter tools accept),
[ADR-0020](0020-oxibase-control-plane.md) (the hosted phase attaches here).

## Context

MCP (Model Context Protocol) is the open standard AI hosts use to reach
external tools and data: Claude Code, Claude Desktop, Cursor, VS Code and the
other agentic IDEs all speak it. A host connects to an MCP *server*, asks it
for a tool list (`tools/list`, each tool carrying a JSON Schema for its
parameters), and lets the model call them (`tools/call`). The protocol is
JSON-RPC 2.0 with a small handshake; transports are **stdio** (the host spawns
the server as a subprocess — the local/dev case) and **streamable HTTP** (a
remote endpoint — the hosted case).

What this means for a database: one MCP server makes the database usable *by
the model directly*. The assistant lists collections, inspects schemas, runs
queries and reads query plans without the user copy-pasting anything. Supabase,
MongoDB and the Postgres ecosystem all ship one, and for Supabase it has become
a real onboarding channel.

Where OxiDB stands today without one:

- The **SQL engine is already reachable by an agent** — ADR-0023 means a model
  with shell access can drive `psql` against OxiDB. That overlap is real and
  this ADR does not pretend otherwise.
- The **document, TSDB and full-text surfaces have no agent story at all.**
  They are reachable only through OxiWire or OxiDB-specific REST — neither of
  which a model can discover or call without custom glue.
- **Orientation is the actual bottleneck.** A model's first problem with any
  database is "what is in here"; today that answer requires knowing OxiWire's
  command names.

Three properties of the existing codebase make an MCP server cheap:

1. **`oxidb-client` already exists** (ADR-0021): length-prefixed JSON over
   TCP, client-side SCRAM-SHA-256, a self-healing connection pool, zero engine
   dependencies. The MCP server is a thin translation layer on top of it.
2. **`oxidb-http` already exists**, dependency-free, for the eventual HTTP
   transport.
3. **Authorization is already server-side.** SCRAM accounts and RBAC roles are
   enforced by the engine, so an MCP process connected with a Read-role
   account is read-only *at the server* — not by the MCP layer's good manners.
   Most database MCP servers bolt a "read-only mode" onto the client and hope;
   OxiDB does not have to.

The question this ADR answers: **where does MCP support live, what does v1
expose, and what keeps a model-driven client safe by default?**

## Decision

Add a standalone **`oxidb-mcp`** crate (a binary, like `oxibase`) built on
`oxidb-client`. **Nothing in `oxidb-server` changes.** It speaks MCP on one
side and OxiWire on the other.

Not a listener, for three reasons. The stdio transport *requires* a spawnable
local process — a host cannot spawn a port on a remote server, so a listener
could never serve the primary (local dev) case. A separate binary works against
any already-deployed server version, back to whatever `oxidb-client` speaks.
And it keeps the engine's risk at zero: every request is a call into machinery
that already existed, so the failure modes are the engine's, not a second
implementation's — the same argument ADR-0023 made for the PG listener, only
stronger, because here not even the server process is involved.

Configuration in a host is the standard shape:

```json
{
  "mcpServers": {
    "oxidb": {
      "command": "oxidb-mcp",
      "env": {
        "OXIDB_ADDR": "127.0.0.1:4444",
        "OXIDB_USER": "assistant",
        "OXIDB_PASSWORD": "..."
      }
    }
  }
}
```

Four decisions inside that shape the result:

### The protocol subset is hand-rolled, and pinned to a spec revision

The subset v1 needs — `initialize`, `initialized`, `tools/list`, `tools/call`,
`ping` — is a few hundred lines over `serde_json`. The official Rust SDK
(`rmcp`) would bring an async runtime and a dependency tree into a codebase
whose control plane is deliberately dependency-free; for five methods that is
the wrong trade. The MCP spec is young and moves — the binary declares the
spec revision it implements in `initialize` and negotiates down when the host
is older, and anything outside the subset is answered with JSON-RPC
`method not found`, never silently swallowed.

### v1 is stdio and read-only; the tool set covers all three engines

| Tool | Backs onto | Notes |
|---|---|---|
| `list_databases` | `list_databases` | |
| `list_collections` | `list_collections` | names + document counts |
| `list_tables` / `describe_table` | SQL `SHOW TABLES` / `DESCRIBE` | |
| `list_indexes` | `list_indexes` + SQL `SHOW INDEXES` | both engines, labeled |
| `find` / `count` / `aggregate` | document engine | full filter / pipeline JSON |
| `explain` | `explain` | the plan **and** real timing — a model that can read plans can fix its own slow queries |
| `sql_query` | `engine:"sql"` | parameterized (`params` binds `?`) |
| `tsdb_query` | `engine:"tsdb"` `op:"query"` | measurement/field/range/agg |
| `text_search` | FTS | BM25, per-collection |

Every tool takes an optional `db` (ADR-0012); `OXIDB_MCP_DB` pins the process
to one database so an agent can be scoped to a single project.

Write tools (`insert`, `update`, `delete`, `sql_execute`) exist in the code but
are **not registered** in `tools/list` unless `OXIDB_MCP_WRITES=1` — a model
cannot call a tool it was never offered.

### Safety is two independent gates, and the engine's is the one that counts

The tool-registration gate above is client-side and therefore advisory — a
compromised or buggy MCP layer could bypass it. The gate that counts is the
engine's: the documented setup is a dedicated **Read-role account** for the
assistant, at which point writes are refused by RBAC no matter what the MCP
process asks for. `OXIDB_MCP_WRITES=1` plus a ReadWrite account is a deliberate
two-step, not a default.

This posture is also the honest answer to prompt injection: anything the model
reads out of the database enters its context, and a hostile document that talks
a model into calling a write tool is a known attack shape. Read-only-by-default
does not make that impossible; it makes the blast radius zero until an operator
explicitly widens it twice.

### Results are budgeted for a context window, and truncation is stated

A tool result is JSON rendered as MCP text content. Reads default to a small
limit (50 rows/documents) with a hard cap (500) — a model that pulls 100k rows
into its own context has failed at its task, and the server should not help it.
When a cap trims a result, the result *says so* and reports the total (the
index-only count path makes the total cheap). A silent cap reads as "that was
everything", which is the same lie ADR-0023 refused to tell with empty catalog
results — the project rule is that "no more" and "no more that I'll show you"
are different answers.

## Consequences

**What this buys.** Any MCP host can orient itself in an OxiDB instance and
query all three engines plus full-text search — `claude mcp add` and the model
takes it from there. The document, TSDB and FTS surfaces get their first agent
story; `explain` as a first-class tool means the agent can diagnose, not just
query. The setup doubles as the RBAC showcase: "make your assistant a Read
account" is the security documentation.

**What it costs.** Another protocol surface to keep current, on a spec that is
still moving. Mitigations: the subset is five methods; version negotiation is
part of the handshake; and the binary is versioned and released with the
clients, not with the engine.

**Honest overlap.** SQL via MCP overlaps with SQL via `psql` (ADR-0023) for
any agent that has a shell. The unique value is everything that is not SQL —
documents, TSDB, FTS, orientation — plus hosts that have MCP but no shell
(Claude Desktop, mobile/web hosts).

**Verification is against real hosts, not the spec.** ADR-0023's scoping
lesson applies verbatim: every prediction about what clients need is wrong
until the client runs. Acceptance for v1 is the binary registered in Claude
Code and driven through real sessions — orientation, document queries, SQL,
TSDB, FTS, an explain-guided index fix — plus an offline test that speaks
stdio JSON-RPC directly and pins the handshake, the tool schemas, the
truncation notice, and that no write tool appears without the flag.

## Phase 2 — the hosted HTTP endpoint (landed 2026-08-05)

A second transport in the same crate: `POST /mcp/<project-ref>` with the
project's own key as a bearer token, selected by `OXIDB_MCP_HTTP_PORT`.
"Point Claude at your OxiBase project" with nothing installed locally.

**It backs onto the REST surface, not OxiWire — and that is the whole design.**
An OxiBase project authenticates with a per-project JWT, and the component that
verifies those keys, applies the project's row-level rules, and enforces its
per-project rate limit is the REST listener (ADR-0019/0020). Reaching the
engine over OxiWire instead would mean re-implementing all three here, holding
a copy of the seal key. So the hosted mode **forwards the caller's key
untouched** and inherits the gate that already exists. Nothing in `http.rs` or
`rest_wire.rs` decides who may read what; they translate shapes.

Two properties follow, and both are pinned by tests:

- **Every request is independent.** The ref comes from the path, the key from
  the header, and both build a `RestWire` for that one request. No session
  table, no cached credential — there is no state in which one tenant's key
  could be applied to another tenant's database.
- **The `Wire` trait, introduced to make the tools testable, turned out to be
  the seam for the second transport.** The tool logic, the truncation
  accounting and the write gating are byte-for-byte the same on both.

`explain` is refused by name here: it is a wire diagnostic with no REST
equivalent, and a 404 from a path that never existed is a worse answer.

**This work found two real holes in the read path** — both in shipped code,
neither reachable from the MCP layer's own logic, both fixed with red-first
regression tests (0.42.8):

1. **`GET /api/{col}/count` never consulted the read rule.** A collection with
   `read: false` answered `find` with 403 and `count` with the number anyway.
   Because count takes an arbitrary `?q=`, this was not a cardinality leak but
   an **arbitrary-predicate disclosure oracle**: `count?q={"email":"…"}` is an
   existence check, and `$gte` binary-searches a numeric field, all without
   ever being allowed to see a document. Same class as the `aggregate` gap
   closed in 0.39.21; this path was missed in that pass. A row-level rule is
   now *filtered* rather than refused — unlike `aggregate`, a count over the
   visible subset is exact, so refusing it would be gratuitous.
2. **Numeric rule comparisons compared JSON representations.** A rule literal
   parses with `parse::<f64>()`, so `1` in a rule is `Number(1.0)`, while a
   document that stored `1` holds `Number(1)` — and `serde_json::Value`
   equality calls those different. Every numeric comparison in a security rule
   was wrong, **in both directions**: `read: "doc.hidden != 1"` matched the
   drafts too and published them, while `read: "doc.hidden == 0"` — the same
   intent written the other way — hid everything. The leak direction is the
   serious one, and it is the one a developer is more likely to write.

Neither was findable by reading the MCP code; both surfaced from asking the
question *"does a rule actually stop this?"* and running it. That is the same
lesson as ADR-0023's driver work, in a different costume.

**Still deferred:** SSE streaming (nothing to stream — every tool is
request/response), MCP sessions (`Mcp-Session-Id`), resources and prompts, and
serving the endpoint from the `oxibase` control plane rather than beside the
data plane.

**Deliberately missing in v1** (each refused by name where a host asks):
MCP resources and prompts (tools only), sampling/elicitation, notifications
and realtime subscriptions (the WS surface exists; bridging it into MCP
subscriptions is additive later), the HTTP transport, and writes by default.
