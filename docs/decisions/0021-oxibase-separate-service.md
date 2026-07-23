# ADR-0021: OxiBase as a separate control-plane service

**Status:** Accepted & complete — 2026-07-23. Landed: seal-key separation (§4),
the shared `oxidb-http` crate (server + client), a working separate **`oxibase`**
binary (control plane) proven two-process against `oxidb-server` (signup →
provision → the data plane resolves the per-project secret from the shared
store), and the cleanup — the data plane's OxiBase surface is now the single
`tenant_auth::project_secret` hook (`oxidb-server/src/tenant_auth.rs`, ~140
lines); the in-server control-plane handlers are deleted and `/platform/v1` is
no longer served by the data plane. Remaining OxiBase work is additive (dashboard
SPA, (B) JWKS, anon-with-rules). Revises the deployment decision in ADR-0020
(control plane *inside* `oxidb-server`).
**Supersedes:** the "not a new crate for v1" option in [ADR-0020](0020-oxibase-control-plane.md).
**Related:** [ADR-0020](0020-oxibase-control-plane.md) (OxiBase design, secrets,
key roles), [ADR-0012](0012-multi-database.md) (`DatabaseManager` + the Admin-only
`create_database`/`drop_database` wire commands used for provisioning),
[ADR-0019](0019-postgrest-rest-surface.md) (the `/rest/v1` data plane a project
exposes), [`oxidb-server/src/platform/`](../../oxidb-server/src/platform/) (the
skeleton being split out).

## Context

ADR-0020's skeleton put the whole control plane — signup, projects, key
rotation, abuse guards — as a `/platform/v1/*` route family *inside*
`oxidb-server`. That was the right call to validate the design quickly, and it
works end-to-end. But it couples two things that should evolve independently:

- the **data plane** — the database engines + wire + `/rest/v1`, whose job is to
  be a fast, lean, always-on server;
- the **control plane** — accounts, provisioning, billing, email, and
  eventually a **web dashboard**, which is a product surface that changes often
  and drags in heavy dependencies (a web framework, payment SDKs, an email
  client, bundled static assets).

Today the control-plane code is small, so the bloat is minor. But a web
dashboard and billing must **never** enter the core database binary, and the
cheapest time to draw the boundary is now, while the control plane is still
~800 lines — before the coupling calcifies.

The question this ADR answers: **what is the smallest thing that must stay in
the data plane, and how does everything else live as a separate service without
a per-request network hop?**

## Decision

Split OxiBase into a separate service. The data plane keeps exactly **one**
hook; everything else moves out.

```
┌──────────────────────────┐    wire (admin)    ┌──────────────────────────┐
│  oxibase  (new binary)   │ ─────────────────► │  oxidb-server (data)     │
│  • /platform/v1 API      │  create_database   │  • engines + wire + REST │
│  • signup/projects/      │  drop_database     │  • /rest/v1 (ADR-0019)   │
│    rotate/abuse guards    │  generic DB r/w    │  • project_secret()  ◄───┼─ the ONE hook
│  • billing / email        │  (its own state    │    (local read, hot path)│
│  • serves the dashboard   │   lives in OxiDB)  │                          │
└──────────────────────────┘                    └──────────────────────────┘
        ▲
        │ HTTP (anon / service_role keys)
┌───────┴─────────┐
│  Web dashboard  │  static SPA, served by nginx/CDN — never in the Rust binary
└─────────────────┘
```

### 1. The data plane keeps only `project_secret()`

The REST listener must verify a `?db=<ref>` project token on **every**
authenticated request, so the per-project secret lookup cannot be a network
call to the control plane. It stays in `oxidb-server` as a thin reader
(renamed from `platform` to e.g. `tenant_auth`): given a `ref`, read the project
row from a local metadata database and decrypt its secret. ~20 lines + a cache.
Everything else in today's `platform/` module is removed from `oxidb-server`.

### 2. Everything else moves to a new `oxibase` crate/binary

signup, login, projects CRUD, key rotation, and the abuse guards move to a new
`oxibase` binary, deployed and scaled separately. It reaches the data plane
**only through OxiDB's public wire API** — it is, architecturally, just another
client:

- **Provisioning** uses the existing Admin-only `create_database` /
  `drop_database` wire commands (ADR-0012). No new data-plane API is needed.
- **Its own state** (accounts, projects, encrypted project secrets) is stored
  in a normal OxiDB database (name `oxibase`) via the wire — the control plane
  uses OxiDB as its database, like any application.

This mirrors Supabase's own architecture: the dashboard is a separate Next.js
app and the Management API a separate service; neither is compiled into the
per-project database infrastructure.

### 3. How the two planes share a project secret — (A) now, (B) later

Two separate processes cannot open the same data directory, so the metadata
must be reachable by both without a per-request hop.

- **(A) Shared store, data plane reads locally — chosen for now.** The control
  plane writes projects (with AES-GCM-sealed secrets) into the `oxibase`
  database *via the wire*; the data plane's `project_secret` reads that same
  database **locally** (fast, cached). The data plane holds no control-plane
  logic — only the generic wire (already there) plus the thin reader. The
  coupling is a small, stable contract: the `oxibase.projects` schema
  (`ref`, `secret_enc`) and the seal-key. This is the minimal change from the
  skeleton.
- **(B) JWKS / asymmetric — the future.** The control plane mints project tokens
  with a private key (RS256/ES256) and publishes a JWKS; the data plane verifies
  with the **public** key and **never holds the project secret at all**. This is
  the clean end state for a multi-node data plane or hard tenant isolation, but
  it means switching project tokens off HS256 and standing up a JWKS endpoint —
  deferred until the multi-node need is real.

### 4. Secret separation across the boundary

- **`OXIDB_PLATFORM_SECRET`** (signs developer sessions) lives **only** on the
  control plane. The data plane never sees it.
- **`OXIDB_SEAL_KEY`** (seals/unseals per-project secrets) is shared by both —
  the control plane seals at provision time, the data plane unseals in
  `project_secret`. Today both are derived from the master secret; the split
  makes the seal-key its own env value so the data plane can unseal without
  knowing the session-signing secret. (Under (B), the data plane needs neither.)

### 5. The dashboard is fully outside Rust

A static SPA (React/Svelte/…), served by nginx/CDN (or by the `oxibase` binary
as static files), talking to the `/platform/v1` API. Zero impact on either Rust
binary.

## Options considered

1. **Keep it all in `oxidb-server` (ADR-0020's skeleton).** Rejected going
   forward: a web dashboard + billing would bloat and destabilize the core
   database binary. Good only as the validation step it already served.
2. **Separate service, per-request secret lookup over the network.** Rejected:
   a network hop on every authenticated data-plane request is unacceptable
   latency and a hard availability coupling (control plane down ⇒ data plane
   can't auth).
3. **Separate service, data plane reads a shared local metadata store — chosen
   (A).** One extra process, no hot-path hop, minimal data-plane surface, and
   the control plane is "just an OxiDB client".
4. **Push secrets to the data plane via an admin command.** Rejected as the
   default: adds a sync/consistency problem versus simply reading the shared
   store; (A) is simpler and already crash-consistent (it's a database).
5. **Asymmetric/JWKS from day one (B).** Deferred, not rejected — the right end
   state, but more work than the current single-node need justifies.

## Consequences

**Positive**
- `oxidb-server` stays a lean database server; its dependency graph never grows
  a web framework, payment SDK, email client, or bundled assets.
- Control plane and dashboard iterate and deploy on their own cadence.
- The data plane's OxiBase surface shrinks to one auditable function + a schema
  contract — a small, stable coupling.
- Secret separation improves: the data plane no longer holds the
  session-signing master secret.
- The control plane dogfoods OxiDB (stores its own state over the wire),
  validating the product on itself.

**Negative / risks**
- A second process to deploy, configure, and secure (the wire admin credential
  it uses, its `OXIDB_PLATFORM_SECRET`, the shared `OXIDB_SEAL_KEY`).
- The `oxibase.projects` schema + seal-key is a cross-binary contract that must
  stay in lock-step across versions.
- (A) still means the data plane can unseal project secrets (holds the
  seal-key); only (B) removes that. Acceptable single-node; revisit for
  multi-node.
- Local read of the `oxibase` metadata db assumes control plane and data plane
  share a data directory / node. Cross-node deployments need (B) or a replicated
  metadata store — explicitly out of scope here.

## Migration path (for the follow-up implementation)

1. Create the `oxibase` crate (binary `oxibase`); move the `platform` handlers
   (signup/login/projects/rotate/abuse guards) into it.
2. Reduce `oxidb-server`'s module to **only** `project_secret` (rename to
   `tenant_auth`); delete the `/platform/v1` router dispatch.
3. Make `oxibase` a wire admin client of `oxidb-server`: provision via
   `create_database`/`drop_database`; store its own state in the `oxibase`
   database over the wire.
4. Split the seal-key (`OXIDB_SEAL_KEY`) from the master secret; give the data
   plane only the seal-key.
5. (Later) the web dashboard as a static SPA; (later) evolve secret sharing to
   (B) JWKS when multi-node lands.

Deployment specifics (hosts, ports, secrets, tokens) stay in env / config
outside the repo, per the standing rule.
