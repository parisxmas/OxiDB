# ADR-0020: OxiBase — a control plane for multi-tenant provisioning

**Status:** Proposed — 2026-07-23. No code yet; this ADR fixes the shape so the
skeleton (`oxidb-server/src/platform/`) can be built against a decided design.
**Supersedes:** —
**Related:** [ADR-0012](0012-multi-database.md) (`DatabaseManager`, per-database
isolation + roles — the substrate OxiBase provisions),
[ADR-0019](0019-postgrest-rest-surface.md) (the `/rest/v1` data-plane API a
provisioned project exposes),
[`src/database_manager.rs`](../../src/database_manager.rs),
[`oxidb-server/src/rest/mod.rs`](../../oxidb-server/src/rest/mod.rs) (JWT auth +
`?db=` targeting),
[`src/crypto.rs`](../../src/crypto.rs) (AES-GCM, for at-rest secret storage).

## Context

OxiDB already has, separately, every *data-plane* piece a Supabase-style
backend needs: isolated per-tenant databases (`DatabaseManager`, ADR-0012), JWT
auth + self-service end-user signup + security rules (the RLS analog), per-database
RBAC, and — since ADR-0019 — a PostgREST-compatible `/rest/v1` surface over all
three engines. What is missing is the *control plane*: the layer that turns "a
developer signs up" into "here is an isolated database, two API keys, and a URL
that works." In Supabase terms, that is the difference between a **project's
Postgres** (data plane) and **supabase.com + the Management API** (control
plane).

Two distinct notions of "user" must not be conflated:

1. **Platform account** — a *developer* who signs up to the hosting service,
   creates projects (databases), and holds API keys. There are few of these.
2. **End user** — a user of the *developer's app*, who signs up via the
   project's own auth (OxiDB JWT + `/api/auth/signup`) and lands as a row in
   that database. There are many of these, per project.

Only (1) provisions databases. (2) already works today per-database and is out
of scope here except as the thing a provisioned project must support.

The question this ADR answers: **what is the minimal control plane that
provisions and addresses isolated OxiDB tenants, and what is the single change
the data plane needs to support it?**

## Decision

Build **OxiBase** — a thin control plane on top of OxiDB. The name signals the
category (`-base` = Firebase/Supabase-class Backend-as-a-Service) and the
lineage (OxiDB → OxiBase). It lives as a route family `/platform/v1/*` inside
`oxidb-server` (`oxidb-server/src/platform/`), enabled by `OXIDB_PLATFORM=1`
(off by default, zero cost when unused), reusing JWT, `DatabaseManager`,
storage, and crypto rather than adding a new service.

### Control plane vs data plane

- **Data plane** = OxiDB + `/rest/v1` (ADR-0019). A tenant's data, end-user
  auth, and security rules.
- **Control plane** = OxiBase = `/platform/v1`. Accounts, projects (= databases),
  API keys, provisioning.

### Metadata home — dogfooding

OxiBase stores its own state in a dedicated **system database `_oxibase`**,
following ADR-0012's precedent of top-level `_auth`/`_audit`:

| Collection | Contents |
|---|---|
| `accounts` | developer: `{id, email, pw_hash \| oauth, created_at}` |
| `organizations` | billing/grouping `{id, owner, members[]}` (optional, v2) |
| `projects` | **each = one isolated OxiDB database**: `{ref, org, jwt_secret (encrypted), isolation, status, created_at}` |
| `api_keys` | per project `anon` + `service_role` (JWTs signed with the project secret) |

### Two-tier secrets (the security core)

- **Platform master secret** — one, server-held (env). Signs a *developer's
  platform session* JWT (used to call `/platform/v1/*`).
- **Per-project `jwt_secret`** — generated at provisioning, stored **AES-GCM
  encrypted** in `_oxibase.projects` (via `src/crypto.rs`). Signs that project's
  `anon` / `service_role` / **end-user** tokens. This mirrors Supabase exactly:
  every project has its own JWT secret, so a leak is blast-radius-limited to one
  tenant.

### The single data-plane change this requires

The `/rest/v1` (and `/api/*`) listener today verifies bearer tokens against one
global `OXIDB_JWT_SECRET`. For OxiBase it must instead resolve the **per-project
secret** for the `?db=<ref>` (or Host-derived) target: look up `projects[ref]`
in `_oxibase`, decrypt its `jwt_secret`, verify with that. This is the one core
piece of new data-plane work; everything else is existing parts composed.

### API-key roles (Supabase mapping)

| Supabase | OxiBase |
|---|---|
| `anon` key | a `Read`/anonymous-role JWT — **subject to security rules** (the RLS analog); safe in a browser |
| `service_role` key | an `Admin`-role JWT — bypasses rules; **server-side only**, never shipped to a browser |
| project JWT secret | per-project `jwt_secret` (`_oxibase`, encrypted) |
| Management API | `/platform/v1/projects` |

### Provisioning flow

```
1. POST /platform/v1/signup {email, password}
     → write _oxibase.accounts; return a platform-session JWT (master secret)

2. POST /platform/v1/projects {name}                       [auth: platform JWT]
     → mint an unguessable ref
     → generate a per-project jwt_secret
     → DatabaseManager::create_database(ref)                (ADR-0012, exists)
     → mint anon + service_role JWTs (project secret)
     → write _oxibase.projects (secret encrypted) + api_keys
     → return { ref, url, anon_key, service_role_key }

3. Developer's app:
     - anon_key → data plane   /rest/v1/{table}?db=<ref>    (ADR-0019, exists)
     - its OWN end users        → OxiDB /api/auth/signup + rules per that db
```

### Endpoint set (v1)

```
POST   /platform/v1/signup                       developer signup
POST   /platform/v1/login
POST   /platform/v1/projects                     create a project (database)
GET    /platform/v1/projects                      list
GET    /platform/v1/projects/{ref}                details (url + keys)
DELETE /platform/v1/projects/{ref}                drop_database + cleanup
POST   /platform/v1/projects/{ref}/keys/rotate    rotate secret + re-mint keys
```

### Addressing

Given a single apex domain, a two-stage plan (host names are deployment config,
kept out of the repo — see below):

- **v1 — path/param (ships immediately, one certificate):**
  control `https://<host>/platform/v1/…`, data
  `https://<host>/rest/v1/{table}?db=<ref>`. `?db=` targeting already exists
  (ADR-0012); a first-level control-plane host is covered by an ordinary wildcard
  cert. No new infrastructure.
- **v2 — per-project subdomain (more Supabase-like):**
  `https://<ref>.<host>/rest/v1/{table}` — nginx maps the `Host` header → db, no
  query param. This needs a **second-level wildcard cert** (`*.<host>`), which a
  free ACME **DNS-01** issuance covers at the origin; deferred until after a
  working v1.

### Isolation modes — OxiDB's density advantage

Supabase gives each project a **separate Postgres process** (resource isolation,
expensive). OxiDB is embeddable and multi-database, so OxiBase offers two,
recorded per project in `projects.isolation`:

- **`shared` (default):** many logical databases in one process via
  `DatabaseManager` — very cheap, ideal for a free tier.
- **`dedicated` (paid):** a separate OxiDB process per tenant — hard isolation.

The trade-off is honest: `shared` is not as noisy-neighbor-proof as a
process-per-tenant model; the plan tier selects it.

### Repository hygiene (constraint)

Concrete hosts, IPs, SSH ports, emails, Cloudflare/API tokens, and the platform
master secret **never** live in committed files — they are env vars / nginx
config outside the repo. This ADR and the future skeleton use placeholders
(`<host>`, `$OXIBASE_MASTER_SECRET`) only. (Consistent with the project's
standing rule.)

## Options considered

1. **Separate control-plane binary/crate (`oxibase/`).** Rejected for v1: it
   would duplicate JWT/DatabaseManager/storage wiring. A `/platform/v1` route
   module in `oxidb-server` reuses all of it; a dedicated crate earns its keep
   only once a dashboard/UI exists (a later step).
2. **One shared JWT secret for all tenants.** Rejected: a single leaked secret
   would forge tokens for every project. Per-project secrets (Supabase's model)
   bound the blast radius — worth the per-request secret resolution cost.
3. **Process-per-tenant only (Supabase-parity isolation).** Rejected as the
   default: throws away OxiDB's key advantage (thousands of dense logical
   databases in one process). Offered as the `dedicated` tier instead.
4. **Subdomain-per-project from day one.** Deferred: needs a second-level
   wildcard cert. `?db=` path addressing delivers the same capability now with
   no cert work; subdomains are a v2 polish.

## Consequences

**Positive**
- Turns the ADR-0012 + ADR-0019 building blocks into an actual
  provision-a-tenant product with almost no new engine surface — the only core
  new work is per-database JWT-secret resolution.
- Per-project secrets + `anon`/`service_role` split give a real, Supabase-shaped
  security model that a browser client can use safely.
- `shared` isolation makes a genuinely cheap free tier possible — a density
  Supabase's architecture cannot match.
- Self-hosting story: OxiBase stores its own state in OxiDB (`_oxibase`), so the
  whole platform is one binary + one data directory.

**Negative / risks**
- **Per-database secret resolution** adds a lookup (mitigable with a small
  in-memory cache keyed by `ref`, invalidated on key rotation).
- **`shared` isolation** admits noisy-neighbor effects; needs per-database
  quotas/rate-limits (a follow-up) before it is safe at scale.
- **`service_role` key mishandling** is the classic footgun (leaking it into a
  browser bypasses all rules) — docs and key-naming must scream this, exactly as
  Supabase's do.
- Billing, quotas, email delivery, and a dashboard UI are all out of scope here
  and are what separate "a provisioning API" from "a hosted product."

## Next steps (not this ADR)

1. `oxidb-server/src/platform/` skeleton, starting with the highest-risk piece:
   **per-database JWT-secret resolution** in the REST listener.
2. `_oxibase` schema + `signup` / `create-project` endpoints + key minting
   (reusing `jwt.rs`), secrets encrypted via `crypto.rs`.
3. v1 addressing wired (`<host>/rest/v1?db=<ref>`); subdomain + wildcard cert as
   v2.
