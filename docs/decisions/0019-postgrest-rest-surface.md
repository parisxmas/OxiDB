# ADR-0019: PostgREST-compatible auto-REST surface for the document engine

**Status:** Accepted — Phase 1 (document engine: filters, select, order, pagination, full CRUD) + Phase 2a (**resource embedding** for the document engine, `select=*,related(cols)` via a `$lookup`-style stitch) landed & tested, 2026-07-22. Phase 2b (SQL/TSDB engines under the same grammar; SQL embedding via catalog foreign keys; nested embeds) **Proposed**, deferred.
**Supersedes:** —
**Related:** [ADR-0012](0012-multi-database.md) (`?db=<name>` targeting, reused verbatim),
[`oxidb-server/src/rest/postgrest.rs`](../../oxidb-server/src/rest/postgrest.rs) (the translation layer),
[`oxidb-server/src/rest/mod.rs`](../../oxidb-server/src/rest/mod.rs) (router + `rules::check_access` + `rest_permitted`),
[`src/query.rs`](../../src/query.rs) (`FindOptions`, the query AST this compiles to).

## Context

OxiDB already ships the Firebase-style pieces of a "backend-less" data API:
a REST listener (`OXIDB_HTTP_PORT`), JWT auth, per-collection **security
rules**, RBAC, and stored-procedure RPC. But its REST query surface is
OxiDB-shaped: a find is `GET /api/{col}/documents?q=<Mongo-filter-JSON-blob>`.
Every client must know OxiDB's URL conventions; nothing off-the-shelf speaks
them.

PostgREST (and Supabase, which wraps it) established a *de-facto standard* for
"the URL is the query": `GET /rest/v1/products?price=gt.100&select=name,price&order=price.desc`.
An entire ecosystem — `@supabase/postgrest-js`, `supabase-js`, every PostgREST
tutorial and client — targets that contract. The contract's safety rests on
one property: because the auto-generated API is thin and exposes rows directly,
authorization **must** live in the database as row-level security (RLS), not in
hand-written endpoint code.

OxiDB already has the RLS analog (security rules, enforced by
`rules::check_access`) and already has a rich query AST. So the gap between
"what OxiDB does" and "what PostgREST clients expect" is almost entirely a
**URL-grammar translation problem**, not an engine problem.

The question this ADR answers: **can we expose OxiDB as a PostgREST-compatible
API without a new engine, a new storage path, or a new security model — purely
by translating the URL grammar onto the primitives that already exist?**

## Decision

Add a thin, wire-compatible PostgREST surface for the document engine at
`/rest/v1/{table}` (a "table" is a collection), implemented as a translation
layer in `oxidb-server/src/rest/postgrest.rs`. It compiles each request into
the *existing* engine calls — `find_with_options`, `insert_many`, `update`,
`delete` — and routes every one through the *existing* `rules::check_access`
before touching data. No new engine state.

### Grammar supported in Phase 1

| Surface | URL | Compiles to |
|---------|-----|-------------|
| equality / comparison | `id=eq.42` `age=gt.18` `p=lte.9.9` | `{"id":{"$eq":42}}` … |
| negation | `status=not.eq.done` `tag=not.in.(a,b)` | `{"$ne":…}` / `{"$nin":…}` (inverse ops; `$not` wrapper otherwise) |
| membership | `tag=in.(a,b,c)` | `{"$in":[…]}` |
| null / bool | `deleted=is.null` `ok=is.true` | `{"$eq":null}` / `{"$eq":true}` |
| pattern | `name=like.*jo*` `name=ilike.jo*` | anchored `$regex` (+`$options:"i"`) |
| logic | `or=(age.gt.65,vip.is.true)` `and=(…)` | `{"$or":[…]}` / `{"$and":[…]}`, nestable |
| projection | `select=name,price` `select=n:name` | post-find column pick + alias |
| embedding | `select=*,customers(name)` `select=*,orders(*)` `alias:t!fk(cols)` | `$lookup`-style stitch → nested object (belongs-to) / array (has-many) |
| ordering | `order=price.desc,name` | `FindOptions.sort` |
| pagination | `limit=20&offset=40` | `FindOptions.limit/skip`, capped |
| CRUD | `POST`/`PATCH`/`DELETE /rest/v1/{table}` | `insert_many` / `$set`-or-operator update / delete |

`Prefer: return=representation` echoes affected rows (Supabase's `.select()`
after a write); the default is minimal (`201`/`204`, empty body). A
`Content-Range` header is emitted on reads. `?db=<name>` targeting (ADR-0012)
is inherited from the parent router unchanged.

### Safety model (unchanged, reused)

Two gates already in `mod.rs` protect the surface, in order:

1. **`rest_permitted`** (role gate): a `Read`-role token may only `GET`;
   `POST`/`PATCH`/`DELETE` on `/rest/v1/*` require `ReadWrite`. Enforced before
   the handler runs.
2. **`rules::check_access`** (RLS analog): each handler checks read/create/
   update/delete access — for mutations, against every matching document —
   exactly as the native `/api/{col}/documents` handlers do. Proven in the
   smoke test: setting `read:false` makes `GET /rest/v1/{col}` return `403`.

Because the auto-generated API is only ever as open as the security rules
allow, it is safe to expose — the same reasoning that makes PostgREST safe.

### Guardrails

- `OXIDB_PGRST_MAX_ROWS` (default 1000) hard-caps rows per read; an
  unqualified `GET /rest/v1/readings` can never dump a whole collection. A
  caller `limit` is honored up to the cap.
- Schemaless type coercion: a URL value is tried as null/bool/int/float, else
  string; `eq."42"` forces a string (PostgREST quoting rule). This is the one
  place the doc engine is *harder* than typed Postgres columns, hence the
  explicit quoting escape hatch.

## Options considered

1. **A brand-new bespoke REST query language.** Rejected: throws away the
   entire PostgREST/Supabase client ecosystem, which is the whole point.
2. **Translate at the client (an SDK that emits `/api/{col}/documents?q=`).**
   Rejected: doesn't help `postgrest-js` or any existing tool; pushes the
   grammar into every language binding instead of one server module.
3. **Wire-compatible translation layer in the server (chosen).** One module,
   ~18 pure-function unit tests, reuses query AST + security rules + role gate.
   Existing `/api/...` surface is untouched (byte-for-byte back-compatible).

## Consequences

**Positive**
- `@supabase/postgrest-js` and PostgREST clients talk to OxiDB unmodified —
  OxiDB becomes a drop-in Supabase *data layer*, matching the ecosystem-infiltration
  strategy of the `db.php` WordPress drop-in and the EF Core provider.
- Zero new engine surface: filters compile to the same AST that powers indexes,
  so `id=eq.42` uses the `_id`/field index just like the native path.
- The RLS story is honest and reused, not reinvented.

**Resource embedding (Phase 2a, landed)**
- `select=*,related(cols)` nests documents from another collection, stitched in
  the REST layer (fetch parents → collect join keys → one batched `_id`/`fk`
  `$in` query per embed → group and attach). Direction is **inferred from the
  data**, since the document engine has no declared foreign keys: if a parent
  carries `<singular(target)>_id` it is a belongs-to (single object); otherwise
  a has-many keyed by `<singular(parent)>_id` on the child (array). An explicit
  `related!fk(...)` hint names the FK field and lets the presence of that field
  on the parent decide the direction. This is a pragmatic convention, not full
  PostgREST catalog-FK parity — the honest cost of embedding on a schemaless
  store. The SQL engine (Phase 2b) will infer joins from its real catalog FKs.

**Negative / deferred**
- **Nested embeds** (an embed inside an embed) are rejected (one level).
- SQL/TSDB engines under the same `/rest/v1` grammar — Phase 2b.
- Schemaless coercion can surprise (`zip=eq.007` → number 7); mitigated by
  quoting, documented.
- No `Range` **header** pagination yet (query-param `limit`/`offset` only), no
  `Accept: application/vnd.pgrst.object+json` single-object mode, no upsert
  (`Prefer: resolution=merge-duplicates`). All additive later.

## Validation

- 23 translation unit tests in `postgrest.rs` (operator mapping, negation,
  logic nesting, duplicate-column ranges, `like`→regex, quoting, select/alias,
  select-plan split, embed alias + FK hint, unterminated-embed + nested-embed
  rejection, singularization, order, limit/offset).
- End-to-end smoke test against a live server: array insert with
  representation, `gt`+`select`+`order`+`Content-Range`, `or` groups, `like`,
  `PATCH` (`$set` and `$inc` passthrough), `DELETE` `204`, `is.true`, error
  shape (`{"message":…}` 400), the `read:false` → `403` security-rule gate,
  and **embedding**: belongs-to (order → single customer), has-many (customer →
  order array), `alias:t!fk(...)`, `*`+embed, and a dangling reference → `null`.
