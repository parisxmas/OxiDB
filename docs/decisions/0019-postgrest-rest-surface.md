# ADR-0019: PostgREST-compatible auto-REST surface for the document engine

**Status:** Accepted — Phase 1 (document engine: filters, select, order, pagination, full CRUD) + Phase 2a (**resource embedding** for the document engine, `$lookup`-style stitch) + Phase 2b (**SQL engine under the same grammar** — parameterized CRUD routing) + Phase 2c (**SQL resource embedding** via catalog foreign keys) landed & tested, 2026-07-22. Remaining (write-representation over SQL; TSDB under the grammar; nested embeds) **Proposed**, deferred.
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

### SQL engine under the same grammar (Phase 2b, landed)

When the SQL engine is enabled and `{table}` names a SQL table,
`/rest/v1/{table}` is served by the SQL engine instead of the document engine
(`oxidb-server/src/rest/postgrest_sql.rs`; dispatch via
`sql_bridge::sql_table_exists`). The identical URL grammar is translated to
**parameterized SQL** — `price=gt.100` → `WHERE price > ?`, `select=n:name` →
`SELECT name AS n`, `order`/`limit`/`offset`, `in`/`is.null`/`like`/`ilike`,
`or=(…)`/`and=(…)` — and CRUD maps to `INSERT`/`UPDATE`/`DELETE`. The SQL
`{columns, rows}` result is reshaped into PostgREST's array-of-objects. Because
a collection and a SQL table never share a name (architecture invariant), the
dispatch is unambiguous and SQL-off is byte-for-byte the old document path.

Two safety properties are specific to the SQL path:
- **Values are always bound parameters** (`?`), never interpolated.
- **Identifiers** (table/column/alias/order key) are validated against
  `^[A-Za-z_][A-Za-z0-9_]*$` — they cannot be parameters, so this is the name
  injection guard. `select=name,pri;ce` → `400`.

Authorization on the SQL path is **RBAC only** (`rest_permitted` role gate +
the engine's read-only enforcement on GET), not per-row security rules — SQL
tables have no rules layer, exactly like the existing `/api/sql` endpoint. This
is the one behavioral difference from the document path and is intentional.

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
  store. The SQL engine (Phase 2c) will infer joins from its real catalog FKs.

**SQL resource embedding (Phase 2c, landed)**
- `select=*,related(cols)` over a SQL table infers the relationship from the
  **catalog foreign keys** (`sql_bridge::sql_foreign_keys`): a FK from the
  current table → the target is a belongs-to (single object); a FK from the
  target → the current table is a has-many (array); `related!fk(...)` names the
  FK column explicitly. Each embed runs one **batched secondary query**
  (`SELECT … WHERE fk IN (…)`, values bound) and is stitched in Rust — not a
  JOIN, which avoids column-name collisions and reuses the row/projection code.
  Unlike the document path's naming-convention inference, this is exact: the
  join columns come from the declared FK. A missing relationship is a clear
  `400` naming the tables and suggesting a `!fk` hint.

**Negative / deferred**
- **Nested embeds** (an embed inside an embed) are rejected (one level).
- **Write-representation over SQL** (`Prefer: return=representation` echoing
  inserted/updated rows) is deferred — SQL writes return minimal (`201 []` /
  `200 []` / `204`).
- TSDB under the same grammar.
- Schemaless coercion (document path only) can surprise (`zip=eq.007` → number
  7); mitigated by quoting, documented. The SQL path binds params by column
  type and has no such ambiguity.
- No `Range` **header** pagination yet (query-param `limit`/`offset` only), no
  `Accept: application/vnd.pgrst.object+json` single-object mode, no upsert
  (`Prefer: resolution=merge-duplicates`). All additive later.

## Validation

- 23 translation unit tests in `postgrest.rs` (operator mapping, negation,
  logic nesting, duplicate-column ranges, `like`→regex, quoting, select/alias,
  select-plan split, embed alias + FK hint, unterminated-embed + nested-embed
  rejection, singularization, order, limit/offset).
- End-to-end smoke test against a live server (document engine): array insert
  with representation, `gt`+`select`+`order`+`Content-Range`, `or` groups,
  `like`, `PATCH` (`$set` and `$inc` passthrough), `DELETE` `204`, `is.true`,
  error shape (`{"message":…}` 400), the `read:false` → `403` security-rule
  gate, and **embedding**: belongs-to (order → single customer), has-many
  (customer → order array), `alias:t!fk(...)`, `*`+embed, dangling ref → `null`.
- 13 SQL-generation unit tests in `postgrest_sql.rs` (parameterization,
  alias/order/paging, `in`/`is.null`/negation, `like`/`ilike`, `or` groups,
  identifier-injection rejection, force-star for embeds, `where` builder,
  row→object). End-to-end over a live SQL table: `CREATE TABLE`, `/rest/v1`
  INSERT/SELECT(filter+select+order+Content-Range)/`or`/UPDATE/DELETE,
  **co-existence** (a document collection `notes` still routes to the document
  engine), and `select=name,pri;ce` → `400`.
- SQL embedding (Phase 2c) end-to-end over a declared `orders.customer_id
  REFERENCES customers(id)`: belongs-to (`orders?select=item,customers(name)`),
  has-many (`customers?select=name,orders(item)`), alias + `!fk` hint, child
  projection, and a no-FK pair → `400` with a hint suggestion.
- **Real-client conformance** (`tests/postgrest-js-test/`): the unmodified
  `@supabase/postgrest-js` client (the library `supabase-js` wraps) drives the
  surface over **both engines** through one base URL — 18 assertions across
  `gt`/`in`/`or`/`like`, ordering, embedding (belongs-to + has-many), and
  insert/update/delete with `return=representation`, on document collections
  *and* SQL tables. This run surfaced and fixed a real compatibility gap: the
  client emits **SQL-native `%`/`_`** LIKE wildcards, so `like_to_regex` now
  honors `%`/`_` in addition to PostgREST's `*` alias.
