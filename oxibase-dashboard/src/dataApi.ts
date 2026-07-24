// Client for a project's **data plane** (oxidb-server): browse collections over
// the PostgREST surface (`/rest/v1`) and run SQL (`/api/sql`), authenticated
// with the project's key. The data-plane base URL is `VITE_OXIDB_URL`
// (build-time) or same-origin.

const DATA_BASE: string = import.meta.env.VITE_OXIDB_URL ?? "";

export interface SqlResult {
  columns?: string[];
  types?: (string | null)[];
  rows?: unknown[][];
  affected?: number;
  last_insert_id?: number;
  ddl?: boolean;
}

export type Row = Record<string, unknown>;

function withDb(ref: string, path: string): string {
  const sep = path.includes("?") ? "&" : "?";
  return `${DATA_BASE}${path}${sep}db=${encodeURIComponent(ref)}`;
}

async function call<T>(
  method: string,
  ref: string,
  path: string,
  key: string,
  body?: unknown,
  extraHeaders?: Record<string, string>,
): Promise<T> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Authorization: `Bearer ${key}`,
    ...extraHeaders,
  };
  const res = await fetch(withDb(ref, path), {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  const data = text ? JSON.parse(text) : null;
  if (!res.ok) {
    throw new Error((data && (data.message || data.error)) || `HTTP ${res.status}`);
  }
  return data as T;
}

/**
 * A collection the engine manages for its own bookkeeping (alerts, security
 * rules, TTL/retention, profiling, full-text index …). These use a leading
 * underscore by convention and aren't the developer's data, so — like Supabase
 * hides its internal schemas — we keep them out of the dashboard.
 */
export const isSystemCollection = (name: string): boolean => name.startsWith("_");

/** Collections in the project's document engine (system collections hidden). */
export async function listCollections(ref: string, key: string): Promise<string[]> {
  const d = await call<{ collections: string[] }>("GET", ref, "/api/collections", key);
  return (d.collections ?? []).filter((c) => !isSystemCollection(c));
}

/** Total documents across all (non-system) collections. */
export async function countDocuments(ref: string, key: string): Promise<number> {
  const cols = await listCollections(ref, key);
  const counts = await Promise.all(
    cols.map((c) =>
      call<{ count: number }>("GET", ref, `/api/${encodeURIComponent(c)}/count`, key)
        .then((r) => r.count ?? 0)
        .catch(() => 0),
    ),
  );
  return counts.reduce((a, b) => a + b, 0);
}

/** First `limit` rows of a collection. */
export function findRows(ref: string, col: string, key: string, limit = 100): Promise<Row[]> {
  return call("GET", ref, `/rest/v1/${encodeURIComponent(col)}?limit=${limit}`, key);
}

// ── Indexes ─────────────────────────────────────────────────────────────────
export interface IndexInfo {
  name: string;
  index_type: string;
  fields: string[];
  unique: boolean;
  expire_after_seconds?: number;
}

export type IndexSpec =
  | { type: "field"; field: string }
  | { type: "unique"; field: string }
  | { type: "composite"; fields: string[] }
  | { type: "ttl"; field: string; expireAfterSeconds: number };

/** Indexes defined on a collection. */
export function listIndexes(ref: string, col: string, key: string): Promise<IndexInfo[]> {
  return call("GET", ref, `/api/${encodeURIComponent(col)}/indexes`, key);
}

/** Create an index. Indexes are immutable — to change one, drop it and recreate. */
export function createIndex(ref: string, col: string, key: string, spec: IndexSpec): Promise<unknown> {
  return call("POST", ref, `/api/${encodeURIComponent(col)}/indexes`, key, spec);
}

/** Drop an index by name. */
export function dropIndex(ref: string, col: string, key: string, name: string): Promise<unknown> {
  return call("DELETE", ref, `/api/${encodeURIComponent(col)}/indexes/${encodeURIComponent(name)}`, key);
}

/** Insert a document, returning the created row(s). */
export function insertRow(ref: string, col: string, key: string, doc: unknown): Promise<Row[]> {
  return call("POST", ref, `/rest/v1/${encodeURIComponent(col)}`, key, doc, {
    Prefer: "return=representation",
  });
}

/** Delete rows matching a raw PostgREST filter, e.g. `_id=eq.5`. */
export function deleteWhere(ref: string, col: string, key: string, filter: string): Promise<unknown> {
  return call("DELETE", ref, `/rest/v1/${encodeURIComponent(col)}?${filter}`, key);
}

/** Run SQL against the project's SQL engine (requires OXIDB_SQL on the server). */
export async function runSql(ref: string, key: string, sql: string): Promise<SqlResult[]> {
  const d = await call<{ results: SqlResult[] }>("POST", ref, "/api/sql", key, { sql });
  return d.results ?? [];
}

/** Run SQL with bound `?` parameters (values never interpolated into the text). */
export async function runSqlParams(
  ref: string,
  key: string,
  sql: string,
  params: unknown[],
): Promise<SqlResult[]> {
  const d = await call<{ results: SqlResult[] }>("POST", ref, "/api/sql", key, { sql, params });
  return d.results ?? [];
}

/** The four rule expressions of a collection (OxiBase's RLS analog). */
export interface Rules {
  read: string;
  create: string;
  update: string;
  delete: string;
}

/** Current rules for a collection, or `null` if none are defined (open reads;
 *  anon writes denied by default). Admin key required. */
export async function getRules(ref: string, col: string, key: string): Promise<Rules | null> {
  const res = await fetch(withDb(ref, `/api/rules/${encodeURIComponent(col)}`), {
    headers: { Authorization: `Bearer ${key}` },
  });
  if (res.status === 404) return null;
  const text = await res.text();
  const data = text ? JSON.parse(text) : null;
  if (!res.ok) throw new Error((data && (data.message || data.error)) || `HTTP ${res.status}`);
  return { read: data.read, create: data.create, update: data.update, delete: data.delete };
}

/** Set (upsert) the rules for a collection. Admin key required. */
export function setRules(ref: string, col: string, key: string, rules: Rules): Promise<unknown> {
  return call("POST", ref, `/api/rules/${encodeURIComponent(col)}`, key, rules);
}

/** Remove all rules from a collection (back to open reads / anon-write denied). */
export function deleteRules(ref: string, col: string, key: string): Promise<unknown> {
  return call("DELETE", ref, `/api/rules/${encodeURIComponent(col)}`, key);
}

export interface SqlTable {
  name: string;
  rows: number;
}

/** A single result from a batch, or throw if the batch errored. */
async function oneResult(ref: string, key: string, sql: string): Promise<SqlResult> {
  const results = await runSql(ref, key, sql);
  const r = results[results.length - 1];
  if (!r) throw new Error("no result");
  return r;
}

/** Tables in the project's **SQL engine** (distinct from document collections). */
export async function listSqlTables(ref: string, key: string): Promise<SqlTable[]> {
  const r = await oneResult(ref, key, "SHOW TABLES");
  const rows = r.rows ?? [];
  return rows.map((row) => ({ name: String(row[0]), rows: Number(row[1] ?? 0) }));
}

/** Column schema of a SQL table (DESCRIBE). */
export function describeSqlTable(ref: string, key: string, table: string): Promise<SqlResult> {
  return oneResult(ref, key, `DESCRIBE ${quoteIdent(table)}`);
}

/** First `limit` rows of a SQL table. */
export function selectSqlRows(ref: string, key: string, table: string, limit = 100): Promise<SqlResult> {
  return oneResult(ref, key, `SELECT * FROM ${quoteIdent(table)} LIMIT ${Math.max(1, limit | 0)}`);
}

// ── SQL table editing ───────────────────────────────────────────────────────
// Values always travel as bound `?` params; only identifiers (quoted) and a
// validated type keyword are spliced into the statement text.

/** One column of a SQL table, parsed from a DESCRIBE result. */
export interface SqlColumn {
  name: string;
  type: string; // e.g. "INT", "TEXT", "VARCHAR(80)"
  nullable: boolean;
  primaryKey: boolean;
  autoIncrement: boolean;
}

/** Parse a DESCRIBE result (column/type/nullable/primary_key/auto_increment). */
export function parseSchema(r: SqlResult | null): SqlColumn[] {
  return (r?.rows ?? []).map((row) => ({
    name: String(row[0]),
    type: String(row[1]),
    nullable: Boolean(row[2]),
    primaryKey: Boolean(row[3]),
    autoIncrement: Boolean(row[4]),
  }));
}

/** Insert one row (only the listed columns; omitted ones take their default). */
export function insertSqlRow(
  ref: string,
  key: string,
  table: string,
  cols: string[],
  values: unknown[],
): Promise<SqlResult[]> {
  const list = cols.map(quoteIdent).join(", ");
  const ph = cols.map(() => "?").join(", ");
  return runSqlParams(ref, key, `INSERT INTO ${quoteIdent(table)} (${list}) VALUES (${ph})`, values);
}

/** Update one row, addressed by its primary-key value. */
export function updateSqlRow(
  ref: string,
  key: string,
  table: string,
  cols: string[],
  values: unknown[],
  pkCol: string,
  pkVal: unknown,
): Promise<SqlResult[]> {
  const sets = cols.map((c) => `${quoteIdent(c)} = ?`).join(", ");
  return runSqlParams(
    ref,
    key,
    `UPDATE ${quoteIdent(table)} SET ${sets} WHERE ${quoteIdent(pkCol)} = ?`,
    [...values, pkVal],
  );
}

/** Delete one row, addressed by its primary-key value. */
export function deleteSqlRow(
  ref: string,
  key: string,
  table: string,
  pkCol: string,
  pkVal: unknown,
): Promise<SqlResult[]> {
  return runSqlParams(ref, key, `DELETE FROM ${quoteIdent(table)} WHERE ${quoteIdent(pkCol)} = ?`, [
    pkVal,
  ]);
}

// A column type the Add-column UI may splice into DDL: a bare keyword with an
// optional length, e.g. TEXT / VARCHAR(80). Anything else is rejected here.
const TYPE_RE = /^[A-Za-z]+(\(\d+\))?$/;

/** `ALTER TABLE … ADD COLUMN` — metadata-only (O(1)) in the engine. */
export function addSqlColumn(
  ref: string,
  key: string,
  table: string,
  name: string,
  type: string,
): Promise<SqlResult[]> {
  if (!TYPE_RE.test(type)) return Promise.reject(new Error(`invalid column type: ${type}`));
  return runSql(ref, key, `ALTER TABLE ${quoteIdent(table)} ADD COLUMN ${quoteIdent(name)} ${type}`);
}

/** `ALTER TABLE … DROP COLUMN` — metadata-only (O(1)) in the engine. */
export function dropSqlColumn(ref: string, key: string, table: string, name: string): Promise<SqlResult[]> {
  return runSql(ref, key, `ALTER TABLE ${quoteIdent(table)} DROP COLUMN ${quoteIdent(name)}`);
}

/** `ALTER TABLE … ALTER COLUMN … TYPE` — existing values are cast eagerly;
 *  the engine rejects the statement if any value cannot cast. */
export function alterSqlColumnType(
  ref: string,
  key: string,
  table: string,
  column: string,
  type: string,
): Promise<SqlResult[]> {
  if (!TYPE_RE.test(type)) return Promise.reject(new Error(`invalid column type: ${type}`));
  return runSql(
    ref,
    key,
    `ALTER TABLE ${quoteIdent(table)} ALTER COLUMN ${quoteIdent(column)} TYPE ${type}`,
  );
}

/** `ALTER TABLE … RENAME COLUMN old TO new`. */
export function renameSqlColumn(
  ref: string,
  key: string,
  table: string,
  oldName: string,
  newName: string,
): Promise<SqlResult[]> {
  return runSql(
    ref,
    key,
    `ALTER TABLE ${quoteIdent(table)} RENAME COLUMN ${quoteIdent(oldName)} TO ${quoteIdent(newName)}`,
  );
}

/** Drop a SQL table (irreversible). */
export function dropSqlTable(ref: string, key: string, table: string): Promise<SqlResult[]> {
  return runSql(ref, key, `DROP TABLE ${quoteIdent(table)}`);
}

// Table names come from SHOW TABLES (engine-validated identifiers), but quote
// defensively so an unusual name can't break the statement.
function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}
