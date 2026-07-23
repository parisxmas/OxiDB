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

/** Collections in the project's document engine. */
export async function listCollections(ref: string, key: string): Promise<string[]> {
  const d = await call<{ collections: string[] }>("GET", ref, "/api/collections", key);
  return d.collections ?? [];
}

/** First `limit` rows of a collection. */
export function findRows(ref: string, col: string, key: string, limit = 100): Promise<Row[]> {
  return call("GET", ref, `/rest/v1/${encodeURIComponent(col)}?limit=${limit}`, key);
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

// Table names come from SHOW TABLES (engine-validated identifiers), but quote
// defensively so an unusual name can't break the statement.
function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}
