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
