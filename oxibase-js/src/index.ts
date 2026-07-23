// oxibase-js — a Supabase-compatible JavaScript client for OxiBase.
//
// OxiBase's data plane implements PostgREST (ADR-0019), so the data API IS the
// real `@supabase/postgrest-js` query builder — `createClient(url, key).from(t)`
// behaves exactly like `supabase.from(t)`. On top of that we add:
//   • per-project targeting  — every request carries `?db=<ref>`
//   • bearer auth            — the project's anon or service_role key
//   • `.sql()`               — an OxiBase extension for the standalone SQL engine
//
//   import { createClient } from "oxibase-js";
//   const oxibase = createClient(DATA_URL, ANON_KEY, { ref: PROJECT_REF });
//   const { data, error } = await oxibase.from("notes").select("*");
//   await oxibase.from("notes").insert({ body: "hi" });
//   const { results } = await oxibase.sql("SELECT count(*) FROM notes");

import { PostgrestClient } from "@supabase/postgrest-js";

export interface OxibaseOptions {
  /** Project reference (the `ref` shown in the OxiBase dashboard). Sent as `?db=<ref>`. */
  ref?: string;
  /** Extra headers merged into every request. */
  headers?: Record<string, string>;
  /** Custom fetch (e.g. for Node < 18 or a proxy). Defaults to global `fetch`. */
  fetch?: typeof fetch;
}

export interface SqlResult {
  columns?: string[];
  types?: (string | null)[];
  rows?: unknown[][];
  affected?: number;
  last_insert_id?: number;
  ddl?: boolean;
}

export interface OxibaseClient {
  /** PostgREST query builder for a table/collection — the Supabase `.from()`. */
  from: PostgrestClient["from"];
  /** PostgREST stored-procedure call — the Supabase `.rpc()` (if the server exposes it). */
  rpc: PostgrestClient["rpc"];
  /** Run SQL against the project's SQL engine (requires `OXIDB_SQL=1`). */
  sql: (text: string, params?: unknown[]) => Promise<{ results: SqlResult[] | null; error: string | null }>;
  /** The underlying postgrest-js client, if you need it directly. */
  rest: PostgrestClient;
  /** The data-plane base URL (without a trailing slash). */
  url: string;
  /** The project ref this client targets, if any. */
  ref?: string;
}

/**
 * Create an OxiBase client. `url` is the data-plane base (e.g. the OxiDB server
 * REST origin); `key` is the project's anon or service_role key.
 */
export function createClient(url: string, key: string, opts: OxibaseOptions = {}): OxibaseClient {
  const base = url.replace(/\/+$/, "");
  const ref = opts.ref;
  const baseFetch = opts.fetch ?? fetch;
  const headers: Record<string, string> = {
    Authorization: `Bearer ${key}`,
    ...(opts.headers ?? {}),
  };

  // postgrest-js calls fetch with a string URL — rewrite it to add `?db=<ref>`.
  const dbFetch: typeof fetch = (input, init) => {
    if (ref && typeof input === "string") {
      const u = new URL(input);
      u.searchParams.set("db", ref);
      input = u.toString();
    }
    return baseFetch(input, init);
  };

  const rest = new PostgrestClient(`${base}/rest/v1`, { headers, fetch: dbFetch });

  async function sql(text: string, params?: unknown[]) {
    const u = new URL(`${base}/api/sql`);
    if (ref) u.searchParams.set("db", ref);
    let r: Response;
    try {
      r = await baseFetch(u.toString(), {
        method: "POST",
        headers: { "Content-Type": "application/json", ...headers },
        body: JSON.stringify(params ? { sql: text, params } : { sql: text }),
      });
    } catch (e) {
      return { results: null, error: e instanceof Error ? e.message : String(e) };
    }
    const body = (await r.json().catch(() => null)) as { results?: SqlResult[]; error?: string } | null;
    if (!r.ok) return { results: null, error: body?.error ?? `HTTP ${r.status}` };
    return { results: body?.results ?? [], error: null };
  }

  return {
    from: rest.from.bind(rest),
    rpc: rest.rpc.bind(rest),
    sql,
    rest,
    url: base,
    ref,
  };
}

export default { createClient };
