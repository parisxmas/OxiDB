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
  /**
   * The OxiBase **control-plane** base URL, used only by `.auth` (end-user
   * signup/login live on the control plane, which holds the project signing
   * key). Required to use `oxibase.auth`.
   */
  authUrl?: string;
}

export interface AuthResult {
  /** The signed-in user (present on signUp; may be absent on login). */
  user?: { email: string };
  /** The session JWT now used by `.from()`/`.sql()` until `signOut()`. */
  token?: string;
  error: string | null;
}

/** End-user auth for an OxiBase project — the Supabase `supabase.auth` analog. */
export interface OxibaseAuth {
  /** Register an end-user of this project and start their session. */
  signUp(credentials: { email: string; password: string }): Promise<AuthResult>;
  /** Log an end-user in and start their session. */
  signInWithPassword(credentials: { email: string; password: string }): Promise<AuthResult>;
  /** Drop the user session; `.from()` reverts to the client's original key. */
  signOut(): void;
  /** The current session token, or `null` when running as the original key. */
  getSession(): { token: string } | null;
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
  /**
   * PostgREST query builder for a table/collection — the Supabase `.from()`.
   *
   * Engine dispatch (server-side, ADR-0019): if `name` is a **SQL table** the
   * call is served by the SQL engine, otherwise by the **document engine** (a
   * collection, auto-created on first insert). A collection and a SQL table
   * never share a name, so this is unambiguous. For the **time-series engine**,
   * use `.schema("tsdb").from(measurement)` instead.
   */
  from: PostgrestClient["from"];
  /**
   * Select a PostgREST schema profile. `schema("tsdb")` routes `.from()` to the
   * **time-series engine** (sends `Accept-Profile: tsdb`). Requires `OXIDB_TSDB=1`.
   */
  schema: PostgrestClient["schema"];
  /** PostgREST stored-procedure call — the Supabase `.rpc()` (if the server exposes it). */
  rpc: PostgrestClient["rpc"];
  /** Run SQL against the project's SQL engine (requires `OXIDB_SQL=1`). */
  sql: (text: string, params?: unknown[]) => Promise<{ results: SqlResult[] | null; error: string | null }>;
  /** End-user auth (signup/login) — see {@link OxibaseAuth}. Needs `authUrl`. */
  auth: OxibaseAuth;
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
  // The bearer token in force. Starts as the client's key (anon/service_role);
  // `.auth` swaps in an end-user session token, `signOut()` reverts it.
  let token = key;
  const extra = opts.headers ?? {};

  // postgrest-js calls fetch with a string URL — add `?db=<ref>` and stamp the
  // CURRENT token so a mid-session `.auth` login takes effect immediately.
  const dbFetch: typeof fetch = (input, init) => {
    if (ref && typeof input === "string") {
      const u = new URL(input);
      u.searchParams.set("db", ref);
      input = u.toString();
    }
    const headers = new Headers(init?.headers);
    headers.set("Authorization", `Bearer ${token}`);
    return baseFetch(input, { ...init, headers });
  };

  const rest = new PostgrestClient(`${base}/rest/v1`, {
    headers: { Authorization: `Bearer ${key}`, ...extra },
    fetch: dbFetch,
  });

  async function sql(text: string, params?: unknown[]) {
    const u = new URL(`${base}/api/sql`);
    if (ref) u.searchParams.set("db", ref);
    let r: Response;
    try {
      r = await baseFetch(u.toString(), {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}`, ...extra },
        body: JSON.stringify(params ? { sql: text, params } : { sql: text }),
      });
    } catch (e) {
      return { results: null, error: e instanceof Error ? e.message : String(e) };
    }
    const body = (await r.json().catch(() => null)) as { results?: SqlResult[]; error?: string } | null;
    if (!r.ok) return { results: null, error: body?.error ?? `HTTP ${r.status}` };
    return { results: body?.results ?? [], error: null };
  }

  async function authCall(action: "signup" | "login", email: string, password: string): Promise<AuthResult> {
    if (!opts.authUrl) return { error: "auth requires the `authUrl` option (the control-plane base)" };
    if (!ref) return { error: "auth requires a project `ref`" };
    const endpoint = `${opts.authUrl.replace(/\/+$/, "")}/platform/v1/projects/${encodeURIComponent(ref)}/auth/${action}`;
    let r: Response;
    try {
      r = await baseFetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      });
    } catch (e) {
      return { error: e instanceof Error ? e.message : String(e) };
    }
    const body = (await r.json().catch(() => null)) as { user?: { email: string }; token?: string; message?: string } | null;
    if (!r.ok || !body?.token) return { error: body?.message ?? `HTTP ${r.status}` };
    token = body.token; // subsequent .from()/.sql() run as this user
    return { user: body.user, token: body.token, error: null };
  }

  const auth: OxibaseAuth = {
    signUp: ({ email, password }) => authCall("signup", email, password),
    signInWithPassword: ({ email, password }) => authCall("login", email, password),
    signOut: () => {
      token = key;
    },
    getSession: () => (token === key ? null : { token }),
  };

  return {
    from: rest.from.bind(rest),
    schema: rest.schema.bind(rest),
    rpc: rest.rpc.bind(rest),
    sql,
    auth,
    rest,
    url: base,
    ref,
  };
}

export default { createClient };
