// oxibase-js — the OxiBase JavaScript client.
//
// OxiBase's data plane implements PostgREST (ADR-0019), so `.from()` is a
// full-featured PostgREST query builder. On top of that we add:
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
  /**
   * Address the project by **path** (`<url>/<ref>/rest/v1/…`) instead of the
   * `?db=<ref>` query. Use this behind a single-domain, no-wildcard deployment
   * (`oxibase.example.com/<slug>/…`). `ref` may be the project's ref or slug.
   */
  tenantInPath?: boolean;
  /**
   * WebSocket endpoint for `.subscribe()`. Defaults to the data-plane URL with
   * the scheme flipped to ws(s) and `/ws` appended (the standard deployment's
   * proxy path). Point it at `ws://host:<OXIDB_WS_PORT>` for a direct server.
   */
  realtimeUrl?: string;
}

export interface AuthResult {
  /** The signed-in user (present on signUp; may be absent on login). */
  user?: { email: string };
  /** Short-lived access token now used by `.from()`/`.sql()` until `signOut()`. */
  token?: string;
  /** Long-lived refresh token used to renew the access token without re-login. */
  refreshToken?: string;
  /** True when the server requires email verification before sign-in — no
   *  session was started; the user must click the link in their inbox. */
  verificationRequired?: boolean;
  error: string | null;
}

/** End-user auth for an OxiBase project (sign-up / sign-in / sessions). */
export interface OxibaseAuth {
  /** Register an end-user of this project and start their session. */
  signUp(credentials: { email: string; password: string }): Promise<AuthResult>;
  /** Log an end-user in and start their session. */
  signInWithPassword(credentials: { email: string; password: string }): Promise<AuthResult>;
  /**
   * Exchange the refresh token for a fresh access token (the refresh token is
   * rotated). `.from()`/`.sql()` also do this automatically on a 401.
   */
  refreshSession(): Promise<AuthResult>;
  /** Drop the user session; `.from()` reverts to the client's original key. */
  signOut(): void;
  /** The current session, or `null` when running as the original key. */
  getSession(): { token: string; refreshToken: string } | null;
  /**
   * Adopt a session obtained earlier — the counterpart of {@link getSession},
   * for apps that persist it (localStorage, a cookie, a native store) and need
   * the client to resume as that user after a reload. The access token is used
   * as-is; if it has expired, the first call refreshes it automatically.
   */
  setSession(session: { token: string; refreshToken?: string }): void;
  /** Email a password-reset link (always resolves — no user enumeration). */
  resetPasswordForEmail(email: string): Promise<{ error: string | null }>;
  /** Re-send the signup verification email. */
  resendVerification(email: string): Promise<{ error: string | null }>;
  /** Complete a reset started from an emailed link. */
  resetPassword(token: string, password: string): Promise<{ error: string | null }>;
  /**
   * Start a social sign-in. Returns the provider's consent URL and, in a
   * browser, navigates to it. The provider sends the user back to
   * `redirectTo`, which must be listed in the project's allowed redirect URLs;
   * call {@link getSessionFromUrl} on that page to pick the session up.
   */
  signInWithOAuth(opts: {
    provider: OAuthProvider;
    redirectTo?: string;
    /** Set false to get the URL back without navigating (default true). */
    navigate?: boolean;
  }): { url: string; error: string | null };
  /**
   * Sign in with an ID token the app already obtained from the provider —
   * Google Identity Services in the browser, for instance. No redirect.
   */
  signInWithIdToken(opts: { provider: "google"; token: string }): Promise<AuthResult>;
  /**
   * Complete a redirect sign-in: read `#access_token`/`#refresh_token` from the
   * current URL (or `url`), adopt the session, and strip the fragment so the
   * tokens do not linger in the address bar or history.
   */
  getSessionFromUrl(url?: string): AuthResult | null;
  /**
   * Email a passwordless sign-in link. The user clicks it and arrives at
   * `redirectTo` with a session in the fragment — pick it up there with
   * {@link getSessionFromUrl}. Creates the account if it is new, and the click
   * is what verifies the address. `redirectTo` must be an allowed redirect URL.
   *
   * Always resolves the same way for known and unknown addresses.
   */
  signInWithMagicLink(opts: { email: string; redirectTo?: string }): Promise<{ error: string | null }>;
  /** Which sign-in methods this project offers (public; no auth required). */
  getSettings(): Promise<{
    password: boolean;
    /** Passwordless email sign-in is available. */
    magicLink: boolean;
    providers: OAuthProvider[];
    googleClientId: string | null;
    error: string | null;
  }>;
}

/** Social identity providers OxiBase can sign end-users in with. */
export type OAuthProvider = "google" | "github";

export interface SqlResult {
  columns?: string[];
  types?: (string | null)[];
  rows?: unknown[][];
  affected?: number;
  last_insert_id?: number;
  ddl?: boolean;
}

/** One realtime change pushed by the server. */
export interface ChangeEvent {
  /** "insert" | "update" | "delete" */
  op: string;
  collection: string;
  docId?: unknown;
  /** The changed document (absent on deletes). */
  doc?: Record<string, unknown> | null;
}

export interface SubscribeOptions {
  /** Server-side equality filter: only events whose doc matches are pushed. */
  query?: Record<string, unknown>;
  /** Called on subscription errors (access denied, connection loss, …). */
  onError?: (message: string) => void;
}

export interface RealtimeSubscription {
  unsubscribe(): void;
}

/** Metadata of one stored object. */
export interface StorageObject {
  key: string;
  bucket: string;
  size: number;
  content_type: string;
  etag: string;
  created_at: string;
}

/** Operations on one storage bucket. */
export interface StorageBucket {
  /** Upload (or overwrite) an object. `data` may be a Blob/File, ArrayBuffer,
   *  typed array, or string. Requires the service_role key (writes are
   *  RBAC-gated). */
  upload(
    key: string,
    data: Blob | ArrayBuffer | ArrayBufferView | string,
    opts?: { contentType?: string },
  ): Promise<{ data: StorageObject | null; error: string | null }>;
  /** Download an object as a Blob (its stored Content-Type preserved). */
  download(key: string): Promise<{ data: Blob | null; error: string | null }>;
  /** List objects, optionally under a key prefix. */
  list(opts?: {
    prefix?: string;
    limit?: number;
  }): Promise<{ data: StorageObject[] | null; error: string | null }>;
  /** Delete one object. */
  remove(key: string): Promise<{ error: string | null }>;
  /** The object's URL (requests to it still need the Authorization header). */
  getUrl(key: string): string;
}

export interface OxibaseStorage {
  /** Buckets in the project, plus total stored bytes (the quota usage). */
  listBuckets(): Promise<{
    data: { buckets: string[]; totalBytes: number } | null;
    error: string | null;
  }>;
  /** Create a bucket explicitly (uploads also auto-create their bucket). */
  createBucket(name: string): Promise<{ error: string | null }>;
  /** Delete an empty bucket. */
  deleteBucket(name: string): Promise<{ error: string | null }>;
  /** Scope to one bucket. */
  from(bucket: string): StorageBucket;
}

export interface OxibaseClient {
  /**
   * PostgREST query builder for a table/collection.
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
  /** PostgREST stored-procedure call (if the server exposes it). */
  rpc: PostgrestClient["rpc"];
  /** Run SQL against the project's SQL engine (requires `OXIDB_SQL=1`). */
  sql: (text: string, params?: unknown[]) => Promise<{ results: SqlResult[] | null; error: string | null }>;
  /**
   * Subscribe to live changes of a collection (requires `OXIDB_WS_PORT` on the
   * server). Events are pushed over one shared WebSocket; per-row read rules
   * are enforced server-side (a caller only receives rows it may read).
   */
  subscribe: (
    collection: string,
    callback: (event: ChangeEvent) => void,
    opts?: SubscribeOptions,
  ) => RealtimeSubscription;
  /** Per-project file storage (`/api/storage`) — see {@link OxibaseStorage}. */
  storage: OxibaseStorage;
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
  // The tokens in force. `token` starts as the client's key (anon/service_role);
  // `.auth` swaps in an end-user access token + refresh token.
  let token = key;
  let refreshToken = "";
  const extra = opts.headers ?? {};
  const authBase = opts.authUrl?.replace(/\/+$/, "");
  const authEndpoint = (action: string) =>
    `${authBase}/platform/v1/projects/${encodeURIComponent(ref ?? "")}/auth/${action}`;

  // Renew the access token from the refresh token (rotated server-side).
  async function tryRefresh(): Promise<boolean> {
    if (!refreshToken || !authBase || !ref) return false;
    let r: Response;
    try {
      r = await baseFetch(authEndpoint("refresh"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ refresh_token: refreshToken }),
      });
    } catch {
      return false;
    }
    const b = (await r.json().catch(() => null)) as { token?: string; refresh_token?: string } | null;
    if (!r.ok || !b?.token) return false;
    token = b.token;
    if (b.refresh_token) refreshToken = b.refresh_token;
    return true;
  }

  // Stamp the current access token, and on a 401 refresh once and retry.
  async function sendAuthed(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    const attempt = () => {
      const headers = new Headers(init?.headers);
      headers.set("Authorization", `Bearer ${token}`);
      return baseFetch(input, { ...init, headers });
    };
    let res = await attempt();
    if (res.status === 401 && refreshToken && (await tryRefresh())) {
      res = await attempt();
    }
    return res;
  }

  // Two ways to address the project: put the ref/slug in the PATH
  // (`<base>/<ref>/…`, tenantInPath) or as `?db=<ref>` (the default).
  const pathTenant = opts.tenantInPath && ref ? `/${encodeURIComponent(ref)}` : "";

  // postgrest-js calls fetch with a string URL — add `?db=<ref>` when not using
  // path addressing, then send.
  const dbFetch: typeof fetch = (input, init) => {
    if (ref && !opts.tenantInPath && typeof input === "string") {
      const u = new URL(input);
      u.searchParams.set("db", ref);
      input = u.toString();
    }
    return sendAuthed(input, init);
  };

  const rest = new PostgrestClient(`${base}${pathTenant}/rest/v1`, {
    headers: { Authorization: `Bearer ${key}`, ...extra },
    fetch: dbFetch,
  });

  async function sql(text: string, params?: unknown[]) {
    const u = new URL(`${base}${pathTenant}/api/sql`);
    if (ref && !opts.tenantInPath) u.searchParams.set("db", ref);
    let r: Response;
    try {
      r = await sendAuthed(u.toString(), {
        method: "POST",
        headers: { "Content-Type": "application/json", ...extra },
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
    if (!authBase) return { error: "auth requires the `authUrl` option (the control-plane base)" };
    if (!ref) return { error: "auth requires a project `ref`" };
    let r: Response;
    try {
      r = await baseFetch(authEndpoint(action), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      });
    } catch (e) {
      return { error: e instanceof Error ? e.message : String(e) };
    }
    const body = (await r.json().catch(() => null)) as
      | {
          user?: { email: string };
          token?: string;
          refresh_token?: string;
          message?: string;
          verification_required?: boolean;
        }
      | null;
    if (r.ok && body?.verification_required) {
      // Registered, but no session until the emailed link is clicked.
      return { user: body.user, verificationRequired: true, error: null };
    }
    if (!r.ok || !body?.token) return { error: body?.message ?? `HTTP ${r.status}` };
    token = body.token; // subsequent .from()/.sql() run as this user
    refreshToken = body.refresh_token ?? "";
    return { user: body.user, token: body.token, refreshToken, error: null };
  }

  // POST an auth action body; unwrap { message } errors.
  async function authPost(action: string, payload: unknown): Promise<{ error: string | null }> {
    if (!authBase) return { error: "auth requires the `authUrl` option (the control-plane base)" };
    if (!ref) return { error: "auth requires a project `ref`" };
    let r: Response;
    try {
      r = await baseFetch(authEndpoint(action), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
    } catch (e) {
      return { error: e instanceof Error ? e.message : String(e) };
    }
    if (r.ok) return { error: null };
    const b = (await r.json().catch(() => null)) as { message?: string } | null;
    return { error: b?.message ?? `HTTP ${r.status}` };
  }

  const auth: OxibaseAuth = {
    signUp: async (c) => {
      const r = await authCall("signup", c.email, c.password);
      if (!r.error) realtimeReset();
      return r;
    },
    signInWithPassword: async (c) => {
      const r = await authCall("login", c.email, c.password);
      if (!r.error) realtimeReset();
      return r;
    },
    refreshSession: async () =>
      (await tryRefresh()) ? { token, refreshToken, error: null } : { error: "refresh failed" },
    signOut: () => {
      token = key;
      refreshToken = "";
      realtimeReset();
    },
    getSession: () => (refreshToken ? { token, refreshToken } : null),
    setSession: (session) => {
      token = session.token;
      refreshToken = session.refreshToken ?? "";
      realtimeReset();
    },
    resetPasswordForEmail: (email) => authPost("recover", { email }),
    resendVerification: (email) => authPost("resend", { email }),
    resetPassword: (t, password) => authPost("reset", { token: t, password }),

    signInWithOAuth: ({ provider, redirectTo, navigate = true }) => {
      if (!authBase) {
        return { url: "", error: "auth requires the `authUrl` option (the control-plane base)" };
      }
      if (!ref) return { url: "", error: "auth requires a project `ref`" };
      // Default to the current page, which is also the most common allow-list
      // entry; outside a browser it must be given explicitly.
      const target = redirectTo ?? (typeof location !== "undefined" ? location.href : "");
      if (!target) return { url: "", error: "redirectTo is required outside a browser" };
      const url =
        authEndpoint(`authorize/${encodeURIComponent(provider)}`) +
        `?redirect_to=${encodeURIComponent(target)}`;
      if (navigate && typeof location !== "undefined") location.assign(url);
      return { url, error: null };
    },

    signInWithIdToken: async ({ provider, token: credential }) => {
      if (!authBase) return { error: "auth requires the `authUrl` option (the control-plane base)" };
      if (!ref) return { error: "auth requires a project `ref`" };
      let r: Response;
      try {
        r = await baseFetch(authEndpoint(`oauth/${encodeURIComponent(provider)}`), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ credential }),
        });
      } catch (e) {
        return { error: e instanceof Error ? e.message : String(e) };
      }
      const body = (await r.json().catch(() => null)) as
        | { user?: { email: string }; token?: string; refresh_token?: string; message?: string }
        | null;
      if (!r.ok || !body?.token) return { error: body?.message ?? `HTTP ${r.status}` };
      token = body.token;
      refreshToken = body.refresh_token ?? "";
      realtimeReset();
      return { user: body.user, token, refreshToken, error: null };
    },

    getSessionFromUrl: (url) => {
      const href = url ?? (typeof location !== "undefined" ? location.href : "");
      const hash = href.slice(href.indexOf("#") + 1);
      if (!href.includes("#") || !hash) return null;
      const params = new URLSearchParams(hash);
      const err = params.get("error");
      if (err) return { error: err };
      const access = params.get("access_token");
      if (!access) return null;
      token = access;
      refreshToken = params.get("refresh_token") ?? "";
      realtimeReset();
      // Drop the fragment: a session in the address bar ends up in history,
      // bookmarks and anything the user pastes.
      if (!url && typeof history !== "undefined" && typeof location !== "undefined") {
        history.replaceState(null, "", location.pathname + location.search);
      }
      return { token, refreshToken, error: null };
    },

    signInWithMagicLink: ({ email, redirectTo }) => {
      const target = redirectTo ?? (typeof location !== "undefined" ? location.href : "");
      if (!target) return Promise.resolve({ error: "redirectTo is required outside a browser" });
      return authPost("magiclink", { email, redirect_to: target });
    },

    getSettings: async () => {
      const empty = {
        password: true,
        magicLink: false,
        providers: [] as OAuthProvider[],
        googleClientId: null,
      };
      if (!authBase || !ref) return { ...empty, error: "auth requires `authUrl` and a project `ref`" };
      try {
        const r = await baseFetch(authEndpoint("settings"));
        const b = (await r.json().catch(() => null)) as
          | {
              password?: boolean;
              magic_link?: boolean;
              providers?: OAuthProvider[];
              google_client_id?: string | null;
              message?: string;
            }
          | null;
        if (!r.ok) return { ...empty, error: b?.message ?? `HTTP ${r.status}` };
        return {
          password: b?.password ?? true,
          magicLink: b?.magic_link ?? false,
          providers: b?.providers ?? [],
          googleClientId: b?.google_client_id ?? null,
          error: null,
        };
      } catch (e) {
        return { ...empty, error: e instanceof Error ? e.message : String(e) };
      }
    },
  };

  // ── Realtime (`.subscribe`) ──────────────────────────────────────────────
  // One shared WebSocket, lazily opened. The server processes commands
  // sequentially per connection, so plain-`ok` replies are matched FIFO to the
  // commands that caused them. Change events carry a subscription id.
  const wsUrl =
    opts.realtimeUrl ??
    base.replace(/^http/, "ws") + (opts.tenantInPath && ref ? `/${encodeURIComponent(ref)}` : "") + "/ws";

  interface SubEntry {
    collection: string;
    query?: Record<string, unknown>;
    callback: (event: ChangeEvent) => void;
    onError?: (message: string) => void;
    active: boolean;
  }
  let ws: WebSocket | null = null;
  let wsReady = false; // authenticated (or auth not required) and usable
  let nextSubId = 1;
  let retryMs = 500;
  const subsById = new Map<string, SubEntry>();
  // FIFO of handlers for the next plain (non-event) server replies.
  let acks: Array<(ok: boolean, err?: string) => void> = [];

  function realtimeReset() {
    // Identity changed: drop the connection; live subscriptions re-subscribe
    // under the new token on reconnect.
    if (ws) {
      const w = ws;
      ws = null;
      wsReady = false;
      acks = [];
      try {
        w.close();
      } catch {
        /* already closed */
      }
      if (subsById.size > 0) wsConnect();
    }
  }

  function wsSend(obj: Record<string, unknown>, onAck?: (ok: boolean, err?: string) => void) {
    if (!ws || ws.readyState !== 1) return;
    acks.push(onAck ?? (() => {}));
    ws.send(JSON.stringify(obj));
  }

  function subscribeOne(id: string, sub: SubEntry) {
    wsSend(
      {
        cmd: "subscribe",
        id,
        collection: sub.collection,
        ...(sub.query ? { query: sub.query } : {}),
        ...(ref ? { db: ref } : {}),
      },
      (ok, err) => {
        if (!ok) {
          sub.active = false;
          sub.onError?.(err ?? "subscribe failed");
        }
      },
    );
  }

  function wsConnect() {
    if (ws) return;
    let sock: WebSocket;
    try {
      sock = new WebSocket(wsUrl);
    } catch (e) {
      failAll(e instanceof Error ? e.message : String(e));
      return;
    }
    ws = sock;
    wsReady = false;

    sock.onopen = () => {
      retryMs = 500;
      // Authenticate first: project connections verify with the project's own
      // key and get pinned to its database server-side.
      wsSend({ cmd: "auth", token, ...(ref ? { db: ref } : {}) }, (ok, err) => {
        if (!ok) {
          // Open servers reject `auth` ("auth is not enabled") — usable as-is.
          if (err && !/auth is not enabled/.test(err)) {
            failAll(err);
            try {
              sock.close();
            } catch {
              /* ignore */
            }
            return;
          }
        }
        wsReady = true;
        for (const [id, sub] of subsById) {
          if (sub.active) subscribeOne(id, sub);
        }
      });
    };

    sock.onmessage = (ev: MessageEvent) => {
      let msg: Record<string, unknown>;
      try {
        msg = JSON.parse(String(ev.data));
      } catch {
        return;
      }
      if (msg.event === "change") {
        const sub = subsById.get(String(msg.subscription));
        if (sub && sub.active) {
          sub.callback({
            op: String(msg.op ?? ""),
            collection: String(msg.collection ?? ""),
            docId: msg.doc_id,
            doc: (msg.doc as Record<string, unknown> | null) ?? undefined,
          });
        }
        return;
      }
      const handler = acks.shift();
      handler?.(msg.ok === true, typeof msg.error === "string" ? msg.error : undefined);
    };

    sock.onclose = () => {
      if (ws !== sock) return; // superseded (reset)
      ws = null;
      wsReady = false;
      acks = [];
      if (subsById.size > 0) {
        const delay = retryMs;
        retryMs = Math.min(retryMs * 2, 15_000);
        setTimeout(() => {
          if (!ws && subsById.size > 0) wsConnect();
        }, delay);
      }
    };
    sock.onerror = () => {
      /* onclose follows and handles retry */
    };
  }

  function failAll(message: string) {
    for (const sub of subsById.values()) {
      if (sub.active) sub.onError?.(message);
    }
  }

  // ── Storage (`.storage`) ─────────────────────────────────────────────────
  const storageUrl = (path: string) => {
    const u = new URL(`${base}${pathTenant}/api/storage${path}`);
    if (ref && !opts.tenantInPath) u.searchParams.set("db", ref);
    return u.toString();
  };

  async function storageJson<T>(
    method: string,
    path: string,
    body?: BodyInit,
    headers?: Record<string, string>,
  ): Promise<{ data: T | null; error: string | null }> {
    let r: Response;
    try {
      r = await sendAuthed(storageUrl(path), { method, body, headers });
    } catch (e) {
      return { data: null, error: e instanceof Error ? e.message : String(e) };
    }
    const j = (await r.json().catch(() => null)) as (T & { error?: string }) | null;
    if (!r.ok) return { data: null, error: j?.error ?? `HTTP ${r.status}` };
    return { data: j, error: null };
  }

  const storage: OxibaseStorage = {
    async listBuckets() {
      const { data, error } = await storageJson<{ buckets: string[]; total_bytes: number }>(
        "GET",
        "",
      );
      return {
        data: data ? { buckets: data.buckets, totalBytes: data.total_bytes } : null,
        error,
      };
    },
    async createBucket(name) {
      const { error } = await storageJson("POST", `/${encodeURIComponent(name)}`);
      return { error };
    },
    async deleteBucket(name) {
      const { error } = await storageJson("DELETE", `/${encodeURIComponent(name)}`);
      return { error };
    },
    from(bucket) {
      const b = encodeURIComponent(bucket);
      const keyPath = (key: string) =>
        `/${b}/${key.split("/").map(encodeURIComponent).join("/")}`;
      return {
        async upload(key, data, uploadOpts) {
          const contentType =
            uploadOpts?.contentType ??
            (typeof Blob !== "undefined" && data instanceof Blob && data.type
              ? data.type
              : "application/octet-stream");
          return storageJson<StorageObject>("PUT", keyPath(key), data as BodyInit, {
            "Content-Type": contentType,
          });
        },
        async download(key) {
          let r: Response;
          try {
            r = await sendAuthed(storageUrl(keyPath(key)));
          } catch (e) {
            return { data: null, error: e instanceof Error ? e.message : String(e) };
          }
          if (!r.ok) {
            const j = (await r.json().catch(() => null)) as { error?: string } | null;
            return { data: null, error: j?.error ?? `HTTP ${r.status}` };
          }
          return { data: await r.blob(), error: null };
        },
        async list(listOpts) {
          const q = new URLSearchParams();
          if (listOpts?.prefix) q.set("prefix", listOpts.prefix);
          if (listOpts?.limit) q.set("limit", String(listOpts.limit));
          const qs = q.toString();
          const { data, error } = await storageJson<{ objects: StorageObject[] }>(
            "GET",
            `/${b}${qs ? `?${qs}` : ""}`,
          );
          return { data: data?.objects ?? null, error };
        },
        async remove(key) {
          const { error } = await storageJson("DELETE", keyPath(key));
          return { error };
        },
        getUrl(key) {
          return storageUrl(keyPath(key));
        },
      };
    },
  };

  function subscribe(
    collection: string,
    callback: (event: ChangeEvent) => void,
    subOpts: SubscribeOptions = {},
  ): RealtimeSubscription {
    const id = `sub${nextSubId++}`;
    const entry: SubEntry = {
      collection,
      query: subOpts.query,
      callback,
      onError: subOpts.onError,
      active: true,
    };
    subsById.set(id, entry);
    if (ws && wsReady) subscribeOne(id, entry);
    else wsConnect();
    return {
      unsubscribe() {
        entry.active = false;
        subsById.delete(id);
        if (ws && wsReady) wsSend({ cmd: "unsubscribe", id });
        if (subsById.size === 0 && ws) {
          const w = ws;
          ws = null;
          wsReady = false;
          acks = [];
          try {
            w.close();
          } catch {
            /* ignore */
          }
        }
      },
    };
  }

  return {
    from: rest.from.bind(rest),
    schema: rest.schema.bind(rest),
    rpc: rest.rpc.bind(rest),
    sql,
    subscribe,
    storage,
    auth,
    rest,
    url: base,
    ref,
  };
}

export default { createClient };
