// Thin client for the OxiBase control-plane API (`/platform/v1/*`). Pure fetch,
// no framework deps. The base URL is `VITE_OXIBASE_URL` (build-time) or empty
// for same-origin.

export interface Project {
  ref: string;
  name: string;
  created_at: number;
  isolation?: string;
  url?: string | null;
  anon_key?: string;
  service_role_key?: string;
  /** Resource quotas (0 = unlimited). */
  max_collections?: number;
  max_tables?: number;
  max_documents?: number;
  max_storage_bytes?: number;
}

const BASE: string = import.meta.env.VITE_OXIBASE_URL ?? "";
const TOKEN_KEY = "oxibase_token";
const EMAIL_KEY = "oxibase_email";

let token: string | null = localStorage.getItem(TOKEN_KEY);

export function isAuthed(): boolean {
  return token !== null;
}
export function currentEmail(): string | null {
  return localStorage.getItem(EMAIL_KEY);
}
export function logout(): void {
  token = null;
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(EMAIL_KEY);
}

function setSession(t: string, email?: string): void {
  token = t;
  localStorage.setItem(TOKEN_KEY, t);
  if (email) localStorage.setItem(EMAIL_KEY, email);
}

async function req<T>(
  method: string,
  path: string,
  body?: unknown,
  auth = true,
): Promise<T> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (auth && token) headers["Authorization"] = `Bearer ${token}`;
  const res = await fetch(`${BASE}/platform/v1${path}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  const data = text ? JSON.parse(text) : null;
  if (res.status === 401) logout();
  if (!res.ok) {
    throw new Error((data && (data.message || data.error)) || `HTTP ${res.status}`);
  }
  return data as T;
}

export interface PlatformConfig {
  google_client_id?: string | null;
  password_auth: boolean;
}

/** Public bootstrap config — which auth methods the server has enabled. */
export function fetchConfig(): Promise<PlatformConfig> {
  return req<PlatformConfig>("GET", "/config", undefined, false);
}

/** Developer sign-in with a Google ID token (from Google Identity Services). */
export async function authGoogle(credential: string): Promise<void> {
  const d = await req<{ token: string; account?: { email?: string } }>(
    "POST",
    "/auth/google",
    { credential },
    false,
  );
  setSession(d.token, d.account?.email);
}

export function listProjects(): Promise<Project[]> {
  return req<Project[]>("GET", "/projects");
}

export function createProject(name: string): Promise<Project> {
  return req<Project>("POST", "/projects", { name });
}

export function getProject(ref: string): Promise<Project> {
  return req<Project>("GET", `/projects/${encodeURIComponent(ref)}`);
}

export function deleteProject(ref: string): Promise<unknown> {
  return req("DELETE", `/projects/${encodeURIComponent(ref)}`);
}

export function rotateKeys(ref: string): Promise<Project> {
  return req<Project>("POST", `/projects/${encodeURIComponent(ref)}/keys/rotate`);
}

/** Update a project's resource quotas (0 = unlimited). Owner only. */
export function updateProjectLimits(
  ref: string,
  limits: { max_collections?: number; max_tables?: number; max_documents?: number; max_storage_bytes?: number },
): Promise<Project> {
  return req<Project>("PATCH", `/projects/${encodeURIComponent(ref)}/limits`, limits);
}

// ── Social sign-in providers (Users tab) ────────────────────────────────────

export interface ProviderConfig {
  client_id: string | null;
  /** Whether a client secret is stored. The value itself is never returned. */
  secret_set: boolean;
  /** The URL to register with the provider — derived by the control plane. */
  callback_url: string;
}

export interface AuthProviders {
  google: ProviderConfig;
  github: ProviderConfig;
  redirect_urls: string[];
}

export function getAuthProviders(ref: string): Promise<AuthProviders> {
  return req<AuthProviders>("GET", `/projects/${encodeURIComponent(ref)}/auth/providers`);
}

/** Configure providers. Omitted keys are left alone; `null` clears one. */
export function setAuthProviders(
  ref: string,
  patch: {
    google?: { client_id: string; client_secret?: string } | null;
    github?: { client_id: string; client_secret?: string } | null;
    redirect_urls?: string[];
  },
): Promise<AuthProviders> {
  return req<AuthProviders>("PATCH", `/projects/${encodeURIComponent(ref)}/auth/providers`, patch);
}

// ── End-user management (Users tab) ─────────────────────────────────────────

export interface ProjectUser {
  email: string;
  created_at: number;
  verified: boolean;
}

export function listProjectUsers(ref: string): Promise<ProjectUser[]> {
  return req("GET", `/projects/${encodeURIComponent(ref)}/users`);
}

export function deleteProjectUser(ref: string, email: string): Promise<unknown> {
  return req("DELETE", `/projects/${encodeURIComponent(ref)}/users/${encodeURIComponent(email)}`);
}

export function setProjectUserPassword(
  ref: string,
  email: string,
  password: string,
): Promise<unknown> {
  return req(
    "POST",
    `/projects/${encodeURIComponent(ref)}/users/${encodeURIComponent(email)}/password`,
    { password },
  );
}

export function verifyProjectUser(ref: string, email: string): Promise<unknown> {
  return req(
    "POST",
    `/projects/${encodeURIComponent(ref)}/users/${encodeURIComponent(email)}/verify`,
  );
}

/** Generated TypeScript definitions for the project (SQL exact, collections inferred). */
export async function downloadTypes(ref: string): Promise<string> {
  const t = localStorage.getItem("oxibase_token");
  const res = await fetch(`${BASE}/platform/v1/projects/${encodeURIComponent(ref)}/types`, {
    headers: { Authorization: `Bearer ${t}` },
  });
  if (!res.ok) {
    const data = await res.json().catch(() => null);
    throw new Error(data?.message ?? `HTTP ${res.status}`);
  }
  return res.text();
}

/** Complete an end-user password reset (public — token from the email link). */
export async function completePasswordReset(
  ref: string,
  token: string,
  password: string,
): Promise<void> {
  const res = await fetch(
    `${BASE}/platform/v1/projects/${encodeURIComponent(ref)}/auth/reset`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token, password }),
    },
  );
  const data = await res.json().catch(() => null);
  if (!res.ok) throw new Error(data?.message ?? `HTTP ${res.status}`);
}

// ── Request logs (Logs tab) ─────────────────────────────────────────────────

export interface LogRow {
  ts: number;
  method: string;
  path: string;
  status?: number;
  ms?: number;
}

export function listProjectLogs(ref: string, limit = 50, offset = 0): Promise<LogRow[]> {
  return req("GET", `/projects/${encodeURIComponent(ref)}/logs?limit=${limit}&offset=${offset}`);
}
