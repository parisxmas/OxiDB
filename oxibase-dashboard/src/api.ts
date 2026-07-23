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
