// Saved connections, persisted in localStorage. Passwords are only stored
// when the user opts in ("remember"), and then in plaintext — same trade-off
// every desktop DB client makes; the note in the UI says so.

export interface SavedConnection {
  id: string;
  name: string;
  mode: "client" | "embedded";
  host: string;
  port: number;
  username: string;
  password?: string; // present only if remembered
  path?: string; // embedded data dir
}

const KEY = "oxidb-connections";

export function loadConnections(): SavedConnection[] {
  try {
    const arr = JSON.parse(localStorage.getItem(KEY) || "[]");
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

function persist(list: SavedConnection[]) {
  localStorage.setItem(KEY, JSON.stringify(list));
}

/** Insert or update by id; returns the new list. */
export function saveConnection(conn: SavedConnection): SavedConnection[] {
  const list = loadConnections();
  const i = list.findIndex((c) => c.id === conn.id);
  if (i >= 0) list[i] = conn;
  else list.push(conn);
  persist(list);
  return list;
}

export function deleteConnection(id: string): SavedConnection[] {
  const list = loadConnections().filter((c) => c.id !== id);
  persist(list);
  return list;
}

export function newId(): string {
  // No Math.random dependency needed for uniqueness here — time + counter.
  return `c${Date.now().toString(36)}${(idCounter++).toString(36)}`;
}
let idCounter = 0;
