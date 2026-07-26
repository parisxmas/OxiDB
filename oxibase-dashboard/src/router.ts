/** The URL is the state.
 *
 * Which project and which tab you are looking at lived only in React state, so
 * a refresh threw it away and dropped you back at the project list — and no view
 * could be linked to or opened in a second tab. Both now come from the path:
 *
 *     /                     the project list
 *     /p/<ref>              a project (its first tab)
 *     /p/<ref>/<tab>        a specific tab
 *     /docs, /reset         public pages
 *
 * No router library: this is `history.pushState` plus a subscriber list, which
 * is the whole of what a dashboard with a dozen views needs. nginx already
 * serves index.html for any path, so a deep link survives a hard refresh.
 */
import { useEffect, useState } from "react";

const listeners = new Set<(path: string) => void>();

/** Go to a path, adding a history entry (Back returns to where you were). */
export function navigate(to: string): void {
  if (to === window.location.pathname) return;
  window.history.pushState({}, "", to);
  for (const l of listeners) l(to);
}

/** Replace the current entry — for normalising a URL, not for navigation. */
export function replacePath(to: string): void {
  if (to === window.location.pathname) return;
  window.history.replaceState({}, "", to);
  for (const l of listeners) l(to);
}

/** The current pathname, re-rendering on Back/Forward and on navigate(). */
export function usePath(): string {
  const [path, setPath] = useState(window.location.pathname);
  useEffect(() => {
    const onPop = () => setPath(window.location.pathname);
    window.addEventListener("popstate", onPop);
    listeners.add(setPath);
    return () => {
      window.removeEventListener("popstate", onPop);
      listeners.delete(setPath);
    };
  }, []);
  return path;
}

export type Route =
  | { view: "docs" }
  | { view: "reset" }
  | { view: "projects" }
  | { view: "project"; ref: string; tab?: string };

/** What a path means. Unknown paths are the project list, so a stale link lands
 *  somewhere sensible rather than on a blank screen. */
export function parseRoute(path: string): Route {
  if (path.startsWith("/docs")) return { view: "docs" };
  if (path.startsWith("/reset")) return { view: "reset" };
  const parts = path.split("/").filter(Boolean);
  if (parts[0] === "p" && parts[1]) {
    return { view: "project", ref: decodeURIComponent(parts[1]), tab: parts[2] };
  }
  return { view: "projects" };
}

export const projectPath = (ref: string, tab?: string) =>
  `/p/${encodeURIComponent(ref)}${tab ? `/${tab}` : ""}`;
