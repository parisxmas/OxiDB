import { useEffect, useMemo, useRef, useState } from "react";
import { type LogRow, listProjectLogs } from "./api.ts";
import { LogsMap } from "./LogsMap.tsx";

const PAGE_SIZE = 50;

/** How far apart two requests from one address can be and still be one burst.
 *  A page load's requests land within a second or two of each other; a window
 *  much wider than that would start merging separate visits. */
const BURST_SECONDS = 2;

/** Refresh rates offered for Live. */
const POLL_CHOICES = [
  { ms: 2000, label: "every 2s" },
  { ms: 5000, label: "every 5s" },
  { ms: 10000, label: "every 10s" },
  { ms: 30000, label: "every 30s" },
  { ms: 60000, label: "every 1m" },
];

/** Logs tab: the project's recent requests (method, path, status, latency),
 *  newest first — paged, with optional live auto-refresh.
 *
 *  A row expands on click to show who made the request: the caller's address and
 *  whatever location the edge reported. Those fields are only present when
 *  Cloudflare is in front and "Add visitor location headers" is on for the zone
 *  (Rules → Settings → Managed Transforms); `CF-Connecting-IP` and `CF-IPCountry`
 *  arrive without it. A field that is missing is shown as missing rather than
 *  guessed at. */
export function LogsPanel({ projectRef }: { projectRef: string }) {
  const [rows, setRows] = useState<LogRow[]>([]);
  const [page, setPage] = useState(0);
  // One extra row is requested per page purely to know whether a next page exists.
  const [hasNext, setHasNext] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [live, setLive] = useState(false);
  // How often Live refetches, remembered. Someone watching a deploy wants two
  // seconds; someone leaving it open all afternoon wants a minute, and every
  // poll is a query against a collection shared by every project.
  const [pollMs, setPollMs] = useState(() => {
    const saved = Number(localStorage.getItem("oxibase.logs.interval"));
    return POLL_CHOICES.some((c) => c.ms === saved) ? saved : 5000;
  });
  const [filter, setFilter] = useState("");
  const [place, setPlace] = useState("");
  const [open, setOpen] = useState<number | null>(null);

  // Remembered, so the choice survives a reload — the map is useful to some
  // people every time and to others never.
  const [showMap, setShowMap] = useState(
    () => localStorage.getItem("oxibase.logs.map") !== "off",
  );
  // One page load fires a dozen requests within the same second, which fills the
  // table with rows that are really one event. Grouped is the default; the
  // toggle shows every request.
  const [grouped, setGrouped] = useState(
    () => localStorage.getItem("oxibase.logs.grouped") !== "off",
  );
  // Rows accumulate across pages so the map builds a picture as you page back,
  // rather than redrawing from 50 rows each time.
  const [seen, setSeen] = useState<LogRow[]>([]);
  // Places that appeared in the most recent fetch, and a counter so the map can
  // restart the animation even when the same place arrives twice running.
  const [arrivals, setArrivals] = useState<{ places: string[]; nonce: number }>({
    places: [],
    nonce: 0,
  });
  const seenKeys = useRef<Set<string>>(new Set());
  const timer = useRef<number | null>(null);

  async function load(p = page) {
    try {
      const r = await listProjectLogs(projectRef, PAGE_SIZE + 1, p * PAGE_SIZE);
      setHasNext(r.length > PAGE_SIZE);
      const page = r.slice(0, PAGE_SIZE);
      setRows(page);
      // Which of these rows we had not seen before — the map pulses their
      // locations, and `seen` grows only by them.
      const key = (r: LogRow) => `${r.ts}-${r.path}-${r.ip ?? ""}`;
      const fresh = page.filter((r) => !seenKeys.current.has(key(r)));
      for (const r of fresh) seenKeys.current.add(key(r));
      if (fresh.length > 0) {
        setSeen((prev) => [...prev, ...fresh].slice(-2000));
        const places = [...new Set(fresh.filter((r) => !isServerSide(r)).map(placeOf))].filter(
          Boolean,
        );
        if (places.length > 0) setArrivals((a) => ({ places, nonce: a.nonce + 1 }));
      }
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    setLoading(true);
    setPage(0);
    setPlace("");
    setSeen([]);
    seenKeys.current = new Set();
    setArrivals({ places: [], nonce: 0 });
    load(0);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectRef]);

  useEffect(() => {
    load(page);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [page]);

  useEffect(() => {
    if (!live) return;
    // Live follows the newest entries — jump back to the first page.
    setPage(0);

    // Polling pauses while the tab is hidden. Every poll is a query against a
    // log collection shared by every project on the server, and running it
    // every five seconds for a tab nobody is looking at is load for no reader.
    const start = () => {
      if (timer.current) window.clearInterval(timer.current);
      timer.current = window.setInterval(() => load(0), pollMs);
    };
    const stop = () => {
      if (timer.current) window.clearInterval(timer.current);
      timer.current = null;
    };
    const onVisibility = () => {
      if (document.hidden) {
        stop();
      } else {
        load(0);
        start();
      }
    };
    if (!document.hidden) start();
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      stop();
      document.removeEventListener("visibilitychange", onVisibility);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [live, projectRef, pollMs]);

  // Counted from the deduplicated rows, not accumulated per fetch. Live reloads
  // the same page every five seconds, and adding its rows each time inflated
  // every location's count by a factor of however long you left it running.
  const places = useMemo(() => {
    const counts = new Map<string, number>();
    for (const r of seen) {
      const label = placeOf(r);
      if (label) counts.set(label, (counts.get(label) ?? 0) + 1);
    }
    return counts;
  }, [seen]);

  const matchesText = (r: LogRow) =>
    !filter ||
    r.path?.includes(filter) ||
    r.method?.toLowerCase() === filter.toLowerCase() ||
    String(r.status ?? "").startsWith(filter);
  const visible = rows.filter((r) => matchesText(r) && (!place || placeOf(r) === place));

  /** One visit: a page load and whatever it set off.
   *
   *  Grouped by address within a short window, deliberately **not** by identity.
   *  A single page load speaks with three: the reader's own token, the project's
   *  anon key for what needs no session, and CORS preflights carrying no auth at
   *  all. Keying on the identity split every load into a dozen rows.
   *
   *  Requests from your backend are then folded into the visit that caused them.
   *  They come from a different address — a datacentre, not the reader — so
   *  grouping by address alone left a Frankfurt row beside every Kardzhali one,
   *  which is two rows for one thing that happened. They keep their own marker
   *  inside the group, because where they ran is still worth knowing.
   */
  const groups = useMemo(() => {
    type Group = { key: string; rows: LogRow[]; server: boolean };
    const out: Group[] = [];
    for (const r of visible) {
      const server = isServerSide(r);
      const who = server ? "backend" : r.ip || r.user || "";
      const last = out[out.length - 1];
      const near =
        last && Math.abs((last.rows[0].ts ?? 0) - (r.ts ?? 0)) <= BURST_SECONDS;
      if (last && near && last.key === who) last.rows.push(r);
      else out.push({ key: who, rows: [r], server });
    }

    // Rows arrive newest-first, so a backend call sits just before the visit that
    // triggered it as often as just after. Attach it to whichever neighbour is a
    // real visit within the window; if neither is, it stands on its own — a cron
    // job or a webhook is not part of anybody's visit.
    const merged: Group[] = [];
    for (const g of out) {
      if (!g.server) {
        merged.push(g);
        continue;
      }
      const prev = merged[merged.length - 1];
      const fits = (o: Group | undefined) =>
        o &&
        !o.server &&
        Math.abs((o.rows[0].ts ?? 0) - (g.rows[0].ts ?? 0)) <= BURST_SECONDS + 2;
      if (fits(prev)) {
        prev.rows.push(...g.rows);
      } else {
        merged.push(g);
      }
    }
    // A backend call whose visit comes *after* it, in newest-first order.
    for (let i = merged.length - 1; i > 0; i--) {
      const g = merged[i - 1];
      const next = merged[i];
      if (
        g.server &&
        !next.server &&
        Math.abs((g.rows[0].ts ?? 0) - (next.rows[0].ts ?? 0)) <= BURST_SECONDS + 2
      ) {
        next.rows.unshift(...g.rows);
        merged.splice(i - 1, 1);
      }
    }
    return merged;
  }, [visible]);



  const statusColor = (s?: number) =>
    s === undefined ? undefined : s >= 500 ? "#e5484d" : s >= 400 ? "#f0b36e" : "#4caf7d";

  return (
    <div style={{ marginTop: 16 }}>
      <div className="row between">
        <h3 style={{ margin: "4px 0" }}>Request logs</h3>
        <div className="row" style={{ gap: 8 }}>
          <select
            value={place}
            onChange={(e) => setPlace(e.target.value)}
            style={{ padding: "6px 8px", fontSize: 13, maxWidth: 220 }}
            title="Locations seen in the pages loaded so far"
          >
            <option value="">All locations{places.size ? ` (${places.size})` : ""}</option>
            {[...places.entries()]
              .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
              .map(([label, n]) => (
                <option key={label} value={label}>
                  {label} · {n}
                </option>
              ))}
          </select>
          <input
            placeholder="filter path / method / status"
            value={filter}
            spellCheck={false}
            style={{ width: 220, padding: "6px 10px", fontSize: 13 }}
            onChange={(e) => setFilter(e.target.value)}
          />
          <button
            className={grouped ? "ghost active" : "ghost"}
            onClick={() =>
              setGrouped((g) => {
                localStorage.setItem("oxibase.logs.grouped", g ? "off" : "on");
                setOpen(null);
                return !g;
              })
            }
            title={
              grouped
                ? "Showing one row per burst — click for every request"
                : "Showing every request — click to group a page load into one row"
            }
          >
            {grouped ? "⛃ Grouped" : "≣ All requests"}
          </button>
          <button
            className={showMap ? "ghost active" : "ghost"}
            onClick={() =>
              setShowMap((m) => {
                localStorage.setItem("oxibase.logs.map", m ? "off" : "on");
                return !m;
              })
            }
            title={showMap ? "Hide the map" : "Show where requests came from"}
          >
            🗺 Map
          </button>
          <button className={live ? "ghost active" : "ghost"} onClick={() => setLive((l) => !l)}>
            {live ? "⏸ Live" : "▶ Live"}
          </button>
          <select
            value={pollMs}
            onChange={(e) => {
              const ms = Number(e.target.value);
              localStorage.setItem("oxibase.logs.interval", String(ms));
              setPollMs(ms);
            }}
            style={{ padding: "6px 8px", fontSize: 13 }}
            title="How often Live refetches"
          >
            {POLL_CHOICES.map((c) => (
              <option key={c.ms} value={c.ms}>
                {c.label}
              </option>
            ))}
          </select>
          <button className="ghost" onClick={() => load(page)}>
            Refresh
          </button>
        </div>
      </div>

      {showMap && (
        <LogsMap
          rows={seen.filter((r) => !isServerSide(r))}
          serverSide={seen.filter(isServerSide).length}
          arrivals={arrivals}
          selected={place}
          onSelect={(label) => {
            // The dropdown's labels carry a flag and the map's do not, so match
            // on the part after the flag — an exact word, not a suffix test that
            // would let "York" select "New York".
            const bare = (v: string) => v.replace(/^\S*\s/, "").trim();
            const match = [...places.keys()].find((p) => bare(p) === label) ?? label;
            setPlace(place === match ? "" : match);
          }}
        />
      )}

      {error && <div className="error">{error}</div>}

      {loading ? (
        <p className="muted">Loading…</p>
      ) : visible.length === 0 && page === 0 ? (
        <p className="muted">
          No requests logged yet — traffic to this project appears here (the server logs each
          data-plane request when its log sink is enabled).
        </p>
      ) : (
        <>
          <div className="table-wrap">
            <table className="grid-table">
              <thead>
                <tr>
                  <th>time</th>
                  <th>method</th>
                  <th>path</th>
                  <th>user</th>
                  <th>from</th>
                  <th>status</th>
                  <th>ms</th>
                </tr>
              </thead>
              <tbody>
                {grouped
                  ? groups.map((g, i) => {
                      // The visitor is what the row is about; a backend call
                      // attached to the visit should not decide its identity.
                      const first = g.rows.find((r) => !isServerSide(r)) ?? g.rows[0];
                      const worst = g.rows.reduce(
                        (acc, r) => Math.max(acc, r.status ?? 0),
                        0,
                      );
                      const totalMs = g.rows.reduce((acc, r) => acc + (r.ms ?? 0), 0);
                      const methods = [...new Set(g.rows.map((r) => r.method))];
                      return [
                        <tr
                          key={`g${i}`}
                          onClick={() => setOpen(open === i ? null : i)}
                          style={{ cursor: "pointer" }}
                          title={`${g.rows.length} request${g.rows.length === 1 ? "" : "s"} — click to expand`}
                        >
                          <td className="muted">
                            {first.ts
                              ? new Date(first.ts * 1000).toISOString().replace("T", " ").slice(0, 19)
                              : ""}
                          </td>
                          <td style={{ whiteSpace: "nowrap" }}>
                            {methods.length === 1 ? methods[0] : `${methods.length} kinds`}
                          </td>
                          <td style={{ maxWidth: 360, overflow: "hidden", textOverflow: "ellipsis" }}>
                            {g.rows.length === 1 ? (
                              first.path
                            ) : (
                              <>
                                {commonPrefix(g.rows.map((r) => r.path ?? ""))}
                                <span className="muted"> · {g.rows.length} requests</span>
                              </>
                            )}
                          </td>
                          <td
                            className="muted"
                            style={{ maxWidth: 190, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                          >
                            {(() => {
                              const who = [...new Set(g.rows.map((r) => shortUser(r) ?? "—"))];
                              // The signed-in address is the useful one to show;
                              // keys and preflights are what any load also does.
                              const named = who.find((w) => w.includes("@")) ?? who[0];
                              return who.length === 1 ? named : `${named} +${who.length - 1}`;
                            })()}
                          </td>
                          <td className="muted" style={{ whiteSpace: "nowrap" }}>
                            {(() => {
                              const visitor = g.rows.find((r) => !isServerSide(r));
                              const backend = g.rows.filter(isServerSide).length;
                              const shown = visitor ?? first;
                              return (
                                <>
                                  {!visitor && (
                                    <span title="Your app's backend, with no visit attached">🖥 </span>
                                  )}
                                  {placeOf(shown) || shown.ip || "—"}
                                  {visitor && backend > 0 && (
                                    <span
                                      className="muted"
                                      title={`${backend} request${backend === 1 ? "" : "s"} your backend made for this visit`}
                                    >
                                      {" "}
                                      + 🖥{backend}
                                    </span>
                                  )}
                                </>
                              );
                            })()}
                          </td>
                          <td style={{ color: statusColor(worst), fontWeight: 600 }}>
                            {worst}
                            {g.rows.length > 1 && new Set(g.rows.map((r) => r.status)).size > 1 && (
                              <span className="muted small"> mixed</span>
                            )}
                          </td>
                          <td className="muted">{totalMs}</td>
                        </tr>,
                        open === i ? (
                          <tr key={`gd${i}`}>
                            <td colSpan={7} style={{ background: "rgba(127,127,127,0.06)" }}>
                              {g.rows.length > 1 && (
                                <div style={{ padding: "8px 4px 2px" }}>
                                  <div className="muted small" style={{ marginBottom: 6 }}>
                                    {g.rows.length} requests from this caller ·{" "}
                                    {[...new Set(g.rows.map((r) => shortUser(r) ?? "no auth"))].join(
                                      ", ",
                                    )}
                                  </div>
                                  {g.rows.map((r, j) => (
                                    <div key={j} className="row" style={{ gap: 10 }}>
                                      <span className="small" style={{ width: 54, flexShrink: 0 }}>
                                        {r.method}
                                      </span>
                                      <span
                                        className="small"
                                        style={{ fontFamily: "monospace", flex: 1, wordBreak: "break-all" }}
                                      >
                                        {r.path}
                                      </span>
                                      <span
                                        className="small"
                                        style={{ color: statusColor(r.status), width: 40, flexShrink: 0 }}
                                      >
                                        {r.status}
                                      </span>
                                      <span className="muted small" style={{ width: 44, flexShrink: 0 }}>
                                        {r.ms}ms
                                      </span>
                                    </div>
                                  ))}
                                </div>
                              )}
                              <Detail row={first} />
                            </td>
                          </tr>
                        ) : null,
                      ];
                    })
                  : visible.map((r, i) => [
                  <tr
                    key={`r${i}`}
                    onClick={() => setOpen(open === i ? null : i)}
                    style={{ cursor: "pointer" }}
                    title="Click for the caller's address and location"
                  >
                    <td className="muted">
                      {r.ts ? new Date(r.ts * 1000).toISOString().replace("T", " ").slice(0, 19) : ""}
                    </td>
                    <td>{r.method}</td>
                    <td style={{ maxWidth: 360, overflow: "hidden", textOverflow: "ellipsis" }}>
                      {r.path}
                    </td>
                    <td className="muted" style={{ maxWidth: 190, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                      {shortUser(r) ?? "—"}
                    </td>
                    <td className="muted" style={{ whiteSpace: "nowrap" }}>
                      {isServerSide(r) && (
                        <span title="Your app's backend, not a visitor — the service key never runs in a browser">
                          🖥{" "}
                        </span>
                      )}
                      {placeOf(r) || r.ip || "—"}
                    </td>
                    <td style={{ color: statusColor(r.status), fontWeight: 600 }}>{r.status}</td>
                    <td className="muted">{r.ms}</td>
                  </tr>,
                  open === i ? (
                    <tr key={`d${i}`}>
                      <td colSpan={7} style={{ background: "rgba(127,127,127,0.06)" }}>
                        <Detail row={r} />
                      </td>
                    </tr>
                  ) : null,
                    ])}
              </tbody>
            </table>
          </div>
          <div className="row between" style={{ marginTop: 8 }}>
            <span className="muted small">
              page {page + 1} ·{" "}
              {grouped
                ? `${groups.length} group${groups.length === 1 ? "" : "s"} of ${visible.length}`
                : `${visible.length}`}{" "}
              request{visible.length === 1 ? "" : "s"}
              {(filter || place) && ` · filtered from ${rows.length}`}
            </span>
            <div className="row" style={{ gap: 6 }}>
              <button
                className="ghost small"
                disabled={page === 0 || live}
                onClick={() => setPage((p) => Math.max(0, p - 1))}
              >
                ← Newer
              </button>
              <button
                className="ghost small"
                disabled={!hasNext || live}
                onClick={() => setPage((p) => p + 1)}
              >
                Older →
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

/** The longest shared path prefix of a burst, so a group says what it touched
 *  rather than just how many. Trimmed at the last slash so it reads as a path. */
function commonPrefix(paths: string[]): string {
  if (paths.length === 0) return "";
  let prefix = paths[0];
  for (const p of paths.slice(1)) {
    let i = 0;
    while (i < prefix.length && i < p.length && prefix[i] === p[i]) i++;
    prefix = prefix.slice(0, i);
    if (!prefix) break;
  }
  const cut = prefix.lastIndexOf("/");
  return cut > 0 ? `${prefix.slice(0, cut)}/…` : prefix || "various";
}

/** Whether a request came from a machine rather than somebody's browser.
 *
 *  The point is the map: a serverless function's datacentre is not a place any
 *  of your users live, and plotting it among them is misleading.
 *
 *  The key alone does not settle it. A service key usually means a backend — but
 *  this dashboard holds one too, and browses your data with it from your
 *  browser, so keying on the role hid the reader's own requests and left the map
 *  empty. What separates them is the user agent: a browser announces itself, a
 *  fetch from a server does not. */
function isServerSide(r: LogRow): boolean {
  const key = r.role === "admin" || (r.user ?? "").startsWith("admin@");
  const browser = /mozilla|applewebkit|chrome|safari|firefox|edg\//i.test(r.user_agent ?? "");
  return key && !browser;
}

/** The acting identity, shortened for the table.
 *
 *  A project key's subject is `read@<ref>` / `admin@<ref>` — the ref adds
 *  nothing here, since every row belongs to this project, so it is dropped and
 *  the role is what remains. An end user shows as their address. */
function shortUser(r: LogRow): string | undefined {
  if (!r.user) return undefined;
  const [name, host] = r.user.split("@");
  if (host && !host.includes(".")) return `${name} key`;
  return r.user;
}

/** How a row is labelled in the location filter: city when the edge sent one,
 *  else the country, else nothing. */
function placeOf(r: LogRow): string {
  if (r.city) return `${flag(r.country ?? "")} ${r.city}`.trim();
  if (r.country) return `${flag(r.country)} ${r.country}`.trim();
  return "";
}

/** ISO country code → flag, so the table scans at a glance. */
function flag(code: string): string {
  if (!/^[A-Za-z]{2}$/.test(code)) return "";
  // XX is "Cloudflare could not tell", T1 is Tor — neither is a place.
  if (code === "XX" || code === "T1") return "";
  return String.fromCodePoint(
    ...[...code.toUpperCase()].map((c) => 0x1f1e6 + c.charCodeAt(0) - 65),
  );
}

/** Everything known about one request's caller. */
function Detail({ row }: { row: LogRow }) {
  const place = [row.city, row.region, row.country, row.continent].filter(Boolean).join(", ");
  const items: [string, string | undefined][] = [
    ["User", row.user],
    ["Origin", isServerSide(row) ? "your app's backend (service key)" : undefined],
    ["Role", row.role],
    ["IP", row.ip],
    ["Location", place || undefined],
    ["Timezone", row.timezone],
    ["Cloudflare ray", row.cf_ray],
    ["Served by", row.app],
    ["User agent", row.user_agent],
  ];
  const known = items.filter(([, v]) => v);

  return (
    <div style={{ padding: "10px 4px", display: "grid", gap: 6 }}>
      {known.length === 0 ? (
        <span className="muted small">
          Nothing was recorded about the caller. Behind Cloudflare, turn on Rules → Settings →
          Managed Transforms → “Add visitor location headers” to get city and timezone; the address
          and country arrive without it.
        </span>
      ) : (
        known.map(([label, value]) => (
          <div key={label} className="row" style={{ gap: 10 }}>
            <span className="muted small" style={{ width: 120, flexShrink: 0 }}>
              {label}
            </span>
            <span
              className="small"
              style={{ fontFamily: "monospace", wordBreak: "break-all" }}
            >
              {value}
            </span>
          </div>
        ))
      )}
      <div className="row" style={{ gap: 10 }}>
        <span className="muted small" style={{ width: 120, flexShrink: 0 }}>
          Full path
        </span>
        <span className="small" style={{ fontFamily: "monospace", wordBreak: "break-all" }}>
          {row.path}
        </span>
      </div>
    </div>
  );
}
