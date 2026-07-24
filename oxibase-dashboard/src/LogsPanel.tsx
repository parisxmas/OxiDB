import { useEffect, useRef, useState } from "react";
import { type LogRow, listProjectLogs } from "./api.ts";

const PAGE_SIZE = 50;

/** Logs tab: the project's recent data-plane requests (method, path, status,
 *  latency), newest first — paged, with optional live auto-refresh. */
export function LogsPanel({ projectRef }: { projectRef: string }) {
  const [rows, setRows] = useState<LogRow[]>([]);
  const [page, setPage] = useState(0);
  // One extra row is requested per page purely to know whether a next page exists.
  const [hasNext, setHasNext] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [live, setLive] = useState(false);
  const [filter, setFilter] = useState("");
  const timer = useRef<number | null>(null);

  async function load(p = page) {
    try {
      const r = await listProjectLogs(projectRef, PAGE_SIZE + 1, p * PAGE_SIZE);
      setHasNext(r.length > PAGE_SIZE);
      setRows(r.slice(0, PAGE_SIZE));
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
    load(0);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectRef]);

  useEffect(() => {
    load(page);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [page]);

  useEffect(() => {
    if (live) {
      // Live view follows the newest entries — jump back to the first page.
      setPage(0);
      timer.current = window.setInterval(() => load(0), 5000);
      return () => {
        if (timer.current) window.clearInterval(timer.current);
      };
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [live, projectRef]);

  const visible = filter
    ? rows.filter(
        (r) =>
          r.path?.includes(filter) ||
          r.method?.toLowerCase() === filter.toLowerCase() ||
          String(r.status ?? "").startsWith(filter),
      )
    : rows;

  const statusColor = (s?: number) =>
    s === undefined ? undefined : s >= 500 ? "#e5484d" : s >= 400 ? "#f0b36e" : "#4caf7d";

  return (
    <div style={{ marginTop: 16 }}>
      <div className="row between">
        <h3 style={{ margin: "4px 0" }}>Request logs</h3>
        <div className="row" style={{ gap: 8 }}>
          <input
            placeholder="filter path / method / status"
            value={filter}
            spellCheck={false}
            style={{ width: 220, padding: "6px 10px", fontSize: 13 }}
            onChange={(e) => setFilter(e.target.value)}
          />
          <button className={live ? "ghost active" : "ghost"} onClick={() => setLive((l) => !l)}>
            {live ? "⏸ Live" : "▶ Live"}
          </button>
          <button className="ghost" onClick={() => load(page)}>
            Refresh
          </button>
        </div>
      </div>

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
                  <th>status</th>
                  <th>ms</th>
                </tr>
              </thead>
              <tbody>
                {visible.map((r, i) => (
                  <tr key={i}>
                    <td className="muted">
                      {r.ts ? new Date(r.ts * 1000).toISOString().replace("T", " ").slice(0, 19) : ""}
                    </td>
                    <td>{r.method}</td>
                    <td style={{ maxWidth: 420, overflow: "hidden", textOverflow: "ellipsis" }}>
                      {r.path}
                    </td>
                    <td style={{ color: statusColor(r.status), fontWeight: 600 }}>{r.status}</td>
                    <td className="muted">{r.ms}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="row between" style={{ marginTop: 8 }}>
            <span className="muted small">
              page {page + 1}
              {filter && ` · ${visible.length} of ${rows.length} shown`}
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
