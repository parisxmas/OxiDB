import { useEffect, useRef, useState } from "react";
import { type LogRow, listProjectLogs } from "./api.ts";

/** Logs tab: the project's recent data-plane requests (method, path, status,
 *  latency), newest first, with optional auto-refresh. */
export function LogsPanel({ projectRef }: { projectRef: string }) {
  const [rows, setRows] = useState<LogRow[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [live, setLive] = useState(false);
  const [filter, setFilter] = useState("");
  const timer = useRef<number | null>(null);

  async function refresh() {
    try {
      setRows(await listProjectLogs(projectRef, 200));
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    setLoading(true);
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectRef]);

  useEffect(() => {
    if (live) {
      timer.current = window.setInterval(refresh, 5000);
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
        <h3 style={{ margin: "4px 0" }}>
          Request logs <span className="muted small">(last {rows.length})</span>
        </h3>
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
          <button className="ghost" onClick={refresh}>
            Refresh
          </button>
        </div>
      </div>

      {error && <div className="error">{error}</div>}

      {loading ? (
        <p className="muted">Loading…</p>
      ) : visible.length === 0 ? (
        <p className="muted">
          No requests logged yet — traffic to this project appears here (the server logs each
          data-plane request when its log sink is enabled).
        </p>
      ) : (
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
                    {r.ts ? new Date(r.ts * 1000).toISOString().slice(11, 19) : ""}
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
      )}
    </div>
  );
}
