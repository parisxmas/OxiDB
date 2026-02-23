import { useEffect, useState, useRef, useCallback } from "react";
import { executeRawCommand, getConnectionStatus } from "../../api/tauri";
import { useConnection } from "../../context/ConnectionContext";

interface PingResult {
  time: Date;
  latencyMs: number;
  ok: boolean;
}

export function ServerMonitor() {
  const { status } = useConnection();
  const [pings, setPings] = useState<PingResult[]>([]);
  const [autoPing, setAutoPing] = useState(true);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const doPing = useCallback(async () => {
    const start = performance.now();
    try {
      await executeRawCommand({ cmd: "ping" });
      const latency = performance.now() - start;
      setPings((prev) => [
        { time: new Date(), latencyMs: Math.round(latency * 100) / 100, ok: true },
        ...prev.slice(0, 49),
      ]);
    } catch {
      const latency = performance.now() - start;
      setPings((prev) => [
        { time: new Date(), latencyMs: Math.round(latency * 100) / 100, ok: false },
        ...prev.slice(0, 49),
      ]);
    }
  }, []);

  useEffect(() => {
    if (autoPing && status.connected) {
      doPing();
      intervalRef.current = setInterval(doPing, 5000);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [autoPing, status.connected, doPing]);

  const avgLatency =
    pings.length > 0
      ? Math.round(
          (pings.reduce((sum, p) => sum + p.latencyMs, 0) / pings.length) * 100
        ) / 100
      : 0;

  const successRate =
    pings.length > 0
      ? Math.round((pings.filter((p) => p.ok).length / pings.length) * 100)
      : 0;

  return (
    <div>
      <div className="toolbar" style={{ marginBottom: 16 }}>
        <h2 style={{ fontSize: 18, fontWeight: 600 }}>Server Monitor</h2>
        <div style={{ flex: 1 }} />
        <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 13, color: "var(--text-secondary)" }}>
          <input
            type="checkbox"
            checked={autoPing}
            onChange={(e) => setAutoPing(e.target.checked)}
          />
          Auto-ping (5s)
        </label>
        <button className="btn btn-primary btn-sm" onClick={doPing}>
          Ping Now
        </button>
      </div>

      <div style={{ display: "flex", gap: 16, marginBottom: 20 }}>
        <div className="card" style={{ flex: 1, textAlign: "center" }}>
          <div style={{ marginBottom: 4 }}>
            {status.connected ? (
              <span className="status-dot green" style={{ width: 16, height: 16 }} />
            ) : (
              <span className="status-dot red" style={{ width: 16, height: 16 }} />
            )}
          </div>
          <div style={{ fontWeight: 600 }}>
            {status.connected ? "Connected" : "Disconnected"}
          </div>
          <div style={{ color: "var(--text-secondary)", fontSize: 13 }}>
            {status.mode} {status.detail && `\u00B7 ${status.detail}`}
          </div>
        </div>
        <div className="card" style={{ flex: 1, textAlign: "center" }}>
          <div style={{ fontSize: 28, fontWeight: 700, color: "var(--accent)" }}>
            {avgLatency}ms
          </div>
          <div style={{ color: "var(--text-secondary)", fontSize: 13 }}>Avg Latency</div>
        </div>
        <div className="card" style={{ flex: 1, textAlign: "center" }}>
          <div style={{ fontSize: 28, fontWeight: 700, color: successRate >= 90 ? "var(--success)" : "var(--danger)" }}>
            {successRate}%
          </div>
          <div style={{ color: "var(--text-secondary)", fontSize: 13 }}>Success Rate</div>
        </div>
      </div>

      <h3 style={{ fontSize: 15, fontWeight: 600, marginBottom: 12 }}>Ping History</h3>
      {pings.length === 0 ? (
        <div className="empty-state">No pings yet</div>
      ) : (
        <table className="data-table">
          <thead>
            <tr>
              <th>Time</th>
              <th>Status</th>
              <th>Latency</th>
            </tr>
          </thead>
          <tbody>
            {pings.map((p, i) => (
              <tr key={i}>
                <td style={{ fontFamily: "var(--font-mono)", fontSize: 13 }}>
                  {p.time.toLocaleTimeString()}
                </td>
                <td>
                  {p.ok ? (
                    <span className="badge badge-success">OK</span>
                  ) : (
                    <span className="badge badge-danger">FAIL</span>
                  )}
                </td>
                <td>{p.latencyMs}ms</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
