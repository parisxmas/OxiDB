import { useEffect, useState } from "react";
import { getDashboardStats } from "../../api/tauri";
import type { DashboardStats } from "../../api/types";

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

export function DashboardPage() {
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  const loadStats = async () => {
    setLoading(true);
    try {
      const data = await getDashboardStats();
      setStats(data);
      setError("");
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadStats();
  }, []);

  if (loading) return <div className="empty-state"><span className="spinner" /></div>;
  if (error) return <div className="empty-state" style={{ color: "var(--danger)" }}>{error}</div>;
  if (!stats) return null;

  return (
    <div>
      <div className="toolbar" style={{ marginBottom: 16 }}>
        <h2 style={{ flex: 1, fontSize: 18, fontWeight: 600 }}>Dashboard</h2>
        <button className="btn btn-secondary btn-sm" onClick={loadStats}>
          Refresh
        </button>
      </div>

      <div style={{ display: "flex", gap: 16, marginBottom: 20 }}>
        <div className="card" style={{ flex: 1, textAlign: "center" }}>
          <div style={{ fontSize: 28, fontWeight: 700, color: "var(--accent)" }}>
            {stats.collections.length}
          </div>
          <div style={{ color: "var(--text-secondary)", fontSize: 13 }}>Collections</div>
        </div>
        <div className="card" style={{ flex: 1, textAlign: "center" }}>
          <div style={{ fontSize: 28, fontWeight: 700, color: "var(--accent)" }}>
            {stats.total_docs.toLocaleString()}
          </div>
          <div style={{ color: "var(--text-secondary)", fontSize: 13 }}>Total Documents</div>
        </div>
        <div className="card" style={{ flex: 1, textAlign: "center" }}>
          <div style={{ fontSize: 28, fontWeight: 700, color: "var(--accent)" }}>
            {formatBytes(stats.total_storage_bytes)}
          </div>
          <div style={{ color: "var(--text-secondary)", fontSize: 13 }}>Storage</div>
        </div>
      </div>

      <h3 style={{ fontSize: 15, fontWeight: 600, marginBottom: 12 }}>Collections</h3>
      {stats.collections.length === 0 ? (
        <div className="empty-state">No collections yet</div>
      ) : (
        <div className="card-grid">
          {stats.collections.map((col) => (
            <div key={col.name} className="card">
              <div style={{ fontWeight: 600, marginBottom: 4 }}>{col.name}</div>
              <div style={{ color: "var(--text-secondary)", fontSize: 13 }}>
                {col.doc_count.toLocaleString()} docs
                {col.storage_bytes > 0 && ` \u00B7 ${formatBytes(col.storage_bytes)}`}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
