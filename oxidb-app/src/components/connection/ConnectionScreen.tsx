import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { open } from "@tauri-apps/plugin-dialog";
import { openEmbedded, connectRemote } from "../../api/tauri";
import { useConnection } from "../../context/ConnectionContext";

export function ConnectionScreen() {
  const [tab, setTab] = useState<"embedded" | "client">("embedded");
  const [path, setPath] = useState("");
  const [host, setHost] = useState("127.0.0.1");
  const [port, setPort] = useState("4444");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const { setStatus } = useConnection();
  const navigate = useNavigate();

  const handleBrowse = async () => {
    const selected = await open({ directory: true, multiple: false });
    if (selected) {
      setPath(selected as string);
    }
  };

  const handleConnect = async () => {
    setError("");
    setLoading(true);
    try {
      let result;
      if (tab === "embedded") {
        if (!path) {
          setError("Please select a data directory");
          setLoading(false);
          return;
        }
        result = await openEmbedded(path);
      } else {
        result = await connectRemote(host, parseInt(port, 10));
      }
      setStatus(result);
      navigate("/dashboard");
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="connection-screen">
      <div className="connection-box">
        <div className="connection-title">OxiDB</div>
        <div className="connection-subtitle">
          Connect to a database to get started
        </div>

        <div className="tab-bar">
          <button
            className={`tab${tab === "embedded" ? " active" : ""}`}
            onClick={() => setTab("embedded")}
          >
            Embedded
          </button>
          <button
            className={`tab${tab === "client" ? " active" : ""}`}
            onClick={() => setTab("client")}
          >
            Remote Server
          </button>
        </div>

        {tab === "embedded" ? (
          <div>
            <div className="form-group">
              <label>Data Directory</label>
              <div style={{ display: "flex", gap: 8 }}>
                <input
                  style={{ flex: 1 }}
                  value={path}
                  onChange={(e) => setPath(e.target.value)}
                  placeholder="/path/to/oxidb_data"
                />
                <button className="btn btn-secondary" onClick={handleBrowse}>
                  Browse
                </button>
              </div>
            </div>
          </div>
        ) : (
          <div>
            <div className="form-group">
              <label>Host</label>
              <input
                value={host}
                onChange={(e) => setHost(e.target.value)}
                placeholder="127.0.0.1"
              />
            </div>
            <div className="form-group">
              <label>Port</label>
              <input
                value={port}
                onChange={(e) => setPort(e.target.value)}
                placeholder="4444"
                type="number"
              />
            </div>
          </div>
        )}

        {error && (
          <p style={{ color: "var(--danger)", fontSize: 13, marginBottom: 12 }}>
            {error}
          </p>
        )}

        <button
          className="btn btn-primary"
          style={{ width: "100%" }}
          onClick={handleConnect}
          disabled={loading}
        >
          {loading ? <span className="spinner" /> : null}
          {tab === "embedded" ? "Open Database" : "Connect"}
        </button>
      </div>
    </div>
  );
}
