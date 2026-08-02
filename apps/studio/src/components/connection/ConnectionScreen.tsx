import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { open } from "@tauri-apps/plugin-dialog";
import { openEmbedded, connectRemote } from "../../api/tauri";
import { useConnection } from "../../context/ConnectionContext";
import {
  loadConnections,
  saveConnection,
  deleteConnection,
  newId,
  type SavedConnection,
} from "../../api/connections";

export function ConnectionScreen() {
  const [tab, setTab] = useState<"embedded" | "client">("client");
  const [path, setPath] = useState("");
  const [host, setHost] = useState("127.0.0.1");
  const [port, setPort] = useState("4444");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [remember, setRemember] = useState(false);
  const [name, setName] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [saved, setSaved] = useState<SavedConnection[]>(loadConnections);
  const { setStatus } = useConnection();
  const navigate = useNavigate();

  const handleBrowse = async () => {
    const selected = await open({ directory: true, multiple: false });
    if (selected) setPath(selected as string);
  };

  const fillFrom = (c: SavedConnection) => {
    setTab(c.mode);
    setName(c.name);
    if (c.mode === "embedded") {
      setPath(c.path || "");
    } else {
      setHost(c.host);
      setPort(String(c.port));
      setUsername(c.username || "");
      setPassword(c.password || "");
      setRemember(!!c.password);
    }
  };

  const connect = async (c?: SavedConnection) => {
    setError("");
    setLoading(true);
    const mode = c?.mode ?? tab;
    try {
      let result;
      if (mode === "embedded") {
        const p = c?.path ?? path;
        if (!p) {
          setError("Please select a data directory");
          setLoading(false);
          return;
        }
        result = await openEmbedded(p);
      } else {
        const h = c?.host ?? host;
        const pt = c ? c.port : parseInt(port, 10);
        const u = c ? c.username : username;
        const pw = c ? c.password : password;
        result = await connectRemote(h, pt, u || undefined, pw || undefined);
      }
      setStatus(result);
      navigate("/sql");
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  const handleSaveAndConnect = async () => {
    const conn: SavedConnection = {
      id: newId(),
      name:
        name.trim() ||
        (tab === "embedded" ? path.split("/").pop() || "embedded" : `${host}:${port}`),
      mode: tab,
      host,
      port: parseInt(port, 10) || 4444,
      username,
      password: remember ? password : undefined,
      path,
    };
    setSaved(saveConnection(conn));
    connect();
  };

  const handleDelete = (id: string) => setSaved(deleteConnection(id));

  return (
    <div className="connection-screen">
      <div className="connection-box" style={{ width: 460 }}>
        <div className="connection-title">OxiDB Studio</div>
        <div className="connection-subtitle">Connect to a database to get started</div>

        {saved.length > 0 && (
          <div className="saved-conns">
            <div className="saved-conns-label">Saved connections</div>
            {saved.map((c) => (
              <div key={c.id} className="saved-conn">
                <button className="saved-conn-main" onClick={() => connect(c)} disabled={loading}>
                  <span className="saved-conn-name">{c.name}</span>
                  <span className="saved-conn-detail">
                    {c.mode === "embedded"
                      ? c.path
                      : `${c.username ? c.username + "@" : ""}${c.host}:${c.port}`}
                  </span>
                </button>
                <button className="saved-conn-edit" title="Load into form" onClick={() => fillFrom(c)}>
                  ✎
                </button>
                <button className="saved-conn-del" title="Delete" onClick={() => handleDelete(c.id)}>
                  ✕
                </button>
              </div>
            ))}
          </div>
        )}

        <div className="tab-bar">
          <button className={`tab${tab === "client" ? " active" : ""}`} onClick={() => setTab("client")}>
            Remote Server
          </button>
          <button className={`tab${tab === "embedded" ? " active" : ""}`} onClick={() => setTab("embedded")}>
            Embedded
          </button>
        </div>

        {tab === "embedded" ? (
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
        ) : (
          <>
            <div style={{ display: "flex", gap: 8 }}>
              <div className="form-group" style={{ flex: 2 }}>
                <label>Host</label>
                <input value={host} onChange={(e) => setHost(e.target.value)} placeholder="127.0.0.1" />
              </div>
              <div className="form-group" style={{ flex: 1 }}>
                <label>Port</label>
                <input value={port} onChange={(e) => setPort(e.target.value)} placeholder="4444" type="number" />
              </div>
            </div>
            <div style={{ display: "flex", gap: 8 }}>
              <div className="form-group" style={{ flex: 1 }}>
                <label>Username</label>
                <input
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  placeholder="(anonymous)"
                  autoComplete="off"
                />
              </div>
              <div className="form-group" style={{ flex: 1 }}>
                <label>Password</label>
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="(none)"
                  autoComplete="off"
                />
              </div>
            </div>
            <label className="remember-row">
              <input type="checkbox" checked={remember} onChange={(e) => setRemember(e.target.checked)} />
              Remember password (stored in plaintext locally)
            </label>
          </>
        )}

        <div className="form-group" style={{ marginTop: 4 }}>
          <label>Save as (optional)</label>
          <input value={name} onChange={(e) => setName(e.target.value)} placeholder="My server" />
        </div>

        {error && <p style={{ color: "var(--danger)", fontSize: 13, marginBottom: 12 }}>{error}</p>}

        <div style={{ display: "flex", gap: 8 }}>
          <button className="btn btn-secondary" style={{ flex: 1 }} onClick={handleSaveAndConnect} disabled={loading}>
            Save & Connect
          </button>
          <button className="btn btn-primary" style={{ flex: 1 }} onClick={() => connect()} disabled={loading}>
            {loading ? <span className="spinner" /> : null}
            Connect
          </button>
        </div>
      </div>
    </div>
  );
}
