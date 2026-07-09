import { useState, useCallback, useEffect } from "react";
import {
  oximemConnect,
  oximemDisconnect,
  oximemStatus,
  oximemScan,
  oximemGet,
  oximemSetString,
  oximemDel,
} from "../../api/tauri";
import { useToast } from "../common/Toast";
import { useConnection } from "../../context/ConnectionContext";

interface KeyValue {
  type: string;
  ttl: number | null;
  value: { kind: string; value: unknown };
}

export function OxiMemPage() {
  const toast = useToast();
  const { status: dbStatus } = useConnection();
  const [connected, setConnected] = useState(false);
  const [detail, setDetail] = useState("");
  const [host, setHost] = useState(() => {
    // Prefill host from the current DB connection if it's remote.
    const d = dbStatus.detail || "";
    const at = d.includes("@") ? d.split("@")[1] : d;
    return at.split(":")[0] || "127.0.0.1";
  });
  const [port, setPort] = useState("6379");
  const [pattern, setPattern] = useState("*");
  const [keys, setKeys] = useState<string[]>([]);
  const [cursor, setCursor] = useState("0");
  const [scanning, setScanning] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [detailVal, setDetailVal] = useState<KeyValue | null>(null);
  const [editStr, setEditStr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    oximemStatus().then((s) => {
      const st = s as { connected?: boolean; detail?: string };
      if (st.connected) {
        setConnected(true);
        setDetail(st.detail || "");
      }
    });
  }, []);

  const connect = useCallback(async () => {
    setBusy(true);
    try {
      const r = (await oximemConnect(host, parseInt(port, 10))) as { detail?: string };
      setConnected(true);
      setDetail(r.detail || `${host}:${port}`);
      setKeys([]);
      setCursor("0");
    } catch (e) {
      toast(String(e), "error");
    } finally {
      setBusy(false);
    }
  }, [host, port, toast]);

  const disconnect = useCallback(async () => {
    await oximemDisconnect();
    setConnected(false);
    setKeys([]);
    setSelected(null);
    setDetailVal(null);
  }, []);

  const scan = useCallback(
    async (fresh: boolean) => {
      setScanning(true);
      try {
        const cur = fresh ? "0" : cursor;
        const r = (await oximemScan(cur, pattern)) as { cursor: string; keys: string[] };
        setKeys((prev) => (fresh ? r.keys : [...prev, ...r.keys]));
        setCursor(r.cursor);
      } catch (e) {
        toast(String(e), "error");
      } finally {
        setScanning(false);
      }
    },
    [cursor, pattern, toast]
  );

  const openKey = useCallback(
    async (key: string) => {
      setSelected(key);
      setEditStr(null);
      try {
        const r = (await oximemGet(key)) as unknown as KeyValue;
        setDetailVal(r);
        if (r.value.kind === "string") setEditStr(String(r.value.value ?? ""));
      } catch (e) {
        toast(String(e), "error");
      }
    },
    [toast]
  );

  const saveString = useCallback(async () => {
    if (selected === null || editStr === null) return;
    setBusy(true);
    try {
      await oximemSetString(selected, editStr);
      toast("Saved", "success");
      openKey(selected);
    } catch (e) {
      toast(String(e), "error");
    } finally {
      setBusy(false);
    }
  }, [selected, editStr, toast, openKey]);

  const delKey = useCallback(async () => {
    if (selected === null) return;
    setBusy(true);
    try {
      await oximemDel(selected);
      toast("Deleted", "success");
      setKeys((ks) => ks.filter((k) => k !== selected));
      setSelected(null);
      setDetailVal(null);
    } catch (e) {
      toast(String(e), "error");
    } finally {
      setBusy(false);
    }
  }, [selected, toast]);

  if (!connected) {
    return (
      <div className="connection-screen">
        <div className="connection-box" style={{ width: 420 }}>
          <div className="connection-title" style={{ fontSize: 22 }}>OxiMem</div>
          <div className="connection-subtitle">
            Connect to the OxiMem port (RESP / Redis protocol)
          </div>
          <div style={{ display: "flex", gap: 8 }}>
            <div className="form-group" style={{ flex: 2 }}>
              <label>Host</label>
              <input value={host} onChange={(e) => setHost(e.target.value)} />
            </div>
            <div className="form-group" style={{ flex: 1 }}>
              <label>Port</label>
              <input value={port} onChange={(e) => setPort(e.target.value)} type="number" />
            </div>
          </div>
          <button className="btn btn-primary" style={{ width: "100%" }} onClick={connect} disabled={busy}>
            {busy ? <span className="spinner" /> : null}
            Connect
          </button>
        </div>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", height: "calc(100vh - var(--header-height) - 40px)" }}>
      {/* Key list */}
      <div className="oximem-keys">
        <div className="toolbar" style={{ gap: 6 }}>
          <input
            style={{ flex: 1, fontFamily: "var(--font-mono)", fontSize: 12 }}
            value={pattern}
            onChange={(e) => setPattern(e.target.value)}
            placeholder="pattern e.g. user:*"
            onKeyDown={(e) => e.key === "Enter" && scan(true)}
          />
          <button className="btn btn-primary btn-sm" onClick={() => scan(true)} disabled={scanning}>
            Scan
          </button>
        </div>
        <div style={{ flex: 1, overflow: "auto" }}>
          {keys.length === 0 ? (
            <div className="empty-state" style={{ fontSize: 13 }}>
              {scanning ? "Scanning…" : "No keys — press Scan"}
            </div>
          ) : (
            <ul className="oximem-key-list">
              {keys.map((k) => (
                <li
                  key={k}
                  className={`oximem-key${selected === k ? " active" : ""}`}
                  onClick={() => openKey(k)}
                  title={k}
                >
                  {k}
                </li>
              ))}
            </ul>
          )}
        </div>
        <div className="toolbar" style={{ justifyContent: "space-between" }}>
          <span style={{ fontSize: 11, color: "var(--text-secondary)", fontFamily: "var(--font-mono)" }}>
            {keys.length} key{keys.length === 1 ? "" : "s"}
          </span>
          {cursor !== "0" && (
            <button className="btn btn-secondary btn-sm" onClick={() => scan(false)} disabled={scanning}>
              Load more
            </button>
          )}
        </div>
      </div>

      {/* Value panel */}
      <div className="oximem-value">
        <div className="toolbar">
          <span style={{ fontSize: 12, color: "var(--text-secondary)" }}>{detail}</span>
          <div style={{ flex: 1 }} />
          <button className="btn btn-secondary btn-sm" onClick={disconnect}>
            Disconnect
          </button>
        </div>
        <div style={{ flex: 1, overflow: "auto", padding: 16 }}>
          {!selected || !detailVal ? (
            <div className="empty-state">Select a key</div>
          ) : (
            <>
              <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 12 }}>
                <strong style={{ fontFamily: "var(--font-mono)", fontSize: 15 }}>{selected}</strong>
                <span className="badge badge-muted">{detailVal.type}</span>
                {detailVal.ttl != null && detailVal.ttl >= 0 && (
                  <span className="badge badge-muted">TTL {detailVal.ttl}s</span>
                )}
                <div style={{ flex: 1 }} />
                <button className="btn btn-danger btn-sm" onClick={delKey} disabled={busy}>
                  Delete key
                </button>
              </div>
              <OxiMemValue
                kind={detailVal.value.kind}
                value={detailVal.value.value}
                editStr={editStr}
                setEditStr={setEditStr}
                onSave={saveString}
                busy={busy}
              />
            </>
          )}
        </div>
      </div>
    </div>
  );
}

function OxiMemValue({
  kind,
  value,
  editStr,
  setEditStr,
  onSave,
  busy,
}: {
  kind: string;
  value: unknown;
  editStr: string | null;
  setEditStr: (s: string) => void;
  onSave: () => void;
  busy: boolean;
}) {
  if (kind === "string") {
    return (
      <div>
        <textarea
          className="oximem-string"
          value={editStr ?? ""}
          onChange={(e) => setEditStr(e.target.value)}
          spellCheck={false}
        />
        <div style={{ marginTop: 8 }}>
          <button className="btn btn-primary btn-sm" onClick={onSave} disabled={busy}>
            Save
          </button>
        </div>
      </div>
    );
  }
  const rows = (value as unknown[]) || [];
  if (kind === "hash" || kind === "zset") {
    return (
      <table className="data-table">
        <thead>
          <tr>
            <th>{kind === "zset" ? "member" : "field"}</th>
            <th>{kind === "zset" ? "score" : "value"}</th>
          </tr>
        </thead>
        <tbody>
          {(rows as [string, string][]).map((p, i) => (
            <tr key={i}>
              <td style={{ fontFamily: "var(--font-mono)" }}>{p[0]}</td>
              <td style={{ fontFamily: "var(--font-mono)" }}>{p[1]}</td>
            </tr>
          ))}
        </tbody>
      </table>
    );
  }
  // list / set
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th style={{ width: 60 }}>#</th>
          <th>value</th>
        </tr>
      </thead>
      <tbody>
        {(rows as string[]).map((v, i) => (
          <tr key={i}>
            <td style={{ color: "var(--text-secondary)" }}>{i}</td>
            <td style={{ fontFamily: "var(--font-mono)" }}>{v}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
