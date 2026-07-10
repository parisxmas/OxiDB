import { useState } from "react";
import { oximemExec } from "../../api/tauri";
import { useToast } from "../common/Toast";

const TYPES = ["string", "hash", "list", "set", "zset"] as const;
type KeyType = (typeof TYPES)[number];

interface Props {
  onClose: () => void;
  /** Called with the new key name after successful creation. */
  onCreated: (key: string) => void;
}

/** Build the RESP command that creates a key of the chosen type with one
 *  initial element (a Redis key can't exist empty). */
function buildCommand(type: KeyType, key: string, a: string, b: string): string[] | null {
  switch (type) {
    case "string":
      return ["SET", key, a];
    case "hash":
      return a ? ["HSET", key, a, b] : null;
    case "list":
      return a ? ["RPUSH", key, a] : null;
    case "set":
      return a ? ["SADD", key, a] : null;
    case "zset":
      return a ? ["ZADD", key, b || "0", a] : null;
  }
}

export function NewKeyDialog({ onClose, onCreated }: Props) {
  const toast = useToast();
  const [type, setType] = useState<KeyType>("string");
  const [key, setKey] = useState("");
  const [a, setA] = useState("");
  const [b, setB] = useState("");
  const [busy, setBusy] = useState(false);

  const labels: Record<KeyType, [string, string?]> = {
    string: ["value"],
    hash: ["field", "value"],
    list: ["first element"],
    set: ["first member"],
    zset: ["member", "score"],
  };
  const [la, lb] = labels[type];

  const create = async () => {
    if (!key.trim()) return;
    const cmd = buildCommand(type, key.trim(), a, b);
    if (!cmd) {
      toast(`${type} needs an initial ${la}`, "error");
      return;
    }
    setBusy(true);
    try {
      await oximemExec(cmd);
      toast("Key created", "success");
      onCreated(key.trim());
    } catch (e) {
      toast(String(e), "error");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="dialog-overlay" onClick={onClose}>
      <div className="dialog" style={{ width: 440 }} onClick={(e) => e.stopPropagation()}>
        <div className="dialog-title">New key</div>

        <div className="form-group">
          <label>Type</label>
          <div className="idx-chips">
            {TYPES.map((t) => (
              <button
                key={t}
                className={`idx-chip${type === t ? " active" : ""}`}
                onClick={() => setType(t)}
              >
                {t}
              </button>
            ))}
          </div>
        </div>

        <div className="form-group">
          <label>Key</label>
          <input
            autoFocus
            value={key}
            onChange={(e) => setKey(e.target.value)}
            placeholder="my:key"
            style={{ fontFamily: "var(--font-mono)" }}
          />
        </div>

        <div style={{ display: "flex", gap: 8 }}>
          <div className="form-group" style={{ flex: 1 }}>
            <label>{la}</label>
            <input value={a} onChange={(e) => setA(e.target.value)} style={{ fontFamily: "var(--font-mono)" }} />
          </div>
          {lb && (
            <div className="form-group" style={{ flex: 1 }}>
              <label>{lb}</label>
              <input value={b} onChange={(e) => setB(e.target.value)} style={{ fontFamily: "var(--font-mono)" }} />
            </div>
          )}
        </div>

        <div className="dialog-actions">
          <button className="btn btn-secondary" onClick={onClose}>
            Cancel
          </button>
          <button className="btn btn-primary" onClick={create} disabled={busy || !key.trim()}>
            {busy ? <span className="spinner" /> : null}
            Create
          </button>
        </div>
      </div>
    </div>
  );
}
