import { useState, useEffect, useCallback, useMemo } from "react";
import { runSql } from "../../api/tauri";
import type { JsonValue } from "../../api/types";
import { useToast } from "../common/Toast";

interface IndexRow {
  name: string;
  columns: string;
}

function unwrap(resp: unknown): { ok: boolean; error?: string; data?: unknown[] } {
  const r = resp as { ok?: boolean; error?: string; data?: unknown[] };
  return { ok: r?.ok !== false, error: r?.error, data: r?.data };
}

interface Props {
  table: string;
  onClose: () => void;
  /** Bump the schema tree after index DDL. */
  onChanged: () => void;
}

export function IndexDialog({ table, onClose, onChanged }: Props) {
  const toast = useToast();
  const [indexes, setIndexes] = useState<IndexRow[]>([]);
  const [cols, setCols] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);

  // Create-form state
  const [name, setName] = useState("");
  const [nameEdited, setNameEdited] = useState(false);
  const [picked, setPicked] = useState<string[]>([]); // ordered
  const [unique, setUnique] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const ix = unwrap(await runSql(`SHOW INDEXES FROM ${table}`));
      const d = ix.data?.[0] as { columns?: string[]; rows?: JsonValue[][] } | undefined;
      if (ix.ok && d?.columns) {
        const ni = d.columns.indexOf("index");
        const ci = d.columns.indexOf("columns");
        setIndexes((d.rows || []).map((r) => ({ name: String(r[ni]), columns: String(r[ci]) })));
      } else {
        setIndexes([]);
      }
      const de = unwrap(await runSql(`DESCRIBE ${table}`));
      const dd = de.data?.[0] as { columns?: string[]; rows?: JsonValue[][] } | undefined;
      if (de.ok && dd?.columns) {
        const cc = dd.columns.indexOf("column");
        setCols((dd.rows || []).map((r) => String(r[cc])));
      }
    } catch (e) {
      toast(String(e), "error");
    } finally {
      setLoading(false);
    }
  }, [table, toast]);

  useEffect(() => {
    load();
  }, [load]);

  // Suggest a name from the picked columns until the user types their own.
  useEffect(() => {
    if (!nameEdited) {
      setName(picked.length ? `idx_${table}_${picked.join("_")}` : "");
    }
  }, [picked, table, nameEdited]);

  const togglePick = (c: string) =>
    setPicked((p) => (p.includes(c) ? p.filter((x) => x !== c) : [...p, c]));

  const createSql = useMemo(() => {
    if (!name.trim() || picked.length === 0) return "";
    return `CREATE ${unique ? "UNIQUE " : ""}INDEX ${name.trim()} ON ${table} (${picked.join(", ")});`;
  }, [name, picked, unique, table]);

  const runDdl = useCallback(
    async (sql: string, okMsg: string) => {
      setBusy(true);
      try {
        const r = unwrap(await runSql(sql));
        if (!r.ok) {
          toast(r.error || "failed", "error");
          return false;
        }
        toast(okMsg, "success");
        onChanged();
        await load();
        return true;
      } catch (e) {
        toast(String(e), "error");
        return false;
      } finally {
        setBusy(false);
      }
    },
    [toast, onChanged, load]
  );

  const create = async () => {
    if (!createSql) return;
    const ok = await runDdl(createSql, "Index created");
    if (ok) {
      setPicked([]);
      setUnique(false);
      setName("");
      setNameEdited(false);
    }
  };

  return (
    <div className="dialog-overlay">
      <div
        className="dialog"
        style={{ width: 640, maxHeight: "86vh", display: "flex", flexDirection: "column" }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="dialog-title">
          Indexes on <span style={{ fontFamily: "var(--font-mono)" }}>{table}</span>
        </div>

        {loading ? (
          <div className="empty-state">Loading…</div>
        ) : (
          <>
            <div className="ct-section">Existing indexes</div>
            {indexes.length === 0 ? (
              <div className="empty-state" style={{ padding: 12, fontSize: 13 }}>
                No secondary indexes
              </div>
            ) : (
              <table className="data-table" style={{ marginBottom: 12 }}>
                <thead>
                  <tr>
                    <th>Index</th>
                    <th>Columns</th>
                    <th style={{ width: 60 }}></th>
                  </tr>
                </thead>
                <tbody>
                  {indexes.map((ix) => (
                    <tr key={ix.name}>
                      <td style={{ fontFamily: "var(--font-mono)" }}>{ix.name}</td>
                      <td style={{ fontFamily: "var(--font-mono)", color: "var(--text-secondary)" }}>
                        {ix.columns}
                      </td>
                      <td>
                        <button
                          className="row-del"
                          title="Drop index"
                          disabled={busy}
                          onClick={() => runDdl(`DROP INDEX ${ix.name};`, "Index dropped")}
                        >
                          ✕
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}

            <div className="ct-section" style={{ marginTop: 8 }}>Create index</div>
            <div style={{ marginBottom: 8 }}>
              <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 4 }}>
                Columns (click in order)
              </div>
              <div className="idx-chips">
                {cols.map((c) => {
                  const pos = picked.indexOf(c);
                  return (
                    <button
                      key={c}
                      className={`idx-chip${pos >= 0 ? " active" : ""}`}
                      onClick={() => togglePick(c)}
                    >
                      {pos >= 0 && <span className="idx-chip-pos">{pos + 1}</span>}
                      {c}
                    </button>
                  );
                })}
              </div>
            </div>

            <div style={{ display: "flex", gap: 8, alignItems: "flex-end" }}>
              <div className="form-group" style={{ flex: 1, marginBottom: 0 }}>
                <label>Index name</label>
                <input
                  value={name}
                  onChange={(e) => {
                    setName(e.target.value);
                    setNameEdited(true);
                  }}
                  placeholder="idx_name"
                  style={{ fontFamily: "var(--font-mono)" }}
                />
              </div>
              <label className="remember-row" style={{ marginBottom: 6 }}>
                <input type="checkbox" checked={unique} onChange={(e) => setUnique(e.target.checked)} />
                Unique
              </label>
            </div>

            <div className="ct-preview" style={{ marginTop: 10 }}>
              <pre>{createSql || "-- pick one or more columns and name the index"}</pre>
            </div>

            <div className="dialog-actions" style={{ marginTop: 12 }}>
              <button className="btn btn-secondary" onClick={onClose}>
                Close
              </button>
              <button className="btn btn-primary" onClick={create} disabled={!createSql || busy}>
                {busy ? <span className="spinner" /> : null}
                Create Index
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
