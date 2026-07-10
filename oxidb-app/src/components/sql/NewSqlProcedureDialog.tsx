import { useState, useMemo, useCallback } from "react";
import { runSql } from "../../api/tauri";
import { SqlEditor } from "../common/SqlEditor";
import { useToast } from "../common/Toast";
import { useEscapeClose } from "../common/useEscapeClose";

const TYPES = ["INT", "TEXT", "DOUBLE", "DECIMAL", "BOOL", "TIMESTAMP", "BLOB"];

interface Param {
  name: string;
  type: string;
}

interface Props {
  onClose: () => void;
  onCreated: () => void;
  /** Editing an existing SQL procedure — prefills and uses CREATE OR ALTER. */
  initial?: { name: string; params: Param[]; body: string };
}

/**
 * Create/edit a SQL-text stored procedure: `CREATE PROCEDURE name(params) AS
 * BEGIN <dml/select statements> END`. Parameters are referenced by name in
 * the body (the engine rewrites them to $1..$N); the body is DML/SELECT only.
 */
export function NewSqlProcedureDialog({ onClose, onCreated, initial }: Props) {
  const toast = useToast();
  const isEdit = !!initial;
  useEscapeClose(onClose);

  const [name, setName] = useState(initial?.name ?? "");
  const [params, setParams] = useState<Param[]>(initial?.params ?? []);
  const [body, setBody] = useState(
    initial?.body ?? "-- DML / SELECT; reference parameters by name\nSELECT 1;"
  );
  const [busy, setBusy] = useState(false);

  const setParam = (i: number, patch: Partial<Param>) =>
    setParams((ps) => ps.map((p, j) => (j === i ? { ...p, ...patch } : p)));
  const addParam = () => setParams((ps) => [...ps, { name: "", type: "INT" }]);
  const removeParam = (i: number) => setParams((ps) => ps.filter((_, j) => j !== i));

  const createSql = useMemo(() => {
    const t = name.trim();
    if (!t) return "";
    const decls = params
      .filter((p) => p.name.trim())
      .map((p) => `${p.name.trim()} ${p.type}`)
      .join(", ");
    const verb = isEdit ? "CREATE OR ALTER PROCEDURE" : "CREATE PROCEDURE";
    return `${verb} ${t}(${decls}) AS\nBEGIN\n${body.trim()}\nEND`;
  }, [name, params, body, isEdit]);

  const deploy = useCallback(async () => {
    if (!name.trim()) {
      toast("Enter a procedure name", "error");
      return;
    }
    setBusy(true);
    try {
      const resp = (await runSql(createSql)) as unknown as { ok?: boolean; error?: string };
      if (resp && resp.ok === false) {
        toast(resp.error || "create failed", "error");
        setBusy(false);
        return;
      }
      toast(isEdit ? "Procedure updated" : "Procedure created", "success");
      onCreated();
      onClose();
    } catch (e) {
      toast(String(e), "error");
    } finally {
      setBusy(false);
    }
  }, [name, createSql, isEdit, toast, onCreated, onClose]);

  return (
    <div className="dialog-overlay">
      <div
        className="dialog"
        style={{ width: "min(960px, 94vw)", height: "86vh", display: "flex", flexDirection: "column" }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="dialog-title">{isEdit ? "Edit SQL Procedure" : "New SQL Procedure"}</div>

        <div className="form-group" style={{ marginBottom: 10 }}>
          <label>Name</label>
          <input
            autoFocus={!isEdit}
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="bump_prices"
            readOnly={isEdit}
            style={{ fontFamily: "var(--font-mono)", opacity: isEdit ? 0.7 : 1 }}
          />
        </div>

        <div className="ct-section">Parameters (referenced by name in the body)</div>
        {params.length > 0 && (
          <table className="ct-table" style={{ marginBottom: 6 }}>
            <tbody>
              {params.map((p, i) => (
                <tr key={i}>
                  <td>
                    <input
                      value={p.name}
                      onChange={(e) => setParam(i, { name: e.target.value })}
                      placeholder="name"
                      style={{ fontFamily: "var(--font-mono)", width: "100%" }}
                    />
                  </td>
                  <td style={{ width: 120 }}>
                    <select value={p.type} onChange={(e) => setParam(i, { type: e.target.value })}>
                      {TYPES.map((t) => (
                        <option key={t} value={t}>
                          {t}
                        </option>
                      ))}
                    </select>
                  </td>
                  <td style={{ width: 30 }}>
                    <button className="ct-remove" onClick={() => removeParam(i)}>
                      ✕
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        <button className="btn btn-secondary btn-sm" onClick={addParam} style={{ marginBottom: 10 }}>
          + Add parameter
        </button>

        <div className="ct-section">Body (DML / SELECT statements)</div>
        <div style={{ flex: 1, minHeight: 160, border: "1px solid var(--border-color)", borderRadius: "var(--radius-sm)", overflow: "hidden", marginBottom: 10 }}>
          <SqlEditor value={body} onChange={setBody} height="100%" />
        </div>

        <div className="ct-preview" style={{ maxHeight: 90 }}>
          <pre>{createSql || "-- name the procedure to see the CREATE"}</pre>
        </div>

        <div className="dialog-actions" style={{ marginTop: 12 }}>
          <button className="btn btn-secondary" onClick={onClose}>
            Cancel
          </button>
          <button className="btn btn-primary" onClick={deploy} disabled={busy}>
            {busy ? <span className="spinner" /> : null}
            {isEdit ? "Update" : "Create"}
          </button>
        </div>
      </div>
    </div>
  );
}
