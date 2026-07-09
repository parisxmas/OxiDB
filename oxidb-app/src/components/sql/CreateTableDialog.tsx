import { useState, useMemo } from "react";

const TYPES = ["INT", "TEXT", "DOUBLE", "DECIMAL", "BOOL", "TIMESTAMP", "BLOB"];

interface ColDraft {
  name: string;
  type: string;
  primaryKey: boolean;
  notNull: boolean;
  unique: boolean;
  autoInc: boolean;
  def: string;
}

function blankCol(): ColDraft {
  return {
    name: "",
    type: "INT",
    primaryKey: false,
    notNull: false,
    unique: false,
    autoInc: false,
    def: "",
  };
}

/** Build `CREATE TABLE` DDL from the form; returns "" if incomplete. */
function buildSql(table: string, cols: ColDraft[]): string {
  const t = table.trim();
  const defined = cols.filter((c) => c.name.trim());
  if (!t || defined.length === 0) return "";
  const lines = defined.map((c) => {
    let s = `  ${c.name.trim()} ${c.type}`;
    if (c.primaryKey) s += " PRIMARY KEY";
    if (c.autoInc) s += " AUTO_INCREMENT";
    if (c.notNull && !c.primaryKey) s += " NOT NULL";
    if (c.unique && !c.primaryKey) s += " UNIQUE";
    if (c.def.trim()) s += ` DEFAULT ${c.def.trim()}`;
    return s;
  });
  return `CREATE TABLE ${t} (\n${lines.join(",\n")}\n);`;
}

interface Props {
  onCreate: (sql: string) => Promise<boolean>;
  onCancel: () => void;
}

export function CreateTableDialog({ onCreate, onCancel }: Props) {
  const [table, setTable] = useState("");
  const [cols, setCols] = useState<ColDraft[]>([
    { ...blankCol(), name: "id", primaryKey: true, autoInc: true },
  ]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const sql = useMemo(() => buildSql(table, cols), [table, cols]);

  const update = (i: number, patch: Partial<ColDraft>) =>
    setCols((cs) =>
      cs.map((c, j) => {
        if (j !== i) {
          // Only one primary key: clearing others when a new PK is set.
          if (patch.primaryKey) return { ...c, primaryKey: false };
          return c;
        }
        const next = { ...c, ...patch };
        // AUTO_INCREMENT is only valid on an INT PRIMARY KEY.
        if (next.autoInc && (!next.primaryKey || next.type !== "INT"))
          next.autoInc = false;
        return next;
      })
    );

  const addCol = () => setCols((cs) => [...cs, blankCol()]);
  const removeCol = (i: number) =>
    setCols((cs) => (cs.length > 1 ? cs.filter((_, j) => j !== i) : cs));

  const create = async () => {
    if (!sql) return;
    setBusy(true);
    setErr(null);
    const ok = await onCreate(sql);
    setBusy(false);
    if (!ok) setErr("Create failed — see toast for the engine error.");
  };

  return (
    <div className="dialog-overlay" onClick={onCancel}>
      <div
        className="dialog"
        style={{ width: 720, maxHeight: "86vh", display: "flex", flexDirection: "column" }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="dialog-title">Create Table</div>

        <div className="form-group" style={{ marginBottom: 12 }}>
          <label>Table name</label>
          <input
            autoFocus
            value={table}
            onChange={(e) => setTable(e.target.value)}
            placeholder="my_table"
            style={{ fontFamily: "var(--font-mono)" }}
          />
        </div>

        <div style={{ overflow: "auto", flex: "0 1 auto", marginBottom: 12 }}>
          <table className="ct-table">
            <thead>
              <tr>
                <th style={{ minWidth: 130 }}>Column</th>
                <th style={{ minWidth: 110 }}>Type</th>
                <th title="Primary key">PK</th>
                <th title="Not null">NN</th>
                <th title="Unique">UQ</th>
                <th title="Auto increment (INT PK only)">AI</th>
                <th style={{ minWidth: 90 }}>Default</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {cols.map((c, i) => (
                <tr key={i}>
                  <td>
                    <input
                      value={c.name}
                      onChange={(e) => update(i, { name: e.target.value })}
                      placeholder="name"
                      style={{ fontFamily: "var(--font-mono)", width: "100%" }}
                    />
                  </td>
                  <td>
                    <select
                      value={c.type}
                      onChange={(e) => update(i, { type: e.target.value })}
                    >
                      {TYPES.map((t) => (
                        <option key={t} value={t}>
                          {t}
                        </option>
                      ))}
                    </select>
                  </td>
                  <td style={{ textAlign: "center" }}>
                    <input
                      type="checkbox"
                      checked={c.primaryKey}
                      onChange={(e) => update(i, { primaryKey: e.target.checked })}
                    />
                  </td>
                  <td style={{ textAlign: "center" }}>
                    <input
                      type="checkbox"
                      checked={c.notNull || c.primaryKey}
                      disabled={c.primaryKey}
                      onChange={(e) => update(i, { notNull: e.target.checked })}
                    />
                  </td>
                  <td style={{ textAlign: "center" }}>
                    <input
                      type="checkbox"
                      checked={c.unique || c.primaryKey}
                      disabled={c.primaryKey}
                      onChange={(e) => update(i, { unique: e.target.checked })}
                    />
                  </td>
                  <td style={{ textAlign: "center" }}>
                    <input
                      type="checkbox"
                      checked={c.autoInc}
                      disabled={!(c.primaryKey && c.type === "INT")}
                      onChange={(e) => update(i, { autoInc: e.target.checked })}
                    />
                  </td>
                  <td>
                    <input
                      value={c.def}
                      onChange={(e) => update(i, { def: e.target.value })}
                      placeholder="—"
                      style={{ fontFamily: "var(--font-mono)", width: "100%" }}
                    />
                  </td>
                  <td>
                    <button
                      className="ct-remove"
                      title="Remove column"
                      onClick={() => removeCol(i)}
                      disabled={cols.length === 1}
                    >
                      ✕
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <button className="btn btn-secondary btn-sm" onClick={addCol} style={{ marginTop: 8 }}>
            + Add column
          </button>
        </div>

        <div className="ct-preview">
          <pre>{sql || "-- fill in a table name and at least one column"}</pre>
        </div>

        {err && (
          <p style={{ color: "var(--danger)", fontSize: 13, margin: "8px 0 0" }}>{err}</p>
        )}

        <div className="dialog-actions" style={{ marginTop: 12 }}>
          <button className="btn btn-secondary" onClick={onCancel}>
            Cancel
          </button>
          <button className="btn btn-primary" onClick={create} disabled={!sql || busy}>
            {busy ? <span className="spinner" /> : null}
            Create Table
          </button>
        </div>
      </div>
    </div>
  );
}
