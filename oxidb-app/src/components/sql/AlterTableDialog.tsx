import { useState, useEffect, useMemo, useCallback } from "react";
import { runSql } from "../../api/tauri";
import type { JsonValue } from "../../api/types";

const TYPES = ["INT", "TEXT", "DOUBLE", "DECIMAL", "BOOL", "TIMESTAMP", "BLOB"];

interface ExistingCol {
  orig: string; // name as it is in the catalog (never mutated)
  name: string; // editable — a change becomes RENAME COLUMN
  type: string; // read-only (engine has no ALTER COLUMN TYPE)
  primaryKey: boolean;
  drop: boolean;
}

interface NewCol {
  name: string;
  type: string;
  notNull: boolean;
  def: string;
}

/** The ordered list of ALTER statements a diff produces (renames, drops, adds). */
function buildAlters(table: string, existing: ExistingCol[], added: NewCol[]): string[] {
  const out: string[] = [];
  // Rename first so later references are unambiguous; skip columns being dropped.
  for (const c of existing) {
    if (!c.drop && c.name.trim() && c.name.trim() !== c.orig)
      out.push(`ALTER TABLE ${table} RENAME COLUMN ${c.orig} TO ${c.name.trim()};`);
  }
  for (const c of existing) {
    if (c.drop) out.push(`ALTER TABLE ${table} DROP COLUMN ${c.orig};`);
  }
  for (const c of added) {
    if (!c.name.trim()) continue;
    let s = `ALTER TABLE ${table} ADD COLUMN ${c.name.trim()} ${c.type}`;
    if (c.notNull) s += " NOT NULL";
    if (c.def.trim()) s += ` DEFAULT ${c.def.trim()}`;
    out.push(s + ";");
  }
  return out;
}

interface Props {
  table: string;
  /** Run each statement in order; resolves true on full success. */
  onApply: (statements: string[]) => Promise<boolean>;
  onCancel: () => void;
}

export function AlterTableDialog({ table, onApply, onCancel }: Props) {
  const [existing, setExisting] = useState<ExistingCol[]>([]);
  const [added, setAdded] = useState<NewCol[]>([]);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [loadErr, setLoadErr] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      try {
        const resp = (await runSql(`DESCRIBE ${table}`)) as unknown as {
          ok?: boolean;
          error?: string;
          data?: { columns?: string[]; rows?: JsonValue[][] }[];
        };
        const d = resp?.data?.[0];
        if (resp?.ok === false || !d?.columns) {
          setLoadErr(resp?.error || "could not read columns");
        } else {
          const ci = d.columns.indexOf("column");
          const ti = d.columns.indexOf("type");
          const pki = d.columns.indexOf("primary_key");
          setExisting(
            (d.rows || []).map((r) => ({
              orig: String(r[ci]),
              name: String(r[ci]),
              type: String(r[ti]),
              primaryKey: r[pki] === true,
              drop: false,
            }))
          );
        }
      } catch (e) {
        setLoadErr(String(e));
      } finally {
        setLoading(false);
      }
    })();
  }, [table]);

  const statements = useMemo(
    () => buildAlters(table, existing, added),
    [table, existing, added]
  );

  const setExist = (i: number, patch: Partial<ExistingCol>) =>
    setExisting((cs) => cs.map((c, j) => (j === i ? { ...c, ...patch } : c)));
  const setNew = (i: number, patch: Partial<NewCol>) =>
    setAdded((cs) => cs.map((c, j) => (j === i ? { ...c, ...patch } : c)));
  const addNew = () =>
    setAdded((cs) => [...cs, { name: "", type: "INT", notNull: false, def: "" }]);
  const removeNew = (i: number) => setAdded((cs) => cs.filter((_, j) => j !== i));

  const apply = useCallback(async () => {
    if (statements.length === 0) return;
    setBusy(true);
    await onApply(statements);
    setBusy(false);
  }, [statements, onApply]);

  return (
    <div className="dialog-overlay" onClick={onCancel}>
      <div
        className="dialog"
        style={{ width: 720, maxHeight: "86vh", display: "flex", flexDirection: "column" }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="dialog-title">
          Edit Table <span style={{ fontFamily: "var(--font-mono)" }}>{table}</span>
        </div>

        {loading ? (
          <div className="empty-state">Loading columns…</div>
        ) : loadErr ? (
          <p style={{ color: "var(--danger)", fontSize: 13 }}>{loadErr}</p>
        ) : (
          <>
            <div style={{ overflow: "auto", marginBottom: 12 }}>
              <div className="ct-section">Existing columns</div>
              <table className="ct-table">
                <thead>
                  <tr>
                    <th style={{ minWidth: 150 }}>Name</th>
                    <th style={{ minWidth: 100 }}>Type</th>
                    <th>Drop</th>
                  </tr>
                </thead>
                <tbody>
                  {existing.map((c, i) => (
                    <tr key={c.orig} style={c.drop ? { opacity: 0.45 } : undefined}>
                      <td>
                        <input
                          value={c.name}
                          disabled={c.drop}
                          onChange={(e) => setExist(i, { name: e.target.value })}
                          style={{
                            fontFamily: "var(--font-mono)",
                            width: "100%",
                            textDecoration:
                              !c.drop && c.name !== c.orig ? "underline" : "none",
                          }}
                        />
                      </td>
                      <td style={{ fontFamily: "var(--font-mono)", color: "var(--text-secondary)" }}>
                        {c.type}
                        {c.primaryKey ? " · PK" : ""}
                      </td>
                      <td style={{ textAlign: "center" }}>
                        <input
                          type="checkbox"
                          checked={c.drop}
                          title={c.primaryKey ? "Dropping the primary key column" : "Drop this column"}
                          onChange={(e) => setExist(i, { drop: e.target.checked })}
                        />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>

              <div className="ct-section" style={{ marginTop: 14 }}>Add columns</div>
              {added.length > 0 && (
                <table className="ct-table">
                  <thead>
                    <tr>
                      <th style={{ minWidth: 150 }}>Name</th>
                      <th style={{ minWidth: 100 }}>Type</th>
                      <th title="Not null">NN</th>
                      <th style={{ minWidth: 100 }}>Default</th>
                      <th></th>
                    </tr>
                  </thead>
                  <tbody>
                    {added.map((c, i) => (
                      <tr key={i}>
                        <td>
                          <input
                            value={c.name}
                            onChange={(e) => setNew(i, { name: e.target.value })}
                            placeholder="name"
                            style={{ fontFamily: "var(--font-mono)", width: "100%" }}
                          />
                        </td>
                        <td>
                          <select value={c.type} onChange={(e) => setNew(i, { type: e.target.value })}>
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
                            checked={c.notNull}
                            onChange={(e) => setNew(i, { notNull: e.target.checked })}
                          />
                        </td>
                        <td>
                          <input
                            value={c.def}
                            onChange={(e) => setNew(i, { def: e.target.value })}
                            placeholder="—"
                            style={{ fontFamily: "var(--font-mono)", width: "100%" }}
                          />
                        </td>
                        <td>
                          <button className="ct-remove" title="Remove" onClick={() => removeNew(i)}>
                            ✕
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
              <button className="btn btn-secondary btn-sm" onClick={addNew} style={{ marginTop: 8 }}>
                + Add column
              </button>
            </div>

            <div className="ct-preview">
              <pre>
                {statements.length
                  ? statements.join("\n")
                  : "-- no changes yet (rename a column, tick Drop, or add a column)"}
              </pre>
            </div>

            <p style={{ fontSize: 11, color: "var(--text-secondary)", margin: "6px 0 0" }}>
              A NOT NULL column added to a non-empty table needs a DEFAULT. Column type
              changes aren't supported — drop and re-add instead.
            </p>

            <div className="dialog-actions" style={{ marginTop: 10 }}>
              <button className="btn btn-secondary" onClick={onCancel}>
                Cancel
              </button>
              <button
                className="btn btn-primary"
                onClick={apply}
                disabled={statements.length === 0 || busy}
              >
                {busy ? <span className="spinner" /> : null}
                Apply {statements.length > 0 ? `(${statements.length})` : ""}
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
