import { useState, useEffect, useCallback, useRef } from "react";
import { runSql } from "../../api/tauri";
import type { JsonValue } from "../../api/types";
import { useToast } from "../common/Toast";

interface Col {
  name: string;
  type: string;
  primaryKey: boolean;
  nullable: boolean;
}

const PAGE = 200;

/** Coerce an edit-box string into a JSON param value for the given SQL type. */
function coerce(raw: string, type: string): JsonValue {
  if (raw === "") return null as unknown as JsonValue; // empty box → NULL
  switch (type) {
    case "INT":
      return parseInt(raw, 10);
    case "DOUBLE":
    case "DECIMAL":
      return parseFloat(raw);
    case "BOOL":
      return /^(true|1|t|yes)$/i.test(raw.trim());
    default:
      return raw;
  }
}

function cellText(v: JsonValue): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

/** Envelope helper. */
function unwrap(resp: unknown): { ok: boolean; error?: string; data?: unknown[] } {
  const r = resp as { ok?: boolean; error?: string; data?: unknown[] };
  return { ok: r?.ok !== false, error: r?.error, data: r?.data };
}

function downloadFile(name: string, mime: string, content: string) {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.click();
  URL.revokeObjectURL(url);
}

function toCsv(cols: string[], rows: JsonValue[][]): string {
  const esc = (v: JsonValue) => {
    const s = cellText(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [cols.join(","), ...rows.map((r) => r.map(esc).join(","))].join("\n");
}

interface Props {
  table: string;
}

export function TableDataView({ table }: Props) {
  const toast = useToast();
  const [cols, setCols] = useState<Col[]>([]);
  const [colNames, setColNames] = useState<string[]>([]);
  const [rows, setRows] = useState<JsonValue[][]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [edit, setEdit] = useState<{ r: number; c: number } | null>(null);
  const [editVal, setEditVal] = useState("");
  const [adding, setAdding] = useState<string[] | null>(null);
  const [busy, setBusy] = useState(false);
  const editRef = useRef<HTMLInputElement>(null);

  const pkCols = cols.filter((c) => c.primaryKey);
  const editable = pkCols.length > 0;

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const d = unwrap(await runSql(`DESCRIBE ${table}`));
      const desc = d.data?.[0] as { columns?: string[]; rows?: JsonValue[][] } | undefined;
      if (!d.ok || !desc?.columns) {
        setError(d.error || "could not read table");
        setLoading(false);
        return;
      }
      const ci = desc.columns.indexOf("column");
      const ti = desc.columns.indexOf("type");
      const pki = desc.columns.indexOf("primary_key");
      const ni = desc.columns.indexOf("nullable");
      const cs: Col[] = (desc.rows || []).map((r) => ({
        name: String(r[ci]),
        type: String(r[ti]),
        primaryKey: r[pki] === true,
        nullable: r[ni] === true,
      }));
      setCols(cs);

      const s = unwrap(await runSql(`SELECT * FROM ${table} LIMIT ${PAGE}`));
      const sel = s.data?.[0] as { columns?: string[]; rows?: JsonValue[][] } | undefined;
      setColNames(sel?.columns || cs.map((c) => c.name));
      setRows(sel?.rows || []);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [table]);

  useEffect(() => {
    load();
  }, [load]);

  useEffect(() => {
    if (edit && editRef.current) {
      editRef.current.focus();
      editRef.current.select();
    }
  }, [edit]);

  const colIndex = useCallback(
    (name: string) => colNames.indexOf(name),
    [colNames]
  );

  /** WHERE clause + params identifying one row by its primary key. */
  const pkWhere = useCallback(
    (row: JsonValue[]) => {
      const clause = pkCols.map((c) => `${c.name} = ?`).join(" AND ");
      const params = pkCols.map((c) => row[colIndex(c.name)]);
      return { clause, params };
    },
    [pkCols, colIndex]
  );

  const startEdit = (r: number, c: number) => {
    if (!editable) return;
    setEdit({ r, c });
    setEditVal(cellText(rows[r][c]));
  };

  const commitEdit = useCallback(async () => {
    if (!edit) return;
    const { r, c } = edit;
    const col = cols.find((x) => x.name === colNames[c]);
    if (!col) return setEdit(null);
    const newVal = coerce(editVal, col.type);
    if (cellText(rows[r][c]) === cellText(newVal as JsonValue)) return setEdit(null);
    const { clause, params } = pkWhere(rows[r]);
    setBusy(true);
    try {
      const resp = unwrap(
        await runSql(`UPDATE ${table} SET ${col.name} = ? WHERE ${clause}`, [
          newVal as JsonValue,
          ...(params as JsonValue[]),
        ])
      );
      if (!resp.ok) {
        toast(resp.error || "update failed", "error");
      } else {
        setRows((rs) => rs.map((row, i) => (i === r ? row.map((v, j) => (j === c ? (newVal as JsonValue) : v)) : row)));
      }
    } catch (e) {
      toast(String(e), "error");
    } finally {
      setBusy(false);
      setEdit(null);
    }
  }, [edit, editVal, cols, colNames, rows, pkWhere, table, toast]);

  const deleteRow = useCallback(
    async (r: number) => {
      const { clause, params } = pkWhere(rows[r]);
      setBusy(true);
      try {
        const resp = unwrap(
          await runSql(`DELETE FROM ${table} WHERE ${clause}`, params as JsonValue[])
        );
        if (!resp.ok) toast(resp.error || "delete failed", "error");
        else setRows((rs) => rs.filter((_, i) => i !== r));
      } catch (e) {
        toast(String(e), "error");
      } finally {
        setBusy(false);
      }
    },
    [rows, pkWhere, table, toast]
  );

  const commitAdd = useCallback(async () => {
    if (!adding) return;
    // Only send non-empty columns; the rest take their DEFAULT / NULL.
    const names: string[] = [];
    const params: JsonValue[] = [];
    adding.forEach((val, i) => {
      if (val !== "") {
        names.push(cols[i].name);
        params.push(coerce(val, cols[i].type));
      }
    });
    if (names.length === 0) {
      setAdding(null);
      return;
    }
    const placeholders = names.map(() => "?").join(", ");
    setBusy(true);
    try {
      const resp = unwrap(
        await runSql(
          `INSERT INTO ${table} (${names.join(", ")}) VALUES (${placeholders})`,
          params
        )
      );
      if (!resp.ok) toast(resp.error || "insert failed", "error");
      else {
        toast("Row inserted", "success");
        setAdding(null);
        load();
      }
    } catch (e) {
      toast(String(e), "error");
    } finally {
      setBusy(false);
    }
  }, [adding, cols, table, toast, load]);

  if (loading) return <div className="empty-state">Loading {table}…</div>;
  if (error) return <div style={{ padding: 16, color: "var(--danger)", fontFamily: "var(--font-mono)", fontSize: 13 }}>{error}</div>;

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <div className="toolbar">
        <strong style={{ fontFamily: "var(--font-mono)" }}>{table}</strong>
        <span style={{ marginLeft: 8, fontSize: 12, color: "var(--text-secondary)" }}>
          {rows.length} row{rows.length === 1 ? "" : "s"}
          {rows.length >= PAGE ? ` (first ${PAGE})` : ""}
        </span>
        {!editable && (
          <span style={{ marginLeft: 10, fontSize: 12, color: "var(--danger)" }}>
            no primary key — read only
          </span>
        )}
        <div style={{ flex: 1 }} />
        <button className="btn btn-secondary btn-sm" onClick={load} disabled={busy}>
          Reload
        </button>
        <button
          className="btn btn-secondary btn-sm"
          onClick={() => downloadFile(`${table}.csv`, "text/csv", toCsv(colNames, rows))}
        >
          CSV
        </button>
        <button
          className="btn btn-secondary btn-sm"
          onClick={() =>
            downloadFile(
              `${table}.json`,
              "application/json",
              JSON.stringify(
                rows.map((r) => Object.fromEntries(colNames.map((n, i) => [n, r[i]]))),
                null,
                2
              )
            )
          }
        >
          JSON
        </button>
        {editable && (
          <button
            className="btn btn-primary btn-sm"
            disabled={!!adding || busy}
            onClick={() => setAdding(cols.map(() => ""))}
          >
            + Add row
          </button>
        )}
      </div>

      <div style={{ flex: 1, overflow: "auto" }}>
        <table className="data-table" style={{ tableLayout: "auto" }}>
          <thead>
            <tr>
              {editable && <th style={{ width: 34 }}></th>}
              {colNames.map((c) => {
                const meta = cols.find((x) => x.name === c);
                return (
                  <th key={c} title={meta?.type}>
                    {meta?.primaryKey && <span title="primary key">🔑 </span>}
                    {c}
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, r) => (
              <tr key={r}>
                {editable && (
                  <td style={{ textAlign: "center" }}>
                    <button
                      className="row-del"
                      title="Delete row"
                      disabled={busy}
                      onClick={() => deleteRow(r)}
                    >
                      ✕
                    </button>
                  </td>
                )}
                {row.map((v, c) => {
                  const isEditing = edit && edit.r === r && edit.c === c;
                  const isPk = cols.find((x) => x.name === colNames[c])?.primaryKey;
                  return (
                    <td
                      key={c}
                      onDoubleClick={() => startEdit(r, c)}
                      title={editable ? "Double-click to edit" : undefined}
                      style={{ cursor: editable ? "text" : "default" }}
                    >
                      {isEditing ? (
                        <input
                          ref={editRef}
                          className="cell-edit"
                          value={editVal}
                          onChange={(e) => setEditVal(e.target.value)}
                          onBlur={commitEdit}
                          onKeyDown={(e) => {
                            if (e.key === "Enter") commitEdit();
                            else if (e.key === "Escape") setEdit(null);
                          }}
                        />
                      ) : v === null ? (
                        <span style={{ color: "var(--text-secondary)", fontStyle: "italic" }}>
                          {isPk ? "" : "null"}
                        </span>
                      ) : (
                        cellText(v)
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}

            {adding && (
              <tr className="add-row">
                <td style={{ textAlign: "center" }}>
                  <button className="row-del" title="Cancel" onClick={() => setAdding(null)}>
                    ✕
                  </button>
                </td>
                {cols.map((col, i) => (
                  <td key={col.name}>
                    <input
                      className="cell-edit"
                      placeholder={col.nullable || col.primaryKey ? col.type : `${col.type}*`}
                      value={adding[i]}
                      onChange={(e) =>
                        setAdding((a) => a!.map((x, j) => (j === i ? e.target.value : x)))
                      }
                      onKeyDown={(e) => {
                        if (e.key === "Enter") commitAdd();
                        else if (e.key === "Escape") setAdding(null);
                      }}
                    />
                  </td>
                ))}
              </tr>
            )}
          </tbody>
        </table>
        {rows.length === 0 && !adding && (
          <div className="empty-state">No rows</div>
        )}
      </div>

      {adding && (
        <div className="toolbar" style={{ justifyContent: "flex-end" }}>
          <span style={{ fontSize: 12, color: "var(--text-secondary)", marginRight: "auto" }}>
            Empty cell → DEFAULT/NULL · Enter to insert
          </span>
          <button className="btn btn-secondary btn-sm" onClick={() => setAdding(null)}>
            Cancel
          </button>
          <button className="btn btn-primary btn-sm" onClick={commitAdd} disabled={busy}>
            Insert
          </button>
        </div>
      )}
    </div>
  );
}
