import { useState, useEffect, useCallback, useRef } from "react";
import { runSql } from "../../api/tauri";
import type { JsonValue } from "../../api/types";
import { useToast } from "../common/Toast";

interface StmtResult {
  columns?: string[];
  types?: (string | null)[];
  rows?: JsonValue[][];
}

interface ColMeta {
  name: string;
  type: string;
  primaryKey: boolean;
}

/**
 * Detect the single source table of a SELECT so its result can be edited in
 * place. Returns null for joins, aggregates, subqueries, multiple tables, or
 * anything else that has no single writable table.
 */
export function detectTable(sql: string): string | null {
  // Strip line comments + collapse whitespace.
  const clean = sql
    .replace(/--[^\n]*/g, " ")
    .replace(/\/\*[\s\S]*?\*\//g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!/^select\b/i.test(clean)) return null;
  if (/\bjoin\b/i.test(clean)) return null;
  if (/\bgroup\s+by\b/i.test(clean)) return null;
  // FROM <table> up to the next clause keyword.
  const m = clean.match(/\bfrom\s+("?[a-z_][\w]*"?)\s*(where|order|limit|offset|having|;|$)/i);
  if (!m) return null;
  const table = m[1].replace(/"/g, "");
  // A comma after the table name = multiple tables.
  const after = clean.slice(clean.toLowerCase().indexOf(" from ") + 6);
  if (/^[^;]*\bfrom\b[^;]*,/i.test(after)) return null;
  if (/,/.test(after.split(/\bwhere\b|\border\b|\blimit\b|;/i)[0])) return null;
  return table;
}

function cellText(v: JsonValue): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function coerce(raw: string, type: string): JsonValue {
  if (raw === "") return null as unknown as JsonValue;
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

interface Props {
  result: StmtResult;
  sql: string;
}

/**
 * Query-result grid that becomes editable when the result comes from one
 * table whose primary key is present in the columns — double-click a cell to
 * UPDATE, keyed by the primary key. Falls back to read-only otherwise.
 */
export function EditableResultGrid({ result, sql }: Props) {
  const toast = useToast();
  const columns = result.columns || [];
  const [rows, setRows] = useState<JsonValue[][]>(result.rows || []);
  const [meta, setMeta] = useState<ColMeta[] | null>(null);
  const [edit, setEdit] = useState<{ r: number; c: number } | null>(null);
  const [editVal, setEditVal] = useState("");
  const [busy, setBusy] = useState(false);
  const editRef = useRef<HTMLInputElement>(null);
  const table = detectTable(sql);

  // A column reads as numeric if its SQL type is a number type, or — when the
  // type is unknown (computed columns) — its first non-null value is a number.
  // Numeric columns are right-aligned, the convention in SQL/spreadsheet tools.
  const types = result.types || [];
  const isNumericCol = useCallback(
    (c: number): boolean => {
      const t = (types[c] || "").toUpperCase();
      if (t) return /^(INT|INTEGER|BIGINT|SMALLINT|TINYINT|DECIMAL|NUMERIC|DOUBLE|FLOAT|REAL)/.test(t);
      // No declared type (e.g. a Cobra procedure's result set): infer from the
      // first non-null value. Exact decimals arrive as strings like "2391.00"
      // to keep trailing zeros, so a plain numeric string counts as numeric too.
      for (const row of rows) {
        const v = row[c];
        if (v === null || v === undefined) continue;
        if (typeof v === "number") return true;
        return typeof v === "string" && /^-?\d+(\.\d+)?$/.test(v);
      }
      return false;
    },
    [types, rows]
  );

  // Reset when a new result arrives.
  useEffect(() => {
    setRows(result.rows || []);
    setEdit(null);
  }, [result]);

  // Load the source table's columns/PK once, to decide editability.
  useEffect(() => {
    let cancelled = false;
    setMeta(null);
    if (!table) return;
    (async () => {
      try {
        const resp = (await runSql(`DESCRIBE ${table}`)) as unknown as { ok?: boolean; data?: { columns?: string[]; rows?: JsonValue[][] }[] };
        if (cancelled || resp?.ok === false) return;
        const d = resp?.data?.[0];
        if (!d?.columns) return;
        const ci = d.columns.indexOf("column"), ti = d.columns.indexOf("type"), pki = d.columns.indexOf("primary_key");
        setMeta((d.rows || []).map((r) => ({ name: String(r[ci]), type: String(r[ti]), primaryKey: r[pki] === true })));
      } catch {
        /* read-only */
      }
    })();
    return () => { cancelled = true; };
  }, [table]);

  useEffect(() => {
    if (edit && editRef.current) { editRef.current.focus(); editRef.current.select(); }
  }, [edit]);

  const pkCols = (meta || []).filter((c) => c.primaryKey).map((c) => c.name);
  const editable = !!table && pkCols.length > 0 && pkCols.every((p) => columns.includes(p));
  const typeOf = useCallback((col: string) => meta?.find((m) => m.name === col)?.type || "TEXT", [meta]);
  const isPk = useCallback((col: string) => pkCols.includes(col), [pkCols]);

  const startEdit = (r: number, c: number) => {
    if (!editable || isPk(columns[c])) return;
    setEdit({ r, c });
    setEditVal(cellText(rows[r][c]));
  };

  const commit = useCallback(async () => {
    if (!edit || !table) return;
    const { r, c } = edit;
    const col = columns[c];
    const newVal = coerce(editVal, typeOf(col));
    if (cellText(rows[r][c]) === cellText(newVal as JsonValue)) { setEdit(null); return; }
    const where = pkCols.map((p) => `${p} = ?`).join(" AND ");
    const params = pkCols.map((p) => rows[r][columns.indexOf(p)]);
    setBusy(true);
    try {
      const resp = (await runSql(`UPDATE ${table} SET ${col} = ? WHERE ${where}`, [newVal as JsonValue, ...(params as JsonValue[])])) as unknown as { ok?: boolean; error?: string };
      if (resp?.ok === false) toast(resp.error || "update failed", "error");
      else setRows((rs) => rs.map((row, i) => (i === r ? row.map((v, j) => (j === c ? (newVal as JsonValue) : v)) : row)));
    } catch (e) {
      toast(String(e), "error");
    } finally {
      setBusy(false);
      setEdit(null);
    }
  }, [edit, editVal, table, columns, rows, pkCols, typeOf, toast]);

  if (columns.length === 0) return <div className="empty-state">No columns</div>;

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      {table && !editable && (
        <div className="result-readonly-note">read-only — the primary key isn't in the result; add it to edit inline</div>
      )}
      <div style={{ flex: 1, overflow: "auto" }}>
        <table className="data-table">
          <thead>
            <tr>
              {columns.map((c, ci) => (
                <th key={c} style={isNumericCol(ci) ? { textAlign: "right" } : undefined}>
                  {editable && isPk(c) && <span className="schema-pk" style={{ marginRight: 4 }} title="primary key">🔑</span>}
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, r) => (
              <tr key={r}>
                {row.map((v, c) => {
                  const editing = edit && edit.r === r && edit.c === c;
                  const canEdit = editable && !isPk(columns[c]);
                  return (
                    <td
                      key={c}
                      onDoubleClick={() => startEdit(r, c)}
                      title={canEdit ? "Double-click to edit" : undefined}
                      style={{
                        cursor: canEdit ? "text" : "default",
                        textAlign: isNumericCol(c) ? "right" : undefined,
                        fontVariantNumeric: isNumericCol(c) ? "tabular-nums" : undefined,
                      }}
                    >
                      {editing ? (
                        <input
                          ref={editRef}
                          className="cell-edit"
                          value={editVal}
                          disabled={busy}
                          onChange={(e) => setEditVal(e.target.value)}
                          onBlur={commit}
                          onKeyDown={(e) => {
                            if (e.key === "Enter") commit();
                            else if (e.key === "Escape") setEdit(null);
                          }}
                        />
                      ) : v === null ? (
                        <span style={{ color: "var(--text-secondary)", fontStyle: "italic" }}>null</span>
                      ) : (
                        cellText(v)
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
