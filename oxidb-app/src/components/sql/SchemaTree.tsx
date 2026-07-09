import { useState, useCallback, useEffect } from "react";
import { runSql } from "../../api/tauri";
import type { JsonValue } from "../../api/types";

interface StmtResult {
  columns?: string[];
  rows?: JsonValue[][];
  error?: string;
}

interface TableInfo {
  name: string;
  rows: number | null;
}

interface ColumnInfo {
  name: string;
  type: string;
  primaryKey: boolean;
  nullable: boolean;
}

/** Pull the single SELECT result out of the {ok,data:[...]} envelope. */
function firstSelect(resp: unknown): StmtResult | null {
  const r = resp as { ok?: boolean; error?: string; data?: StmtResult[] };
  if (r && r.ok === false) return { error: r.error };
  const d = r?.data?.[0];
  return d && d.columns ? d : null;
}

interface Props {
  /** Insert a snippet (table or column name) into the editor at the cursor. */
  onInsert: (text: string) => void;
  /** Replace the editor with a ready-made query. */
  onQuery: (sql: string) => void;
  /** Bumped by the parent after a DDL run, to force a refresh. */
  refreshKey?: number;
}

export function SchemaTree({ onInsert, onQuery, refreshKey }: Props) {
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [cols, setCols] = useState<Record<string, ColumnInfo[]>>({});
  const [open, setOpen] = useState<Record<string, boolean>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadTables = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const sel = firstSelect(await runSql("SHOW TABLES"));
      if (!sel) {
        setTables([]);
      } else if (sel.error) {
        setError(sel.error);
        setTables([]);
      } else {
        setTables(
          (sel.rows || []).map((r) => ({
            name: String(r[0]),
            rows: typeof r[1] === "number" ? r[1] : null,
          }))
        );
      }
    } catch (e) {
      setError(String(e));
      setTables([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadTables();
  }, [loadTables, refreshKey]);

  const loadColumns = useCallback(async (table: string) => {
    const sel = firstSelect(await runSql(`DESCRIBE ${table}`));
    if (sel && !sel.error) {
      const idx = (name: string) => (sel.columns || []).indexOf(name);
      const ci = idx("column"),
        ti = idx("type"),
        pki = idx("primary_key"),
        ni = idx("nullable");
      setCols((prev) => ({
        ...prev,
        [table]: (sel.rows || []).map((r) => ({
          name: String(r[ci]),
          type: String(r[ti]),
          primaryKey: r[pki] === true,
          nullable: r[ni] === true,
        })),
      }));
    }
  }, []);

  const toggle = useCallback(
    (table: string) => {
      const next = !open[table];
      setOpen((p) => ({ ...p, [table]: next }));
      if (next && !cols[table]) loadColumns(table);
    },
    [open, cols, loadColumns]
  );

  return (
    <div className="schema-tree">
      <div className="schema-tree-head">
        <span>SCHEMA</span>
        <button
          className="schema-refresh"
          title="Refresh"
          onClick={loadTables}
          disabled={loading}
        >
          {loading ? "…" : "⟳"}
        </button>
      </div>

      {error ? (
        <div className="schema-empty">{error}</div>
      ) : tables.length === 0 ? (
        <div className="schema-empty">
          {loading ? "Loading…" : "No tables"}
        </div>
      ) : (
        <ul className="schema-list">
          {tables.map((t) => (
            <li key={t.name}>
              <div className="schema-table-row">
                <button
                  className="schema-caret"
                  onClick={() => toggle(t.name)}
                  aria-label="expand"
                >
                  {open[t.name] ? "▾" : "▸"}
                </button>
                <span
                  className="schema-table-name"
                  title="Click to insert · double-click to SELECT *"
                  onClick={() => onInsert(t.name)}
                  onDoubleClick={() =>
                    onQuery(`SELECT * FROM ${t.name} LIMIT 100;`)
                  }
                >
                  {t.name}
                </span>
                {t.rows !== null && (
                  <span className="schema-rowcount">{t.rows}</span>
                )}
              </div>
              {open[t.name] && (
                <ul className="schema-cols">
                  {(cols[t.name] || []).map((c) => (
                    <li
                      key={c.name}
                      className="schema-col"
                      title={`${c.type}${c.nullable ? " · nullable" : " · not null"}`}
                      onClick={() => onInsert(c.name)}
                    >
                      {c.primaryKey && (
                        <span className="schema-pk" title="primary key">
                          🔑
                        </span>
                      )}
                      <span className="schema-col-name">{c.name}</span>
                      <span className="schema-col-type">{c.type}</span>
                    </li>
                  ))}
                  {(cols[t.name]?.length ?? 0) === 0 && (
                    <li className="schema-col schema-empty">…</li>
                  )}
                </ul>
              )}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
