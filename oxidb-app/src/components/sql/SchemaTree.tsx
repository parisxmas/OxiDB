import { useState, useCallback, useEffect } from "react";
import {
  runSql,
  listDatabases,
  createDatabase,
  dropDatabase,
  setCurrentDb,
  getCurrentDb,
} from "../../api/tauri";
import type { JsonValue } from "../../api/types";
import { ContextMenu } from "../common/ContextMenu";
import type { MenuItem } from "../common/ContextMenu";
import { ConfirmDialog } from "../common/ConfirmDialog";
import { CreateTableDialog } from "./CreateTableDialog";
import { AlterTableDialog } from "./AlterTableDialog";
import { IndexDialog } from "./IndexDialog";
import { ProcedureDialog, type ProcInfo } from "./ProcedureDialog";
import { ImportDialog } from "./ImportDialog";
import { useToast } from "../common/Toast";

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
  /** Open a table in the editable data grid. */
  onBrowse: (table: string) => void;
  /** Bumped by the parent after a DDL run, to force a refresh. */
  refreshKey?: number;
}

export function SchemaTree({ onInsert, onQuery, onBrowse, refreshKey }: Props) {
  const toast = useToast();
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [cols, setCols] = useState<Record<string, ColumnInfo[]>>({});
  const [open, setOpen] = useState<Record<string, boolean>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [menu, setMenu] = useState<{ x: number; y: number; items: MenuItem[] } | null>(null);
  const [confirm, setConfirm] = useState<{ title: string; message: string; sql: string } | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const [editTable, setEditTable] = useState<string | null>(null);
  const [indexTable, setIndexTable] = useState<string | null>(null);
  const [procs, setProcs] = useState<ProcInfo[]>([]);
  const [procsOpen, setProcsOpen] = useState(true);
  const [viewProc, setViewProc] = useState<ProcInfo | null>(null);
  const [showImport, setShowImport] = useState(false);
  const [databases, setDatabases] = useState<string[]>([]);
  const [db, setDb] = useState<string>(() => getCurrentDb() || "");

  // Load the database list once; select the current/default.
  useEffect(() => {
    listDatabases()
      .then((dbs) => {
        setDatabases(dbs);
        if (!db && dbs.length) {
          const def = dbs.includes("oxidb") ? "oxidb" : dbs[0];
          setDb(def);
          setCurrentDb(def);
        }
      })
      .catch(() => setDatabases([]));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const switchDb = useCallback((name: string) => {
    setDb(name);
    setCurrentDb(name);
    setCols({});
    setProcs([]);
    loadTables();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const newDatabase = useCallback(async () => {
    const name = window.prompt("New database name:");
    if (!name?.trim()) return;
    try {
      await createDatabase(name.trim());
      const dbs = await listDatabases();
      setDatabases(dbs);
      switchDb(name.trim());
      toast("Database created", "success");
    } catch (e) {
      toast(String(e), "error");
    }
  }, [toast, switchDb]);

  const removeDatabase = useCallback(async () => {
    if (!db) return;
    if (!window.confirm(`Drop database "${db}" and everything in it?`)) return;
    try {
      await dropDatabase(db);
      const dbs = await listDatabases();
      setDatabases(dbs);
      switchDb(dbs.includes("oxidb") ? "oxidb" : dbs[0] || "");
      toast("Database dropped", "success");
    } catch (e) {
      toast(String(e), "error");
    }
  }, [db, toast, switchDb]);

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
      // Stored procedures (best-effort — empty when none / unsupported).
      const ps = firstSelect(await runSql("SHOW PROCEDURES"));
      if (ps && !ps.error) {
        const pi = (ps.columns || []).indexOf("procedure");
        const pp = (ps.columns || []).indexOf("params");
        const pd = (ps.columns || []).indexOf("definition");
        setProcs(
          (ps.rows || []).map((r) => ({
            name: String(r[pi]),
            params: String(r[pp] ?? ""),
            definition: String(r[pd] ?? ""),
          }))
        );
      } else {
        setProcs([]);
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

  /** Run a DDL/DML statement from the menu, toast the outcome, refresh. */
  const runDdl = useCallback(
    async (sql: string, okMsg: string) => {
      try {
        const resp = (await runSql(sql)) as unknown as {
          ok?: boolean;
          error?: string;
        };
        if (resp && resp.ok === false) {
          toast(resp.error || "statement failed", "error");
          return;
        }
        toast(okMsg, "success");
        setCols({}); // drop cached columns (a table may be gone/changed)
        loadTables();
      } catch (e) {
        toast(String(e), "error");
      }
    },
    [toast, loadTables]
  );

  const openTableMenu = useCallback(
    (e: React.MouseEvent, table: string) => {
      e.preventDefault();
      e.stopPropagation();
      const items: MenuItem[] = [
        { label: "Browse & edit data", onClick: () => onBrowse(table) },
        { label: "SELECT * (100 rows)", onClick: () => onQuery(`SELECT * FROM ${table} LIMIT 100;`) },
        { label: "SELECT COUNT(*)", onClick: () => onQuery(`SELECT COUNT(*) FROM ${table};`) },
        { label: "Describe columns", onClick: () => onQuery(`DESCRIBE ${table};`) },
        { label: "Show indexes", onClick: () => onQuery(`SHOW INDEXES FROM ${table};`) },
        { label: "", onClick: () => {}, separator: true },
        { label: "Edit table…", onClick: () => setEditTable(table) },
        { label: "Manage indexes…", onClick: () => setIndexTable(table) },
        { label: "Insert name into editor", onClick: () => onInsert(table) },
        { label: "", onClick: () => {}, separator: true },
        {
          label: "Truncate (delete all rows)",
          danger: true,
          onClick: () =>
            setConfirm({
              title: `Truncate ${table}?`,
              message: `This deletes every row in "${table}". The table itself stays.`,
              sql: `DELETE FROM ${table};`,
            }),
        },
        {
          label: "Drop table",
          danger: true,
          onClick: () =>
            setConfirm({
              title: `Drop ${table}?`,
              message: `This permanently removes the table "${table}" and all its data.`,
              sql: `DROP TABLE ${table};`,
            }),
        },
      ];
      setMenu({ x: e.clientX, y: e.clientY, items });
    },
    [onQuery, onInsert, onBrowse]
  );

  return (
    <div className="schema-tree">
      {databases.length > 0 && (
        <div className="schema-db-bar">
          <select
            className="schema-db-select"
            value={db}
            onChange={(e) => switchDb(e.target.value)}
            title="Current database"
          >
            {databases.map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
          </select>
          <button className="schema-refresh" title="New database" onClick={newDatabase}>
            +
          </button>
          <button
            className="schema-refresh"
            title="Drop database"
            onClick={removeDatabase}
            disabled={db === "oxidb" || db === "postgres"}
          >
            🗑
          </button>
        </div>
      )}

      <div className="schema-tree-head">
        <span>SCHEMA</span>
        <div style={{ display: "flex", gap: 2 }}>
          <button
            className="schema-refresh"
            title="Import CSV / JSON"
            onClick={() => setShowImport(true)}
          >
            ⇪
          </button>
          <button
            className="schema-refresh"
            title="New table"
            onClick={() => setShowCreate(true)}
          >
            +
          </button>
          <button
            className="schema-refresh"
            title="Refresh"
            onClick={loadTables}
            disabled={loading}
          >
            {loading ? "…" : "⟳"}
          </button>
        </div>
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
              <div
                className="schema-table-row"
                onContextMenu={(e) => openTableMenu(e, t.name)}
              >
                <button
                  className="schema-caret"
                  onClick={() => toggle(t.name)}
                  aria-label="expand"
                >
                  {open[t.name] ? "▾" : "▸"}
                </button>
                <span
                  className="schema-table-name"
                  title="Click to insert · double-click to browse & edit · right-click for menu"
                  onClick={() => onInsert(t.name)}
                  onDoubleClick={() => onBrowse(t.name)}
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

      {procs.length > 0 && (
        <div className="schema-procs">
          <div className="schema-section-head" onClick={() => setProcsOpen((o) => !o)}>
            <span className="schema-caret">{procsOpen ? "▾" : "▸"}</span>
            PROCEDURES ({procs.length})
          </div>
          {procsOpen && (
            <ul className="schema-list" style={{ flex: "none" }}>
              {procs.map((p) => (
                <li key={p.name}>
                  <div className="schema-table-row">
                    <span className="schema-caret" style={{ visibility: "hidden" }}>
                      ▸
                    </span>
                    <span
                      className="schema-table-name"
                      title="Click to view · double-click to insert CALL"
                      onClick={() => setViewProc(p)}
                      onDoubleClick={() => {
                        const args = p.params
                          .split(",")
                          .map((s) => s.trim())
                          .filter(Boolean)
                          .map(() => "?")
                          .join(", ");
                        onQuery(`CALL ${p.name}(${args});`);
                      }}
                    >
                      ⚙ {p.name}
                    </span>
                  </div>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}

      {menu && (
        <ContextMenu
          x={menu.x}
          y={menu.y}
          items={menu.items}
          onClose={() => setMenu(null)}
        />
      )}

      {confirm && (
        <ConfirmDialog
          title={confirm.title}
          message={confirm.message}
          confirmLabel="Yes, do it"
          danger
          onCancel={() => setConfirm(null)}
          onConfirm={() => {
            const { sql } = confirm;
            setConfirm(null);
            runDdl(sql, "Done");
          }}
        />
      )}

      {showCreate && (
        <CreateTableDialog
          onCancel={() => setShowCreate(false)}
          onCreate={async (sql) => {
            let ok = false;
            try {
              const resp = (await runSql(sql)) as unknown as {
                ok?: boolean;
                error?: string;
              };
              if (resp && resp.ok === false) {
                toast(resp.error || "create failed", "error");
              } else {
                ok = true;
                toast("Table created", "success");
                setShowCreate(false);
                setCols({});
                loadTables();
              }
            } catch (e) {
              toast(String(e), "error");
            }
            return ok;
          }}
        />
      )}

      {editTable && (
        <AlterTableDialog
          table={editTable}
          onCancel={() => setEditTable(null)}
          onApply={async (statements) => {
            // Run each ALTER in order; stop at the first engine error so a
            // partial rename/drop is visible rather than silently continued.
            for (let i = 0; i < statements.length; i++) {
              try {
                const resp = (await runSql(statements[i])) as unknown as {
                  ok?: boolean;
                  error?: string;
                };
                if (resp && resp.ok === false) {
                  toast(`Step ${i + 1}/${statements.length}: ${resp.error}`, "error");
                  setCols({});
                  loadTables();
                  return false;
                }
              } catch (e) {
                toast(String(e), "error");
                return false;
              }
            }
            toast(`Applied ${statements.length} change(s)`, "success");
            setEditTable(null);
            setCols({});
            loadTables();
            return true;
          }}
        />
      )}

      {indexTable && (
        <IndexDialog
          table={indexTable}
          onClose={() => setIndexTable(null)}
          onChanged={loadTables}
        />
      )}

      {showImport && (
        <ImportDialog
          tables={tables.map((t) => t.name)}
          onClose={() => setShowImport(false)}
          onDone={() => {
            setCols({});
            loadTables();
          }}
        />
      )}

      {viewProc && (
        <ProcedureDialog
          proc={viewProc}
          onClose={() => setViewProc(null)}
          onInsert={onInsert}
          onDrop={(procName) => {
            setViewProc(null);
            setConfirm({
              title: `Drop ${procName}?`,
              message: `This permanently removes the stored procedure "${procName}".`,
              sql: `DROP PROCEDURE ${procName};`,
            });
          }}
        />
      )}
    </div>
  );
}
