import { useState, useCallback, useEffect } from "react";
import { runSql, getCurrentDb } from "../../api/tauri";
import { useDatabase } from "../../context/DatabaseContext";
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
  onInsert: (text: string) => void;
  onQuery: (sql: string) => void;
  onBrowse: (table: string) => void;
  /** Bumped by the parent after a DDL run, to force a refresh. */
  refreshKey?: number;
}

export function SchemaTree({ onInsert, onQuery, onBrowse, refreshKey }: Props) {
  const toast = useToast();
  const { db, databases, setDb, reload: reloadDatabases } = useDatabase();

  // Everything is keyed by database name so several databases can stay
  // expanded at once, each with its own tables/procedures.
  const [openDbs, setOpenDbs] = useState<Record<string, boolean>>({});
  const [loadedDbs, setLoadedDbs] = useState<Record<string, boolean>>({});
  const [loadingDb, setLoadingDb] = useState<string | null>(null);
  const [errorByDb, setErrorByDb] = useState<Record<string, string | null>>({});
  const [tablesByDb, setTablesByDb] = useState<Record<string, TableInfo[]>>({});
  const [procsByDb, setProcsByDb] = useState<Record<string, ProcInfo[]>>({});
  const [colsByDb, setColsByDb] = useState<Record<string, Record<string, ColumnInfo[]>>>({});
  const [openTables, setOpenTables] = useState<Record<string, boolean>>({}); // `${db}::${table}`

  const [menu, setMenu] = useState<{ x: number; y: number; items: MenuItem[] } | null>(null);
  const [confirm, setConfirm] = useState<{ title: string; message: string; sql: string; db: string } | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const [editTable, setEditTable] = useState<string | null>(null);
  const [indexTable, setIndexTable] = useState<string | null>(null);
  const [viewProc, setViewProc] = useState<ProcInfo | null>(null);
  const [showImport, setShowImport] = useState(false);

  /** Load one database's tables + procedures (scoped explicitly to it). */
  const load = useCallback(async (dbName: string) => {
    setLoadingDb(dbName);
    setErrorByDb((p) => ({ ...p, [dbName]: null }));
    try {
      const sel = firstSelect(await runSql("SHOW TABLES", undefined, dbName));
      if (sel?.error) {
        setErrorByDb((p) => ({ ...p, [dbName]: sel.error || "error" }));
        setTablesByDb((p) => ({ ...p, [dbName]: [] }));
      } else {
        setTablesByDb((p) => ({
          ...p,
          [dbName]: (sel?.rows || []).map((r) => ({
            name: String(r[0]),
            rows: typeof r[1] === "number" ? r[1] : null,
          })),
        }));
      }
      const ps = firstSelect(await runSql("SHOW PROCEDURES", undefined, dbName));
      const pi = (ps?.columns || []).indexOf("procedure");
      const pp = (ps?.columns || []).indexOf("params");
      const pd = (ps?.columns || []).indexOf("definition");
      setProcsByDb((p) => ({
        ...p,
        [dbName]: ps && !ps.error
          ? (ps.rows || []).map((r) => ({
              name: String(r[pi]),
              params: String(r[pp] ?? ""),
              definition: String(r[pd] ?? ""),
            }))
          : [],
      }));
    } catch (e) {
      setErrorByDb((p) => ({ ...p, [dbName]: String(e) }));
    } finally {
      setLoadingDb(null);
      setLoadedDbs((p) => ({ ...p, [dbName]: true }));
      setOpenDbs((p) => ({ ...p, [dbName]: true }));
    }
  }, []);

  /** Reload whatever database the current operation targeted. */
  const reloadCurrent = useCallback(() => {
    const d = getCurrentDb() || db;
    setColsByDb((p) => ({ ...p, [d]: {} }));
    if (d) load(d);
  }, [db, load]);

  // Expand/collapse a database root node; make it current; load on first open.
  const toggleDb = useCallback(
    (dbName: string) => {
      setDb(dbName);
      setOpenDbs((prev) => {
        const next = !prev[dbName];
        if (next && !loadedDbs[dbName]) load(dbName);
        return { ...prev, [dbName]: next };
      });
    },
    [setDb, loadedDbs, load]
  );

  // A DDL run from the SQL editor bumps refreshKey — reload the current db if
  // it's already loaded (otherwise leave it collapsed).
  useEffect(() => {
    const d = getCurrentDb() || db;
    if (refreshKey && d && loadedDbs[d]) load(d);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refreshKey]);

  const loadColumns = useCallback(async (dbName: string, table: string) => {
    const sel = firstSelect(await runSql(`DESCRIBE ${table}`, undefined, dbName));
    if (sel && !sel.error) {
      const idx = (name: string) => (sel.columns || []).indexOf(name);
      const ci = idx("column"), ti = idx("type"), pki = idx("primary_key"), ni = idx("nullable");
      setColsByDb((prev) => ({
        ...prev,
        [dbName]: {
          ...(prev[dbName] || {}),
          [table]: (sel.rows || []).map((r) => ({
            name: String(r[ci]),
            type: String(r[ti]),
            primaryKey: r[pki] === true,
            nullable: r[ni] === true,
          })),
        },
      }));
    }
  }, []);

  const toggleTable = useCallback(
    (dbName: string, table: string) => {
      const key = `${dbName}::${table}`;
      const next = !openTables[key];
      setOpenTables((p) => ({ ...p, [key]: next }));
      if (next && !colsByDb[dbName]?.[table]) loadColumns(dbName, table);
    },
    [openTables, colsByDb, loadColumns]
  );

  /** Run a DDL/DML statement scoped to a db, toast, then reload that db. */
  const runDdl = useCallback(
    async (sql: string, dbName: string, okMsg: string) => {
      try {
        const resp = (await runSql(sql, undefined, dbName)) as unknown as { ok?: boolean; error?: string };
        if (resp && resp.ok === false) {
          toast(resp.error || "statement failed", "error");
          return;
        }
        toast(okMsg, "success");
        setColsByDb((p) => ({ ...p, [dbName]: {} }));
        load(dbName);
      } catch (e) {
        toast(String(e), "error");
      }
    },
    [toast, load]
  );

  const openTableMenu = useCallback(
    (e: React.MouseEvent, dbName: string, table: string) => {
      e.preventDefault();
      e.stopPropagation();
      const scope = () => setDb(dbName); // point operations at this table's db
      const items: MenuItem[] = [
        { label: "Browse & edit data", onClick: () => { scope(); onBrowse(table); } },
        { label: "SELECT * (100 rows)", onClick: () => { scope(); onQuery(`SELECT * FROM ${table} LIMIT 100;`); } },
        { label: "SELECT COUNT(*)", onClick: () => { scope(); onQuery(`SELECT COUNT(*) FROM ${table};`); } },
        { label: "Describe columns", onClick: () => { scope(); onQuery(`DESCRIBE ${table};`); } },
        { label: "Show indexes", onClick: () => { scope(); onQuery(`SHOW INDEXES FROM ${table};`); } },
        { label: "", onClick: () => {}, separator: true },
        { label: "Edit table…", onClick: () => { scope(); setEditTable(table); } },
        { label: "Manage indexes…", onClick: () => { scope(); setIndexTable(table); } },
        { label: "Insert name into editor", onClick: () => onInsert(table) },
        { label: "", onClick: () => {}, separator: true },
        {
          label: "Truncate (delete all rows)",
          danger: true,
          onClick: () => setConfirm({ title: `Truncate ${table}?`, message: `This deletes every row in "${table}". The table itself stays.`, sql: `DELETE FROM ${table};`, db: dbName }),
        },
        {
          label: "Drop table",
          danger: true,
          onClick: () => setConfirm({ title: `Drop ${table}?`, message: `This permanently removes the table "${table}" and all its data.`, sql: `DROP TABLE ${table};`, db: dbName }),
        },
      ];
      setMenu({ x: e.clientX, y: e.clientY, items });
    },
    [onQuery, onInsert, onBrowse, setDb]
  );

  const currentDb = getCurrentDb() || db;

  return (
    <div className="schema-tree">
      <div className="schema-tree-head">
        <span>DATABASES</span>
        <div style={{ display: "flex", gap: 2 }}>
          <button className="schema-refresh" title="Import CSV / JSON" onClick={() => setShowImport(true)}>⇪</button>
          <button className="schema-refresh" title="New table (in current database)" onClick={() => setShowCreate(true)}>+</button>
          <button
            className="schema-refresh"
            title="Refresh"
            onClick={() => { reloadDatabases(); Object.keys(loadedDbs).filter((d) => loadedDbs[d]).forEach(load); }}
            disabled={!!loadingDb}
          >
            {loadingDb ? "…" : "⟳"}
          </button>
        </div>
      </div>

      <div className="schema-db-children" style={{ paddingLeft: 0 }}>
        {databases.length === 0 ? (
          <div className="schema-empty">No databases</div>
        ) : (
          <ul className="schema-list">
            {databases.map((dbName) => {
              const isOpen = !!openDbs[dbName];
              const tables = tablesByDb[dbName] || [];
              const procs = procsByDb[dbName] || [];
              const err = errorByDb[dbName];
              const loadingThis = loadingDb === dbName;
              return (
                <li key={dbName}>
                  <div
                    className={`schema-db-node${dbName === currentDb ? " open" : ""}`}
                    onClick={() => toggleDb(dbName)}
                    title="Click to expand — loads tables (SHOW TABLES)"
                  >
                    <span className="schema-caret">{isOpen ? "▾" : "▸"}</span>
                    <span className="schema-db-node-icon">🗄</span>
                    <span className="schema-db-node-name">{dbName}</span>
                    {loadingThis && <span className="schema-db-node-loading">…</span>}
                  </div>

                  {isOpen && (
                    <div style={{ paddingLeft: 10 }}>
                      {err ? (
                        <div className="schema-empty">{err}</div>
                      ) : loadingThis && !loadedDbs[dbName] ? (
                        <div className="schema-empty">Loading…</div>
                      ) : tables.length === 0 ? (
                        <div className="schema-empty">No tables</div>
                      ) : (
                        <ul className="schema-list">
                          {tables.map((t) => {
                            const tkey = `${dbName}::${t.name}`;
                            const tOpen = !!openTables[tkey];
                            const tcols = colsByDb[dbName]?.[t.name] || [];
                            return (
                              <li key={t.name}>
                                <div className="schema-table-row" onContextMenu={(e) => openTableMenu(e, dbName, t.name)}>
                                  <button className="schema-caret" onClick={() => toggleTable(dbName, t.name)} aria-label="expand">
                                    {tOpen ? "▾" : "▸"}
                                  </button>
                                  <span
                                    className="schema-table-name"
                                    title="Click to expand columns · double-click to browse & edit · right-click for menu"
                                    onClick={() => toggleTable(dbName, t.name)}
                                    onDoubleClick={() => { setDb(dbName); onBrowse(t.name); }}
                                  >
                                    {t.name}
                                  </span>
                                  {t.rows !== null && <span className="schema-rowcount">{t.rows}</span>}
                                </div>
                                {tOpen && (
                                  <ul className="schema-cols">
                                    {tcols.map((c) => (
                                      <li key={c.name} className="schema-col" title={`${c.type}${c.nullable ? " · nullable" : " · not null"}`} onClick={() => onInsert(c.name)}>
                                        {c.primaryKey && <span className="schema-pk" title="primary key">🔑</span>}
                                        <span className="schema-col-name">{c.name}</span>
                                        <span className="schema-col-type">{c.type}</span>
                                      </li>
                                    ))}
                                    {tcols.length === 0 && <li className="schema-col schema-empty">…</li>}
                                  </ul>
                                )}
                              </li>
                            );
                          })}
                        </ul>
                      )}

                      {procs.length > 0 && (
                        <div className="schema-procs">
                          <div className="schema-section-head">PROCEDURES ({procs.length})</div>
                          <ul className="schema-list" style={{ flex: "none" }}>
                            {procs.map((p) => (
                              <li key={p.name}>
                                <div className="schema-table-row">
                                  <span className="schema-caret" style={{ visibility: "hidden" }}>▸</span>
                                  <span
                                    className="schema-table-name"
                                    title="Click to view · double-click to insert CALL"
                                    onClick={() => { setDb(dbName); setViewProc(p); }}
                                    onDoubleClick={() => {
                                      setDb(dbName);
                                      const args = p.params.split(",").map((s) => s.trim()).filter(Boolean).map(() => "?").join(", ");
                                      onQuery(`CALL ${p.name}(${args});`);
                                    }}
                                  >
                                    ⚙ {p.name}
                                  </span>
                                </div>
                              </li>
                            ))}
                          </ul>
                        </div>
                      )}
                    </div>
                  )}
                </li>
              );
            })}
          </ul>
        )}
      </div>

      {menu && <ContextMenu x={menu.x} y={menu.y} items={menu.items} onClose={() => setMenu(null)} />}

      {confirm && (
        <ConfirmDialog
          title={confirm.title}
          message={confirm.message}
          confirmLabel="Yes, do it"
          danger
          onCancel={() => setConfirm(null)}
          onConfirm={() => {
            const { sql, db: cdb } = confirm;
            setConfirm(null);
            runDdl(sql, cdb, "Done");
          }}
        />
      )}

      {showCreate && (
        <CreateTableDialog
          onCancel={() => setShowCreate(false)}
          onCreate={async (sql) => {
            let ok = false;
            try {
              const resp = (await runSql(sql)) as unknown as { ok?: boolean; error?: string };
              if (resp && resp.ok === false) toast(resp.error || "create failed", "error");
              else { ok = true; toast("Table created", "success"); setShowCreate(false); reloadCurrent(); }
            } catch (e) { toast(String(e), "error"); }
            return ok;
          }}
        />
      )}

      {editTable && (
        <AlterTableDialog
          table={editTable}
          onCancel={() => setEditTable(null)}
          onApply={async (statements) => {
            for (let i = 0; i < statements.length; i++) {
              try {
                const resp = (await runSql(statements[i])) as unknown as { ok?: boolean; error?: string };
                if (resp && resp.ok === false) { toast(`Step ${i + 1}/${statements.length}: ${resp.error}`, "error"); reloadCurrent(); return false; }
              } catch (e) { toast(String(e), "error"); return false; }
            }
            toast(`Applied ${statements.length} change(s)`, "success");
            setEditTable(null);
            reloadCurrent();
            return true;
          }}
        />
      )}

      {indexTable && (
        <IndexDialog table={indexTable} onClose={() => setIndexTable(null)} onChanged={reloadCurrent} />
      )}

      {showImport && (
        <ImportDialog
          tables={(tablesByDb[currentDb] || []).map((t) => t.name)}
          onClose={() => setShowImport(false)}
          onDone={reloadCurrent}
        />
      )}

      {viewProc && (
        <ProcedureDialog
          proc={viewProc}
          onClose={() => setViewProc(null)}
          onInsert={onInsert}
          onDrop={(procName) => {
            setViewProc(null);
            setConfirm({ title: `Drop ${procName}?`, message: `This permanently removes the stored procedure "${procName}".`, sql: `DROP PROCEDURE ${procName};`, db: currentDb });
          }}
        />
      )}
    </div>
  );
}
