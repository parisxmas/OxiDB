import { useState, useCallback, useEffect } from "react";
import { runSql, getCurrentDb } from "../../api/tauri";
import { useDatabase } from "../../context/DatabaseContext";
import type { JsonValue } from "../../api/types";
import { ContextMenu } from "../common/ContextMenu";
import type { MenuItem } from "../common/ContextMenu";
import { ConfirmDialog } from "../common/ConfirmDialog";
import { PromptDialog } from "../common/PromptDialog";
import { CreateTableDialog } from "./CreateTableDialog";
import { AlterTableDialog } from "./AlterTableDialog";
import { IndexDialog } from "./IndexDialog";
import { ProcedureDialog, type ProcInfo } from "./ProcedureDialog";
import { ImportDialog } from "./ImportDialog";
import { NewProcedureDialog } from "./NewProcedureDialog";
import { NewSqlProcedureDialog } from "./NewSqlProcedureDialog";
import { formatSql } from "../../utils/formatSql";
import { IconSql, IconTable, IconKey, IconFunction } from "../layout/NavIcons";
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

interface IndexInfo {
  name: string;
  columns: string; // e.g. "customer_id" or "a, b"
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
  const { db, databases, setDb, reload: reloadDatabases, createDb, dropDb } = useDatabase();

  // Everything is keyed by database name so several databases can stay
  // expanded at once, each with its own tables/procedures.
  const [openDbs, setOpenDbs] = useState<Record<string, boolean>>({});
  const [loadedDbs, setLoadedDbs] = useState<Record<string, boolean>>({});
  const [loadingDb, setLoadingDb] = useState<string | null>(null);
  const [errorByDb, setErrorByDb] = useState<Record<string, string | null>>({});
  const [tablesByDb, setTablesByDb] = useState<Record<string, TableInfo[]>>({});
  const [procsByDb, setProcsByDb] = useState<Record<string, ProcInfo[]>>({});
  const [colsByDb, setColsByDb] = useState<Record<string, Record<string, ColumnInfo[]>>>({});
  const [idxByDb, setIdxByDb] = useState<Record<string, Record<string, IndexInfo[]>>>({});
  const [openTables, setOpenTables] = useState<Record<string, boolean>>({}); // `${db}::${table}`
  // Tables / Stored Procedures group nodes, keyed `${db}::tables|procs`.
  // Undefined = open (groups default to expanded).
  const [collapsedGroups, setCollapsedGroups] = useState<Record<string, boolean>>({});

  const [menu, setMenu] = useState<{ x: number; y: number; items: MenuItem[] } | null>(null);
  const [confirm, setConfirm] = useState<{ title: string; message: string; sql: string; db: string } | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const [editTable, setEditTable] = useState<string | null>(null);
  const [indexTable, setIndexTable] = useState<string | null>(null);
  const [viewProc, setViewProc] = useState<ProcInfo | null>(null);
  const [showImport, setShowImport] = useState(false);
  const [showNewDb, setShowNewDb] = useState(false);
  const [dropDbName, setDropDbName] = useState<string | null>(null);
  const [showNewProc, setShowNewProc] = useState(false);
  const [showNewSqlProc, setShowNewSqlProc] = useState(false);
  const [editProc, setEditProc] = useState<{ name: string; params: { name: string; type: string }[]; source: string } | null>(null);
  const [editSqlProc, setEditSqlProc] = useState<{ name: string; params: { name: string; type: string }[]; body: string } | null>(null);

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
      const pl = (ps?.columns || []).indexOf("language"); // older servers lack it
      setProcsByDb((p) => ({
        ...p,
        [dbName]: ps && !ps.error
          ? (ps.rows || []).map((r) => ({
              name: String(r[pi]),
              params: String(r[pp] ?? ""),
              definition: String(r[pd] ?? ""),
              language: pl >= 0 ? String(r[pl] ?? "sql") : "sql",
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

  /**
   * Open a procedure in the viewer with source fetched FRESH from the server,
   * not the (possibly stale) copy cached in the tree. Another client — or an
   * earlier deploy — may have changed the body since the tree last loaded;
   * editing a stale copy and saving would silently clobber the newer version.
   * Falls back to the cached ProcInfo if the refetch fails.
   */
  const openProc = useCallback(async (dbName: string, cached: ProcInfo) => {
    setDb(dbName);
    setViewProc(cached); // show immediately; upgrade to fresh below
    try {
      const ps = firstSelect(await runSql("SHOW PROCEDURES", undefined, dbName));
      if (!ps || ps.error) return;
      const pi = (ps.columns || []).indexOf("procedure");
      const pp = (ps.columns || []).indexOf("params");
      const pd = (ps.columns || []).indexOf("definition");
      const pl = (ps.columns || []).indexOf("language");
      const fresh = (ps.rows || [])
        .map((r) => ({
          name: String(r[pi]),
          params: String(r[pp] ?? ""),
          definition: String(r[pd] ?? ""),
          language: pl >= 0 ? String(r[pl] ?? "sql") : "sql",
        }))
        .find((p) => p.name === cached.name);
      if (fresh) {
        setViewProc(fresh);
        setProcsByDb((prev) => ({
          ...prev,
          [dbName]: (prev[dbName] || []).map((p) => (p.name === fresh.name ? fresh : p)),
        }));
      }
    } catch {
      /* keep the cached copy already shown */
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
    // Secondary indexes on this table (SHOW INDEXES → index, table, columns).
    const ix = firstSelect(await runSql(`SHOW INDEXES FROM ${table}`, undefined, dbName));
    if (ix && !ix.error) {
      const ni = (ix.columns || []).indexOf("index");
      const ci = (ix.columns || []).indexOf("columns");
      setIdxByDb((prev) => ({
        ...prev,
        [dbName]: {
          ...(prev[dbName] || {}),
          [table]: (ix.rows || []).map((r) => ({
            name: String(r[ni]),
            columns: String(r[ci]),
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

  const toggleGroup = useCallback((dbName: string, kind: "tables" | "procs") => {
    const key = `${dbName}::${kind}`;
    setCollapsedGroups((p) => ({ ...p, [key]: !p[key] }));
  }, []);

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

  const createDatabaseNamed = useCallback(
    async (name: string) => {
      setShowNewDb(false);
      try {
        await createDb(name);
        toast("Database created", "success");
      } catch (e) {
        toast(String(e), "error");
      }
    },
    [createDb, toast]
  );

  const openDbMenu = useCallback(
    (e: React.MouseEvent, dbName: string) => {
      e.preventDefault();
      e.stopPropagation();
      const builtin = dbName === "oxidb" || dbName === "postgres";
      const items: MenuItem[] = [
        { label: "New table…", onClick: () => { setDb(dbName); setShowCreate(true); } },
        { label: "New SQL procedure…", onClick: () => { setDb(dbName); setShowNewSqlProc(true); } },
        { label: "New Cobra procedure…", onClick: () => { setDb(dbName); setShowNewProc(true); } },
        { label: "Import CSV / JSON…", onClick: () => { setDb(dbName); setShowImport(true); } },
        { label: "Refresh", onClick: () => load(dbName) },
        { label: "", onClick: () => {}, separator: true },
        {
          label: builtin ? "Drop database (built-in)" : "Drop database",
          danger: true,
          onClick: () => {
            if (builtin) { toast("The built-in oxidb/postgres database can't be dropped", "error"); return; }
            setDropDbName(dbName);
          },
        },
      ];
      setMenu({ x: e.clientX, y: e.clientY, items });
    },
    [setDb, load, dropDb, toast]
  );

  const currentDb = getCurrentDb() || db;

  return (
    <div className="schema-tree">
      <div className="schema-tree-head">
        <span>DATABASES</span>
        <div style={{ display: "flex", gap: 2 }}>
          <button className="schema-refresh" title="New database" onClick={() => setShowNewDb(true)}>+</button>
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
                    onContextMenu={(e) => openDbMenu(e, dbName)}
                    title="Click to expand · right-click for menu (new table, drop database…)"
                  >
                    <span className="schema-caret">{isOpen ? "▾" : "▸"}</span>
                    <span className="schema-db-node-icon"><IconSql size={14} /></span>
                    <span className="schema-db-node-name">{dbName}</span>
                    {loadingThis && <span className="schema-db-node-loading">…</span>}
                  </div>

                  {isOpen && (
                    <div style={{ paddingLeft: 10 }}>
                      {err ? (
                        <div className="schema-empty">{err}</div>
                      ) : loadingThis && !loadedDbs[dbName] ? (
                        <div className="schema-empty">Loading…</div>
                      ) : (
                        <>
                          {/* ── Tables group ── */}
                          {(() => {
                            const tablesOpen = !collapsedGroups[`${dbName}::tables`];
                            return (
                              <div>
                                <div
                                  className="schema-group-node"
                                  onClick={() => toggleGroup(dbName, "tables")}
                                  title="Tables"
                                >
                                  <span className="schema-caret">{tablesOpen ? "▾" : "▸"}</span>
                                  <span className="schema-group-name">Tables ({tables.length})</span>
                                </div>
                                {tablesOpen &&
                                  (tables.length === 0 ? (
                                    <div className="schema-empty" style={{ paddingLeft: 18 }}>No tables</div>
                                  ) : (
                                    <ul className="schema-list" style={{ paddingLeft: 10 }}>
                                      {tables.map((t) => {
                                        const tkey = `${dbName}::${t.name}`;
                                        const tOpen = !!openTables[tkey];
                                        const tcols = colsByDb[dbName]?.[t.name] || [];
                                        const tidx = idxByDb[dbName]?.[t.name] || [];
                                        return (
                                          <li key={t.name}>
                                            <div className="schema-table-row" onContextMenu={(e) => openTableMenu(e, dbName, t.name)}>
                                              <button className="schema-caret" onClick={() => toggleTable(dbName, t.name)} aria-label="expand">
                                                {tOpen ? "▾" : "▸"}
                                              </button>
                                              <span className="schema-row-icon"><IconTable size={13} /></span>
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
                                                    {c.primaryKey && <span className="schema-pk" title="primary key"><IconKey size={12} /></span>}
                                                    <span className="schema-col-name">{c.name}</span>
                                                    <span className="schema-col-type">{c.type}</span>
                                                  </li>
                                                ))}
                                                {tcols.length === 0 && <li className="schema-col schema-empty">…</li>}
                                                {tidx.length > 0 && (
                                                  <li className="schema-col schema-idx-head">Indexes</li>
                                                )}
                                                {tidx.map((ix) => (
                                                  <li
                                                    key={ix.name}
                                                    className="schema-col schema-idx"
                                                    title={`index on (${ix.columns})`}
                                                    onClick={() => onInsert(ix.columns)}
                                                  >
                                                    <span className="schema-idx-icon"><IconKey size={11} /></span>
                                                    <span className="schema-col-name">{ix.name}</span>
                                                    <span className="schema-col-type">{ix.columns}</span>
                                                  </li>
                                                ))}
                                              </ul>
                                            )}
                                          </li>
                                        );
                                      })}
                                    </ul>
                                  ))}
                              </div>
                            );
                          })()}

                          {/* ── Stored Procedures group ── */}
                          {(() => {
                            const procsOpen = !collapsedGroups[`${dbName}::procs`];
                            return (
                              <div style={{ marginTop: 4 }}>
                                <div
                                  className="schema-group-node"
                                  onClick={() => toggleGroup(dbName, "procs")}
                                  title="Stored Procedures"
                                >
                                  <span className="schema-caret">{procsOpen ? "▾" : "▸"}</span>
                                  <span className="schema-group-name">Stored Procedures ({procs.length})</span>
                                </div>
                                {procsOpen &&
                                  (procs.length === 0 ? (
                                    <div className="schema-empty" style={{ paddingLeft: 18 }}>No procedures</div>
                                  ) : (
                                    <ul className="schema-list" style={{ paddingLeft: 10, flex: "none" }}>
                                      {procs.map((p) => (
                                        <li key={p.name}>
                                          <div className="schema-table-row">
                                            <span className="schema-caret" style={{ visibility: "hidden" }}>▸</span>
                                            <span className="schema-row-icon"><IconFunction size={13} /></span>
                                            <span
                                              className="schema-table-name"
                                              title="Click to view · double-click to insert CALL"
                                              onClick={() => openProc(dbName, p)}
                                              onDoubleClick={() => {
                                                setDb(dbName);
                                                const args = p.params.split(",").map((s) => s.trim()).filter(Boolean).map(() => "?").join(", ");
                                                onQuery(`CALL ${p.name}(${args});`);
                                              }}
                                            >
                                              {p.name}
                                            </span>
                                            {p.language === "cobra" && (
                                              <span className="schema-lang-badge">cobra</span>
                                            )}
                                          </div>
                                        </li>
                                      ))}
                                    </ul>
                                  ))}
                              </div>
                            );
                          })()}
                        </>
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

      {showNewDb && (
        <PromptDialog
          title="New database"
          label="Database name"
          placeholder="my_database"
          confirmLabel="Create"
          onConfirm={createDatabaseNamed}
          onCancel={() => setShowNewDb(false)}
        />
      )}

      {dropDbName && (
        <ConfirmDialog
          title={`Drop ${dropDbName}?`}
          message={`This permanently removes the database "${dropDbName}" and everything in it.`}
          confirmLabel="Drop database"
          danger
          onCancel={() => setDropDbName(null)}
          onConfirm={() => {
            const name = dropDbName;
            setDropDbName(null);
            dropDb(name).then(() => toast("Database dropped", "success")).catch((err) => toast(String(err), "error"));
          }}
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

      {showNewProc && (
        <NewProcedureDialog
          onClose={() => setShowNewProc(false)}
          onCreated={reloadCurrent}
        />
      )}

      {editProc && (
        <NewProcedureDialog
          initial={editProc}
          onClose={() => setEditProc(null)}
          onCreated={reloadCurrent}
        />
      )}

      {showNewSqlProc && (
        <NewSqlProcedureDialog
          onClose={() => setShowNewSqlProc(false)}
          onCreated={reloadCurrent}
        />
      )}

      {editSqlProc && (
        <NewSqlProcedureDialog
          initial={editSqlProc}
          onClose={() => setEditSqlProc(null)}
          onCreated={reloadCurrent}
        />
      )}

      {viewProc && (
        <ProcedureDialog
          proc={viewProc}
          onClose={() => setViewProc(null)}
          onInsert={onInsert}
          onEdit={(p) => {
            // params string "a INT, b TEXT" → typed rows
            const params = p.params
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean)
              .map((s) => {
                const [n, ...ty] = s.split(/\s+/);
                return { name: n, type: (ty.join(" ") || "INT").toUpperCase() };
              });
            setViewProc(null);
            if (p.language === "cobra") {
              const source = p.definition.startsWith("<cobra bytecode") ? "" : p.definition;
              setEditProc({ name: p.name, params, source });
            } else {
              // SHOW returns the body with parameters rewritten to $1..$N;
              // turn them back into names so the edit form + re-CREATE parse
              // (the engine re-rewrites names → $N on save). The body is stored
              // on one line, so pretty-print it for editing.
              const named = p.definition.replace(
                /\$(\d+)/g,
                (m, n) => params[Number(n) - 1]?.name ?? m
              );
              setEditSqlProc({ name: p.name, params, body: formatSql(named) });
            }
          }}
          onDrop={(procName) => {
            setViewProc(null);
            setConfirm({ title: `Drop ${procName}?`, message: `This permanently removes the stored procedure "${procName}".`, sql: `DROP PROCEDURE ${procName};`, db: currentDb });
          }}
        />
      )}
    </div>
  );
}
