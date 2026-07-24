import { useEffect, useState } from "react";
import {
  type SqlResult,
  type SqlTable,
  type SqlColumn,
  listSqlTables,
  describeSqlTable,
  selectSqlRows,
  parseSchema,
  insertSqlRow,
  updateSqlRow,
  deleteSqlRow,
  addSqlColumn,
  dropSqlColumn,
  renameSqlColumn,
  alterSqlColumnType,
  dropSqlTable,
} from "./dataApi.ts";

export function SqlTables({ projectRef, apiKey }: { projectRef: string; apiKey: string }) {
  const [tables, setTables] = useState<SqlTable[]>([]);
  const [active, setActive] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  async function loadTables() {
    setLoading(true);
    try {
      const t = await listSqlTables(projectRef, apiKey);
      setTables(t);
      setError(null);
      if (t.length && (!active || !t.some((x) => x.name === active))) setActive(t[0].name);
      if (!t.length) setActive(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadTables();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectRef]);

  return (
    <div className="browser">
      <aside className="collections">
        <div className="side-title">SQL Tables</div>
        {loading && tables.length === 0 && <div className="muted small">loading…</div>}
        {!loading && tables.length === 0 && <div className="muted small">no tables</div>}
        {tables.map((t) => (
          <button
            key={t.name}
            className={t.name === active ? "coll active" : "coll"}
            onClick={() => setActive(t.name)}
            title={`${t.rows} rows`}
          >
            <span className="ellip">{t.name}</span>
            <span className="rowcount">{t.rows}</span>
          </button>
        ))}
        <button className="ghost small" style={{ marginTop: 8 }} onClick={loadTables}>
          Refresh
        </button>
      </aside>

      <div className="rows-pane">
        {error && <div className="error">{error}</div>}
        {active ? (
          <TableView
            projectRef={projectRef}
            apiKey={apiKey}
            table={active}
            onMutate={loadTables}
            onDropped={() => {
              setActive(null);
              loadTables();
            }}
          />
        ) : (
          !loading && (
            <p className="muted">
              No SQL tables yet. Create one in the <strong>SQL</strong> tab.
            </p>
          )
        )}
      </div>
    </div>
  );
}

function TableView({
  projectRef,
  apiKey,
  table,
  onMutate,
  onDropped,
}: {
  projectRef: string;
  apiKey: string;
  table: string;
  onMutate: () => void;
  onDropped: () => void;
}) {
  const [view, setView] = useState<"rows" | "schema">("rows");
  const [rows, setRows] = useState<SqlResult | null>(null);
  const [schema, setSchema] = useState<SqlColumn[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  // The row being edited (its cell values, aligned with `rows.columns`), or
  // "new" for the insert form, or null when no form is open.
  const [editing, setEditing] = useState<unknown[] | "new" | null>(null);
  const [busy, setBusy] = useState(false);

  const pk = schema.find((c) => c.primaryKey);

  async function refresh() {
    setLoading(true);
    setError(null);
    try {
      const [r, s] = await Promise.all([
        selectSqlRows(projectRef, apiKey, table, 100),
        describeSqlTable(projectRef, apiKey, table),
      ]);
      setRows(r);
      setSchema(parseSchema(s));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    setEditing(null);
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [table]);

  async function mutate(op: () => Promise<unknown>) {
    setBusy(true);
    setError(null);
    try {
      await op();
      setEditing(null);
      await refresh();
      onMutate();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  function del(row: unknown[]) {
    if (!pk) return;
    const pkIdx = (rows?.columns ?? []).indexOf(pk.name);
    if (pkIdx < 0) return;
    if (!confirm("Delete this row?")) return;
    mutate(() => deleteSqlRow(projectRef, apiKey, table, pk.name, row[pkIdx]));
  }

  function dropTable() {
    if (!confirm(`Drop table "${table}" and all its rows? This cannot be undone.`)) return;
    setBusy(true);
    setError(null);
    dropSqlTable(projectRef, apiKey, table)
      .then(onDropped)
      .catch((e) => {
        setError(e instanceof Error ? e.message : String(e));
        setBusy(false);
      });
  }

  const cols = rows?.columns ?? [];
  const dataRows = (rows?.rows ?? []) as unknown[][];

  return (
    <div>
      <div className="row between">
        <h3 style={{ margin: "4px 0" }}>{table}</h3>
        <div className="row" style={{ gap: 8 }}>
          <div className="tabs">
            <button className={view === "rows" ? "tab active" : "tab"} onClick={() => setView("rows")}>
              Rows
            </button>
            <button className={view === "schema" ? "tab active" : "tab"} onClick={() => setView("schema")}>
              Schema
            </button>
          </div>
          <button className="ghost" onClick={refresh}>
            Refresh
          </button>
          {view === "rows" ? (
            <button
              className="primary"
              disabled={busy || schema.length === 0}
              onClick={() => setEditing((e) => (e === "new" ? null : "new"))}
            >
              {editing === "new" ? "Cancel" : "Add row"}
            </button>
          ) : (
            <button className="ghost danger" disabled={busy} onClick={dropTable}>
              Drop table
            </button>
          )}
        </div>
      </div>

      {error && <div className="error">{error}</div>}

      {editing !== null && view === "rows" && (
        <RowForm
          key={editing === "new" ? "new" : `edit:${fmt(editing[(rows?.columns ?? []).indexOf(pk?.name ?? "")])}`}
          schema={schema}
          columns={cols}
          initial={editing === "new" ? null : editing}
          pk={pk}
          busy={busy}
          onSubmit={(colNames, values, pkVal) =>
            mutate(() =>
              editing === "new"
                ? insertSqlRow(projectRef, apiKey, table, colNames, values)
                : updateSqlRow(projectRef, apiKey, table, colNames, values, pk!.name, pkVal),
            )
          }
        />
      )}

      {loading ? (
        <p className="muted">Loading…</p>
      ) : view === "rows" ? (
        dataRows.length === 0 ? (
          <p className="muted">No rows. Add one above.</p>
        ) : (
          <>
            <div className="table-wrap">
              <table className="grid-table">
                <thead>
                  <tr>
                    {cols.map((c, i) => (
                      <th key={i}>{c}</th>
                    ))}
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {dataRows.map((row, ri) => (
                    <tr key={ri}>
                      {row.map((v, ci) => (
                        <td key={ci}>{fmt(v)}</td>
                      ))}
                      <td className="rowdel">
                        {pk ? (
                          <span className="row" style={{ gap: 4, justifyContent: "center" }}>
                            <button
                              className="ghost small"
                              disabled={busy}
                              title="Edit row"
                              onClick={() => setEditing(row)}
                            >
                              ✎
                            </button>
                            <button
                              className="ghost danger small"
                              disabled={busy}
                              title="Delete row"
                              onClick={() => del(row)}
                            >
                              ✕
                            </button>
                          </span>
                        ) : null}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="row between">
              <div className="muted small">
                {dataRows.length} row{dataRows.length === 1 ? "" : "s"}
              </div>
              {!pk && (
                <div className="muted small">no primary key — row editing disabled</div>
              )}
            </div>
          </>
        )
      ) : (
        <SchemaView
          projectRef={projectRef}
          apiKey={apiKey}
          table={table}
          schema={schema}
          busy={busy}
          onMutate={mutate}
        />
      )}
    </div>
  );
}

/** Insert/edit form: one input per column, typed by the declared column type. */
function RowForm({
  schema,
  columns,
  initial,
  pk,
  busy,
  onSubmit,
}: {
  schema: SqlColumn[];
  columns: string[];
  initial: unknown[] | null; // null = insert
  pk: SqlColumn | undefined;
  busy: boolean;
  onSubmit: (cols: string[], values: unknown[], pkVal: unknown) => void;
}) {
  const isEdit = initial !== null;
  // Draft keyed by column name; NULL and omitted render as "".
  const [draft, setDraft] = useState<Record<string, string>>(() => {
    const d: Record<string, string> = {};
    for (const c of schema) {
      const idx = columns.indexOf(c.name);
      const v = initial && idx >= 0 ? initial[idx] : null;
      d[c.name] = v === null || v === undefined ? "" : String(v);
    }
    return d;
  });
  const [err, setErr] = useState<string | null>(null);

  const pkVal = (() => {
    if (!isEdit || !pk) return undefined;
    const idx = columns.indexOf(pk.name);
    return idx >= 0 ? initial![idx] : undefined;
  })();

  function submit() {
    setErr(null);
    const colNames: string[] = [];
    const values: unknown[] = [];
    try {
      for (const c of schema) {
        const raw = draft[c.name] ?? "";
        if (isEdit) {
          if (pk && c.name === pk.name) continue; // key is the address, not a change
          colNames.push(c.name);
          values.push(toParam(raw, c));
        } else {
          // Insert: blank fields are omitted so defaults / auto-increment apply.
          if (raw.trim() === "") continue;
          colNames.push(c.name);
          values.push(toParam(raw, c));
        }
      }
      if (colNames.length === 0) throw new Error("enter at least one value");
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
      return;
    }
    onSubmit(colNames, values, pkVal);
  }

  return (
    <div className="card add-row" style={{ marginBottom: 12 }}>
      <div className="row between" style={{ marginBottom: 8 }}>
        <strong>{isEdit ? `Edit row (${pk?.name} = ${fmt(pkVal)})` : "New row"}</strong>
        {!isEdit && <span className="muted small">blank = default / NULL</span>}
        {isEdit && <span className="muted small">blank = NULL</span>}
      </div>
      <div className="row" style={{ gap: 10, flexWrap: "wrap", alignItems: "flex-end" }}>
        {schema.map((c) => {
          const disabled = isEdit && pk !== undefined && c.name === pk.name;
          const base = baseType(c.type);
          return (
            <label key={c.name} className="small" style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              <span>
                {c.name} <span className="muted">({c.type.toLowerCase()}{c.primaryKey ? " · pk" : ""}
                {c.autoIncrement ? " · auto" : ""})</span>
              </span>
              {base === "BOOL" ? (
                <select
                  value={draft[c.name] ?? ""}
                  disabled={disabled}
                  onChange={(e) => setDraft((d) => ({ ...d, [c.name]: e.target.value }))}
                  style={{ width: 130 }}
                >
                  <option value="">{c.nullable ? "(null)" : ""}</option>
                  <option value="true">true</option>
                  <option value="false">false</option>
                </select>
              ) : (
                <input
                  style={{ width: base === "TEXT" ? 200 : 140 }}
                  value={draft[c.name] ?? ""}
                  disabled={disabled}
                  spellCheck={false}
                  placeholder={c.autoIncrement ? "auto" : placeholderFor(base)}
                  onChange={(e) => setDraft((d) => ({ ...d, [c.name]: e.target.value }))}
                />
              )}
            </label>
          );
        })}
        <button className="primary" disabled={busy} onClick={submit}>
          {isEdit ? "Save" : "Insert"}
        </button>
      </div>
      {err && <div className="error small" style={{ marginTop: 8 }}>{err}</div>}
    </div>
  );
}

/** Schema grid with column add/rename/drop (instant, metadata-only DDL). */
function SchemaView({
  projectRef,
  apiKey,
  table,
  schema,
  busy,
  onMutate,
}: {
  projectRef: string;
  apiKey: string;
  table: string;
  schema: SqlColumn[];
  busy: boolean;
  onMutate: (op: () => Promise<unknown>) => void;
}) {
  const [name, setName] = useState("");
  const [type, setType] = useState("TEXT");
  const [len, setLen] = useState("80");
  // Column being edited, or null. Drafts hold the edited name/type.
  const [editCol, setEditCol] = useState<string | null>(null);
  const [eName, setEName] = useState("");
  const [eType, setEType] = useState("TEXT");
  const [eLen, setELen] = useState("80");

  function add() {
    const n = name.trim();
    if (!n) return;
    const ty = type === "VARCHAR" ? `VARCHAR(${Math.max(1, Number(len) | 0)})` : type;
    onMutate(() => addSqlColumn(projectRef, apiKey, table, n, ty));
    setName("");
  }

  function startEdit(c: SqlColumn) {
    setEditCol(c.name);
    setEName(c.name);
    const m = /^([A-Za-z]+)(?:\((\d+)\))?$/.exec(c.type);
    setEType(m?.[1]?.toUpperCase() ?? "TEXT");
    setELen(m?.[2] ?? "80");
  }

  function saveEdit(c: SqlColumn) {
    const newName = eName.trim();
    if (!newName) return;
    const newType = eType === "VARCHAR" ? `VARCHAR(${Math.max(1, Number(eLen) | 0)})` : eType;
    const typeChanged = newType.toUpperCase() !== c.type.toUpperCase();
    const nameChanged = newName !== c.name;
    if (!typeChanged && !nameChanged) {
      setEditCol(null);
      return;
    }
    onMutate(async () => {
      // Type first (under the old name), then rename.
      if (typeChanged) await alterSqlColumnType(projectRef, apiKey, table, c.name, newType);
      if (nameChanged) await renameSqlColumn(projectRef, apiKey, table, c.name, newName);
      setEditCol(null);
    });
  }

  function drop(col: string) {
    if (!confirm(`Drop column "${col}"? Its values become unreachable.`)) return;
    onMutate(() => dropSqlColumn(projectRef, apiKey, table, col));
  }

  return (
    <div>
      {schema.length === 0 ? (
        <p className="muted">No columns.</p>
      ) : (
        <div className="table-wrap">
          <table className="grid-table">
            <thead>
              <tr>
                <th>column</th>
                <th>type</th>
                <th>nullable</th>
                <th>primary_key</th>
                <th>auto_increment</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {schema.map((c) =>
                editCol === c.name ? (
                  <tr key={c.name}>
                    <td>
                      <input
                        style={{ width: 140, padding: "5px 8px", fontSize: 13 }}
                        value={eName}
                        spellCheck={false}
                        onChange={(e) => setEName(e.target.value)}
                      />
                    </td>
                    <td>
                      <span className="row" style={{ gap: 6 }}>
                        <select
                          style={{ padding: "5px 8px", fontSize: 13 }}
                          value={eType}
                          disabled={c.primaryKey || c.autoIncrement}
                          title={
                            c.primaryKey || c.autoIncrement
                              ? "key columns keep their type"
                              : "changing the type casts existing values"
                          }
                          onChange={(e) => setEType(e.target.value)}
                        >
                          {COLUMN_TYPES.map((t) => (
                            <option key={t}>{t}</option>
                          ))}
                        </select>
                        {eType === "VARCHAR" && (
                          <input
                            style={{ width: 70, padding: "5px 8px", fontSize: 13 }}
                            type="number"
                            min={1}
                            value={eLen}
                            onChange={(e) => setELen(e.target.value)}
                            title="max length (characters)"
                          />
                        )}
                      </span>
                    </td>
                    <td>{String(c.nullable)}</td>
                    <td>{String(c.primaryKey)}</td>
                    <td>{String(c.autoIncrement)}</td>
                    <td className="rowdel">
                      <span className="row" style={{ gap: 4, justifyContent: "center" }}>
                        <button className="primary small" disabled={busy} onClick={() => saveEdit(c)}>
                          Save
                        </button>
                        <button className="ghost small" disabled={busy} onClick={() => setEditCol(null)}>
                          Cancel
                        </button>
                      </span>
                    </td>
                  </tr>
                ) : (
                  <tr key={c.name}>
                    <td>{c.name}</td>
                    <td>{c.type}</td>
                    <td>{String(c.nullable)}</td>
                    <td>{String(c.primaryKey)}</td>
                    <td>{String(c.autoIncrement)}</td>
                    <td className="rowdel">
                      <span className="row" style={{ gap: 4, justifyContent: "center" }}>
                        <button
                          className="ghost small"
                          disabled={busy}
                          title="Edit column (name / type)"
                          onClick={() => startEdit(c)}
                        >
                          ✎
                        </button>
                        <button
                          className="ghost danger small"
                          disabled={busy || c.primaryKey}
                          title={c.primaryKey ? "cannot drop the primary key" : "Drop column"}
                          onClick={() => drop(c.name)}
                        >
                          ✕
                        </button>
                      </span>
                    </td>
                  </tr>
                ),
              )}
            </tbody>
          </table>
        </div>
      )}

      <div className="row" style={{ gap: 8, flexWrap: "wrap", alignItems: "center", marginTop: 12 }}>
        <input
          style={{ minWidth: 160 }}
          placeholder="new column"
          value={name}
          spellCheck={false}
          onChange={(e) => setName(e.target.value)}
        />
        <select value={type} onChange={(e) => setType(e.target.value)}>
          {COLUMN_TYPES.map((t) => (
            <option key={t}>{t}</option>
          ))}
        </select>
        {type === "VARCHAR" && (
          <input
            style={{ width: 90 }}
            type="number"
            min={1}
            value={len}
            onChange={(e) => setLen(e.target.value)}
            title="max length (characters)"
          />
        )}
        <button className="primary" disabled={busy || !name.trim()} onClick={add}>
          Add column
        </button>
        <span className="muted small">new columns are nullable; instant (no table rewrite)</span>
      </div>
    </div>
  );
}

export function ResultGrid({ result, emptyText }: { result: SqlResult | null; emptyText: string }) {
  const cols = result?.columns ?? [];
  const rows = result?.rows ?? [];
  if (rows.length === 0) return <p className="muted">{emptyText}</p>;
  return (
    <>
      <div className="table-wrap">
        <table className="grid-table">
          <thead>
            <tr>
              {cols.map((c, i) => (
                <th key={i}>{c}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, ri) => (
              <tr key={ri}>
                {(row as unknown[]).map((v, ci) => (
                  <td key={ci}>{fmt(v)}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="muted small">
        {rows.length} row{rows.length === 1 ? "" : "s"}
      </div>
    </>
  );
}

/** Types the schema editor offers (VARCHAR takes a length input). */
const COLUMN_TYPES = ["TEXT", "VARCHAR", "INT", "DOUBLE", "DECIMAL", "BOOL", "TIMESTAMP", "BLOB"];

/** The bare type keyword: "VARCHAR(80)" → "VARCHAR" → mapped to its storage class. */
function baseType(t: string): string {
  const kw = t.replace(/\(.*/, "").toUpperCase();
  if (["INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"].includes(kw)) return "INT";
  if (["DOUBLE", "FLOAT", "REAL"].includes(kw)) return "DOUBLE";
  if (["DECIMAL", "NUMERIC", "DEC"].includes(kw)) return "DECIMAL";
  if (["BOOL", "BOOLEAN"].includes(kw)) return "BOOL";
  if (["TIMESTAMP", "DATETIME"].includes(kw)) return "TIMESTAMP";
  if (["BLOB", "BYTEA", "BINARY", "VARBINARY"].includes(kw)) return "BLOB";
  return "TEXT";
}

function placeholderFor(base: string): string {
  switch (base) {
    case "INT":
      return "42";
    case "DOUBLE":
    case "DECIMAL":
      return "3.14";
    case "TIMESTAMP":
      return "epoch ms or ISO date";
    case "BLOB":
      return "base64";
    default:
      return "";
  }
}

/** Coerce a form string to a JSON param matching the column's declared type. */
function toParam(raw: string, col: SqlColumn): unknown {
  const s = raw.trim();
  if (s === "") return null;
  switch (baseType(col.type)) {
    case "INT": {
      const n = Number(s);
      if (!Number.isInteger(n)) throw new Error(`${col.name}: not an integer`);
      return n;
    }
    case "DOUBLE": {
      const n = Number(s);
      if (!Number.isFinite(n)) throw new Error(`${col.name}: not a number`);
      return n;
    }
    // DECIMAL travels as text so the engine parses it exactly (no float detour).
    case "DECIMAL": {
      if (!Number.isFinite(Number(s))) throw new Error(`${col.name}: not a number`);
      return s;
    }
    case "BOOL": {
      if (/^(true|t|1)$/i.test(s)) return true;
      if (/^(false|f|0)$/i.test(s)) return false;
      throw new Error(`${col.name}: true or false`);
    }
    case "TIMESTAMP": {
      if (/^-?\d+$/.test(s)) return Number(s); // epoch ms
      const t = Date.parse(s);
      if (Number.isNaN(t)) throw new Error(`${col.name}: epoch ms or an ISO date`);
      return t;
    }
    default:
      return raw; // TEXT keeps the raw (untrimmed) string; BLOB is base64 text
  }
}

function fmt(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}
