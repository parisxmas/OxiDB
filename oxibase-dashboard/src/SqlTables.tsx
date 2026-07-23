import { useEffect, useState } from "react";
import {
  type SqlResult,
  type SqlTable,
  listSqlTables,
  describeSqlTable,
  selectSqlRows,
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
          <TableView projectRef={projectRef} apiKey={apiKey} table={active} />
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

function TableView({ projectRef, apiKey, table }: { projectRef: string; apiKey: string; table: string }) {
  const [view, setView] = useState<"rows" | "schema">("rows");
  const [rows, setRows] = useState<SqlResult | null>(null);
  const [schema, setSchema] = useState<SqlResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  async function refresh() {
    setLoading(true);
    setError(null);
    try {
      const [r, s] = await Promise.all([
        selectSqlRows(projectRef, apiKey, table, 100),
        describeSqlTable(projectRef, apiKey, table),
      ]);
      setRows(r);
      setSchema(s);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [table]);

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
        </div>
      </div>

      {error && <div className="error">{error}</div>}
      {loading ? (
        <p className="muted">Loading…</p>
      ) : view === "rows" ? (
        <ResultGrid result={rows} emptyText="No rows." />
      ) : (
        <ResultGrid result={schema} emptyText="No columns." />
      )}
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

function fmt(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}
