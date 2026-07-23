import { useEffect, useState } from "react";
import { type Row, listCollections, findRows, insertRow, deleteWhere } from "./dataApi.ts";

export function DataBrowser({ projectRef, apiKey }: { projectRef: string; apiKey: string }) {
  const [collections, setCollections] = useState<string[]>([]);
  const [active, setActive] = useState<string | null>(null);
  const [newCol, setNewCol] = useState("");
  const [error, setError] = useState<string | null>(null);

  async function loadCollections(select?: string) {
    try {
      const cols = await listCollections(projectRef, apiKey);
      setCollections(cols);
      setError(null);
      if (select) setActive(select);
      else if (!active && cols.length) setActive(cols[0]);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  useEffect(() => {
    loadCollections();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectRef]);

  return (
    <div className="browser">
      <aside className="collections">
        <div className="side-title">Collections</div>
        {collections.length === 0 && <div className="muted small">none yet</div>}
        {collections.map((c) => (
          <button
            key={c}
            className={c === active ? "coll active" : "coll"}
            onClick={() => setActive(c)}
          >
            {c}
          </button>
        ))}
        <form
          className="newcoll"
          onSubmit={(e) => {
            e.preventDefault();
            const n = newCol.trim();
            if (n) {
              setActive(n);
              setNewCol("");
              if (!collections.includes(n)) setCollections((cs) => [...cs, n]);
            }
          }}
        >
          <input placeholder="new collection" value={newCol} onChange={(e) => setNewCol(e.target.value)} />
        </form>
      </aside>

      <div className="rows-pane">
        {error && <div className="error">{error}</div>}
        {active ? (
          <RowsTable
            projectRef={projectRef}
            apiKey={apiKey}
            collection={active}
            onFirstInsert={() => loadCollections(active)}
          />
        ) : (
          <p className="muted">Select or create a collection.</p>
        )}
      </div>
    </div>
  );
}

function RowsTable({
  projectRef,
  apiKey,
  collection,
  onFirstInsert,
}: {
  projectRef: string;
  apiKey: string;
  collection: string;
  onFirstInsert: () => void;
}) {
  const [rows, setRows] = useState<Row[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const [draft, setDraft] = useState('{\n  "name": "example"\n}');

  async function refresh() {
    setLoading(true);
    try {
      setRows(await findRows(projectRef, collection, apiKey, 100));
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [collection]);

  async function add() {
    let doc: unknown;
    try {
      doc = JSON.parse(draft);
    } catch {
      setError("row must be valid JSON");
      return;
    }
    try {
      await insertRow(projectRef, collection, apiKey, doc);
      setAdding(false);
      const wasEmpty = rows.length === 0;
      await refresh();
      if (wasEmpty) onFirstInsert();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  async function del(row: Row) {
    const id = row["_id"];
    if (id === undefined) return;
    if (!confirm("Delete this row?")) return;
    try {
      await deleteWhere(projectRef, collection, apiKey, `_id=eq.${encodeURIComponent(String(id))}`);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  const columns = Array.from(
    rows.reduce((set, r) => {
      Object.keys(r).forEach((k) => set.add(k));
      return set;
    }, new Set<string>()),
  );

  return (
    <div>
      <div className="row between">
        <h3 style={{ margin: "4px 0" }}>{collection}</h3>
        <div className="row" style={{ gap: 8 }}>
          <button className="ghost" onClick={refresh}>
            Refresh
          </button>
          <button className="primary" onClick={() => setAdding((a) => !a)}>
            {adding ? "Cancel" : "Add row"}
          </button>
        </div>
      </div>

      {adding && (
        <div className="card add-row">
          <textarea value={draft} onChange={(e) => setDraft(e.target.value)} rows={5} spellCheck={false} />
          <div className="row" style={{ justifyContent: "flex-end", marginTop: 8 }}>
            <button className="primary" onClick={add}>
              Insert
            </button>
          </div>
        </div>
      )}

      {error && <div className="error">{error}</div>}
      {loading ? (
        <p className="muted">Loading…</p>
      ) : rows.length === 0 ? (
        <p className="muted">No rows. Add one above.</p>
      ) : (
        <div className="table-wrap">
          <table className="grid-table">
            <thead>
              <tr>
                {columns.map((c) => (
                  <th key={c}>{c}</th>
                ))}
                <th />
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={i}>
                  {columns.map((c) => (
                    <td key={c}>{cell(r[c])}</td>
                  ))}
                  <td className="rowdel">
                    {r["_id"] !== undefined && (
                      <button className="ghost danger small" onClick={() => del(r)}>
                        ✕
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function cell(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}
