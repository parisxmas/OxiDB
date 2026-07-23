import { useEffect, useState } from "react";
import {
  type Row,
  type IndexInfo,
  type IndexSpec,
  listCollections,
  findRows,
  insertRow,
  deleteWhere,
  listIndexes,
  createIndex,
  dropIndex,
} from "./dataApi.ts";

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
  const [showIdx, setShowIdx] = useState(false);
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
          <button className={showIdx ? "ghost active" : "ghost"} onClick={() => setShowIdx((s) => !s)}>
            Indexes
          </button>
          <button className="primary" onClick={() => setAdding((a) => !a)}>
            {adding ? "Cancel" : "Add row"}
          </button>
        </div>
      </div>

      {showIdx && <IndexPanel projectRef={projectRef} apiKey={apiKey} collection={collection} />}

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

function IndexPanel({
  projectRef,
  apiKey,
  collection,
}: {
  projectRef: string;
  apiKey: string;
  collection: string;
}) {
  const [indexes, setIndexes] = useState<IndexInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const [kind, setKind] = useState<IndexSpec["type"]>("field");
  const [fields, setFields] = useState("");
  const [ttl, setTtl] = useState("3600");

  async function refresh() {
    setLoading(true);
    try {
      setIndexes(await listIndexes(projectRef, collection, apiKey));
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
    const parts = fields
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    if (parts.length === 0) {
      setError("enter a field name");
      return;
    }
    let spec: IndexSpec;
    if (kind === "composite") {
      if (parts.length < 2) {
        setError("composite indexes need at least two fields (comma-separated)");
        return;
      }
      spec = { type: "composite", fields: parts };
    } else if (kind === "ttl") {
      const secs = Number(ttl);
      if (!Number.isFinite(secs) || secs <= 0) {
        setError("TTL needs a positive number of seconds");
        return;
      }
      spec = { type: "ttl", field: parts[0], expireAfterSeconds: secs };
    } else {
      spec = { type: kind, field: parts[0] };
    }
    setBusy(true);
    setError(null);
    try {
      await createIndex(projectRef, collection, apiKey, spec);
      setFields("");
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function drop(name: string) {
    if (!confirm(`Drop index "${name}"?`)) return;
    setBusy(true);
    setError(null);
    try {
      await dropIndex(projectRef, collection, apiKey, name);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  const label = (ix: IndexInfo) => {
    const t = ix.unique ? "unique" : ix.index_type;
    const ttlNote = ix.expire_after_seconds ? ` · expires ${ix.expire_after_seconds}s` : "";
    return `${t}${ttlNote}`;
  };

  return (
    <div className="card add-row" style={{ marginBottom: 12 }}>
      <div className="row between" style={{ marginBottom: 8 }}>
        <strong>Indexes</strong>
        <span className="muted small">Indexes are immutable — to change one, drop it and recreate.</span>
      </div>

      {loading ? (
        <p className="muted small">Loading…</p>
      ) : indexes.length === 0 ? (
        <p className="muted small">No indexes on this collection.</p>
      ) : (
        <table className="grid-table" style={{ marginBottom: 8 }}>
          <thead>
            <tr>
              <th>name</th>
              <th>type</th>
              <th>fields</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {indexes.map((ix) => (
              <tr key={ix.name}>
                <td>{ix.name}</td>
                <td>{label(ix)}</td>
                <td>{ix.fields.join(", ")}</td>
                <td className="rowdel">
                  <button className="ghost danger small" disabled={busy} onClick={() => drop(ix.name)}>
                    ✕
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <div className="row" style={{ gap: 8, flexWrap: "wrap", alignItems: "center" }}>
        <select value={kind} onChange={(e) => setKind(e.target.value as IndexSpec["type"])}>
          <option value="field">field</option>
          <option value="unique">unique</option>
          <option value="composite">composite</option>
          <option value="ttl">ttl</option>
        </select>
        <input
          style={{ flex: 1, minWidth: 160 }}
          placeholder={kind === "composite" ? "field1, field2" : "field"}
          value={fields}
          spellCheck={false}
          onChange={(e) => setFields(e.target.value)}
        />
        {kind === "ttl" && (
          <input
            style={{ width: 120 }}
            type="number"
            min={1}
            placeholder="seconds"
            value={ttl}
            onChange={(e) => setTtl(e.target.value)}
          />
        )}
        <button className="primary" disabled={busy} onClick={add}>
          Create
        </button>
      </div>
      {error && <div className="error small" style={{ marginTop: 8 }}>{error}</div>}
    </div>
  );
}

function cell(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}
