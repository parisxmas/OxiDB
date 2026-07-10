import { useEffect, useState, useCallback } from "react";
import {
  listCollections,
  findDocuments,
  countDocuments,
  insertDocument,
  updateDocuments,
  deleteDocuments,
  createCollection,
  dropCollection,
} from "../../api/tauri";
import type { JsonValue } from "../../api/types";
import { DataTable } from "../common/DataTable";
import { JsonEditor } from "../common/JsonEditor";
import { JsonViewer } from "../common/JsonViewer";
import { ConfirmDialog } from "../common/ConfirmDialog";
import { Pagination } from "../common/Pagination";
import { useToast } from "../common/Toast";

function downloadJson(name: string, data: unknown) {
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.click();
  URL.revokeObjectURL(url);
}

export function CollectionBrowser() {
  const toast = useToast();
  const [collections, setCollections] = useState<string[]>([]);
  const [counts, setCounts] = useState<Record<string, number>>({});
  const [selected, setSelected] = useState<string | null>(null);
  const [docs, setDocs] = useState<JsonValue[]>([]);
  const [total, setTotal] = useState<number | null>(null);
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [filter, setFilter] = useState("");
  const [filterErr, setFilterErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [viewDoc, setViewDoc] = useState<JsonValue | null>(null);
  const [showInsert, setShowInsert] = useState(false);
  const [showEdit, setShowEdit] = useState(false);
  const [insertJson, setInsertJson] = useState("{}");
  const [editJson, setEditJson] = useState("{}");
  const [editDocId, setEditDocId] = useState<number | null>(null);
  const [confirmDrop, setConfirmDrop] = useState<string | null>(null);
  const [newCollName, setNewCollName] = useState("");
  const [showNewColl, setShowNewColl] = useState(false);

  const loadCollections = useCallback(async () => {
    try {
      // Hide internal collections (empty name, or "_"-prefixed system ones
      // like _alerts / _schedules / _fts).
      const names = (await listCollections())
        .filter((n) => n && !n.startsWith("_"))
        .sort();
      setCollections(names);
      // Per-collection document counts (best-effort, in parallel).
      const entries = await Promise.all(
        names.map(async (n) => {
          try {
            return [n, await countDocuments(n)] as const;
          } catch {
            return [n, -1] as const;
          }
        })
      );
      setCounts(Object.fromEntries(entries));
    } catch (e) {
      toast(String(e), "error");
    }
  }, [toast]);

  useEffect(() => {
    loadCollections();
  }, [loadCollections]);

  const parseFilter = useCallback((): Record<string, JsonValue> | null => {
    const t = filter.trim();
    if (!t) return {};
    try {
      const q = JSON.parse(t);
      setFilterErr(null);
      return q;
    } catch (e) {
      setFilterErr(String(e));
      return null;
    }
  }, [filter]);

  const loadDocs = useCallback(
    async (col: string, pageNum: number, size: number) => {
      const query = parseFilter();
      if (query === null) return; // invalid filter JSON
      setLoading(true);
      try {
        const [results, n] = await Promise.all([
          findDocuments({
            collection: col,
            query,
            skip: pageNum * size,
            limit: size,
            sort: { _id: -1 },
          }),
          countDocuments(col, query as JsonValue).catch(() => -1),
        ]);
        setDocs(results);
        setTotal(n >= 0 ? n : null);
      } catch (e) {
        toast(String(e), "error");
      } finally {
        setLoading(false);
      }
    },
    [toast, parseFilter]
  );

  const handleSelect = (name: string) => {
    setSelected(name);
    setPage(0);
    setViewDoc(null);
    loadDocs(name, 0, pageSize);
  };

  const applyFilter = () => {
    if (!selected) return;
    setPage(0);
    loadDocs(selected, 0, pageSize);
  };

  const gotoPage = (p: number) => {
    if (!selected) return;
    setPage(p);
    loadDocs(selected, p, pageSize);
  };

  const changePageSize = (s: number) => {
    setPageSize(s);
    setPage(0);
    if (selected) loadDocs(selected, 0, s);
  };

  const handleInsert = async () => {
    if (!selected) return;
    try {
      const doc = JSON.parse(insertJson);
      await insertDocument(selected, doc);
      toast("Document inserted", "success");
      setShowInsert(false);
      setInsertJson("{}");
      loadDocs(selected, page, pageSize);
    } catch (e) {
      toast(String(e), "error");
    }
  };

  const handleEditOpen = (doc: JsonValue) => {
    if (doc && typeof doc === "object" && !Array.isArray(doc)) {
      const d = doc as Record<string, unknown>;
      setEditDocId(d._id as number);
      const { _id, _version, ...rest } = d;
      setEditJson(JSON.stringify(rest, null, 2));
      setShowEdit(true);
    }
  };

  const handleEditSave = async () => {
    if (!selected || editDocId === null) return;
    try {
      const update = JSON.parse(editJson);
      await updateDocuments(selected, { _id: editDocId }, { $set: update });
      toast("Document updated", "success");
      setShowEdit(false);
      loadDocs(selected, page, pageSize);
    } catch (e) {
      toast(String(e), "error");
    }
  };

  const handleDelete = async (doc: JsonValue) => {
    if (!selected || !doc || typeof doc !== "object" || Array.isArray(doc)) return;
    const d = doc as Record<string, unknown>;
    try {
      await deleteDocuments(selected, { _id: d._id } as Record<string, JsonValue>);
      toast("Document deleted", "success");
      loadDocs(selected, page, pageSize);
      setViewDoc(null);
    } catch (e) {
      toast(String(e), "error");
    }
  };

  const handleCreateCollection = async () => {
    if (!newCollName.trim()) return;
    try {
      await createCollection(newCollName.trim());
      toast("Collection created", "success");
      setShowNewColl(false);
      setNewCollName("");
      loadCollections();
    } catch (e) {
      toast(String(e), "error");
    }
  };

  const handleDropCollection = async (name: string) => {
    try {
      await dropCollection(name);
      toast("Collection dropped", "success");
      setConfirmDrop(null);
      if (selected === name) {
        setSelected(null);
        setDocs([]);
      }
      loadCollections();
    } catch (e) {
      toast(String(e), "error");
    }
  };

  return (
    <div className="split-view" style={{ height: "calc(100vh - var(--header-height) - 40px)" }}>
      <div className="split-left">
        <div className="toolbar">
          <strong style={{ flex: 1 }}>Collections</strong>
          <button className="btn btn-primary btn-sm" onClick={() => setShowNewColl(true)}>
            +
          </button>
        </div>
        {collections.map((name) => (
          <div
            key={name}
            style={{
              display: "flex",
              alignItems: "center",
              padding: "6px 8px",
              borderRadius: "var(--radius-sm)",
              cursor: "pointer",
              background: name === selected ? "var(--accent-bg)" : "transparent",
              color: name === selected ? "var(--accent)" : "var(--text-primary)",
              marginBottom: 2,
            }}
          >
            <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} onClick={() => handleSelect(name)}>
              {name}
            </span>
            {counts[name] != null && counts[name] >= 0 && (
              <span style={{ fontSize: 11, color: "var(--text-secondary)", fontFamily: "var(--font-mono)", marginRight: 4 }}>
                {counts[name]}
              </span>
            )}
            <button
              className="btn btn-sm"
              style={{ padding: "2px 6px", color: "var(--danger)", background: "none" }}
              onClick={(e) => { e.stopPropagation(); setConfirmDrop(name); }}
              title="Drop collection"
            >
              ×
            </button>
          </div>
        ))}
        {collections.length === 0 && (
          <div style={{ padding: 12, fontSize: 13, color: "var(--text-secondary)", textAlign: "center" }}>
            No collections yet.
            <br />
            <button className="btn btn-primary btn-sm" style={{ marginTop: 8 }} onClick={() => setShowNewColl(true)}>
              + Create collection
            </button>
          </div>
        )}
      </div>

      <div className="split-right">
        {selected ? (
          <>
            <div className="toolbar">
              <strong>{selected}</strong>
              {total != null && (
                <span style={{ marginLeft: 8, fontSize: 12, color: "var(--text-secondary)" }}>
                  {total.toLocaleString()} doc{total === 1 ? "" : "s"}
                </span>
              )}
              <div style={{ flex: 1 }} />
              <button
                className="btn btn-secondary btn-sm"
                onClick={() => downloadJson(`${selected}.json`, docs)}
                title="Export the loaded page as JSON"
                disabled={docs.length === 0}
              >
                Export
              </button>
              <button className="btn btn-primary btn-sm" onClick={() => { setInsertJson("{}"); setShowInsert(true); }}>
                Insert
              </button>
              <button className="btn btn-secondary btn-sm" onClick={() => loadDocs(selected, page, pageSize)}>
                Refresh
              </button>
            </div>

            {/* JSON query filter */}
            <div className="toolbar" style={{ gap: 6 }}>
              <input
                style={{ flex: 1, fontFamily: "var(--font-mono)", fontSize: 12 }}
                placeholder='filter, e.g. {"age": {"$gt": 30}}'
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && applyFilter()}
              />
              <button className="btn btn-primary btn-sm" onClick={applyFilter}>
                Find
              </button>
              {filter && (
                <button
                  className="btn btn-secondary btn-sm"
                  onClick={() => { setFilter(""); setFilterErr(null); setPage(0); if (selected) loadDocs(selected, 0, pageSize); }}
                >
                  Clear
                </button>
              )}
            </div>
            {filterErr && (
              <div style={{ padding: "4px 12px", fontSize: 12, color: "var(--danger)", fontFamily: "var(--font-mono)" }}>
                {filterErr}
              </div>
            )}

            {loading ? (
              <div className="empty-state"><span className="spinner" /></div>
            ) : (
              <div style={{ flex: 1, display: "flex", flexDirection: "column", minHeight: 0 }}>
                <div style={{ flex: 1, overflow: "auto" }}>
                  <DataTable data={docs} onRowClick={(row) => setViewDoc(row)} />
                </div>
                {((total ?? 0) > pageSize || page > 0) && (
                  <Pagination
                    page={page}
                    pageSize={pageSize}
                    total={total ?? undefined}
                    currentCount={docs.length}
                    onPage={gotoPage}
                    onPageSize={changePageSize}
                    busy={loading}
                  />
                )}
              </div>
            )}
          </>
        ) : (
          <div className="empty-state">Select a collection to browse documents</div>
        )}
      </div>

      {/* View document */}
      {viewDoc && (
        <div className="dialog-overlay" onClick={() => setViewDoc(null)}>
          <div className="dialog" style={{ minWidth: 560 }} onClick={(e) => e.stopPropagation()}>
            <div className="dialog-title">Document</div>
            <JsonViewer data={viewDoc} />
            <div className="dialog-actions">
              <button className="btn btn-danger btn-sm" onClick={() => { handleDelete(viewDoc); }}>
                Delete
              </button>
              <button className="btn btn-primary btn-sm" onClick={() => { handleEditOpen(viewDoc); setViewDoc(null); }}>
                Edit
              </button>
              <button className="btn btn-secondary btn-sm" onClick={() => setViewDoc(null)}>
                Close
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Insert dialog */}
      {showInsert && (
        <div className="dialog-overlay" onClick={() => setShowInsert(false)}>
          <div className="dialog" style={{ minWidth: 520 }} onClick={(e) => e.stopPropagation()}>
            <div className="dialog-title">Insert Document</div>
            <JsonEditor value={insertJson} onChange={setInsertJson} height="250px" />
            <div className="dialog-actions">
              <button className="btn btn-secondary" onClick={() => setShowInsert(false)}>Cancel</button>
              <button className="btn btn-primary" onClick={handleInsert}>Insert</button>
            </div>
          </div>
        </div>
      )}

      {/* Edit dialog */}
      {showEdit && (
        <div className="dialog-overlay" onClick={() => setShowEdit(false)}>
          <div className="dialog" style={{ minWidth: 520 }} onClick={(e) => e.stopPropagation()}>
            <div className="dialog-title">Edit Document (ID: {editDocId})</div>
            <JsonEditor value={editJson} onChange={setEditJson} height="250px" />
            <div className="dialog-actions">
              <button className="btn btn-secondary" onClick={() => setShowEdit(false)}>Cancel</button>
              <button className="btn btn-primary" onClick={handleEditSave}>Save</button>
            </div>
          </div>
        </div>
      )}

      {/* Create collection dialog */}
      {showNewColl && (
        <div className="dialog-overlay" onClick={() => setShowNewColl(false)}>
          <div className="dialog" onClick={(e) => e.stopPropagation()}>
            <div className="dialog-title">Create Collection</div>
            <div className="form-group">
              <label>Collection Name</label>
              <input
                value={newCollName}
                onChange={(e) => setNewCollName(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && handleCreateCollection()}
                autoFocus
              />
            </div>
            <div className="dialog-actions">
              <button className="btn btn-secondary" onClick={() => setShowNewColl(false)}>Cancel</button>
              <button className="btn btn-primary" onClick={handleCreateCollection}>Create</button>
            </div>
          </div>
        </div>
      )}

      {/* Drop confirmation */}
      {confirmDrop && (
        <ConfirmDialog
          title="Drop Collection"
          message={`Are you sure you want to drop "${confirmDrop}"? This cannot be undone.`}
          confirmLabel="Drop"
          danger
          onConfirm={() => handleDropCollection(confirmDrop)}
          onCancel={() => setConfirmDrop(null)}
        />
      )}
    </div>
  );
}
