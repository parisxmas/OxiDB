import { useEffect, useState, useCallback } from "react";
import {
  listCollections,
  findDocuments,
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
import { useToast } from "../common/Toast";

export function CollectionBrowser() {
  const toast = useToast();
  const [collections, setCollections] = useState<string[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [docs, setDocs] = useState<JsonValue[]>([]);
  const [page, setPage] = useState(0);
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

  const PAGE_SIZE = 50;

  const loadCollections = useCallback(async () => {
    try {
      const names = await listCollections();
      setCollections(names.sort());
    } catch (e) {
      toast(String(e), "error");
    }
  }, [toast]);

  useEffect(() => {
    loadCollections();
  }, [loadCollections]);

  const loadDocs = useCallback(async (col: string, pageNum: number) => {
    setLoading(true);
    try {
      const results = await findDocuments({
        collection: col,
        skip: pageNum * PAGE_SIZE,
        limit: PAGE_SIZE,
        sort: { _id: -1 },
      });
      setDocs(results);
    } catch (e) {
      toast(String(e), "error");
    } finally {
      setLoading(false);
    }
  }, [toast]);

  const handleSelect = (name: string) => {
    setSelected(name);
    setPage(0);
    setViewDoc(null);
    loadDocs(name, 0);
  };

  const handleInsert = async () => {
    if (!selected) return;
    try {
      const doc = JSON.parse(insertJson);
      await insertDocument(selected, doc);
      toast("Document inserted", "success");
      setShowInsert(false);
      setInsertJson("{}");
      loadDocs(selected, page);
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
      loadDocs(selected, page);
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
      loadDocs(selected, page);
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
            <span style={{ flex: 1 }} onClick={() => handleSelect(name)}>
              {name}
            </span>
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
          <div style={{ color: "var(--text-muted)", padding: 8, fontSize: 13 }}>
            No collections
          </div>
        )}
      </div>

      <div className="split-right">
        {selected ? (
          <>
            <div className="toolbar">
              <strong>{selected}</strong>
              <div style={{ flex: 1 }} />
              <button className="btn btn-primary btn-sm" onClick={() => { setInsertJson("{}"); setShowInsert(true); }}>
                Insert
              </button>
              <button className="btn btn-secondary btn-sm" onClick={() => loadDocs(selected, page)}>
                Refresh
              </button>
            </div>
            {loading ? (
              <div className="empty-state"><span className="spinner" /></div>
            ) : (
              <>
                <DataTable
                  data={docs}
                  onRowClick={(row) => setViewDoc(row)}
                />
                <div className="pagination">
                  <button className="btn btn-secondary btn-sm" disabled={page === 0} onClick={() => { setPage(page - 1); loadDocs(selected, page - 1); }}>
                    Prev
                  </button>
                  <span style={{ color: "var(--text-secondary)", fontSize: 13 }}>Page {page + 1}</span>
                  <button className="btn btn-secondary btn-sm" disabled={docs.length < PAGE_SIZE} onClick={() => { setPage(page + 1); loadDocs(selected, page + 1); }}>
                    Next
                  </button>
                </div>
              </>
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
