import { useEffect, useState, useCallback } from "react";
import {
  listCollections,
  listIndexes,
  createIndex,
  createUniqueIndex,
  createCompositeIndex,
  createTextIndex,
  dropIndex,
} from "../../api/tauri";
import type { IndexInfo } from "../../api/types";
import { ConfirmDialog } from "../common/ConfirmDialog";
import { useToast } from "../common/Toast";

export function IndexManager() {
  const toast = useToast();
  const [collections, setCollections] = useState<string[]>([]);
  const [selected, setSelected] = useState("");
  const [indexes, setIndexes] = useState<IndexInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [showCreate, setShowCreate] = useState(false);
  const [idxType, setIdxType] = useState<"field" | "unique" | "composite" | "text">("field");
  const [fieldInput, setFieldInput] = useState("");
  const [confirmDrop, setConfirmDrop] = useState<string | null>(null);

  const loadCollections = useCallback(async () => {
    try {
      const names = await listCollections();
      setCollections(names.sort());
    } catch (e) {
      toast(String(e), "error");
    }
  }, [toast]);

  useEffect(() => { loadCollections(); }, [loadCollections]);

  const loadIndexes = useCallback(async (col: string) => {
    if (!col) return;
    setLoading(true);
    try {
      const data = await listIndexes(col);
      setIndexes(Array.isArray(data) ? data as unknown as IndexInfo[] : []);
    } catch (e) {
      toast(String(e), "error");
    } finally {
      setLoading(false);
    }
  }, [toast]);

  useEffect(() => {
    if (selected) loadIndexes(selected);
  }, [selected, loadIndexes]);

  const handleCreate = async () => {
    if (!selected || !fieldInput.trim()) return;
    try {
      const fields = fieldInput.split(",").map((f) => f.trim()).filter(Boolean);
      if (idxType === "field") {
        await createIndex(selected, fields[0]);
      } else if (idxType === "unique") {
        await createUniqueIndex(selected, fields[0]);
      } else if (idxType === "composite") {
        await createCompositeIndex(selected, fields);
      } else {
        await createTextIndex(selected, fields);
      }
      toast("Index created", "success");
      setShowCreate(false);
      setFieldInput("");
      loadIndexes(selected);
    } catch (e) {
      toast(String(e), "error");
    }
  };

  const handleDrop = async (name: string) => {
    if (!selected) return;
    try {
      await dropIndex(selected, name);
      toast("Index dropped", "success");
      setConfirmDrop(null);
      loadIndexes(selected);
    } catch (e) {
      toast(String(e), "error");
    }
  };

  return (
    <div>
      <div className="toolbar" style={{ marginBottom: 16 }}>
        <h2 style={{ fontSize: 18, fontWeight: 600 }}>Indexes</h2>
        <div style={{ flex: 1 }} />
        <select
          value={selected}
          onChange={(e) => setSelected(e.target.value)}
          style={{ minWidth: 180 }}
        >
          <option value="">Select collection...</option>
          {collections.map((c) => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>
        {selected && (
          <button className="btn btn-primary btn-sm" onClick={() => setShowCreate(true)}>
            Create Index
          </button>
        )}
      </div>

      {loading ? (
        <div className="empty-state"><span className="spinner" /></div>
      ) : !selected ? (
        <div className="empty-state">Select a collection to manage indexes</div>
      ) : indexes.length === 0 ? (
        <div className="empty-state">No indexes on this collection</div>
      ) : (
        <table className="data-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Type</th>
              <th>Fields</th>
              <th>Unique</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {indexes.map((idx) => (
              <tr key={idx.name}>
                <td style={{ fontFamily: "var(--font-mono)", fontSize: 13 }}>{idx.name}</td>
                <td><span className="badge badge-muted">{idx.index_type}</span></td>
                <td>{idx.fields.join(", ")}</td>
                <td>{idx.unique ? "Yes" : "No"}</td>
                <td>
                  <button
                    className="btn btn-danger btn-sm"
                    onClick={() => setConfirmDrop(idx.name)}
                  >
                    Drop
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {showCreate && (
        <div className="dialog-overlay" onClick={() => setShowCreate(false)}>
          <div className="dialog" onClick={(e) => e.stopPropagation()}>
            <div className="dialog-title">Create Index on {selected}</div>
            <div className="form-group">
              <label>Index Type</label>
              <select value={idxType} onChange={(e) => setIdxType(e.target.value as typeof idxType)}>
                <option value="field">Field</option>
                <option value="unique">Unique</option>
                <option value="composite">Composite</option>
                <option value="text">Text</option>
              </select>
            </div>
            <div className="form-group">
              <label>
                {idxType === "composite" || idxType === "text"
                  ? "Fields (comma-separated)"
                  : "Field Name"}
              </label>
              <input
                value={fieldInput}
                onChange={(e) => setFieldInput(e.target.value)}
                placeholder={idxType === "composite" ? "field1, field2" : "fieldName"}
                onKeyDown={(e) => e.key === "Enter" && handleCreate()}
                autoFocus
              />
            </div>
            <div className="dialog-actions">
              <button className="btn btn-secondary" onClick={() => setShowCreate(false)}>Cancel</button>
              <button className="btn btn-primary" onClick={handleCreate}>Create</button>
            </div>
          </div>
        </div>
      )}

      {confirmDrop && (
        <ConfirmDialog
          title="Drop Index"
          message={`Drop index "${confirmDrop}"?`}
          confirmLabel="Drop"
          danger
          onConfirm={() => handleDrop(confirmDrop)}
          onCancel={() => setConfirmDrop(null)}
        />
      )}
    </div>
  );
}
