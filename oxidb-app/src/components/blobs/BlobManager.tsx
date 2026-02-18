import { useEffect, useState, useCallback } from "react";
import {
  listBuckets,
  createBucket,
  deleteBucket,
  listObjects,
  putObject,
  getObject,
  deleteObject,
  searchObjects,
} from "../../api/tauri";
import type { JsonValue } from "../../api/types";
import { ConfirmDialog } from "../common/ConfirmDialog";
import { JsonViewer } from "../common/JsonViewer";
import { useToast } from "../common/Toast";

export function BlobManager() {
  const toast = useToast();
  const [buckets, setBuckets] = useState<string[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [objects, setObjects] = useState<JsonValue[]>([]);
  const [loading, setLoading] = useState(false);
  const [showUpload, setShowUpload] = useState(false);
  const [uploadKey, setUploadKey] = useState("");
  const [uploadType, setUploadType] = useState("application/octet-stream");
  const [uploadData, setUploadData] = useState("");
  const [newBucket, setNewBucket] = useState("");
  const [showNewBucket, setShowNewBucket] = useState(false);
  const [confirmDeleteBucket, setConfirmDeleteBucket] = useState<string | null>(null);
  const [viewObject, setViewObject] = useState<JsonValue | null>(null);
  const [searchQuery, setSearchQuery] = useState("");

  const loadBuckets = useCallback(async () => {
    try {
      const b = await listBuckets();
      setBuckets(b.sort());
    } catch (e) {
      toast(String(e), "error");
    }
  }, [toast]);

  useEffect(() => { loadBuckets(); }, [loadBuckets]);

  const loadObjects = useCallback(async (bucket: string) => {
    setLoading(true);
    try {
      const objs = await listObjects(bucket, undefined, 100);
      setObjects(objs);
    } catch (e) {
      toast(String(e), "error");
    } finally {
      setLoading(false);
    }
  }, [toast]);

  const handleSelectBucket = (name: string) => {
    setSelected(name);
    loadObjects(name);
  };

  const handleCreateBucket = async () => {
    if (!newBucket.trim()) return;
    try {
      await createBucket(newBucket.trim());
      toast("Bucket created", "success");
      setShowNewBucket(false);
      setNewBucket("");
      loadBuckets();
    } catch (e) {
      toast(String(e), "error");
    }
  };

  const handleDeleteBucket = async (name: string) => {
    try {
      await deleteBucket(name);
      toast("Bucket deleted", "success");
      setConfirmDeleteBucket(null);
      if (selected === name) { setSelected(null); setObjects([]); }
      loadBuckets();
    } catch (e) {
      toast(String(e), "error");
    }
  };

  const handleUpload = async () => {
    if (!selected || !uploadKey.trim()) return;
    try {
      // Convert text to base64
      const b64 = btoa(uploadData);
      await putObject(selected, uploadKey.trim(), b64, uploadType || undefined);
      toast("Object uploaded", "success");
      setShowUpload(false);
      setUploadKey("");
      setUploadData("");
      loadObjects(selected);
    } catch (e) {
      toast(String(e), "error");
    }
  };

  const handleDownload = async (key: string) => {
    if (!selected) return;
    try {
      const result = await getObject(selected, key);
      setViewObject(result);
    } catch (e) {
      toast(String(e), "error");
    }
  };

  const handleDeleteObject = async (key: string) => {
    if (!selected) return;
    try {
      await deleteObject(selected, key);
      toast("Object deleted", "success");
      loadObjects(selected);
    } catch (e) {
      toast(String(e), "error");
    }
  };

  const handleSearch = async () => {
    if (!searchQuery.trim()) return;
    setLoading(true);
    try {
      const results = await searchObjects(searchQuery, selected || undefined, 20);
      setObjects(results);
    } catch (e) {
      toast(String(e), "error");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="split-view" style={{ height: "calc(100vh - var(--header-height) - 40px)" }}>
      <div className="split-left">
        <div className="toolbar">
          <strong style={{ flex: 1 }}>Buckets</strong>
          <button className="btn btn-primary btn-sm" onClick={() => setShowNewBucket(true)}>+</button>
        </div>
        {buckets.map((name) => (
          <div
            key={name}
            style={{
              display: "flex", alignItems: "center", padding: "6px 8px",
              borderRadius: "var(--radius-sm)", cursor: "pointer",
              background: name === selected ? "var(--accent-bg)" : "transparent",
              color: name === selected ? "var(--accent)" : "var(--text-primary)",
              marginBottom: 2,
            }}
          >
            <span style={{ flex: 1 }} onClick={() => handleSelectBucket(name)}>{name}</span>
            <button
              className="btn btn-sm"
              style={{ padding: "2px 6px", color: "var(--danger)", background: "none" }}
              onClick={(e) => { e.stopPropagation(); setConfirmDeleteBucket(name); }}
            >×</button>
          </div>
        ))}
        {buckets.length === 0 && (
          <div style={{ color: "var(--text-muted)", padding: 8, fontSize: 13 }}>No buckets</div>
        )}
      </div>

      <div className="split-right">
        {selected ? (
          <>
            <div className="toolbar">
              <strong>{selected}</strong>
              <div style={{ flex: 1 }} />
              <input
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="FTS search..."
                style={{ width: 200 }}
                onKeyDown={(e) => e.key === "Enter" && handleSearch()}
              />
              <button className="btn btn-secondary btn-sm" onClick={handleSearch}>Search</button>
              <button className="btn btn-primary btn-sm" onClick={() => setShowUpload(true)}>Upload</button>
              <button className="btn btn-secondary btn-sm" onClick={() => loadObjects(selected)}>Refresh</button>
            </div>
            {loading ? (
              <div className="empty-state"><span className="spinner" /></div>
            ) : objects.length === 0 ? (
              <div className="empty-state">No objects</div>
            ) : (
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Key</th>
                    <th>Content Type</th>
                    <th>Size</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {objects.map((obj, i) => {
                    const o = obj as Record<string, unknown>;
                    const key = (o.key || o.name || "") as string;
                    return (
                      <tr key={i}>
                        <td style={{ fontFamily: "var(--font-mono)", fontSize: 13 }}>{key}</td>
                        <td>{(o.content_type || "") as string}</td>
                        <td>{(o.size || "") as string}</td>
                        <td>
                          <div style={{ display: "flex", gap: 4 }}>
                            <button className="btn btn-secondary btn-sm" onClick={() => handleDownload(key)}>View</button>
                            <button className="btn btn-danger btn-sm" onClick={() => handleDeleteObject(key)}>Delete</button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </>
        ) : (
          <div className="empty-state">Select a bucket to browse objects</div>
        )}
      </div>

      {showNewBucket && (
        <div className="dialog-overlay" onClick={() => setShowNewBucket(false)}>
          <div className="dialog" onClick={(e) => e.stopPropagation()}>
            <div className="dialog-title">Create Bucket</div>
            <div className="form-group">
              <label>Bucket Name</label>
              <input value={newBucket} onChange={(e) => setNewBucket(e.target.value)} onKeyDown={(e) => e.key === "Enter" && handleCreateBucket()} autoFocus />
            </div>
            <div className="dialog-actions">
              <button className="btn btn-secondary" onClick={() => setShowNewBucket(false)}>Cancel</button>
              <button className="btn btn-primary" onClick={handleCreateBucket}>Create</button>
            </div>
          </div>
        </div>
      )}

      {showUpload && (
        <div className="dialog-overlay" onClick={() => setShowUpload(false)}>
          <div className="dialog" style={{ minWidth: 480 }} onClick={(e) => e.stopPropagation()}>
            <div className="dialog-title">Upload Object to {selected}</div>
            <div className="form-group">
              <label>Key</label>
              <input value={uploadKey} onChange={(e) => setUploadKey(e.target.value)} placeholder="file.txt" autoFocus />
            </div>
            <div className="form-group">
              <label>Content Type</label>
              <input value={uploadType} onChange={(e) => setUploadType(e.target.value)} placeholder="application/octet-stream" />
            </div>
            <div className="form-group">
              <label>Content (text)</label>
              <textarea
                value={uploadData}
                onChange={(e) => setUploadData(e.target.value)}
                rows={6}
                style={{ fontFamily: "var(--font-mono)", fontSize: 13 }}
              />
            </div>
            <div className="dialog-actions">
              <button className="btn btn-secondary" onClick={() => setShowUpload(false)}>Cancel</button>
              <button className="btn btn-primary" onClick={handleUpload}>Upload</button>
            </div>
          </div>
        </div>
      )}

      {viewObject && (
        <div className="dialog-overlay" onClick={() => setViewObject(null)}>
          <div className="dialog" style={{ minWidth: 520 }} onClick={(e) => e.stopPropagation()}>
            <div className="dialog-title">Object</div>
            <JsonViewer data={viewObject} />
            <div className="dialog-actions">
              <button className="btn btn-secondary" onClick={() => setViewObject(null)}>Close</button>
            </div>
          </div>
        </div>
      )}

      {confirmDeleteBucket && (
        <ConfirmDialog
          title="Delete Bucket"
          message={`Delete bucket "${confirmDeleteBucket}" and all its objects?`}
          confirmLabel="Delete"
          danger
          onConfirm={() => handleDeleteBucket(confirmDeleteBucket)}
          onCancel={() => setConfirmDeleteBucket(null)}
        />
      )}
    </div>
  );
}
