import { useEffect, useState, useCallback } from "react";
import { open, save } from "@tauri-apps/plugin-dialog";
import {
  listBuckets,
  createBucket,
  deleteBucket,
  listObjects,
  putObject,
  getObject,
  deleteObject,
  searchObjects,
  readFileBase64,
  writeFileBase64,
} from "../../api/tauri";
import type { JsonValue } from "../../api/types";
import { ConfirmDialog } from "../common/ConfirmDialog";
import { JsonViewer } from "../common/JsonViewer";
import { useToast } from "../common/Toast";

/** UTF-8-safe base64 (btoa alone throws on non-Latin1 characters). */
function utf8ToBase64(s: string): string {
  const bytes = new TextEncoder().encode(s);
  let bin = "";
  bytes.forEach((b) => (bin += String.fromCharCode(b)));
  return btoa(bin);
}

function humanSize(n: unknown): string {
  const b = typeof n === "number" ? n : parseInt(String(n), 10);
  if (!Number.isFinite(b)) return "";
  if (b < 1024) return `${b} B`;
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`;
  return `${(b / (1024 * 1024)).toFixed(1)} MB`;
}

const EXT_TYPES: Record<string, string> = {
  txt: "text/plain", json: "application/json", csv: "text/csv",
  html: "text/html", png: "image/png", jpg: "image/jpeg", jpeg: "image/jpeg",
  gif: "image/gif", pdf: "application/pdf", zip: "application/zip",
};

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
  const [uploadB64, setUploadB64] = useState<string | null>(null); // set when a file is chosen
  const [uploadFileName, setUploadFileName] = useState("");
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

  const openUpload = () => {
    setUploadKey("");
    setUploadType("application/octet-stream");
    setUploadData("");
    setUploadB64(null);
    setUploadFileName("");
    setShowUpload(true);
  };

  const chooseFile = async () => {
    const sel = await open({ multiple: false });
    if (!sel || typeof sel !== "string") return;
    try {
      const b64 = await readFileBase64(sel);
      const name = sel.split(/[/\\]/).pop() || sel;
      setUploadB64(b64);
      setUploadFileName(name);
      if (!uploadKey) setUploadKey(name);
      const ext = name.split(".").pop()?.toLowerCase() || "";
      if (EXT_TYPES[ext]) setUploadType(EXT_TYPES[ext]);
    } catch (e) {
      toast(String(e), "error");
    }
  };

  const handleUpload = async () => {
    if (!selected || !uploadKey.trim()) return;
    try {
      // A chosen file uploads its raw bytes; otherwise the typed text (UTF-8).
      const b64 = uploadB64 ?? utf8ToBase64(uploadData);
      await putObject(selected, uploadKey.trim(), b64, uploadType || undefined);
      toast("Object uploaded", "success");
      setShowUpload(false);
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

  const handleSaveToDisk = async (key: string) => {
    if (!selected) return;
    try {
      const result = (await getObject(selected, key)) as { content?: string };
      if (!result?.content) {
        toast("nothing to save", "error");
        return;
      }
      const path = await save({ defaultPath: key.split("/").pop() || key });
      if (!path) return;
      await writeFileBase64(path, result.content);
      toast("Saved to disk", "success");
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
          <div style={{ padding: 12, fontSize: 13, color: "var(--text-secondary)", textAlign: "center" }}>
            No buckets yet.
            <br />
            <button className="btn btn-primary btn-sm" style={{ marginTop: 8 }} onClick={() => setShowNewBucket(true)}>
              + Create bucket
            </button>
          </div>
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
              <button className="btn btn-primary btn-sm" onClick={openUpload}>Upload</button>
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
                    <th>Modified</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {objects.map((obj, i) => {
                    const o = obj as Record<string, unknown>;
                    const key = (o.key || o.name || "") as string;
                    const created = (o.created_at || "") as string;
                    return (
                      <tr key={i}>
                        <td style={{ fontFamily: "var(--font-mono)", fontSize: 13 }}>{key}</td>
                        <td style={{ color: "var(--text-secondary)" }}>{(o.content_type || "") as string}</td>
                        <td style={{ fontFamily: "var(--font-mono)" }}>{humanSize(o.size)}</td>
                        <td style={{ color: "var(--text-secondary)", fontSize: 12 }}>
                          {created ? created.replace("T", " ").replace("Z", "") : ""}
                        </td>
                        <td>
                          <div style={{ display: "flex", gap: 4 }}>
                            <button className="btn btn-secondary btn-sm" onClick={() => handleDownload(key)}>View</button>
                            <button className="btn btn-secondary btn-sm" onClick={() => handleSaveToDisk(key)}>Save</button>
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
        <div className="dialog-overlay">
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
        <div className="dialog-overlay">
          <div className="dialog" style={{ minWidth: 480 }} onClick={(e) => e.stopPropagation()}>
            <div className="dialog-title">Upload Object to {selected}</div>
            <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 12 }}>
              <button className="btn btn-secondary btn-sm" onClick={chooseFile}>Choose file…</button>
              <span style={{ fontSize: 12, color: "var(--text-secondary)", fontFamily: "var(--font-mono)" }}>
                {uploadFileName || "or type text below"}
              </span>
              {uploadB64 && (
                <button
                  className="btn btn-sm"
                  style={{ marginLeft: "auto", background: "none", color: "var(--text-secondary)" }}
                  onClick={() => { setUploadB64(null); setUploadFileName(""); }}
                >
                  clear
                </button>
              )}
            </div>
            <div className="form-group">
              <label>Key</label>
              <input value={uploadKey} onChange={(e) => setUploadKey(e.target.value)} placeholder="file.txt" autoFocus />
            </div>
            <div className="form-group">
              <label>Content Type</label>
              <input value={uploadType} onChange={(e) => setUploadType(e.target.value)} placeholder="application/octet-stream" />
            </div>
            {!uploadB64 && (
              <div className="form-group">
                <label>Content (text)</label>
                <textarea
                  value={uploadData}
                  onChange={(e) => setUploadData(e.target.value)}
                  rows={6}
                  style={{ fontFamily: "var(--font-mono)", fontSize: 13 }}
                />
              </div>
            )}
            <div className="dialog-actions">
              <button className="btn btn-secondary" onClick={() => setShowUpload(false)}>Cancel</button>
              <button className="btn btn-primary" onClick={handleUpload}>Upload</button>
            </div>
          </div>
        </div>
      )}

      {viewObject && (
        <div className="dialog-overlay">
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
