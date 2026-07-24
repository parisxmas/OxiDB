import { useEffect, useRef, useState } from "react";
import {
  type StorageObject,
  listBuckets,
  createBucket,
  deleteBucket,
  listObjects,
  uploadObject,
  downloadObject,
  deleteObject,
} from "./dataApi.ts";

/** Files tab: the project's per-database blob store (buckets + objects). */
export function FilesBrowser({ projectRef, apiKey }: { projectRef: string; apiKey: string }) {
  const [buckets, setBuckets] = useState<string[]>([]);
  const [usage, setUsage] = useState<number | null>(null);
  const [active, setActive] = useState<string | null>(null);
  const [newBucket, setNewBucket] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  async function loadBuckets(select?: string) {
    setLoading(true);
    try {
      const d = await listBuckets(projectRef, apiKey);
      setBuckets(d.buckets);
      setUsage(d.total_bytes);
      setError(null);
      if (select) setActive(select);
      else if (d.buckets.length && (!active || !d.buckets.includes(active)))
        setActive(d.buckets[0]);
      if (!d.buckets.length) setActive(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadBuckets();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectRef]);

  async function addBucket(e: React.FormEvent) {
    e.preventDefault();
    const name = newBucket.trim();
    if (!name) return;
    try {
      await createBucket(projectRef, apiKey, name);
      setNewBucket("");
      await loadBuckets(name);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  return (
    <div className="browser">
      <aside className="collections">
        <div className="side-title">Buckets</div>
        {loading && buckets.length === 0 && <div className="muted small">loading…</div>}
        {!loading && buckets.length === 0 && <div className="muted small">no buckets</div>}
        {buckets.map((b) => (
          <button key={b} className={b === active ? "coll active" : "coll"} onClick={() => setActive(b)}>
            <span className="ellip">{b}</span>
          </button>
        ))}
        <form className="newcoll" onSubmit={addBucket}>
          <input
            placeholder="new bucket"
            value={newBucket}
            spellCheck={false}
            onChange={(e) => setNewBucket(e.target.value)}
          />
        </form>
        {usage !== null && (
          <div className="muted small" style={{ marginTop: 8 }}>
            {fmtBytes(usage)} stored
          </div>
        )}
      </aside>

      <div className="rows-pane">
        {error && <div className="error">{error}</div>}
        {active ? (
          <BucketView
            projectRef={projectRef}
            apiKey={apiKey}
            bucket={active}
            onMutate={() => loadBuckets(active)}
            onDeleted={() => {
              setActive(null);
              loadBuckets();
            }}
          />
        ) : (
          !loading && <p className="muted">Create a bucket, then upload files into it.</p>
        )}
      </div>
    </div>
  );
}

function BucketView({
  projectRef,
  apiKey,
  bucket,
  onMutate,
  onDeleted,
}: {
  projectRef: string;
  apiKey: string;
  bucket: string;
  onMutate: () => void;
  onDeleted: () => void;
}) {
  const [objects, setObjects] = useState<StorageObject[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const fileInput = useRef<HTMLInputElement>(null);

  async function refresh() {
    setLoading(true);
    try {
      setObjects(await listObjects(projectRef, apiKey, bucket));
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
  }, [bucket]);

  async function upload(files: FileList | null) {
    if (!files || files.length === 0) return;
    setBusy(true);
    setError(null);
    try {
      for (const file of Array.from(files)) {
        await uploadObject(projectRef, apiKey, bucket, file.name, file);
      }
      await refresh();
      onMutate();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
      if (fileInput.current) fileInput.current.value = "";
    }
  }

  async function download(o: StorageObject) {
    try {
      const blob = await downloadObject(projectRef, apiKey, bucket, o.key);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = o.key.split("/").pop() ?? o.key;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  async function remove(o: StorageObject) {
    if (!confirm(`Delete "${o.key}"?`)) return;
    setBusy(true);
    try {
      await deleteObject(projectRef, apiKey, bucket, o.key);
      await refresh();
      onMutate();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function dropBucket() {
    if (!confirm(`Delete bucket "${bucket}"? It must be empty.`)) return;
    setBusy(true);
    try {
      await deleteBucket(projectRef, apiKey, bucket);
      onDeleted();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  }

  return (
    <div>
      <div className="row between">
        <h3 style={{ margin: "4px 0" }}>{bucket}</h3>
        <div className="row" style={{ gap: 8 }}>
          <button className="ghost" onClick={refresh}>
            Refresh
          </button>
          <button className="ghost danger" disabled={busy} onClick={dropBucket}>
            Delete bucket
          </button>
          <input
            ref={fileInput}
            type="file"
            multiple
            style={{ display: "none" }}
            onChange={(e) => upload(e.target.files)}
          />
          <button className="primary" disabled={busy} onClick={() => fileInput.current?.click()}>
            {busy ? "Uploading…" : "Upload files"}
          </button>
        </div>
      </div>

      {error && <div className="error">{error}</div>}

      {loading ? (
        <p className="muted">Loading…</p>
      ) : objects.length === 0 ? (
        <p className="muted">Empty bucket. Upload files above.</p>
      ) : (
        <>
          <div className="table-wrap">
            <table className="grid-table">
              <thead>
                <tr>
                  <th>key</th>
                  <th>size</th>
                  <th>type</th>
                  <th>uploaded</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {objects.map((o) => (
                  <tr key={o.key}>
                    <td>
                      <a onClick={() => download(o)} title="Download">
                        {o.key}
                      </a>
                    </td>
                    <td>{fmtBytes(o.size)}</td>
                    <td className="muted">{o.content_type}</td>
                    <td className="muted">{o.created_at?.slice(0, 19).replace("T", " ")}</td>
                    <td className="rowdel">
                      <button
                        className="ghost danger small"
                        disabled={busy}
                        title="Delete"
                        onClick={() => remove(o)}
                      >
                        ✕
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="muted small">
            {objects.length} object{objects.length === 1 ? "" : "s"}
          </div>
        </>
      )}
    </div>
  );
}

export function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
  return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}
