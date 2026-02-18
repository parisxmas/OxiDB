import { useState, useCallback, useEffect, useRef } from "react";
import { executeRawCommand } from "../../api/tauri";
import type { JsonValue } from "../../api/types";
import { JsonEditor } from "../common/JsonEditor";
import { DataTable } from "../common/DataTable";
import { JsonViewer } from "../common/JsonViewer";
import { useToast } from "../common/Toast";

const HISTORY_KEY = "oxidb-query-history";

function loadHistory(): string[] {
  try {
    return JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]");
  } catch {
    return [];
  }
}

function saveHistory(items: string[]) {
  localStorage.setItem(HISTORY_KEY, JSON.stringify(items.slice(0, 50)));
}

export function QueryEditor() {
  const toast = useToast();
  const [query, setQuery] = useState(
    '{\n  "cmd": "find",\n  "collection": "",\n  "query": {}\n}'
  );
  const [result, setResult] = useState<JsonValue | null>(null);
  const [viewMode, setViewMode] = useState<"table" | "json">("table");
  const [loading, setLoading] = useState(false);
  const [history, setHistory] = useState<string[]>(loadHistory);
  const [queryTime, setQueryTime] = useState<number | null>(null);
  const [editDoc, setEditDoc] = useState<string | null>(null);
  const [editOriginal, setEditOriginal] = useState<Record<string, unknown> | null>(null);
  const [saving, setSaving] = useState(false);
  const runRef = useRef<() => void>(() => {});
  const containerRef = useRef<HTMLDivElement>(null);
  const [splitPct, setSplitPct] = useState(45);
  const draggingRef = useRef(false);

  const onSplitterMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    draggingRef.current = true;
    const onMouseMove = (ev: MouseEvent) => {
      if (!draggingRef.current || !containerRef.current) return;
      const rect = containerRef.current.getBoundingClientRect();
      const pct = ((ev.clientY - rect.top) / rect.height) * 100;
      setSplitPct(Math.max(15, Math.min(85, pct)));
    };
    const onMouseUp = () => {
      draggingRef.current = false;
      document.removeEventListener("mousemove", onMouseMove);
      document.removeEventListener("mouseup", onMouseUp);
    };
    document.addEventListener("mousemove", onMouseMove);
    document.addEventListener("mouseup", onMouseUp);
  }, []);

  const run = useCallback(async () => {
    setLoading(true);
    setQueryTime(null);
    const start = performance.now();
    try {
      const cmd = JSON.parse(query);
      const resp = await executeRawCommand(cmd);
      setQueryTime(performance.now() - start);
      setResult(resp);
      const newHistory = [query, ...history.filter((h) => h !== query)].slice(0, 50);
      setHistory(newHistory);
      saveHistory(newHistory);
    } catch (e) {
      setQueryTime(performance.now() - start);
      toast(String(e), "error");
      setResult({ ok: false, error: String(e) } as unknown as JsonValue);
    } finally {
      setLoading(false);
    }
  }, [query, history, toast]);

  runRef.current = run;

  const getCollectionFromQuery = useCallback((): string | null => {
    try {
      const cmd = JSON.parse(query);
      return cmd.collection || null;
    } catch {
      return null;
    }
  }, [query]);

  const handleRowClick = useCallback((row: JsonValue) => {
    if (row && typeof row === "object" && !Array.isArray(row)) {
      const doc = row as Record<string, unknown>;
      setEditOriginal(doc);
      setEditDoc(JSON.stringify(doc, null, 2));
    }
  }, []);

  const handleSave = useCallback(async () => {
    if (!editDoc || !editOriginal) return;
    const collection = getCollectionFromQuery();
    if (!collection) {
      toast("Cannot determine collection from query. Ensure your command has a 'collection' field.", "error");
      return;
    }
    setSaving(true);
    try {
      const updated = JSON.parse(editDoc);
      const id = editOriginal._id;
      if (id === undefined) {
        toast("Document has no _id field, cannot update", "error");
        setSaving(false);
        return;
      }
      const resp = await executeRawCommand({
        cmd: "update",
        collection,
        query: { _id: id },
        update: { $set: updated },
      } as unknown as JsonValue);
      const respObj = resp as unknown as Record<string, unknown>;
      if (respObj.ok) {
        toast("Document updated", "success");
        setEditDoc(null);
        setEditOriginal(null);
        runRef.current();
      } else {
        toast(String(respObj.error || "Update failed"), "error");
      }
    } catch (e) {
      toast(String(e), "error");
    } finally {
      setSaving(false);
    }
  }, [editDoc, editOriginal, getCollectionFromQuery, toast]);

  // Cmd+Enter handler
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
        e.preventDefault();
        run();
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [run]);

  const resultData =
    result &&
    typeof result === "object" &&
    !Array.isArray(result) &&
    "data" in result
      ? (result as Record<string, unknown>).data
      : result;

  const isArray = Array.isArray(resultData);

  return (
    <div ref={containerRef} style={{ display: "flex", flexDirection: "column", height: "calc(100vh - var(--header-height) - 40px)" }}>
      {/* Top: Editor + History */}
      <div style={{ flex: `0 0 ${splitPct}%`, display: "flex", flexDirection: "column", minHeight: 0 }}>
        <div className="toolbar">
          <strong>Command</strong>
          {history.length > 0 && (
            <select
              style={{
                marginLeft: 12,
                fontSize: 12,
                padding: "2px 6px",
                background: "var(--bg-secondary)",
                color: "var(--text-secondary)",
                border: "1px solid var(--border-color)",
                borderRadius: "var(--radius-sm)",
                fontFamily: "var(--font-mono)",
                maxWidth: 300,
              }}
              value=""
              onChange={(e) => { if (e.target.value) setQuery(e.target.value); }}
            >
              <option value="">History ({history.length})</option>
              {history.map((h, i) => (
                <option key={i} value={h}>
                  {h.replace(/\n/g, " ").substring(0, 80)}
                </option>
              ))}
            </select>
          )}
          <div style={{ flex: 1 }} />
          <button className="btn btn-primary btn-sm" onClick={run} disabled={loading}>
            {loading ? <span className="spinner" /> : null}
            Run (⌘+Enter)
          </button>
        </div>
        <div style={{ flex: 1, minHeight: 0 }}>
          <JsonEditor value={query} onChange={setQuery} height="100%" />
        </div>
      </div>

      {/* Draggable Splitter */}
      <div
        onMouseDown={onSplitterMouseDown}
        style={{
          height: 6,
          flexShrink: 0,
          cursor: "row-resize",
          background: "var(--border-color)",
          borderRadius: 3,
          margin: "2px 0",
          transition: draggingRef.current ? "none" : "background 0.15s",
        }}
        onMouseOver={(e) => (e.currentTarget.style.background = "var(--accent)")}
        onMouseOut={(e) => { if (!draggingRef.current) e.currentTarget.style.background = "var(--border-color)"; }}
      />

      {/* Bottom: Results */}
      <div style={{ flex: 1, display: "flex", flexDirection: "column", minHeight: 0 }}>
        <div className="toolbar">
          <strong>Results</strong>
          {queryTime !== null && (
            <span style={{ marginLeft: 12, fontSize: 12, color: "var(--text-secondary)", fontFamily: "var(--font-mono)" }}>
              {queryTime.toFixed(1)} ms
            </span>
          )}
          {isArray && (
            <span style={{ marginLeft: 8, fontSize: 12, color: "var(--text-secondary)" }}>
              ({(resultData as JsonValue[]).length} docs)
            </span>
          )}
          <div style={{ flex: 1 }} />
          {isArray && (
            <>
              <button
                className={`btn btn-sm ${viewMode === "table" ? "btn-primary" : "btn-secondary"}`}
                onClick={() => setViewMode("table")}
              >
                Table
              </button>
              <button
                className={`btn btn-sm ${viewMode === "json" ? "btn-primary" : "btn-secondary"}`}
                onClick={() => setViewMode("json")}
              >
                JSON
              </button>
            </>
          )}
        </div>
        <div style={{ flex: 1, overflow: "auto" }}>
          {result === null ? (
            <div className="empty-state">Run a command to see results</div>
          ) : isArray && viewMode === "table" ? (
            <DataTable data={resultData as JsonValue[]} onRowClick={handleRowClick} />
          ) : (
            <JsonViewer data={resultData} />
          )}
        </div>
      </div>

      {/* Edit Document Dialog */}
      {editDoc !== null && (
        <div className="dialog-overlay" onClick={() => setEditDoc(null)}>
          <div
            className="dialog"
            style={{ width: 700, maxHeight: "80vh", display: "flex", flexDirection: "column" }}
            onClick={(e) => e.stopPropagation()}
          >
            <h3 style={{ margin: "0 0 12px" }}>Edit Document</h3>
            <div style={{ flex: 1, minHeight: 300 }}>
              <JsonEditor value={editDoc} onChange={(v) => setEditDoc(v)} height="300px" />
            </div>
            <div style={{ display: "flex", gap: 8, justifyContent: "flex-end", marginTop: 12 }}>
              <button className="btn btn-secondary btn-sm" onClick={() => setEditDoc(null)}>
                Cancel
              </button>
              <button className="btn btn-primary btn-sm" onClick={handleSave} disabled={saving}>
                {saving ? <span className="spinner" /> : null}
                Save
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
