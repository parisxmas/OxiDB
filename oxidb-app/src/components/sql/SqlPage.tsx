import { useState, useCallback, useRef } from "react";
import type { editor as MonacoEditor } from "monaco-editor";
import { runSql } from "../../api/tauri";
import type { JsonValue } from "../../api/types";
import { SqlEditor } from "../common/SqlEditor";
import { SchemaTree } from "./SchemaTree";
import { TableDataView } from "./TableDataView";
import { DataTable } from "../common/DataTable";
import { useToast } from "../common/Toast";
import { useConnection } from "../../context/ConnectionContext";

const HISTORY_KEY = "oxidb-sql-history";

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

interface StmtResult {
  columns?: string[];
  types?: (string | null)[];
  rows?: JsonValue[][];
  affected?: number;
  last_insert_id?: number;
  ddl?: boolean;
  transaction?: boolean;
}

/** Turn a {columns, rows} result into row objects the DataTable understands. */
function toRowObjects(r: StmtResult): Record<string, JsonValue>[] {
  const cols = r.columns || [];
  return (r.rows || []).map((row) => {
    const obj: Record<string, JsonValue> = {};
    cols.forEach((c, i) => (obj[c] = row[i]));
    return obj;
  });
}

function summarize(r: StmtResult): string {
  if (r.columns) return `${r.rows?.length ?? 0} row(s)`;
  if (r.affected !== undefined)
    return `${r.affected} row(s) affected${
      r.last_insert_id !== undefined ? `, last id ${r.last_insert_id}` : ""
    }`;
  if (r.ddl) return "OK — schema changed";
  if (r.transaction) return "OK — transaction";
  return "OK";
}

export function SqlPage() {
  const toast = useToast();
  const { status } = useConnection();
  const [sql, setSql] = useState(
    "SELECT name FROM sqlite_schema;\n-- ⌘/Ctrl+Enter to run"
  );
  const [results, setResults] = useState<StmtResult[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [elapsed, setElapsed] = useState<number | null>(null);
  const [active, setActive] = useState(0);
  const [history, setHistory] = useState<string[]>(loadHistory);
  const [schemaKey, setSchemaKey] = useState(0);
  const [browseTable, setBrowseTable] = useState<string | null>(null);
  const [resultTab, setResultTab] = useState<"query" | "data">("query");
  const containerRef = useRef<HTMLDivElement>(null);
  const editorRef = useRef<MonacoEditor.IStandaloneCodeEditor | null>(null);
  const [splitPct, setSplitPct] = useState(42);
  const draggingRef = useRef(false);

  /** Insert an identifier at the cursor (schema-tree click). */
  const insertAtCursor = useCallback((text: string) => {
    const ed = editorRef.current;
    if (!ed) {
      setSql((s) => s + text);
      return;
    }
    const sel = ed.getSelection();
    if (sel) {
      ed.executeEdits("schema-insert", [{ range: sel, text, forceMoveMarkers: true }]);
    }
    ed.focus();
  }, []);

  const onSplitterMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    draggingRef.current = true;
    const onMove = (ev: MouseEvent) => {
      if (!draggingRef.current || !containerRef.current) return;
      const rect = containerRef.current.getBoundingClientRect();
      const pct = ((ev.clientY - rect.top) / rect.height) * 100;
      setSplitPct(Math.max(15, Math.min(85, pct)));
    };
    const onUp = () => {
      draggingRef.current = false;
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  }, []);

  const run = useCallback(async () => {
    const text = sql.trim();
    if (!text) return;
    setResultTab("query");
    setLoading(true);
    setError(null);
    const start = performance.now();
    try {
      const resp = (await runSql(text)) as unknown as {
        ok?: boolean;
        error?: string;
        data?: StmtResult[];
      };
      setElapsed(performance.now() - start);
      if (resp && resp.ok === false) {
        setError(resp.error || "query failed");
        setResults(null);
      } else {
        const data = (resp?.data as StmtResult[]) || [];
        setResults(data);
        setActive(data.length - 1); // focus the last statement's result
        // A DDL statement changed the catalog — refresh the schema tree.
        if (data.some((d) => d.ddl)) setSchemaKey((k) => k + 1);
        const nh = [text, ...history.filter((h) => h !== text)].slice(0, 50);
        setHistory(nh);
        saveHistory(nh);
      }
    } catch (e) {
      setElapsed(performance.now() - start);
      setError(String(e));
      setResults(null);
      toast(String(e), "error");
    } finally {
      setLoading(false);
    }
  }, [sql, history, toast]);

  const cur = results && results[active] ? results[active] : null;
  const isSelect = !!cur?.columns;

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "row",
        height: "calc(100vh - var(--header-height) - 40px)",
        gap: 0,
      }}
    >
      {/* Left: schema tree */}
      <SchemaTree
        refreshKey={schemaKey}
        onInsert={insertAtCursor}
        onQuery={(q) => setSql(q)}
        onBrowse={(t) => {
          setBrowseTable(t);
          setResultTab("data");
        }}
      />

      {/* Right: editor + results (the original vertical split) */}
      <div
        ref={containerRef}
        style={{
          flex: 1,
          minWidth: 0,
          display: "flex",
          flexDirection: "column",
        }}
      >
      {/* Editor */}
      <div
        style={{
          flex: `0 0 ${splitPct}%`,
          display: "flex",
          flexDirection: "column",
          minHeight: 0,
        }}
      >
        <div className="toolbar">
          <strong>SQL</strong>
          <span
            style={{
              marginLeft: 10,
              fontSize: 11,
              color: "var(--text-secondary)",
              fontFamily: "var(--font-mono)",
            }}
          >
            {status.mode === "client" ? status.detail : "not on a server"}
          </span>
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
                maxWidth: 320,
              }}
              value=""
              onChange={(e) => {
                if (e.target.value) setSql(e.target.value);
              }}
            >
              <option value="">History ({history.length})</option>
              {history.map((h, i) => (
                <option key={i} value={h}>
                  {h.replace(/\n/g, " ").substring(0, 90)}
                </option>
              ))}
            </select>
          )}
          <div style={{ flex: 1 }} />
          <button
            className="btn btn-primary btn-sm"
            onClick={run}
            disabled={loading}
          >
            {loading ? <span className="spinner" /> : null}
            Run (⌘+Enter)
          </button>
        </div>
        <div style={{ flex: 1, minHeight: 0 }}>
          <SqlEditor
            value={sql}
            onChange={setSql}
            onRun={run}
            onReady={(ed) => (editorRef.current = ed)}
            height="100%"
          />
        </div>
      </div>

      {/* Splitter */}
      <div
        onMouseDown={onSplitterMouseDown}
        style={{
          height: 6,
          flexShrink: 0,
          cursor: "row-resize",
          background: "var(--border-color)",
          borderRadius: 3,
          margin: "2px 0",
        }}
      />

      {/* Results */}
      <div
        style={{
          flex: 1,
          display: "flex",
          flexDirection: "column",
          minHeight: 0,
        }}
      >
        <div className="toolbar">
          <button
            className={`result-tab${resultTab === "query" ? " active" : ""}`}
            onClick={() => setResultTab("query")}
          >
            Query Result
          </button>
          {browseTable && (
            <button
              className={`result-tab${resultTab === "data" ? " active" : ""}`}
              onClick={() => setResultTab("data")}
            >
              Data: <span style={{ fontFamily: "var(--font-mono)" }}>{browseTable}</span>
              <span
                className="result-tab-close"
                title="Close"
                onClick={(e) => {
                  e.stopPropagation();
                  setBrowseTable(null);
                  setResultTab("query");
                }}
              >
                ✕
              </span>
            </button>
          )}
          {resultTab === "query" && elapsed !== null && (
            <span
              style={{
                marginLeft: 12,
                fontSize: 12,
                color: "var(--text-secondary)",
                fontFamily: "var(--font-mono)",
              }}
            >
              {elapsed.toFixed(1)} ms
            </span>
          )}
          {resultTab === "query" && cur && (
            <span
              style={{
                marginLeft: 8,
                fontSize: 12,
                color: "var(--text-secondary)",
              }}
            >
              {summarize(cur)}
            </span>
          )}
          <div style={{ flex: 1 }} />
          {resultTab === "query" && results && results.length > 1 && (
            <div style={{ display: "flex", gap: 4 }}>
              {results.map((_, i) => (
                <button
                  key={i}
                  className={`btn btn-sm ${
                    i === active ? "btn-primary" : "btn-secondary"
                  }`}
                  onClick={() => setActive(i)}
                >
                  #{i + 1}
                </button>
              ))}
            </div>
          )}
        </div>
        <div style={{ flex: 1, overflow: "auto" }}>
          {resultTab === "data" && browseTable ? (
            <TableDataView key={browseTable} table={browseTable} />
          ) : error ? (
            <div
              style={{
                padding: 16,
                color: "var(--danger)",
                fontFamily: "var(--font-mono)",
                fontSize: 13,
                whiteSpace: "pre-wrap",
              }}
            >
              {error}
            </div>
          ) : !cur ? (
            <div className="empty-state">Run a statement to see results</div>
          ) : isSelect ? (
            <DataTable data={toRowObjects(cur) as JsonValue[]} />
          ) : (
            <div className="empty-state">{summarize(cur)}</div>
          )}
        </div>
      </div>
      </div>
    </div>
  );
}
