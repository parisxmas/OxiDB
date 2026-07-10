import { useState, useCallback, useRef, useEffect } from "react";
import { listen } from "@tauri-apps/api/event";
import type { editor as MonacoEditor } from "monaco-editor";
import { runSql } from "../../api/tauri";
import type { JsonValue } from "../../api/types";
import { SqlEditor } from "../common/SqlEditor";
import { SchemaTree } from "./SchemaTree";
import { TableDataView } from "./TableDataView";
import { EditableResultGrid } from "./EditableResultGrid";
import { Pagination } from "../common/Pagination";
import { useToast } from "../common/Toast";
import { useConnection } from "../../context/ConnectionContext";
import {
  useSqlSession,
  newTab,
  type StmtResult,
  type QueryTab,
} from "../../context/SqlSessionContext";

const HISTORY_KEY = "oxidb-sql-history";

function saveHistory(items: string[]) {
  localStorage.setItem(HISTORY_KEY, JSON.stringify(items.slice(0, 50)));
}

function summarize(r: StmtResult): string {
  if (r.columns) return `${r.rows?.length ?? 0} row(s)`;
  if (r.affected !== undefined)
    return `${r.affected} row(s) affected${r.last_insert_id !== undefined ? `, last id ${r.last_insert_id}` : ""}`;
  if (r.ddl) return "OK — schema changed";
  if (r.transaction) return "OK — transaction";
  return "OK";
}

export function SqlPage() {
  const toast = useToast();
  const { status } = useConnection();

  // Session state (tabs, active tab, history) lives above the router so it
  // survives navigating to another view and back.
  const { tabs, setTabs, activeIdx, setActiveIdx, allocId, history, setHistory } =
    useSqlSession();
  const [schemaKey, setSchemaKey] = useState(0);

  const containerRef = useRef<HTMLDivElement>(null);
  const editorRef = useRef<MonacoEditor.IStandaloneCodeEditor | null>(null);
  const [splitPct, setSplitPct] = useState(42);
  const [treeWidth, setTreeWidth] = useState<number>(() => {
    const v = parseInt(localStorage.getItem("oxidb-tree-width") || "260", 10);
    return Number.isFinite(v) ? v : 260;
  });
  const draggingRef = useRef(false);
  const treeWidthRef = useRef(treeWidth);
  treeWidthRef.current = treeWidth;

  const t = tabs[activeIdx] ?? tabs[0];

  /** Patch a tab by id (safe across async + tab switches). */
  const patch = useCallback((id: number, p: Partial<QueryTab>) => {
    setTabs((ts) => ts.map((tab) => (tab.id === id ? { ...tab, ...p } : tab)));
  }, []);
  /** Patch the currently active tab. */
  const patchActive = useCallback(
    (p: Partial<QueryTab>) => patch(t.id, p),
    [patch, t.id]
  );

  // ── Tab management ────────────────────────────────────────────────
  const addTab = useCallback(() => {
    const id = allocId();
    setTabs((ts) => {
      setActiveIdx(ts.length); // focus the appended tab
      return [...ts, newTab(id)];
    });
  }, [allocId, setTabs, setActiveIdx]);

  /** Open a table's data view in a NEW tab (double-clicking a table), instead
   *  of replacing whatever the active tab is showing. */
  const openBrowseTab = useCallback(
    (tbl: string) => {
      const id = allocId();
      setTabs((ts) => {
        setActiveIdx(ts.length);
        return [
          ...ts,
          {
            ...newTab(id),
            name: tbl,
            // A useful starting query for the table (editable/runnable), while
            // the data view shows its rows right away.
            sql: `SELECT * FROM ${tbl} LIMIT 100;`,
            browseTable: tbl,
            resultTab: "data" as const,
          },
        ];
      });
    },
    [allocId, setTabs, setActiveIdx]
  );

  const closeTab = useCallback(
    (idx: number) => {
      setTabs((ts) => {
        if (ts.length === 1) return ts; // keep at least one
        const next = ts.filter((_, i) => i !== idx);
        setActiveIdx((cur) => (cur >= next.length ? next.length - 1 : cur > idx ? cur - 1 : cur));
        return next;
      });
    },
    []
  );

  // ── Splitters ─────────────────────────────────────────────────────
  const onTreeResize = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      const startX = e.clientX;
      const startW = treeWidth;
      const onMove = (ev: MouseEvent) => setTreeWidth(Math.max(160, Math.min(640, startW + ev.clientX - startX)));
      const onUp = () => {
        document.removeEventListener("mousemove", onMove);
        document.removeEventListener("mouseup", onUp);
        localStorage.setItem("oxidb-tree-width", String(treeWidthRef.current));
      };
      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    },
    [treeWidth]
  );

  const onSplitterMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    draggingRef.current = true;
    const onMove = (ev: MouseEvent) => {
      if (!draggingRef.current || !containerRef.current) return;
      const rect = containerRef.current.getBoundingClientRect();
      setSplitPct(Math.max(15, Math.min(85, ((ev.clientY - rect.top) / rect.height) * 100)));
    };
    const onUp = () => {
      draggingRef.current = false;
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  }, []);

  const insertAtCursor = useCallback(
    (text: string) => {
      const ed = editorRef.current;
      if (!ed) {
        patchActive({ sql: t.sql + text });
        return;
      }
      const sel = ed.getSelection();
      if (sel) ed.executeEdits("schema-insert", [{ range: sel, text, forceMoveMarkers: true }]);
      ed.focus();
    },
    [patchActive, t.sql]
  );

  // ── Run ───────────────────────────────────────────────────────────
  const run = useCallback(async () => {
    const tab = tabs[activeIdx];
    if (!tab) return;
    const text = tab.sql.trim();
    if (!text) return;
    const id = tab.id;
    patch(id, { resultTab: "query", page: 0, loading: true, error: null });
    const start = performance.now();
    try {
      const resp = (await runSql(text)) as unknown as { ok?: boolean; error?: string; data?: StmtResult[] };
      const elapsed = performance.now() - start;
      if (resp && resp.ok === false) {
        patch(id, { elapsed, error: resp.error || "query failed", results: null, loading: false });
      } else {
        const data = (resp?.data as StmtResult[]) || [];
        patch(id, { elapsed, results: data, active: data.length - 1, loading: false });
        if (data.some((d) => d.ddl)) setSchemaKey((k) => k + 1);
        const nh = [text, ...history.filter((h) => h !== text)].slice(0, 50);
        setHistory(nh);
        saveHistory(nh);
      }
    } catch (e) {
      patch(id, { elapsed: performance.now() - start, error: String(e), results: null, loading: false });
      toast(String(e), "error");
    }
  }, [tabs, activeIdx, history, toast, patch]);

  // F5 arrives as a backend event (a global shortcut, so it works on macOS
  // without holding Fn). Only the SQL page listens, so F5 only runs here.
  useEffect(() => {
    const un = listen("run-sql-shortcut", () => {
      void run();
    });
    return () => {
      void un.then((f) => f());
    };
  }, [run]);

  const cur = t.results && t.results[t.active] ? t.results[t.active] : null;
  const isSelect = !!cur?.columns;
  const totalRows = isSelect ? cur!.rows?.length ?? 0 : 0;
  const pagedResult: StmtResult | null =
    cur && isSelect
      ? { columns: cur.columns, types: cur.types, rows: (cur.rows || []).slice(t.page * t.pageSize, (t.page + 1) * t.pageSize) }
      : null;
  const pagedCount = pagedResult?.rows?.length ?? 0;

  return (
    <div style={{ display: "flex", flexDirection: "row", height: "calc(100vh - var(--header-height) - 40px)" }}>
      {/* Left: schema tree (resizable) */}
      <div style={{ width: treeWidth, flexShrink: 0, minWidth: 0, display: "flex" }}>
        <SchemaTree
          refreshKey={schemaKey}
          onInsert={insertAtCursor}
          onQuery={(q) => patchActive({ sql: q })}
          onBrowse={openBrowseTab}
        />
      </div>

      <div className="col-resizer" onMouseDown={onTreeResize} title="Drag to resize" />

      {/* Right: query tabs + editor + results */}
      <div ref={containerRef} style={{ flex: 1, minWidth: 0, display: "flex", flexDirection: "column" }}>
        {/* Query tab bar */}
        <div className="query-tabs">
          {tabs.map((tab, i) => (
            <div
              key={tab.id}
              className={`query-tab${i === activeIdx ? " active" : ""}`}
              onClick={() => setActiveIdx(i)}
              title={tab.name}
            >
              <span className="query-tab-name">{tab.name}</span>
              {tabs.length > 1 && (
                <span
                  className="query-tab-close"
                  title="Close"
                  onClick={(e) => {
                    e.stopPropagation();
                    closeTab(i);
                  }}
                >
                  ✕
                </span>
              )}
            </div>
          ))}
          <button className="query-tab-new" title="New query" onClick={addTab}>
            +
          </button>
        </div>

        {/* Editor */}
        <div style={{ flex: `0 0 ${splitPct}%`, display: "flex", flexDirection: "column", minHeight: 0 }}>
          <div className="toolbar">
            <strong>SQL</strong>
            <span style={{ marginLeft: 10, fontSize: 11, color: "var(--text-secondary)", fontFamily: "var(--font-mono)" }}>
              {status.mode === "client" ? status.detail : "not on a server"}
            </span>
            {history.length > 0 && (
              <select
                style={{ marginLeft: 12, fontSize: 12, padding: "2px 6px", background: "var(--bg-secondary)", color: "var(--text-secondary)", border: "1px solid var(--border-color)", borderRadius: "var(--radius-sm)", fontFamily: "var(--font-mono)", maxWidth: 320 }}
                value=""
                onChange={(e) => { if (e.target.value) patchActive({ sql: e.target.value }); }}
              >
                <option value="">History ({history.length})</option>
                {history.map((h, i) => (
                  <option key={i} value={h}>{h.replace(/\n/g, " ").substring(0, 90)}</option>
                ))}
              </select>
            )}
            <div style={{ flex: 1 }} />
            <button className="btn btn-primary btn-sm" onClick={run} disabled={t.loading}>
              {t.loading ? <span className="spinner" /> : null}
              Run (F5)
            </button>
          </div>
          <div style={{ flex: 1, minHeight: 0 }}>
            <SqlEditor
              key={t.id}
              value={t.sql}
              onChange={(v) => patchActive({ sql: v })}
              onRun={run}
              onReady={(ed) => (editorRef.current = ed)}
              height="100%"
            />
          </div>
        </div>

        {/* Splitter */}
        <div
          onMouseDown={onSplitterMouseDown}
          style={{ height: 6, flexShrink: 0, cursor: "row-resize", background: "var(--border-color)", borderRadius: 3, margin: "2px 0" }}
        />

        {/* Results */}
        <div style={{ flex: 1, display: "flex", flexDirection: "column", minHeight: 0 }}>
          <div className="toolbar">
            <button className={`result-tab${t.resultTab === "query" ? " active" : ""}`} onClick={() => patchActive({ resultTab: "query" })}>
              Query Result
            </button>
            {t.browseTable && (
              <button className={`result-tab${t.resultTab === "data" ? " active" : ""}`} onClick={() => patchActive({ resultTab: "data" })}>
                Data: <span style={{ fontFamily: "var(--font-mono)" }}>{t.browseTable}</span>
                <span
                  className="result-tab-close"
                  title="Close"
                  onClick={(e) => { e.stopPropagation(); patchActive({ browseTable: null, resultTab: "query" }); }}
                >
                  ✕
                </span>
              </button>
            )}
            {t.resultTab === "query" && t.elapsed !== null && (
              <span style={{ marginLeft: 12, fontSize: 12, color: "var(--text-secondary)", fontFamily: "var(--font-mono)" }}>{t.elapsed.toFixed(1)} ms</span>
            )}
            {t.resultTab === "query" && cur && (
              <span style={{ marginLeft: 8, fontSize: 12, color: "var(--text-secondary)" }}>{summarize(cur)}</span>
            )}
            <div style={{ flex: 1 }} />
            {t.resultTab === "query" && t.results && t.results.length > 1 && (
              <div style={{ display: "flex", gap: 4 }}>
                {t.results.map((_, i) => (
                  <button
                    key={i}
                    className={`btn btn-sm ${i === t.active ? "btn-primary" : "btn-secondary"}`}
                    onClick={() => patchActive({ active: i, page: 0 })}
                  >
                    #{i + 1}
                  </button>
                ))}
              </div>
            )}
          </div>
          {/* Procedure print() output (notices) */}
          {t.resultTab === "query" && cur?.notices && cur.notices.length > 0 && (
            <div className="notices">
              {cur.notices.map((n, i) => (
                <div key={i} className="notice-line">
                  <span className="notice-tag">NOTICE</span>
                  {n}
                </div>
              ))}
            </div>
          )}
          <div style={{ flex: 1, overflow: "auto" }}>
            {t.resultTab === "data" && t.browseTable ? (
              <TableDataView key={t.browseTable} table={t.browseTable} />
            ) : t.error ? (
              <div style={{ padding: 16, color: "var(--danger)", fontFamily: "var(--font-mono)", fontSize: 13, whiteSpace: "pre-wrap" }}>{t.error}</div>
            ) : !cur ? (
              <div className="empty-state">Run a statement to see results</div>
            ) : isSelect && pagedResult ? (
              <EditableResultGrid result={pagedResult} sql={t.sql} />
            ) : (
              <div className="empty-state">{summarize(cur)}</div>
            )}
          </div>
          {t.resultTab === "query" && isSelect && totalRows > t.pageSize && (
            <Pagination
              page={t.page}
              pageSize={t.pageSize}
              total={totalRows}
              currentCount={pagedCount}
              onPage={(p) => patchActive({ page: p })}
              onPageSize={(s) => patchActive({ pageSize: s, page: 0 })}
            />
          )}
        </div>
      </div>
    </div>
  );
}
