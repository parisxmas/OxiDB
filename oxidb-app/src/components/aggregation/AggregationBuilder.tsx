import { useState, useCallback, useEffect } from "react";
import { listCollections, runAggregation } from "../../api/tauri";
import type { JsonValue } from "../../api/types";
import { JsonEditor } from "../common/JsonEditor";
import { DataTable } from "../common/DataTable";
import { JsonViewer } from "../common/JsonViewer";
import { useToast } from "../common/Toast";
import { useDatabase } from "../../context/DatabaseContext";

const STAGE_TYPES = [
  "$match",
  "$group",
  "$sort",
  "$project",
  "$limit",
  "$skip",
  "$count",
  "$unwind",
  "$addFields",
  "$lookup",
  "$facet",
  "$setWindowFields",
  "$dateHistogram",
];

const STAGE_DEFAULTS: Record<string, string> = {
  "$match": "{}",
  "$group": '{"_id": "$field", "count": {"$sum": 1}}',
  "$sort": '{"field": 1}',
  "$project": '{"field": 1}',
  "$limit": "10",
  "$skip": "0",
  "$count": '"total"',
  "$unwind": '"$field"',
  "$addFields": '{"newField": "value"}',
  "$lookup": '{"from": "other", "localField": "fk", "foreignField": "_id", "as": "joined"}',
  "$facet": '{"a": [{"$match": {}}], "b": [{"$count": "n"}]}',
  "$setWindowFields": '{"partitionBy": "$field", "sortBy": {"ts": 1}, "output": {"run": {"$sum": 1, "window": {"documents": ["unbounded", "current"]}}}}',
  "$dateHistogram": '{"field": "$ts", "interval": "1h", "output": "count"}',
};

interface Stage {
  id: number;
  type: string;
  json: string;
}

let nextStageId = 1;

/** true if the stage body parses as JSON. */
function stageValid(json: string): boolean {
  try {
    JSON.parse(json);
    return true;
  } catch {
    return false;
  }
}

export function AggregationBuilder() {
  const toast = useToast();
  const { db } = useDatabase();
  const [collections, setCollections] = useState<string[]>([]);
  const [selected, setSelected] = useState("");
  const [stages, setStages] = useState<Stage[]>([]);
  const [results, setResults] = useState<JsonValue[] | null>(null);
  const [viewMode, setViewMode] = useState<"table" | "json">("table");
  const [loading, setLoading] = useState(false);
  const [elapsed, setElapsed] = useState<number | null>(null);

  const loadCollections = useCallback(async () => {
    try {
      const names = (await listCollections()).filter((n) => n && !n.startsWith("_"));
      setCollections(names.sort());
    } catch (e) {
      toast(String(e), "error");
    }
  }, [toast]);

  useEffect(() => {
    setSelected("");
    setResults(null);
    loadCollections();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loadCollections, db]);

  const addStage = (type: string) => {
    setStages((prev) => [
      ...prev,
      { id: nextStageId++, type, json: STAGE_DEFAULTS[type] || "{}" },
    ]);
  };

  const removeStage = (id: number) => {
    setStages((prev) => prev.filter((s) => s.id !== id));
  };

  const updateStage = (id: number, json: string) => {
    setStages((prev) => prev.map((s) => (s.id === id ? { ...s, json } : s)));
  };

  const moveStage = (idx: number, dir: -1 | 1) => {
    setStages((prev) => {
      const next = [...prev];
      const j = idx + dir;
      if (j < 0 || j >= next.length) return prev;
      [next[idx], next[j]] = [next[j], next[idx]];
      return next;
    });
  };

  const copyPipeline = () => {
    navigator.clipboard.writeText(JSON.stringify(buildPipeline(), null, 2));
    toast("Pipeline copied", "success");
  };

  const exportResults = () => {
    if (!results) return;
    const blob = new Blob([JSON.stringify(results, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${selected || "aggregation"}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const buildPipeline = (): JsonValue[] => {
    return stages.map((s) => {
      try {
        const val = JSON.parse(s.json);
        return { [s.type]: val };
      } catch {
        return { [s.type]: {} };
      }
    });
  };

  const handleRun = async () => {
    if (!selected) {
      toast("Select a collection first", "error");
      return;
    }
    const bad = stages.find((s) => !stageValid(s.json));
    if (bad) {
      toast(`Invalid JSON in a ${bad.type} stage`, "error");
      return;
    }
    setLoading(true);
    setElapsed(null);
    const start = performance.now();
    try {
      const pipeline = buildPipeline();
      const data = await runAggregation(selected, pipeline);
      setElapsed(performance.now() - start);
      setResults(data);
    } catch (e) {
      toast(String(e), "error");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ display: "flex", gap: 16, height: "calc(100vh - var(--header-height) - 40px)" }}>
      {/* Pipeline builder */}
      <div style={{ width: 400, display: "flex", flexDirection: "column", flexShrink: 0 }}>
        <div className="toolbar">
          <select
            value={selected}
            onChange={(e) => setSelected(e.target.value)}
            style={{ flex: 1, minWidth: 120 }}
          >
            <option value="">Collection...</option>
            {collections.map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
          <button className="btn btn-primary btn-sm" onClick={handleRun} disabled={loading}>
            {loading ? <span className="spinner" /> : "Run"}
          </button>
        </div>

        <div style={{ marginBottom: 8 }}>
          <div style={{ fontSize: 12, color: "var(--text-secondary)", fontWeight: 600, marginBottom: 4 }}>
            ADD STAGE
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
            {STAGE_TYPES.map((type) => (
              <button
                key={type}
                className="btn btn-secondary btn-sm"
                onClick={() => addStage(type)}
              >
                {type}
              </button>
            ))}
          </div>
        </div>

        <div style={{ flex: 1, overflow: "auto" }}>
          {stages.length === 0 ? (
            <div className="empty-state" style={{ padding: 24 }}>
              Add stages to build a pipeline
            </div>
          ) : (
            stages.map((stage, idx) => {
              const valid = stageValid(stage.json);
              return (
                <div key={stage.id} className="stage-card">
                  <div className="stage-card-header">
                    <span style={{ color: "var(--text-secondary)", fontSize: 11, marginRight: 6 }}>{idx + 1}</span>
                    <span className="stage-type">{stage.type}</span>
                    {!valid && <span title="Invalid JSON" style={{ color: "var(--danger)", marginLeft: 6 }}>⚠</span>}
                    <div style={{ flex: 1 }} />
                    <button className="stage-move" title="Move up" disabled={idx === 0} onClick={() => moveStage(idx, -1)}>↑</button>
                    <button className="stage-move" title="Move down" disabled={idx === stages.length - 1} onClick={() => moveStage(idx, 1)}>↓</button>
                    <button
                      className="btn btn-sm"
                      style={{ padding: "2px 6px", color: "var(--danger)", background: "none" }}
                      onClick={() => removeStage(stage.id)}
                    >
                      ×
                    </button>
                  </div>
                  <JsonEditor
                    value={stage.json}
                    onChange={(v) => updateStage(stage.id, v)}
                    height="80px"
                  />
                </div>
              );
            })
          )}
        </div>

        {stages.length > 0 && (
          <div style={{ marginTop: 8, padding: 8, background: "var(--bg-tertiary)", borderRadius: "var(--radius-sm)", fontSize: 12, fontFamily: "var(--font-mono)", maxHeight: 120, overflow: "auto" }}>
            <div style={{ display: "flex", alignItems: "center", marginBottom: 4 }}>
              <span style={{ fontSize: 11, color: "var(--text-secondary)", fontWeight: 600 }}>PIPELINE JSON</span>
              <div style={{ flex: 1 }} />
              <button className="stage-move" title="Copy" onClick={copyPipeline}>⧉</button>
              <button className="stage-move" title="Clear all stages" onClick={() => setStages([])}>×</button>
            </div>
            {JSON.stringify(buildPipeline(), null, 2)}
          </div>
        )}
      </div>

      {/* Results */}
      <div style={{ flex: 1, display: "flex", flexDirection: "column" }}>
        <div className="toolbar">
          <strong>Results</strong>
          {elapsed !== null && (
            <span style={{ marginLeft: 10, fontSize: 12, color: "var(--text-secondary)", fontFamily: "var(--font-mono)" }}>
              {elapsed.toFixed(1)} ms
            </span>
          )}
          <div style={{ flex: 1 }} />
          {results && (
            <>
              <span style={{ color: "var(--text-secondary)", fontSize: 13 }}>
                {results.length} docs
              </span>
              <button className="btn btn-secondary btn-sm" onClick={exportResults} disabled={results.length === 0}>
                Export
              </button>
              <button
                className={`btn btn-sm ${viewMode === "table" ? "btn-primary" : "btn-secondary"}`}
                onClick={() => setViewMode("table")}
              >Table</button>
              <button
                className={`btn btn-sm ${viewMode === "json" ? "btn-primary" : "btn-secondary"}`}
                onClick={() => setViewMode("json")}
              >JSON</button>
            </>
          )}
        </div>
        <div style={{ flex: 1, overflow: "auto" }}>
          {results === null ? (
            <div className="empty-state">Run the pipeline to see results</div>
          ) : viewMode === "table" ? (
            <DataTable data={results} />
          ) : (
            <JsonViewer data={results} />
          )}
        </div>
      </div>
    </div>
  );
}
