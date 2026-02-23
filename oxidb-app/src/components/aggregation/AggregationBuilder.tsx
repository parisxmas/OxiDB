import { useState, useCallback, useEffect } from "react";
import { listCollections, runAggregation } from "../../api/tauri";
import type { JsonValue } from "../../api/types";
import { JsonEditor } from "../common/JsonEditor";
import { DataTable } from "../common/DataTable";
import { JsonViewer } from "../common/JsonViewer";
import { useToast } from "../common/Toast";

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
];

interface Stage {
  id: number;
  type: string;
  json: string;
}

let nextStageId = 1;

export function AggregationBuilder() {
  const toast = useToast();
  const [collections, setCollections] = useState<string[]>([]);
  const [selected, setSelected] = useState("");
  const [stages, setStages] = useState<Stage[]>([]);
  const [results, setResults] = useState<JsonValue[] | null>(null);
  const [viewMode, setViewMode] = useState<"table" | "json">("table");
  const [loading, setLoading] = useState(false);

  const loadCollections = useCallback(async () => {
    try {
      const names = await listCollections();
      setCollections(names.sort());
    } catch (e) {
      toast(String(e), "error");
    }
  }, [toast]);

  useEffect(() => { loadCollections(); }, [loadCollections]);

  const addStage = (type: string) => {
    const defaults: Record<string, string> = {
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
    };
    setStages((prev) => [
      ...prev,
      { id: nextStageId++, type, json: defaults[type] || "{}" },
    ]);
  };

  const removeStage = (id: number) => {
    setStages((prev) => prev.filter((s) => s.id !== id));
  };

  const updateStage = (id: number, json: string) => {
    setStages((prev) =>
      prev.map((s) => (s.id === id ? { ...s, json } : s))
    );
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
    setLoading(true);
    try {
      const pipeline = buildPipeline();
      const data = await runAggregation(selected, pipeline);
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
            stages.map((stage) => (
              <div key={stage.id} className="stage-card">
                <div className="stage-card-header">
                  <span className="stage-type">{stage.type}</span>
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
            ))
          )}
        </div>

        {stages.length > 0 && (
          <div style={{ marginTop: 8, padding: 8, background: "var(--bg-tertiary)", borderRadius: "var(--radius-sm)", fontSize: 12, fontFamily: "var(--font-mono)", maxHeight: 120, overflow: "auto" }}>
            <div style={{ fontSize: 11, color: "var(--text-secondary)", fontWeight: 600, marginBottom: 4 }}>PIPELINE JSON</div>
            {JSON.stringify(buildPipeline(), null, 2)}
          </div>
        )}
      </div>

      {/* Results */}
      <div style={{ flex: 1, display: "flex", flexDirection: "column" }}>
        <div className="toolbar">
          <strong>Results</strong>
          <div style={{ flex: 1 }} />
          {results && (
            <>
              <span style={{ color: "var(--text-secondary)", fontSize: 13 }}>
                {results.length} documents
              </span>
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
