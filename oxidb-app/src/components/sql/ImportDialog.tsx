import { useState, useMemo, useCallback } from "react";
import { open } from "@tauri-apps/plugin-dialog";
import { readFileText, runSql } from "../../api/tauri";
import type { JsonValue } from "../../api/types";
import { useToast } from "../common/Toast";
import {
  parseCsv,
  parseJson,
  inferType,
  coerceValue,
  type ParsedData,
} from "../../lib/importParse";

const TYPES = ["INT", "TEXT", "DOUBLE", "BOOL", "TIMESTAMP", "BLOB"];
const BATCH = 200;

function unwrap(resp: unknown): { ok: boolean; error?: string } {
  const r = resp as { ok?: boolean; error?: string };
  return { ok: r?.ok !== false, error: r?.error };
}

interface Props {
  tables: string[];
  onClose: () => void;
  onDone: () => void;
}

export function ImportDialog({ tables, onClose, onDone }: Props) {
  const toast = useToast();
  const [fileName, setFileName] = useState("");
  const [data, setData] = useState<ParsedData | null>(null);
  const [parseErr, setParseErr] = useState<string | null>(null);

  const [mode, setMode] = useState<"new" | "existing">("new");
  const [newName, setNewName] = useState("");
  const [existing, setExisting] = useState("");
  const [types, setTypes] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);
  const [progress, setProgress] = useState<string | null>(null);

  const pickFile = useCallback(async () => {
    const sel = await open({
      multiple: false,
      filters: [{ name: "Data", extensions: ["csv", "json"] }],
    });
    if (!sel || typeof sel !== "string") return;
    setParseErr(null);
    setData(null);
    try {
      const text = await readFileText(sel);
      const base = sel.split(/[/\\]/).pop() || sel;
      setFileName(base);
      const parsed = base.toLowerCase().endsWith(".json")
        ? parseJson(text)
        : parseCsv(text);
      if (parsed.columns.length === 0) throw new Error("no columns detected");
      setData(parsed);
      setTypes(parsed.columns.map((_, i) => inferType(parsed.rows.map((r) => r[i]))));
      setNewName(base.replace(/\.[^.]+$/, "").replace(/[^A-Za-z0-9_]/g, "_"));
    } catch (e) {
      setParseErr(String(e));
    }
  }, []);

  const preview = useMemo(() => (data ? data.rows.slice(0, 5) : []), [data]);

  const doImport = useCallback(async () => {
    if (!data) return;
    const target = mode === "new" ? newName.trim() : existing;
    if (!target) {
      toast(mode === "new" ? "Enter a table name" : "Pick a table", "error");
      return;
    }
    setBusy(true);
    setProgress("Preparing…");
    try {
      // 1. Create the table when importing into a new one.
      if (mode === "new") {
        const defs = data.columns.map((c, i) => `${c} ${types[i]}`).join(", ");
        const create = unwrap(await runSql(`CREATE TABLE ${target} (${defs})`));
        if (!create.ok) {
          toast(create.error || "create table failed", "error");
          setBusy(false);
          setProgress(null);
          return;
        }
      }
      // 2. Batch parameterized INSERTs.
      const cols = data.columns.join(", ");
      let done = 0;
      for (let off = 0; off < data.rows.length; off += BATCH) {
        const chunk = data.rows.slice(off, off + BATCH);
        const placeholders = chunk
          .map(() => `(${data.columns.map(() => "?").join(", ")})`)
          .join(", ");
        const params: JsonValue[] = [];
        for (const row of chunk) {
          data.columns.forEach((_, i) => params.push(coerceValue(row[i], types[i] || "TEXT") as JsonValue));
        }
        const resp = unwrap(
          await runSql(`INSERT INTO ${target} (${cols}) VALUES ${placeholders}`, params)
        );
        if (!resp.ok) {
          toast(`Row ${off + 1}: ${resp.error}`, "error");
          setProgress(`Stopped after ${done} rows`);
          setBusy(false);
          onDone();
          return;
        }
        done += chunk.length;
        setProgress(`Imported ${done} / ${data.rows.length}…`);
      }
      toast(`Imported ${done} row(s) into ${target}`, "success");
      onDone();
      onClose();
    } catch (e) {
      toast(String(e), "error");
    } finally {
      setBusy(false);
    }
  }, [data, mode, newName, existing, types, toast, onDone, onClose]);

  return (
    <div className="dialog-overlay">
      <div
        className="dialog"
        style={{ width: 720, maxHeight: "88vh", display: "flex", flexDirection: "column" }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="dialog-title">Import CSV / JSON</div>

        <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 12 }}>
          <button className="btn btn-secondary btn-sm" onClick={pickFile} disabled={busy}>
            Choose file…
          </button>
          <span style={{ fontSize: 13, color: "var(--text-secondary)", fontFamily: "var(--font-mono)" }}>
            {fileName || "no file selected"}
          </span>
        </div>

        {parseErr && (
          <p style={{ color: "var(--danger)", fontSize: 13 }}>{parseErr}</p>
        )}

        {data && (
          <>
            <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 8 }}>
              {data.columns.length} columns · {data.rows.length.toLocaleString()} rows
            </div>

            {/* Target selection */}
            <div style={{ display: "flex", gap: 16, marginBottom: 10 }}>
              <label className="remember-row" style={{ marginBottom: 0 }}>
                <input type="radio" checked={mode === "new"} onChange={() => setMode("new")} />
                New table
              </label>
              <label className="remember-row" style={{ marginBottom: 0 }}>
                <input
                  type="radio"
                  checked={mode === "existing"}
                  onChange={() => setMode("existing")}
                  disabled={tables.length === 0}
                />
                Existing table
              </label>
              {mode === "new" ? (
                <input
                  style={{ flex: 1, fontFamily: "var(--font-mono)" }}
                  value={newName}
                  onChange={(e) => setNewName(e.target.value)}
                  placeholder="table_name"
                />
              ) : (
                <select style={{ flex: 1 }} value={existing} onChange={(e) => setExisting(e.target.value)}>
                  <option value="">— pick a table —</option>
                  {tables.map((t) => (
                    <option key={t} value={t}>
                      {t}
                    </option>
                  ))}
                </select>
              )}
            </div>

            {/* Column preview + (new only) type editing */}
            <div style={{ overflow: "auto", flex: "0 1 auto", border: "1px solid var(--border-color)", borderRadius: "var(--radius-sm)" }}>
              <table className="data-table">
                <thead>
                  <tr>
                    {data.columns.map((c, i) => (
                      <th key={c}>
                        <div style={{ fontFamily: "var(--font-mono)" }}>{c}</div>
                        {mode === "new" && (
                          <select
                            style={{ marginTop: 2, fontSize: 11 }}
                            value={types[i]}
                            onChange={(e) =>
                              setTypes((ts) => ts.map((t, j) => (j === i ? e.target.value : t)))
                            }
                          >
                            {TYPES.map((t) => (
                              <option key={t} value={t}>
                                {t}
                              </option>
                            ))}
                          </select>
                        )}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {preview.map((row, r) => (
                    <tr key={r}>
                      {row.map((v, c) => (
                        <td key={c} style={{ fontFamily: "var(--font-mono)" }}>
                          {v === null ? <span style={{ color: "var(--text-secondary)", fontStyle: "italic" }}>null</span> : v}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {mode === "existing" && (
              <p style={{ fontSize: 11, color: "var(--text-secondary)", margin: "6px 0 0" }}>
                Columns are matched by name; the table must already have these columns.
              </p>
            )}
          </>
        )}

        <div className="dialog-actions" style={{ marginTop: 12 }}>
          {progress && (
            <span style={{ marginRight: "auto", fontSize: 12, color: "var(--text-secondary)" }}>
              {progress}
            </span>
          )}
          <button className="btn btn-secondary" onClick={onClose} disabled={busy}>
            Cancel
          </button>
          <button className="btn btn-primary" onClick={doImport} disabled={!data || busy}>
            {busy ? <span className="spinner" /> : null}
            Import
          </button>
        </div>
      </div>
    </div>
  );
}
