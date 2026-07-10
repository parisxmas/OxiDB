import { useState, useEffect, useMemo, useCallback } from "react";
import Editor from "@monaco-editor/react";
import { cobraDetect, cobraCompile, runSql } from "../../api/tauri";
import { useToast } from "../common/Toast";
import { useTheme } from "../../context/ThemeContext";
import { useFontScale } from "../../context/FontScaleContext";
import { useEscapeClose } from "../common/useEscapeClose";

const TYPES = ["INT", "TEXT", "DOUBLE", "DECIMAL", "BOOL", "TIMESTAMP", "BLOB"];
const PATH_KEY = "oxidb-cobra-path";

/** UTF-8-safe base64 (btoa alone throws on non-Latin1). */
function utf8ToBase64(s: string): string {
  const bytes = new TextEncoder().encode(s);
  let bin = "";
  bytes.forEach((b) => (bin += String.fromCharCode(b)));
  return btoa(bin);
}

interface Param {
  name: string;
  type: string;
}

/** `def run(db, p1, p2)` skeleton from the declared parameters. */
function template(params: Param[]): string {
  const names = params.map((p) => p.name.trim()).filter(Boolean);
  const sig = ["db", ...names].join(", ");
  return `def run(${sig})
    # db.query(sql, [params]) -> list of rows (dicts)
    # db.execute(sql, [params]) -> affected row count
    return {"ok": true}
end
`;
}

interface Props {
  onClose: () => void;
  onCreated: () => void;
  /** When editing an existing procedure — prefills and uses CREATE OR ALTER. */
  initial?: { name: string; params: Param[]; source: string };
}

export function NewProcedureDialog({ onClose, onCreated, initial }: Props) {
  const toast = useToast();
  const { theme } = useTheme();
  const { scale } = useFontScale();
  const isEdit = !!initial;
  useEscapeClose(onClose);

  const [name, setName] = useState(initial?.name ?? "");
  const [params, setParams] = useState<Param[]>(initial?.params ?? []);
  const [source, setSource] = useState(initial?.source ?? template([]));
  const [srcEdited, setSrcEdited] = useState(isEdit); // don't overwrite a loaded source
  const [cobraPath, setCobraPath] = useState(() => localStorage.getItem(PATH_KEY) || "");
  const [detected, setDetected] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<{ where: string; msg: string } | null>(null);

  // Detect a cobra binary once (and whenever the user edits the path).
  useEffect(() => {
    cobraDetect(cobraPath || undefined).then(setDetected);
  }, [cobraPath]);

  // Keep the skeleton in sync with params until the user edits the source.
  useEffect(() => {
    if (!srcEdited) setSource(template(params));
  }, [params, srcEdited]);

  const setParam = (i: number, patch: Partial<Param>) =>
    setParams((ps) => ps.map((p, j) => (j === i ? { ...p, ...patch } : p)));
  const addParam = () => setParams((ps) => [...ps, { name: "", type: "INT" }]);
  const removeParam = (i: number) => setParams((ps) => ps.filter((_, j) => j !== i));

  const createSql = useMemo(() => {
    const t = name.trim();
    if (!t) return "";
    const decls = params
      .filter((p) => p.name.trim())
      .map((p) => `${p.name.trim()} ${p.type}`)
      .join(", ");
    return `CREATE PROCEDURE ${t}(${decls}) LANGUAGE COBRA AS '…'`;
  }, [name, params]);

  const fail = useCallback(
    (where: string, msg: string) => {
      setErr({ where, msg });
      // Also toast — the in-dialog box can scroll below the fold.
      toast(`${where === "compile" ? "Compile" : where === "deploy" ? "Deploy" : ""} ${msg}`.trim(), "error");
    },
    [toast]
  );

  const compileAndDeploy = useCallback(async () => {
    if (!name.trim()) {
      fail("form", "Enter a procedure name");
      return;
    }
    setBusy(true);
    setErr(null);
    try {
      // 1. Compile locally via the cobra CLI.
      let b64: string;
      try {
        b64 = await cobraCompile(source, cobraPath || undefined);
        localStorage.setItem(PATH_KEY, cobraPath);
      } catch (e) {
        fail("compile", String(e));
        setBusy(false);
        return;
      }
      // 2. Deploy — server re-validates (determinism + run arity). The source
      // rides along in a SOURCE clause so it can be shown/edited later.
      const decls = params
        .filter((p) => p.name.trim())
        .map((p) => `${p.name.trim()} ${p.type}`)
        .join(", ");
      const srcB64 = utf8ToBase64(source);
      const verb = isEdit ? "CREATE OR ALTER PROCEDURE" : "CREATE PROCEDURE";
      const sql = `${verb} ${name.trim()}(${decls}) LANGUAGE COBRA AS '${b64}' SOURCE '${srcB64}'`;
      const resp = (await runSql(sql)) as unknown as { ok?: boolean; error?: string };
      if (resp && resp.ok === false) {
        fail("deploy", resp.error || "create failed");
        setBusy(false);
        return;
      }
      toast(isEdit ? "Procedure updated" : "Procedure created", "success");
      onCreated();
      onClose();
    } catch (e) {
      fail("deploy", String(e));
    } finally {
      setBusy(false);
    }
  }, [name, params, source, cobraPath, toast, onCreated, onClose, isEdit, fail]);

  return (
    <div className="dialog-overlay">
      <div
        className="dialog"
        style={{ width: "min(1500px, 96vw)", height: "90vh", display: "flex", flexDirection: "column" }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="dialog-title">{isEdit ? "Edit Cobra Procedure" : "New Cobra Procedure"}</div>

        <div style={{ display: "flex", gap: 8, marginBottom: 10 }}>
          <div className="form-group" style={{ flex: 1, marginBottom: 0 }}>
            <label>Name</label>
            <input
              autoFocus={!isEdit}
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="transfer"
              readOnly={isEdit}
              style={{ fontFamily: "var(--font-mono)", opacity: isEdit ? 0.7 : 1 }}
            />
          </div>
        </div>

        {/* Parameters */}
        <div className="ct-section">Parameters (the run() signature is db + these)</div>
        {params.length > 0 && (
          <table className="ct-table" style={{ marginBottom: 6 }}>
            <tbody>
              {params.map((p, i) => (
                <tr key={i}>
                  <td>
                    <input
                      value={p.name}
                      onChange={(e) => setParam(i, { name: e.target.value })}
                      placeholder="name"
                      style={{ fontFamily: "var(--font-mono)", width: "100%" }}
                    />
                  </td>
                  <td style={{ width: 120 }}>
                    <select value={p.type} onChange={(e) => setParam(i, { type: e.target.value })}>
                      {TYPES.map((t) => (
                        <option key={t} value={t}>
                          {t}
                        </option>
                      ))}
                    </select>
                  </td>
                  <td style={{ width: 30 }}>
                    <button className="ct-remove" onClick={() => removeParam(i)}>
                      ✕
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        <button className="btn btn-secondary btn-sm" onClick={addParam} style={{ marginBottom: 10 }}>
          + Add parameter
        </button>

        {/* Cobra source */}
        <div className="ct-section">
          Source (.cobra)
          {!srcEdited && (
            <span style={{ fontWeight: 400, color: "var(--text-secondary)", marginLeft: 8 }}>
              — auto-synced to params until you edit
            </span>
          )}
        </div>
        <div style={{ flex: 1, minHeight: 160, border: "1px solid var(--border-color)", borderRadius: "var(--radius-sm)", overflow: "hidden", marginBottom: 10 }}>
          <Editor
            height="100%"
            defaultLanguage="python"
            theme={theme === "dark" ? "vs-dark" : "light"}
            value={source}
            onChange={(v) => {
              setSource(v || "");
              setSrcEdited(true);
            }}
            options={{
              minimap: { enabled: false },
              fontSize: Math.round(13 * scale),
              fontFamily: "var(--font-mono)",
              scrollBeyondLastLine: false,
              automaticLayout: true,
              tabSize: 4,
              // Don't box non-ASCII/"ambiguous" chars — Turkish letters (ı, ş…)
              // in comments are legitimate, not homoglyph attacks.
              unicodeHighlight: {
                ambiguousCharacters: false,
                invisibleCharacters: false,
                nonBasicASCII: false,
              },
            }}
          />
        </div>

        {/* Compiler path */}
        <div className="form-group" style={{ marginBottom: 10 }}>
          <label>
            cobra compiler{" "}
            <span style={{ fontWeight: 400, color: detected ? "var(--success, #40c057)" : "var(--danger)" }}>
              {detected ? `found: ${detected}` : "not found — build it or set the path"}
            </span>
          </label>
          <input
            value={cobraPath}
            onChange={(e) => setCobraPath(e.target.value)}
            placeholder="auto-detect (leave blank), or /path/to/cobra"
            style={{ fontFamily: "var(--font-mono)" }}
          />
        </div>

        <div className="ct-preview" style={{ maxHeight: 60 }}>
          <pre>{createSql || "-- name the procedure to see the CREATE"}</pre>
        </div>

        {err && (
          <div style={{ marginTop: 8, padding: 8, background: "var(--bg-secondary)", border: "1px solid var(--danger)", borderRadius: "var(--radius-sm)" }}>
            <div style={{ fontSize: 11, color: "var(--danger)", textTransform: "uppercase", marginBottom: 2 }}>
              {err.where === "compile" ? "Compile error" : err.where === "deploy" ? "Deploy error (server validation)" : "Error"}
            </div>
            <pre style={{ margin: 0, fontFamily: "var(--font-mono)", fontSize: 12, whiteSpace: "pre-wrap", color: "var(--text-primary)" }}>
              {err.msg}
            </pre>
          </div>
        )}

        <div className="dialog-actions" style={{ marginTop: 12 }}>
          <button className="btn btn-secondary" onClick={onClose}>
            Cancel
          </button>
          <button className="btn btn-primary" onClick={compileAndDeploy} disabled={busy}>
            {busy ? <span className="spinner" /> : null}
            {isEdit ? "Recompile & Update" : "Compile & Deploy"}
          </button>
        </div>
      </div>
    </div>
  );
}
