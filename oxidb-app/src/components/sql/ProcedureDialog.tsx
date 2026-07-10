import { SqlEditor } from "../common/SqlEditor";
import { useEscapeClose } from "../common/useEscapeClose";
import { formatSql } from "../../utils/formatSql";

export interface ProcInfo {
  name: string;
  params: string; // e.g. "delta INT, name TEXT"
  definition: string; // body SQL ($1..$N), or a bytecode placeholder for cobra
  language: string; // "sql" | "cobra"
}

/** Build a `CALL name(?, ?)` template with a comment naming each parameter. */
function callTemplate(p: ProcInfo): string {
  const params = p.params
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const args = params.map(() => "?").join(", ");
  const note = params.length ? ` -- ${params.join(", ")}` : "";
  return `CALL ${p.name}(${args});${note}`;
}

interface Props {
  proc: ProcInfo;
  onClose: () => void;
  onInsert: (text: string) => void;
  onDrop: (name: string) => void;
  /** Open the cobra editor prefilled with this procedure's source. */
  onEdit?: (proc: ProcInfo) => void;
}

export function ProcedureDialog({ proc, onClose, onInsert, onDrop, onEdit }: Props) {
  useEscapeClose(onClose);
  const params = proc.params
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  // For cobra, `definition` is the .cobra source unless it was uploaded
  // without one (then it's the "<cobra bytecode, N bytes>" placeholder).
  const hasSource = proc.language === "cobra" && !proc.definition.startsWith("<cobra bytecode");

  return (
    <div className="dialog-overlay">
      <div
        className="dialog"
        style={{ width: "min(900px, 92vw)", height: "84vh", display: "flex", flexDirection: "column" }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="dialog-title">
          Procedure <span style={{ fontFamily: "var(--font-mono)" }}>{proc.name}</span>
          {proc.language === "cobra" && (
            <span className="badge badge-muted" style={{ marginLeft: 8 }}>COBRA (compiled)</span>
          )}
        </div>

        <div className="ct-section">Parameters</div>
        {params.length === 0 ? (
          <div className="empty-state" style={{ padding: 8, fontSize: 13 }}>
            No parameters
          </div>
        ) : (
          <table className="data-table" style={{ marginBottom: 12 }}>
            <thead>
              <tr>
                <th style={{ width: 40 }}>#</th>
                <th>Name</th>
                <th>Type</th>
              </tr>
            </thead>
            <tbody>
              {params.map((p, i) => {
                const [n, ...ty] = p.split(/\s+/);
                return (
                  <tr key={i}>
                    <td style={{ color: "var(--text-secondary)" }}>${i + 1}</td>
                    <td style={{ fontFamily: "var(--font-mono)" }}>{n}</td>
                    <td style={{ fontFamily: "var(--font-mono)", color: "var(--text-secondary)" }}>
                      {ty.join(" ")}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}

        <div className="ct-section" style={{ marginTop: 4 }}>
          {proc.language === "cobra" ? (hasSource ? "Source (.cobra)" : "Body") : "Body"}
        </div>
        <div style={{ flex: 1, minHeight: 160, border: "1px solid var(--border-color)", borderRadius: "var(--radius-sm)", overflow: "hidden", marginBottom: 12 }}>
          <SqlEditor
            value={
              proc.language === "cobra" ? proc.definition : formatSql(proc.definition)
            }
            readOnly
            height="100%"
            language={proc.language === "cobra" && hasSource ? "python" : "sql"}
          />
        </div>

        <div className="dialog-actions">
          <button
            className="btn btn-danger btn-sm"
            style={{ marginRight: "auto" }}
            onClick={() => onDrop(proc.name)}
          >
            Drop procedure
          </button>
          {onEdit && (
            <button
              className="btn btn-secondary"
              onClick={() => onEdit(proc)}
              title={
                proc.language === "cobra"
                  ? hasSource
                    ? "Edit source and recompile"
                    : "No source stored — paste new source to replace"
                  : "Edit the procedure body"
              }
            >
              Edit…
            </button>
          )}
          <button className="btn btn-secondary" onClick={onClose}>
            Close
          </button>
          <button
            className="btn btn-primary"
            onClick={() => {
              onInsert(callTemplate(proc));
              onClose();
            }}
          >
            Insert CALL
          </button>
        </div>
      </div>
    </div>
  );
}
