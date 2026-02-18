import { useState, useRef, useCallback } from "react";
import type { JsonValue } from "../../api/types";

interface Props {
  data: JsonValue[];
  onRowClick?: (row: JsonValue) => void;
}

function getColumns(data: JsonValue[]): string[] {
  const colSet = new Set<string>();
  for (const row of data.slice(0, 50)) {
    if (row && typeof row === "object" && !Array.isArray(row)) {
      for (const key of Object.keys(row)) {
        colSet.add(key);
      }
    }
  }
  // Put _id first, then _version, then alphabetical
  const cols = Array.from(colSet);
  cols.sort((a, b) => {
    if (a === "_id") return -1;
    if (b === "_id") return 1;
    if (a === "_version") return -1;
    if (b === "_version") return 1;
    return a.localeCompare(b);
  });
  return cols;
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

export function DataTable({ data, onRowClick }: Props) {
  const [colWidths, setColWidths] = useState<Record<string, number>>({});
  const dragRef = useRef<{ col: string; startX: number; startW: number } | null>(null);

  const onMouseDown = useCallback((e: React.MouseEvent, col: string, thEl: HTMLTableCellElement) => {
    e.preventDefault();
    const startW = thEl.offsetWidth;
    dragRef.current = { col, startX: e.clientX, startW };

    const onMouseMove = (ev: MouseEvent) => {
      if (!dragRef.current) return;
      const diff = ev.clientX - dragRef.current.startX;
      const newW = Math.max(40, dragRef.current.startW + diff);
      setColWidths((prev) => ({ ...prev, [dragRef.current!.col]: newW }));
    };

    const onMouseUp = () => {
      dragRef.current = null;
      document.removeEventListener("mousemove", onMouseMove);
      document.removeEventListener("mouseup", onMouseUp);
    };

    document.addEventListener("mousemove", onMouseMove);
    document.addEventListener("mouseup", onMouseUp);
  }, []);

  if (data.length === 0) {
    return <div className="empty-state">No documents found</div>;
  }

  const columns = getColumns(data);

  return (
    <div style={{ overflow: "auto" }}>
      <table className="data-table" style={{ tableLayout: "fixed" }}>
        <thead>
          <tr>
            {columns.map((col) => (
              <th
                key={col}
                style={{
                  width: colWidths[col] || undefined,
                  minWidth: 40,
                  position: "relative",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}
              >
                {col}
                <div
                  onMouseDown={(e) => {
                    const th = e.currentTarget.parentElement as HTMLTableCellElement;
                    onMouseDown(e, col, th);
                  }}
                  style={{
                    position: "absolute",
                    right: 0,
                    top: 0,
                    bottom: 0,
                    width: 5,
                    cursor: "col-resize",
                    userSelect: "none",
                  }}
                />
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, i) => (
            <tr
              key={i}
              onClick={() => onRowClick?.(row)}
              style={{ cursor: onRowClick ? "pointer" : "default" }}
            >
              {columns.map((col) => (
                <td key={col} style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {formatCell(
                    row && typeof row === "object" && !Array.isArray(row)
                      ? (row as Record<string, unknown>)[col]
                      : undefined
                  )}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
