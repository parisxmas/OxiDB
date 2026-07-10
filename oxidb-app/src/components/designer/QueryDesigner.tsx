import { useState, useCallback, useEffect, useRef, useMemo } from "react";
import { useNavigate } from "react-router-dom";
import { runSql } from "../../api/tauri";
import type { JsonValue } from "../../api/types";
import { useDatabase } from "../../context/DatabaseContext";
import { useSqlSession, newTab } from "../../context/SqlSessionContext";
import { useToast } from "../common/Toast";

// A one-directional visual query builder: drop tables on the canvas, connect
// columns to make joins, tick output columns — the SQL is generated live and
// can be sent to the SQL editor. (Visual → SQL only; no SQL → visual parse.)

const BOX_W = 200;
const HEADER_H = 30;
const ROW_H = 24;

interface Col {
  name: string;
  type: string;
  pk: boolean;
}
interface DTable {
  uid: string;
  name: string;
  x: number;
  y: number;
  cols: Col[];
}
interface DJoin {
  id: string;
  aUid: string;
  aCol: string;
  bUid: string;
  bCol: string;
  kind: "INNER" | "LEFT";
}

function firstSelect(resp: unknown): { columns?: string[]; rows?: JsonValue[][]; error?: string } | null {
  const r = resp as { ok?: boolean; error?: string; data?: { columns?: string[]; rows?: JsonValue[][] }[] };
  if (r && r.ok === false) return { error: r.error };
  const d = r?.data?.[0];
  return d && d.columns ? d : null;
}

let uidSeq = 1;

export function QueryDesigner() {
  const toast = useToast();
  const navigate = useNavigate();
  const { db } = useDatabase();
  const { setTabs, setActiveIdx, allocId } = useSqlSession();

  const [allTables, setAllTables] = useState<string[]>([]);
  const [tables, setTables] = useState<DTable[]>([]);
  const [output, setOutput] = useState<Set<string>>(new Set()); // `${uid}::${col}`
  const [sorts, setSorts] = useState<Record<string, "ASC" | "DESC">>({});
  const [joins, setJoins] = useState<DJoin[]>([]);
  const [pending, setPending] = useState<{ uid: string; col: string } | null>(null);
  const [whereText, setWhereText] = useState("");
  const [distinct, setDistinct] = useState(false);
  const canvasRef = useRef<HTMLDivElement>(null);
  // Live drag-to-connect state.
  const [dragLine, setDragLine] = useState<{ x1: number; y1: number; x2: number; y2: number } | null>(null);
  const [dropHint, setDropHint] = useState<string | null>(null); // `${uid}::${col}` under cursor
  const dragRef = useRef<{ uid: string; col: string } | null>(null);
  const hoverRef = useRef<{ uid: string; col: string } | null>(null);

  // Load the database's table list for the palette.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const sel = firstSelect(await runSql("SHOW TABLES", undefined, db));
      if (!cancelled && sel && !sel.error) {
        setAllTables((sel.rows || []).map((r) => String(r[0])));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [db]);

  const byUid = useMemo(() => {
    const m = new Map<string, DTable>();
    for (const t of tables) m.set(t.uid, t);
    return m;
  }, [tables]);

  const addTable = useCallback(
    async (name: string) => {
      if (tables.some((t) => t.name === name)) {
        toast(`${name} is already on the canvas`, "error");
        return;
      }
      const sel = firstSelect(await runSql(`DESCRIBE ${name}`, undefined, db));
      if (!sel || sel.error) {
        toast(sel?.error || "could not describe table", "error");
        return;
      }
      const ci = (sel.columns || []).indexOf("column");
      const ti = (sel.columns || []).indexOf("type");
      const pki = (sel.columns || []).indexOf("primary_key");
      const cols: Col[] = (sel.rows || []).map((r) => ({
        name: String(r[ci]),
        type: String(r[ti]),
        pk: r[pki] === true,
      }));
      const uid = `t${uidSeq++}`;
      const offset = tables.length * 30;
      setTables((ts) => [...ts, { uid, name, x: 30 + offset, y: 20 + offset, cols }]);
    },
    [tables, db, toast]
  );

  const removeTable = useCallback((uid: string) => {
    setTables((ts) => ts.filter((t) => t.uid !== uid));
    setJoins((js) => js.filter((j) => j.aUid !== uid && j.bUid !== uid));
    setOutput((o) => {
      const next = new Set<string>();
      o.forEach((k) => {
        if (!k.startsWith(`${uid}::`)) next.add(k);
      });
      return next;
    });
  }, []);

  // Click a column: first click arms it, second click (on another table) makes
  // a join.
  const clickColumn = useCallback(
    (uid: string, col: string) => {
      if (!pending) {
        setPending({ uid, col });
        return;
      }
      if (pending.uid === uid) {
        // same table — just re-arm on the new column
        setPending({ uid, col });
        return;
      }
      const id = `j${uidSeq++}`;
      setJoins((js) => [
        ...js,
        { id, aUid: pending.uid, aCol: pending.col, bUid: uid, bCol: col, kind: "INNER" },
      ]);
      setPending(null);
    },
    [pending]
  );

  // Cursor position in canvas-content coordinates (matches table x/y).
  const canvasPoint = useCallback((clientX: number, clientY: number) => {
    const el = canvasRef.current;
    if (!el) return { x: clientX, y: clientY };
    const rect = el.getBoundingClientRect();
    return { x: clientX - rect.left + el.scrollLeft, y: clientY - rect.top + el.scrollTop };
  }, []);

  // Drag from a column's port → draw a line to the cursor → drop on another
  // table's column to create the join.
  const startConn = useCallback(
    (uid: string, col: string, e: React.MouseEvent) => {
      e.preventDefault();
      e.stopPropagation();
      dragRef.current = { uid, col };
      hoverRef.current = null;
      const t = byUid.get(uid);
      if (!t) return;
      const a = anchor(t, col, "r");
      const p = canvasPoint(e.clientX, e.clientY);
      setDragLine({ x1: a.x, y1: a.y, x2: p.x, y2: p.y });
      const onMove = (ev: MouseEvent) => {
        const pt = canvasPoint(ev.clientX, ev.clientY);
        setDragLine((dl) => (dl ? { ...dl, x2: pt.x, y2: pt.y } : null));
      };
      const onUp = () => {
        document.removeEventListener("mousemove", onMove);
        document.removeEventListener("mouseup", onUp);
        const from = dragRef.current;
        const to = hoverRef.current;
        if (from && to && from.uid !== to.uid) {
          const id = `j${uidSeq++}`;
          setJoins((js) => [
            ...js,
            { id, aUid: from.uid, aCol: from.col, bUid: to.uid, bCol: to.col, kind: "INNER" },
          ]);
        }
        dragRef.current = null;
        hoverRef.current = null;
        setDragLine(null);
        setDropHint(null);
      };
      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    },
    // anchor/byUid are stable enough per render; canvasPoint is memoized
    [byUid, canvasPoint]
  );

  const onColEnter = useCallback((uid: string, col: string) => {
    if (dragRef.current && dragRef.current.uid !== uid) {
      hoverRef.current = { uid, col };
      setDropHint(`${uid}::${col}`);
    }
  }, []);
  const onColLeave = useCallback((uid: string, col: string) => {
    if (hoverRef.current && hoverRef.current.uid === uid && hoverRef.current.col === col) {
      hoverRef.current = null;
      setDropHint(null);
    }
  }, []);

  const toggleOutput = useCallback((uid: string, col: string) => {
    const key = `${uid}::${col}`;
    setOutput((o) => {
      const next = new Set(o);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);

  const cycleSort = useCallback((uid: string, col: string) => {
    const key = `${uid}::${col}`;
    setSorts((s) => {
      const cur = s[key];
      const next = { ...s };
      if (!cur) next[key] = "ASC";
      else if (cur === "ASC") next[key] = "DESC";
      else delete next[key];
      return next;
    });
  }, []);

  // ── Drag a table box ──
  const dragBox = useCallback((uid: string, e: React.MouseEvent) => {
    e.preventDefault();
    const startX = e.clientX;
    const startY = e.clientY;
    let ox = 0;
    let oy = 0;
    setTables((ts) => {
      const t = ts.find((x) => x.uid === uid);
      if (t) {
        ox = t.x;
        oy = t.y;
      }
      return ts;
    });
    const onMove = (ev: MouseEvent) => {
      const nx = Math.max(0, ox + ev.clientX - startX);
      const ny = Math.max(0, oy + ev.clientY - startY);
      setTables((ts) => ts.map((t) => (t.uid === uid ? { ...t, x: nx, y: ny } : t)));
    };
    const onUp = () => {
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  }, []);

  // Anchor point (x,y) of a column on a table box, on the given side.
  const anchor = (t: DTable, col: string, side: "l" | "r") => {
    const idx = Math.max(0, t.cols.findIndex((c) => c.name === col));
    const y = t.y + HEADER_H + idx * ROW_H + ROW_H / 2;
    const x = side === "r" ? t.x + BOX_W : t.x;
    return { x, y };
  };

  // ── SQL generation ──
  const sql = useMemo(() => {
    if (tables.length === 0) return "";
    const q = (s: string) => s; // identifiers as-is (engine is case-insensitive)
    // FROM ordering: start at the first table, then greedily attach joined ones.
    const placed = new Set<string>([tables[0].uid]);
    const fromParts: string[] = [`FROM ${q(tables[0].name)}`];
    const usedJoins = new Set<string>();
    let changed = true;
    while (changed) {
      changed = false;
      for (const j of joins) {
        if (usedJoins.has(j.id)) continue;
        const aIn = placed.has(j.aUid);
        const bIn = placed.has(j.bUid);
        if (aIn === bIn) continue; // both in (redundant) or both out (later)
        const newUid = aIn ? j.bUid : j.aUid;
        const nt = byUid.get(newUid);
        const at = byUid.get(j.aUid);
        const bt = byUid.get(j.bUid);
        if (!nt || !at || !bt) continue;
        fromParts.push(
          `${j.kind} JOIN ${q(nt.name)} ON ${q(at.name)}.${j.aCol} = ${q(bt.name)}.${j.bCol}`
        );
        placed.add(newUid);
        usedJoins.add(j.id);
        changed = true;
      }
    }
    // SELECT list (checked columns, in table/column order).
    const selCols: string[] = [];
    const orderCols: string[] = [];
    for (const t of tables) {
      for (const c of t.cols) {
        const key = `${t.uid}::${c.name}`;
        if (output.has(key)) selCols.push(`${q(t.name)}.${c.name}`);
        if (sorts[key]) orderCols.push(`${q(t.name)}.${c.name} ${sorts[key]}`);
      }
    }
    const select = selCols.length ? selCols.join(", ") : "*";
    let out = `SELECT ${distinct ? "DISTINCT " : ""}${select}\n${fromParts.join("\n")}`;
    if (whereText.trim()) out += `\nWHERE ${whereText.trim()}`;
    if (orderCols.length) out += `\nORDER BY ${orderCols.join(", ")}`;
    out += ";";
    // Warn about tables that never got attached.
    const orphan = tables.filter((t) => !placed.has(t.uid)).map((t) => t.name);
    if (orphan.length) out = `-- not joined (ignored): ${orphan.join(", ")}\n` + out;
    return out;
  }, [tables, joins, output, sorts, whereText, distinct, byUid]);

  const openInEditor = useCallback(() => {
    if (!sql) return;
    const id = allocId();
    setTabs((ts) => {
      setActiveIdx(ts.length);
      return [...ts, { ...newTab(id), name: "Designer", sql }];
    });
    navigate("/sql");
  }, [sql, allocId, setTabs, setActiveIdx, navigate]);

  return (
    <div className="qd-root">
      {/* Palette */}
      <div className="qd-palette">
        <div className="qd-palette-head">Tables{db ? ` · ${db}` : ""}</div>
        {allTables.length === 0 ? (
          <div className="schema-empty">No tables</div>
        ) : (
          <ul className="qd-palette-list">
            {allTables.map((name) => (
              <li key={name}>
                <button className="qd-palette-item" onClick={() => addTable(name)} title="Add to canvas">
                  + {name}
                </button>
              </li>
            ))}
          </ul>
        )}
        <div className="qd-hint">
          Click a column, then a column on another table to join. Tick a column to
          output it; click ⇅ to sort.
        </div>
      </div>

      {/* Canvas + SQL */}
      <div className="qd-main">
        <div className="qd-canvas" ref={canvasRef}>
          <svg className="qd-svg">
            <defs>
              <marker id="qd-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
                <path d="M 0 0 L 10 5 L 0 10 z" className="qd-arrow-head" />
              </marker>
            </defs>
            {dragLine && (
              <line
                x1={dragLine.x1}
                y1={dragLine.y1}
                x2={dragLine.x2}
                y2={dragLine.y2}
                className="qd-drag-line"
                markerEnd="url(#qd-arrow)"
              />
            )}
            {joins.map((j) => {
              const at = byUid.get(j.aUid);
              const bt = byUid.get(j.bUid);
              if (!at || !bt) return null;
              const aRight = at.x <= bt.x;
              const p1 = anchor(at, j.aCol, aRight ? "r" : "l");
              const p2 = anchor(bt, j.bCol, aRight ? "l" : "r");
              const dx = Math.abs(p2.x - p1.x) / 2 + 20;
              const d = `M ${p1.x} ${p1.y} C ${p1.x + (aRight ? dx : -dx)} ${p1.y}, ${p2.x + (aRight ? -dx : dx)} ${p2.y}, ${p2.x} ${p2.y}`;
              return (
                <g key={j.id}>
                  <path d={d} className="qd-join-line" markerEnd="url(#qd-arrow)" />
                  <circle cx={p1.x} cy={p1.y} r={3} className="qd-join-dot" />
                </g>
              );
            })}
          </svg>

          {tables.map((t) => (
            <div key={t.uid} className="qd-table" style={{ left: t.x, top: t.y, width: BOX_W }}>
              <div className="qd-table-head" onMouseDown={(e) => dragBox(t.uid, e)}>
                <span>{t.name}</span>
                <button className="qd-x" onClick={() => removeTable(t.uid)} title="Remove">
                  ×
                </button>
              </div>
              {t.cols.map((c) => {
                const key = `${t.uid}::${c.name}`;
                const armed = pending && pending.uid === t.uid && pending.col === c.name;
                const isDrop = dropHint === key;
                return (
                  <div
                    key={c.name}
                    className={`qd-col${armed ? " armed" : ""}${isDrop ? " drop" : ""}`}
                    style={{ height: ROW_H }}
                    onMouseEnter={() => onColEnter(t.uid, c.name)}
                    onMouseLeave={() => onColLeave(t.uid, c.name)}
                  >
                    <input
                      type="checkbox"
                      checked={output.has(key)}
                      onChange={() => toggleOutput(t.uid, c.name)}
                      title="Output this column"
                    />
                    <span className="qd-col-name" onClick={() => clickColumn(t.uid, c.name)} title="Drag the ● to another column to join (or click two columns)">
                      {c.pk && <span className="schema-pk">🔑</span>}
                      {c.name}
                    </span>
                    <button
                      className={`qd-sort${sorts[key] ? " on" : ""}`}
                      onClick={() => cycleSort(t.uid, c.name)}
                      title="Sort (none → ASC → DESC)"
                    >
                      {sorts[key] === "ASC" ? "↑" : sorts[key] === "DESC" ? "↓" : "⇅"}
                    </button>
                    <span
                      className="qd-port"
                      title="Drag to another column to create a join"
                      onMouseDown={(e) => startConn(t.uid, c.name, e)}
                    />
                  </div>
                );
              })}
            </div>
          ))}

          {tables.length === 0 && (
            <div className="qd-canvas-empty">Add tables from the left to start building a query.</div>
          )}
        </div>

        {/* Joins + WHERE + SQL */}
        <div className="qd-bottom">
          <div className="qd-controls">
            {joins.length > 0 && (
              <div className="qd-joins">
                {joins.map((j) => {
                  const at = byUid.get(j.aUid);
                  const bt = byUid.get(j.bUid);
                  return (
                    <div key={j.id} className="qd-join-row">
                      <select
                        value={j.kind}
                        onChange={(e) =>
                          setJoins((js) => js.map((x) => (x.id === j.id ? { ...x, kind: e.target.value as "INNER" | "LEFT" } : x)))
                        }
                      >
                        <option value="INNER">INNER</option>
                        <option value="LEFT">LEFT</option>
                      </select>
                      <span className="qd-join-text">
                        {at?.name}.{j.aCol} = {bt?.name}.{j.bCol}
                      </span>
                      <button className="qd-x" onClick={() => setJoins((js) => js.filter((x) => x.id !== j.id))}>
                        ×
                      </button>
                    </div>
                  );
                })}
              </div>
            )}
            <label className="qd-where">
              WHERE
              <input
                value={whereText}
                onChange={(e) => setWhereText(e.target.value)}
                placeholder="e.g. amount > 100 AND status = 'paid'"
              />
            </label>
            <label className="qd-distinct">
              <input type="checkbox" checked={distinct} onChange={(e) => setDistinct(e.target.checked)} />
              DISTINCT
            </label>
          </div>

          <div className="qd-sql">
            <div className="qd-sql-head">
              <span>Generated SQL</span>
              <button className="btn btn-primary btn-sm" onClick={openInEditor} disabled={!sql}>
                Open in editor →
              </button>
            </div>
            <pre className="qd-sql-body">{sql || "-- add tables to build a query"}</pre>
          </div>
        </div>
      </div>
    </div>
  );
}
