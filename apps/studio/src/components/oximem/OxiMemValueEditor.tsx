import { useState, useCallback } from "react";
import { oximemExec } from "../../api/tauri";
import { useToast } from "../common/Toast";

interface Detail {
  type: string;
  value: { kind: string; value: unknown };
}

interface Props {
  keyName: string;
  detail: Detail;
  editStr: string | null;
  setEditStr: (s: string) => void;
  onReload: () => void;
}

/** One inline-editable cell: double-click → input, Enter commits, Esc cancels. */
function EditCell({
  text,
  onCommit,
}: {
  text: string;
  onCommit: (v: string) => void;
}) {
  const [editing, setEditing] = useState(false);
  const [val, setVal] = useState(text);
  if (!editing) {
    return (
      <span
        onDoubleClick={() => {
          setVal(text);
          setEditing(true);
        }}
        title="Double-click to edit"
        style={{ cursor: "text", fontFamily: "var(--font-mono)" }}
      >
        {text}
      </span>
    );
  }
  return (
    <input
      className="cell-edit"
      autoFocus
      value={val}
      onChange={(e) => setVal(e.target.value)}
      onBlur={() => {
        setEditing(false);
        if (val !== text) onCommit(val);
      }}
      onKeyDown={(e) => {
        if (e.key === "Enter") {
          setEditing(false);
          if (val !== text) onCommit(val);
        } else if (e.key === "Escape") setEditing(false);
      }}
    />
  );
}

export function OxiMemValueEditor({ keyName, detail, editStr, setEditStr, onReload }: Props) {
  const toast = useToast();
  const [busy, setBusy] = useState(false);
  const [add1, setAdd1] = useState("");
  const [add2, setAdd2] = useState("");

  const run = useCallback(
    async (args: string[], quiet = false) => {
      setBusy(true);
      try {
        await oximemExec(args);
        if (!quiet) onReload();
        return true;
      } catch (e) {
        toast(String(e), "error");
        return false;
      } finally {
        setBusy(false);
      }
    },
    [toast, onReload]
  );

  const kind = detail.value.kind;
  const rows = (detail.value.value as unknown[]) || [];

  // ── string ──────────────────────────────────────────────────────────
  if (kind === "string") {
    return (
      <div>
        <textarea
          className="oximem-string"
          value={editStr ?? ""}
          onChange={(e) => setEditStr(e.target.value)}
          spellCheck={false}
        />
        <div style={{ marginTop: 8 }}>
          <button
            className="btn btn-primary btn-sm"
            disabled={busy}
            onClick={() => run(["SET", keyName, editStr ?? ""])}
          >
            Save
          </button>
        </div>
      </div>
    );
  }

  // ── hash ────────────────────────────────────────────────────────────
  if (kind === "hash") {
    const pairs = rows as [string, string][];
    return (
      <>
        <table className="data-table">
          <thead>
            <tr>
              <th>field</th>
              <th>value</th>
              <th style={{ width: 40 }} />
            </tr>
          </thead>
          <tbody>
            {pairs.map(([f, v]) => (
              <tr key={f}>
                <td style={{ fontFamily: "var(--font-mono)" }}>{f}</td>
                <td>
                  <EditCell text={v} onCommit={(nv) => run(["HSET", keyName, f, nv])} />
                </td>
                <td>
                  <button className="row-del" disabled={busy} onClick={() => run(["HDEL", keyName, f])}>
                    ✕
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        <AddRow
          placeholders={["field", "value"]}
          a={add1}
          b={add2}
          setA={setAdd1}
          setB={setAdd2}
          busy={busy}
          onAdd={async () => {
            if (!add1) return;
            if (await run(["HSET", keyName, add1, add2])) {
              setAdd1("");
              setAdd2("");
            }
          }}
        />
      </>
    );
  }

  // ── zset ────────────────────────────────────────────────────────────
  if (kind === "zset") {
    const pairs = rows as [string, string][];
    return (
      <>
        <table className="data-table">
          <thead>
            <tr>
              <th>member</th>
              <th>score</th>
              <th style={{ width: 40 }} />
            </tr>
          </thead>
          <tbody>
            {pairs.map(([m, s]) => (
              <tr key={m}>
                <td style={{ fontFamily: "var(--font-mono)" }}>{m}</td>
                <td>
                  <EditCell text={s} onCommit={(ns) => run(["ZADD", keyName, ns, m])} />
                </td>
                <td>
                  <button className="row-del" disabled={busy} onClick={() => run(["ZREM", keyName, m])}>
                    ✕
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        <AddRow
          placeholders={["member", "score"]}
          a={add1}
          b={add2}
          setA={setAdd1}
          setB={setAdd2}
          busy={busy}
          onAdd={async () => {
            if (!add1) return;
            if (await run(["ZADD", keyName, add2 || "0", add1])) {
              setAdd1("");
              setAdd2("");
            }
          }}
        />
      </>
    );
  }

  // ── list ────────────────────────────────────────────────────────────
  if (kind === "list") {
    const items = rows as string[];
    return (
      <>
        <table className="data-table">
          <thead>
            <tr>
              <th style={{ width: 50 }}>#</th>
              <th>value</th>
              <th style={{ width: 40 }} />
            </tr>
          </thead>
          <tbody>
            {items.map((v, i) => (
              <tr key={i}>
                <td style={{ color: "var(--text-secondary)" }}>{i}</td>
                <td>
                  <EditCell text={v} onCommit={(nv) => run(["LSET", keyName, String(i), nv])} />
                </td>
                <td>
                  {/* Remove first occurrence of this value. */}
                  <button className="row-del" disabled={busy} onClick={() => run(["LREM", keyName, "1", v])}>
                    ✕
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        <div className="oximem-add">
          <input
            className="cell-edit"
            style={{ flex: 1 }}
            placeholder="value"
            value={add1}
            onChange={(e) => setAdd1(e.target.value)}
          />
          <button
            className="btn btn-secondary btn-sm"
            disabled={busy || !add1}
            onClick={async () => (await run(["LPUSH", keyName, add1])) && setAdd1("")}
          >
            Push front
          </button>
          <button
            className="btn btn-primary btn-sm"
            disabled={busy || !add1}
            onClick={async () => (await run(["RPUSH", keyName, add1])) && setAdd1("")}
          >
            Push back
          </button>
        </div>
      </>
    );
  }

  // ── set ─────────────────────────────────────────────────────────────
  if (kind === "set") {
    const members = rows as string[];
    return (
      <>
        <table className="data-table">
          <thead>
            <tr>
              <th>member</th>
              <th style={{ width: 40 }} />
            </tr>
          </thead>
          <tbody>
            {members.map((m) => (
              <tr key={m}>
                <td style={{ fontFamily: "var(--font-mono)" }}>{m}</td>
                <td>
                  <button className="row-del" disabled={busy} onClick={() => run(["SREM", keyName, m])}>
                    ✕
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        <div className="oximem-add">
          <input
            className="cell-edit"
            style={{ flex: 1 }}
            placeholder="member"
            value={add1}
            onChange={(e) => setAdd1(e.target.value)}
          />
          <button
            className="btn btn-primary btn-sm"
            disabled={busy || !add1}
            onClick={async () => (await run(["SADD", keyName, add1])) && setAdd1("")}
          >
            Add
          </button>
        </div>
      </>
    );
  }

  return <div className="empty-state">Unsupported type</div>;
}

function AddRow({
  placeholders,
  a,
  b,
  setA,
  setB,
  busy,
  onAdd,
}: {
  placeholders: [string, string];
  a: string;
  b: string;
  setA: (s: string) => void;
  setB: (s: string) => void;
  busy: boolean;
  onAdd: () => void;
}) {
  return (
    <div className="oximem-add">
      <input className="cell-edit" style={{ flex: 1 }} placeholder={placeholders[0]} value={a} onChange={(e) => setA(e.target.value)} />
      <input className="cell-edit" style={{ flex: 1 }} placeholder={placeholders[1]} value={b} onChange={(e) => setB(e.target.value)} />
      <button className="btn btn-primary btn-sm" disabled={busy || !a} onClick={onAdd}>
        Add
      </button>
    </div>
  );
}
