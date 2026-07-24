import { useCallback, useEffect, useState } from "react";
import { type Project, getProject, updateProjectLimits } from "./api.ts";
import { listCollections, runSql, countDocuments, listBuckets } from "./dataApi.ts";
import { FilesBrowser, fmtBytes } from "./FilesBrowser.tsx";
import { UsersPanel } from "./UsersPanel.tsx";
import { LogsPanel } from "./LogsPanel.tsx";
import { DataBrowser } from "./DataBrowser.tsx";
import { SqlTables } from "./SqlTables.tsx";
import { SqlRunner } from "./SqlRunner.tsx";
import { RulesEditor } from "./RulesEditor.tsx";

type Tab = "collections" | "sqltables" | "sql" | "files" | "users" | "logs" | "rules";

export function ProjectView({ projectRef, onBack }: { projectRef: string; onBack: () => void }) {
  const [project, setProject] = useState<Project | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<Tab>("collections");

  useEffect(() => {
    getProject(projectRef)
      .then(setProject)
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, [projectRef]);

  // Data-plane calls use the service_role key (full access — this is the
  // developer's own admin console).
  const key = project?.service_role_key;

  return (
    <section>
      <div className="row between">
        <div className="row" style={{ gap: 10 }}>
          <button className="ghost" onClick={onBack}>
            ← Projects
          </button>
          <h1 style={{ margin: 0 }}>{project?.name || projectRef}</h1>
          <code className="ref">{projectRef}</code>
        </div>
        <div className="tabs">
          <button
            className={tab === "collections" ? "tab active" : "tab"}
            onClick={() => setTab("collections")}
          >
            Collections
          </button>
          <button
            className={tab === "sqltables" ? "tab active" : "tab"}
            onClick={() => setTab("sqltables")}
          >
            SQL Tables
          </button>
          <button className={tab === "sql" ? "tab active" : "tab"} onClick={() => setTab("sql")}>
            SQL
          </button>
          <button className={tab === "files" ? "tab active" : "tab"} onClick={() => setTab("files")}>
            Files
          </button>
          <button className={tab === "users" ? "tab active" : "tab"} onClick={() => setTab("users")}>
            Users
          </button>
          <button className={tab === "logs" ? "tab active" : "tab"} onClick={() => setTab("logs")}>
            Logs
          </button>
          <button className={tab === "rules" ? "tab active" : "tab"} onClick={() => setTab("rules")}>
            Rules
          </button>
        </div>
      </div>

      {error && <div className="error">{error}</div>}
      {key && project && (
        <Quota project={project} apiKey={key} onChange={setProject} />
      )}
      {!key ? (
        <p className="muted">Loading project…</p>
      ) : tab === "collections" ? (
        <DataBrowser projectRef={projectRef} apiKey={key} />
      ) : tab === "sqltables" ? (
        <SqlTables projectRef={projectRef} apiKey={key} />
      ) : tab === "sql" ? (
        <SqlRunner projectRef={projectRef} apiKey={key} />
      ) : tab === "files" ? (
        <FilesBrowser projectRef={projectRef} apiKey={key} />
      ) : tab === "users" ? (
        <UsersPanel projectRef={projectRef} />
      ) : tab === "logs" ? (
        <LogsPanel projectRef={projectRef} />
      ) : (
        <RulesEditor projectRef={projectRef} apiKey={key} />
      )}
    </section>
  );
}

function Quota({
  project,
  apiKey,
  onChange,
}: {
  project: Project;
  apiKey: string;
  onChange: (p: Project) => void;
}) {
  const [cols, setCols] = useState<number | null>(null);
  const [tables, setTables] = useState<number | null>(null);
  const [docs, setDocs] = useState<number | null>(null);
  const [storage, setStorage] = useState<number | null>(null);
  const [editing, setEditing] = useState(false);
  const [mc, setMc] = useState(String(project.max_collections ?? 5));
  const [mt, setMt] = useState(String(project.max_tables ?? 5));
  const [mdoc, setMdoc] = useState(String(project.max_documents ?? 10000));
  const [mstore, setMstore] = useState(String(Math.round((project.max_storage_bytes ?? 104857600) / (1024 * 1024))));
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const count = useCallback(async () => {
    try {
      const [c, t, d, s] = await Promise.all([
        listCollections(project.ref, apiKey).then((l) => l.length),
        runSql(project.ref, apiKey, "SHOW TABLES")
          .then((r) => r[0]?.rows?.length ?? 0)
          .catch(() => 0), // SQL engine may be off
        countDocuments(project.ref, apiKey).catch(() => 0),
        listBuckets(project.ref, apiKey)
          .then((b) => b.total_bytes)
          .catch(() => 0),
      ]);
      setCols(c);
      setTables(t);
      setDocs(d);
      setStorage(s);
    } catch {
      /* counts are best-effort */
    }
  }, [project.ref, apiKey]);

  useEffect(() => {
    count();
  }, [count]);

  useEffect(() => {
    setMc(String(project.max_collections ?? 5));
    setMt(String(project.max_tables ?? 5));
    setMdoc(String(project.max_documents ?? 10000));
    setMstore(String(Math.round((project.max_storage_bytes ?? 104857600) / (1024 * 1024))));
  }, [project.max_collections, project.max_tables, project.max_documents, project.max_storage_bytes]);

  async function save() {
    setBusy(true);
    setErr(null);
    try {
      const updated = await updateProjectLimits(project.ref, {
        max_collections: Number(mc),
        max_tables: Number(mt),
        max_documents: Number(mdoc),
        max_storage_bytes: Math.round(Number(mstore) * 1024 * 1024),
      });
      onChange({ ...project, ...updated });
      setEditing(false);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  const meter = (label: string, used: number | null, limit?: number, bytes = false) => {
    const cap = limit ?? 0;
    const unlimited = cap === 0;
    const u = used ?? 0;
    const pct = unlimited ? 0 : Math.min(100, Math.round((u / cap) * 100));
    const full = !unlimited && u >= cap;
    return (
      <div style={{ flex: 1, minWidth: 180 }}>
        <div className="row between" style={{ marginBottom: 4 }}>
          <span className="muted small">{label}</span>
          <span className="small" style={{ color: full ? "#e5484d" : undefined }}>
            {used === null ? "…" : bytes ? fmtBytes(u) : u} / {unlimited ? "∞" : bytes ? fmtBytes(cap) : cap}
          </span>
        </div>
        <div style={{ height: 6, borderRadius: 3, background: "#262d39", overflow: "hidden" }}>
          <div
            style={{
              height: "100%",
              width: `${unlimited ? 0 : pct}%`,
              background: full ? "#e5484d" : "#e2784a",
            }}
          />
        </div>
      </div>
    );
  };

  return (
    <div className="card" style={{ margin: "12px 0", padding: 14 }}>
      <div className="row between" style={{ marginBottom: 10 }}>
        <strong className="small">Usage &amp; quotas</strong>
        <button className="ghost small" onClick={() => setEditing((e) => !e)}>
          {editing ? "Cancel" : "Edit limits"}
        </button>
      </div>
      {editing ? (
        <div className="row" style={{ gap: 12, flexWrap: "wrap", alignItems: "flex-end" }}>
          <label className="small" style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            Max collections
            <input type="number" min={0} value={mc} onChange={(e) => setMc(e.target.value)} style={{ width: 120 }} />
          </label>
          <label className="small" style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            Max SQL tables
            <input type="number" min={0} value={mt} onChange={(e) => setMt(e.target.value)} style={{ width: 120 }} />
          </label>
          <label className="small" style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            Max documents
            <input type="number" min={0} value={mdoc} onChange={(e) => setMdoc(e.target.value)} style={{ width: 120 }} />
          </label>
          <label className="small" style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            Max storage (MB)
            <input type="number" min={0} value={mstore} onChange={(e) => setMstore(e.target.value)} style={{ width: 120 }} />
          </label>
          <button className="primary small" disabled={busy} onClick={save}>
            Save
          </button>
          <span className="muted small">0 = unlimited</span>
        </div>
      ) : (
        <div className="row" style={{ gap: 20, flexWrap: "wrap" }}>
          {meter("Collections", cols, project.max_collections)}
          {meter("SQL tables", tables, project.max_tables)}
          {meter("Documents", docs, project.max_documents)}
          {meter("Storage", storage, project.max_storage_bytes, true)}
        </div>
      )}
      {err && <div className="error small" style={{ marginTop: 8 }}>{err}</div>}
    </div>
  );
}
