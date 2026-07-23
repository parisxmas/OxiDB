import { useEffect, useState } from "react";
import { type Project, getProject } from "./api.ts";
import { DataBrowser } from "./DataBrowser.tsx";
import { SqlRunner } from "./SqlRunner.tsx";

type Tab = "tables" | "sql";

export function ProjectView({ projectRef, onBack }: { projectRef: string; onBack: () => void }) {
  const [project, setProject] = useState<Project | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<Tab>("tables");

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
          <button className={tab === "tables" ? "tab active" : "tab"} onClick={() => setTab("tables")}>
            Tables
          </button>
          <button className={tab === "sql" ? "tab active" : "tab"} onClick={() => setTab("sql")}>
            SQL
          </button>
        </div>
      </div>

      {error && <div className="error">{error}</div>}
      {!key ? (
        <p className="muted">Loading project…</p>
      ) : tab === "tables" ? (
        <DataBrowser projectRef={projectRef} apiKey={key} />
      ) : (
        <SqlRunner projectRef={projectRef} apiKey={key} />
      )}
    </section>
  );
}
