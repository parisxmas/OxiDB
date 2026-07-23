import { useEffect, useState } from "react";
import {
  type Project,
  listProjects,
  createProject,
  getProject,
  deleteProject,
  rotateKeys,
} from "./api.ts";

export function Projects() {
  const [projects, setProjects] = useState<Project[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [creating, setCreating] = useState(false);

  async function refresh() {
    setLoading(true);
    try {
      setProjects(await listProjects());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refresh();
  }, []);

  async function create(e: React.FormEvent) {
    e.preventDefault();
    if (!name.trim()) return;
    setCreating(true);
    setError(null);
    try {
      const p = await createProject(name.trim());
      setName("");
      // The create response carries the keys — show it at the top immediately.
      setProjects((prev) => [p, ...prev]);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setCreating(false);
    }
  }

  return (
    <section>
      <div className="row between">
        <h1>Projects</h1>
        <span className="muted">{projects.length} total</span>
      </div>

      <form className="card create" onSubmit={create}>
        <input
          placeholder="New project name"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
        <button className="primary" disabled={creating || !name.trim()}>
          {creating ? "Provisioning…" : "New project"}
        </button>
      </form>

      {error && <div className="error">{error}</div>}
      {loading ? (
        <p className="muted">Loading…</p>
      ) : projects.length === 0 ? (
        <p className="muted">No projects yet — create one above.</p>
      ) : (
        <div className="grid">
          {projects.map((p) => (
            <ProjectCard key={p.ref} project={p} onDeleted={refresh} />
          ))}
        </div>
      )}
    </section>
  );
}

function ProjectCard({ project, onDeleted }: { project: Project; onDeleted: () => void }) {
  const [full, setFull] = useState<Project>(project);
  const [showKeys, setShowKeys] = useState(!!project.anon_key);
  const [busy, setBusy] = useState(false);

  async function toggleKeys() {
    if (!showKeys && !full.anon_key) {
      setBusy(true);
      try {
        setFull(await getProject(project.ref));
      } finally {
        setBusy(false);
      }
    }
    setShowKeys((s) => !s);
  }

  async function rotate() {
    if (!confirm("Rotate keys? The current anon and service_role keys stop working immediately.")) return;
    setBusy(true);
    try {
      setFull(await rotateKeys(project.ref));
      setShowKeys(true);
    } finally {
      setBusy(false);
    }
  }

  async function remove() {
    if (!confirm(`Delete project "${project.name || project.ref}" and its database? This cannot be undone.`)) return;
    setBusy(true);
    try {
      await deleteProject(project.ref);
      onDeleted();
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="card project">
      <div className="row between">
        <div>
          <div className="pname">{project.name || <em className="muted">unnamed</em>}</div>
          <code className="ref">{project.ref}</code>
        </div>
        <div className="actions">
          <button className="ghost" onClick={toggleKeys} disabled={busy}>
            {showKeys ? "Hide keys" : "API keys"}
          </button>
          <button className="ghost warn" onClick={rotate} disabled={busy}>
            Rotate
          </button>
          <button className="ghost danger" onClick={remove} disabled={busy}>
            Delete
          </button>
        </div>
      </div>

      <div className="endpoint muted">
        Data API: <code>{full.url || `/rest/v1?db=${project.ref}`}</code>
      </div>

      {showKeys && (
        <div className="keys">
          <KeyRow label="anon key" value={full.anon_key} hint="public — safe in a browser" />
          <KeyRow
            label="service_role key"
            value={full.service_role_key}
            hint="secret — server-side only, bypasses rules"
            secret
          />
        </div>
      )}
    </div>
  );
}

function KeyRow({
  label,
  value,
  hint,
  secret,
}: {
  label: string;
  value?: string;
  hint: string;
  secret?: boolean;
}) {
  const [copied, setCopied] = useState(false);
  if (!value) return null;
  async function copy() {
    await navigator.clipboard.writeText(value!);
    setCopied(true);
    setTimeout(() => setCopied(false), 1200);
  }
  return (
    <div className="keyrow">
      <div className="keymeta">
        <span className={`keylabel ${secret ? "danger" : ""}`}>{label}</span>
        <span className="muted small">{hint}</span>
      </div>
      <div className="keyval">
        <code>{value.slice(0, 24)}…{value.slice(-8)}</code>
        <button className="ghost" onClick={copy}>
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
    </div>
  );
}
