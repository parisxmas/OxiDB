import { useEffect, useState } from "react";
import { type Rules, listCollections, getRules, setRules, deleteRules } from "./dataApi.ts";

const EMPTY: Rules = { read: "true", create: "true", update: "true", delete: "true" };

const PRESETS: { label: string; hint: string; rules: Rules }[] = [
  { label: "Public", hint: "anyone can read & write", rules: { read: "true", create: "true", update: "true", delete: "true" } },
  { label: "Public read-only", hint: "anyone reads, nobody writes", rules: { read: "true", create: "false", update: "false", delete: "false" } },
  { label: "Signed-in only", hint: "must be authenticated", rules: { read: "auth != null", create: "auth != null", update: "auth != null", delete: "auth != null" } },
  { label: "Owner only", hint: "reads public, owner writes", rules: { read: "true", create: "auth != null", update: "auth.username == doc.owner", delete: "auth.username == doc.owner" } },
];

export function RulesEditor({ projectRef, apiKey }: { projectRef: string; apiKey: string }) {
  const [collections, setCollections] = useState<string[]>([]);
  const [active, setActive] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    listCollections(projectRef, apiKey)
      .then((cols) => {
        setCollections(cols);
        setActive((a) => a ?? cols[0] ?? null);
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectRef]);

  return (
    <div className="browser">
      <aside className="collections">
        <div className="side-title">Collections</div>
        {collections.length === 0 && <div className="muted small">none yet</div>}
        {collections.map((c) => (
          <button key={c} className={c === active ? "coll active" : "coll"} onClick={() => setActive(c)}>
            <span className="ellip">{c}</span>
          </button>
        ))}
      </aside>
      <div className="rows-pane">
        {error && <div className="error">{error}</div>}
        {active ? (
          <RuleForm key={active} projectRef={projectRef} apiKey={apiKey} collection={active} />
        ) : (
          <p className="muted">Select a collection to edit its rules.</p>
        )}
      </div>
    </div>
  );
}

function RuleForm({ projectRef, apiKey, collection }: { projectRef: string; apiKey: string; collection: string }) {
  const [rules, setLocal] = useState<Rules>(EMPTY);
  const [defined, setDefined] = useState(false);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);

  async function load() {
    setLoading(true);
    try {
      const r = await getRules(projectRef, collection, apiKey);
      setDefined(r !== null);
      setLocal(r ?? EMPTY);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [collection]);

  async function save() {
    setBusy(true);
    setError(null);
    try {
      await setRules(projectRef, collection, apiKey, rules);
      setDefined(true);
      setSaved(true);
      setTimeout(() => setSaved(false), 1500);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function clear() {
    if (!confirm(`Remove all rules from "${collection}"? Reads become open again and anon writes are denied by default.`)) return;
    setBusy(true);
    setError(null);
    try {
      await deleteRules(projectRef, collection, apiKey);
      setDefined(false);
      setLocal(EMPTY);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  const field = (k: keyof Rules, label: string) => (
    <label className="rulefield">
      <span>{label}</span>
      <input
        value={rules[k]}
        spellCheck={false}
        onChange={(e) => setLocal({ ...rules, [k]: e.target.value })}
        placeholder="true"
      />
    </label>
  );

  if (loading) return <p className="muted">Loading…</p>;

  return (
    <div>
      <div className="row between">
        <h3 style={{ margin: "4px 0" }}>{collection}</h3>
        <span className={defined ? "badge on" : "badge off"}>
          {defined ? "rules active" : "no rules (reads open · anon writes denied)"}
        </span>
      </div>

      <div className="presets">
        <span className="muted small">Presets:</span>
        {PRESETS.map((p) => (
          <button key={p.label} className="ghost small" title={p.hint} onClick={() => setLocal(p.rules)}>
            {p.label}
          </button>
        ))}
      </div>

      <div className="card rules">
        {field("read", "read")}
        {field("create", "create")}
        {field("update", "update")}
        {field("delete", "delete")}
        <p className="muted small hint">
          Boolean expressions. Available: <code>auth</code>, <code>auth.username</code>, <code>auth.role</code>,
          <code> doc.&lt;field&gt;</code>, string literals <code>'x'</code>, <code>true</code>/<code>false</code>;
          operators <code>== != &amp;&amp; || !</code>. Example: <code>auth.username == doc.owner</code>.
        </p>
      </div>

      {error && <div className="error">{error}</div>}
      <div className="row" style={{ gap: 8, marginTop: 12 }}>
        <button className="primary" onClick={save} disabled={busy}>
          {saved ? "Saved ✓" : "Save rules"}
        </button>
        {defined && (
          <button className="ghost danger" onClick={clear} disabled={busy}>
            Clear rules
          </button>
        )}
      </div>
    </div>
  );
}
