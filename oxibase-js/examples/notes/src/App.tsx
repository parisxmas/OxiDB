import { useEffect, useState } from "react";
import { oxibase, configured } from "./oxibase.ts";

// A document collection (auto-created on first insert). Named to avoid colliding
// with any SQL-engine table of the same name in the project.
const TABLE = "demo_notes";

interface Note {
  _id?: string;
  body: string;
  done: boolean;
}

export function App() {
  const [notes, setNotes] = useState<Note[]>([]);
  const [draft, setDraft] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  async function load() {
    setLoading(true);
    // Exactly like Supabase: oxibase.from(TABLE).select(...)
    const { data, error } = await oxibase.from(TABLE).select("*").order("_id", { ascending: false });
    if (error) setError(error.message);
    else {
      setNotes((data as Note[]) ?? []);
      setError(null);
    }
    setLoading(false);
  }

  useEffect(() => {
    if (configured) load();
    else setLoading(false);
  }, []);

  async function add(e: React.FormEvent) {
    e.preventDefault();
    const body = draft.trim();
    if (!body) return;
    setDraft("");
    const { error } = await oxibase.from(TABLE).insert({ body, done: false });
    if (error) setError(error.message);
    else load();
  }

  async function toggle(n: Note) {
    const { error } = await oxibase.from(TABLE).update({ done: !n.done }).eq("_id", n._id);
    if (error) setError(error.message);
    else load();
  }

  async function remove(n: Note) {
    const { error } = await oxibase.from(TABLE).delete().eq("_id", n._id);
    if (error) setError(error.message);
    else load();
  }

  if (!configured) {
    return (
      <div className="app">
        <h1>◇ OxiBase Notes</h1>
        <div className="card setup">
          <p>Set the project connection in <code>.env</code> (copy <code>.env.example</code>):</p>
          <pre>{`VITE_OXIBASE_URL=http://127.0.0.1:8087
VITE_OXIBASE_REF=<your project ref>
VITE_OXIBASE_KEY=<service_role key>`}</pre>
          <p className="muted">Get the ref and key from the OxiBase dashboard → Open a project → API keys.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="app">
      <h1>◇ OxiBase Notes</h1>
      <p className="muted sub">
        A tiny React app talking to OxiBase with <code>oxibase-js</code> — the same
        <code> createClient().from("notes").select()</code> API as Supabase.
      </p>

      <form className="add" onSubmit={add}>
        <input
          placeholder="Write a note…"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          autoFocus
        />
        <button type="submit">Add</button>
      </form>

      {error && <div className="error">{error}</div>}

      {loading ? (
        <p className="muted">Loading…</p>
      ) : notes.length === 0 ? (
        <p className="muted">No notes yet. Add one above.</p>
      ) : (
        <ul className="notes">
          {notes.map((n) => (
            <li key={n._id} className={n.done ? "done" : ""}>
              <label>
                <input type="checkbox" checked={n.done} onChange={() => toggle(n)} />
                <span>{n.body}</span>
              </label>
              <button className="del" onClick={() => remove(n)} title="Delete">
                ✕
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
