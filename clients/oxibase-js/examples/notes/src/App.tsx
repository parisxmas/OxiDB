import { useEffect, useState } from "react";
import { oxibase, configured } from "./oxibase.ts";

// The demo drives BOTH engines of the `barisdb` project:
//  • `addressses` — a SQL-engine table (id PK, name, surname, address, birthdate)
//  • `deneme`     — a document collection ({ name } docs)
const SQL_TABLE = `"addressses"`;
const COLLECTION = "deneme";

interface Address {
  id: number;
  name: string | null;
  surname: string | null;
  address: string | null;
  birthdate: number | null; // epoch ms
}

interface Doc {
  _id?: number | string;
  name?: string;
  [k: string]: unknown;
}

export function App() {
  if (!configured) {
    return (
      <div className="app">
        <h1>◇ barisdb demo</h1>
        <div className="card setup">
          <p>Set the project connection in <code>.env</code> (copy <code>.env.example</code>):</p>
          <pre>{`VITE_OXIBASE_URL=<data-plane origin>
VITE_OXIBASE_REF=<project ref>
VITE_OXIBASE_KEY=<service_role key>`}</pre>
          <p className="muted">Get the ref and key from the OxiBase dashboard → Open a project → API keys.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="app wide">
      <h1>◇ barisdb</h1>
      <p className="muted sub">
        One OxiBase project, both engines: the <code>addressses</code> SQL table via{" "}
        <code>oxibase.sql()</code> and the <code>deneme</code> collection via{" "}
        <code>oxibase.from()</code>.
      </p>
      <div className="panels">
        <Addresses />
        <Deneme />
      </div>
    </div>
  );
}

/** SQL engine — the `addressses` table, parameterized statements only. */
function Addresses() {
  const [rows, setRows] = useState<Address[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [name, setName] = useState("");
  const [surname, setSurname] = useState("");
  const [address, setAddress] = useState("");
  const [birthdate, setBirthdate] = useState("");

  async function load() {
    setLoading(true);
    const { results, error } = await oxibase.sql(
      `SELECT "id", "name", "surname", "address", "birthdate" FROM ${SQL_TABLE} ORDER BY "id"`,
    );
    if (error) setError(error);
    else {
      const r = results?.[0];
      setRows(
        (r?.rows ?? []).map((row) => ({
          id: Number(row[0]),
          name: row[1] as string | null,
          surname: row[2] as string | null,
          address: row[3] as string | null,
          birthdate: row[4] as number | null,
        })),
      );
      setError(null);
    }
    setLoading(false);
  }

  useEffect(() => {
    load();
  }, []);

  async function add(e: React.FormEvent) {
    e.preventDefault();
    if (!name.trim()) return;
    // `id` is a plain INT PRIMARY KEY (no auto-increment) — take MAX+1.
    const next = await oxibase.sql(`SELECT COALESCE(MAX("id"), 0) + 1 FROM ${SQL_TABLE}`);
    if (next.error) return setError(next.error);
    const id = Number(next.results?.[0]?.rows?.[0]?.[0] ?? 1);
    const bd = birthdate ? Date.parse(birthdate) : null;
    const ins = await oxibase.sql(
      `INSERT INTO ${SQL_TABLE} ("id", "name", "surname", "address", "birthdate") VALUES (?, ?, ?, ?, ?)`,
      [id, name.trim(), surname.trim() || null, address.trim() || null, bd],
    );
    if (ins.error) setError(ins.error);
    else {
      setName("");
      setSurname("");
      setAddress("");
      setBirthdate("");
      load();
    }
  }

  async function remove(a: Address) {
    const { error } = await oxibase.sql(`DELETE FROM ${SQL_TABLE} WHERE "id" = ?`, [a.id]);
    if (error) setError(error);
    else load();
  }

  return (
    <section className="panel">
      <h2>
        Addresses <span className="muted tag">SQL · addressses</span>
      </h2>

      <form className="addr-form" onSubmit={add}>
        <input placeholder="name" value={name} onChange={(e) => setName(e.target.value)} />
        <input placeholder="surname" value={surname} onChange={(e) => setSurname(e.target.value)} />
        <input
          placeholder="address"
          className="span2"
          value={address}
          onChange={(e) => setAddress(e.target.value)}
        />
        <input
          type="date"
          title="birthdate"
          value={birthdate}
          onChange={(e) => setBirthdate(e.target.value)}
        />
        <button type="submit">Add</button>
      </form>

      {error && <div className="error">{error}</div>}

      {loading ? (
        <p className="muted">Loading…</p>
      ) : rows.length === 0 ? (
        <p className="muted">No addresses yet. Add one above.</p>
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>id</th>
                <th>name</th>
                <th>surname</th>
                <th>address</th>
                <th>birthdate</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {rows.map((a) => (
                <tr key={a.id}>
                  <td>{a.id}</td>
                  <td>{a.name}</td>
                  <td>{a.surname}</td>
                  <td>{a.address}</td>
                  <td>{a.birthdate ? new Date(a.birthdate).toISOString().slice(0, 10) : ""}</td>
                  <td>
                    <button className="del" onClick={() => remove(a)} title="Delete">
                      ✕
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

/** Document engine — the `deneme` collection via `.from()`. */
function Deneme() {
  const [docs, setDocs] = useState<Doc[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [draft, setDraft] = useState("");

  async function load() {
    setLoading(true);
    const { data, error } = await oxibase
      .from(COLLECTION)
      .select("*")
      .order("_id", { ascending: false });
    if (error) setError(error.message);
    else {
      setDocs((data as Doc[]) ?? []);
      setError(null);
    }
    setLoading(false);
  }

  useEffect(() => {
    load();
  }, []);

  async function add(e: React.FormEvent) {
    e.preventDefault();
    const name = draft.trim();
    if (!name) return;
    setDraft("");
    const { error } = await oxibase.from(COLLECTION).insert({ name });
    if (error) setError(error.message);
    else load();
  }

  async function remove(d: Doc) {
    const { error } = await oxibase.from(COLLECTION).delete().eq("_id", d._id);
    if (error) setError(error.message);
    else load();
  }

  return (
    <section className="panel">
      <h2>
        Deneme <span className="muted tag">collection · deneme</span>
      </h2>

      <form className="add" onSubmit={add}>
        <input placeholder="name…" value={draft} onChange={(e) => setDraft(e.target.value)} />
        <button type="submit">Add</button>
      </form>

      {error && <div className="error">{error}</div>}

      {loading ? (
        <p className="muted">Loading…</p>
      ) : docs.length === 0 ? (
        <p className="muted">No documents yet. Add one above.</p>
      ) : (
        <ul className="notes">
          {docs.map((d) => (
            <li key={String(d._id)}>
              <label>
                <span className="muted id">#{String(d._id)}</span>
                <span>{d.name ?? JSON.stringify(d)}</span>
              </label>
              <button className="del" onClick={() => remove(d)} title="Delete">
                ✕
              </button>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
