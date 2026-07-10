import { useDatabase } from "../../context/DatabaseContext";
import { useToast } from "../common/Toast";

const BUILTIN = ["oxidb", "postgres"];

/** Global current-database picker (ADR-0012), shown in the header. */
export function DatabaseSelector() {
  const { db, databases, setDb, createDb, dropDb } = useDatabase();
  const toast = useToast();

  if (databases.length === 0) return null;

  const onNew = async () => {
    const name = window.prompt("New database name:");
    if (!name?.trim()) return;
    try {
      await createDb(name.trim());
      toast("Database created", "success");
    } catch (e) {
      toast(String(e), "error");
    }
  };

  const onDrop = async () => {
    if (!db || BUILTIN.includes(db)) return;
    if (!window.confirm(`Drop database "${db}" and everything in it?`)) return;
    try {
      await dropDb(db);
      toast("Database dropped", "success");
    } catch (e) {
      toast(String(e), "error");
    }
  };

  return (
    <div className="db-selector" title="Current database">
      <span className="db-selector-icon">🗄</span>
      <select value={db} onChange={(e) => setDb(e.target.value)}>
        {databases.map((d) => (
          <option key={d} value={d}>
            {d}
          </option>
        ))}
      </select>
      <button className="db-selector-btn" title="New database" onClick={onNew}>
        +
      </button>
      <button
        className="db-selector-btn"
        title="Drop current database"
        onClick={onDrop}
        disabled={BUILTIN.includes(db)}
      >
        🗑
      </button>
    </div>
  );
}
