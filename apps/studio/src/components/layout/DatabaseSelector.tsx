import { useState } from "react";
import { useDatabase } from "../../context/DatabaseContext";
import { useToast } from "../common/Toast";
import { PromptDialog } from "../common/PromptDialog";
import { ConfirmDialog } from "../common/ConfirmDialog";
import { IconSql } from "./NavIcons";

const BUILTIN = ["oxidb", "postgres"];

/** Global current-database picker (ADR-0012), shown in the header. */
export function DatabaseSelector() {
  const { db, databases, setDb, createDb, dropDb } = useDatabase();
  const toast = useToast();
  const [showNew, setShowNew] = useState(false);
  const [showDrop, setShowDrop] = useState(false);

  if (databases.length === 0) return null;

  const create = async (name: string) => {
    setShowNew(false);
    try {
      await createDb(name);
      toast("Database created", "success");
    } catch (e) {
      toast(String(e), "error");
    }
  };

  const drop = async () => {
    setShowDrop(false);
    try {
      await dropDb(db);
      toast("Database dropped", "success");
    } catch (e) {
      toast(String(e), "error");
    }
  };

  return (
    <div className="db-selector" title="Current database">
      <span className="db-selector-icon"><IconSql size={13} /></span>
      <select value={db} onChange={(e) => setDb(e.target.value)}>
        {databases.map((d) => (
          <option key={d} value={d}>
            {d}
          </option>
        ))}
      </select>
      <button className="db-selector-btn" title="New database" onClick={() => setShowNew(true)}>
        +
      </button>
      <button
        className="db-selector-btn"
        title="Drop current database"
        onClick={() => setShowDrop(true)}
        disabled={BUILTIN.includes(db)}
      >
        🗑
      </button>

      {showNew && (
        <PromptDialog
          title="New database"
          label="Database name"
          placeholder="my_database"
          confirmLabel="Create"
          onConfirm={create}
          onCancel={() => setShowNew(false)}
        />
      )}
      {showDrop && (
        <ConfirmDialog
          title={`Drop ${db}?`}
          message={`This permanently removes the database "${db}" and everything in it.`}
          confirmLabel="Drop database"
          danger
          onConfirm={drop}
          onCancel={() => setShowDrop(false)}
        />
      )}
    </div>
  );
}
