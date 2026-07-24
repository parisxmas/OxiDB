import { useState } from "react";
import { type SqlResult, runSql } from "./dataApi.ts";
import { ResultGrid } from "./SqlTables.tsx";
import { SqlEditor } from "./SqlEditor.tsx";

const SAMPLE = `CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, body TEXT);
INSERT INTO notes (id, body) VALUES (1, 'hello from OxiBase');
SELECT * FROM notes;`;

export function SqlRunner({ projectRef, apiKey }: { projectRef: string; apiKey: string }) {
  const [sql, setSql] = useState(SAMPLE);
  const [results, setResults] = useState<SqlResult[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function run() {
    setBusy(true);
    setError(null);
    try {
      setResults(await runSql(projectRef, apiKey, sql));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setResults(null);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="sql">
      <div className="card">
        <SqlEditor value={sql} onChange={setSql} onRun={run} />
        <div className="row between" style={{ marginTop: 8 }}>
          <span className="muted small">⌘/Ctrl + Enter to run</span>
          <button className="primary" onClick={run} disabled={busy}>
            {busy ? "Running…" : "Run"}
          </button>
        </div>
      </div>

      {error && (
        <div className="error">
          {error}
          {/SQL engine is not enabled|OXIDB_SQL/i.test(error) && (
            <div className="small" style={{ marginTop: 6 }}>
              Start the data plane with <code>OXIDB_SQL=1</code> to use the SQL engine.
            </div>
          )}
        </div>
      )}

      {results?.map((r, i) => (
        <ResultBlock key={i} result={r} />
      ))}
    </div>
  );
}

function ResultBlock({ result }: { result: SqlResult }) {
  if (result.ddl) return <div className="result muted">✓ statement executed</div>;
  if (result.affected !== undefined) {
    return (
      <div className="result muted">
        ✓ {result.affected} row{result.affected === 1 ? "" : "s"} affected
        {result.last_insert_id !== undefined && ` (last id ${result.last_insert_id})`}
      </div>
    );
  }
  return (
    <div className="result">
      <ResultGrid result={result} emptyText="0 rows" />
    </div>
  );
}
