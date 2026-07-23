import { useState } from "react";
import { type SqlResult, runSql } from "./dataApi.ts";

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

  function onKey(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      e.preventDefault();
      run();
    }
  }

  return (
    <div className="sql">
      <div className="card">
        <textarea
          className="sql-input"
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          onKeyDown={onKey}
          rows={8}
          spellCheck={false}
          placeholder="SELECT * FROM …"
        />
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
  const cols = result.columns ?? [];
  const rows = result.rows ?? [];
  return (
    <div className="result">
      <div className="table-wrap">
        <table className="grid-table">
          <thead>
            <tr>
              {cols.map((c, i) => (
                <th key={i}>{c}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, ri) => (
              <tr key={ri}>
                {(row as unknown[]).map((v, ci) => (
                  <td key={ci}>{v === null ? "" : typeof v === "object" ? JSON.stringify(v) : String(v)}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="muted small">{rows.length} row{rows.length === 1 ? "" : "s"}</div>
    </div>
  );
}
