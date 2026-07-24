import { useEffect, useState } from "react";
import {
  type ProjectUser,
  listProjectUsers,
  deleteProjectUser,
  setProjectUserPassword,
  verifyProjectUser,
} from "./api.ts";
import { ProvidersPanel } from "./ProvidersPanel.tsx";

/** Users tab: the project's end users (signup via `oxibase.auth`). */
export function UsersPanel({ projectRef }: { projectRef: string }) {
  const [users, setUsers] = useState<ProjectUser[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);

  async function refresh() {
    setLoading(true);
    try {
      setUsers(await listProjectUsers(projectRef));
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectRef]);

  async function act(op: () => Promise<unknown>) {
    setBusy(true);
    setError(null);
    try {
      await op();
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  function setPassword(u: ProjectUser) {
    const pw = prompt(`New password for ${u.email} (min 8 characters):`);
    if (!pw) return;
    if (pw.length < 8) {
      setError("password must be at least 8 characters");
      return;
    }
    act(() => setProjectUserPassword(projectRef, u.email, pw));
  }

  function remove(u: ProjectUser) {
    if (!confirm(`Delete user "${u.email}" and revoke their sessions?`)) return;
    act(() => deleteProjectUser(projectRef, u.email));
  }

  return (
    <div style={{ marginTop: 16 }}>
      <div className="row between">
        <h3 style={{ margin: "4px 0" }}>
          End users <span className="muted small">({users.length})</span>
        </h3>
        <button className="ghost" onClick={refresh}>
          Refresh
        </button>
      </div>
      <p className="muted small">
        Accounts created through <code>oxibase.auth.signUp()</code>. Setting a password or deleting
        a user revokes their sessions.
      </p>

      {error && <div className="error">{error}</div>}

      {loading ? (
        <p className="muted">Loading…</p>
      ) : users.length === 0 ? (
        <p className="muted">
          No users yet — your app creates them with <code>oxibase.auth.signUp()</code>.
        </p>
      ) : (
        <div className="table-wrap">
          <table className="grid-table">
            <thead>
              <tr>
                <th>email</th>
                <th>created</th>
                <th>status</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {users.map((u) => (
                <tr key={u.email}>
                  <td>{u.email}</td>
                  <td className="muted">
                    {u.created_at ? new Date(u.created_at * 1000).toISOString().slice(0, 10) : ""}
                  </td>
                  <td>
                    {u.verified ? (
                      <span className="badge on">verified</span>
                    ) : (
                      <span className="badge off">unverified</span>
                    )}
                  </td>
                  <td className="rowdel">
                    <span className="row" style={{ gap: 4, justifyContent: "flex-end" }}>
                      {!u.verified && (
                        <button
                          className="ghost small"
                          disabled={busy}
                          title="Mark verified (support path)"
                          onClick={() => act(() => verifyProjectUser(projectRef, u.email))}
                        >
                          Verify
                        </button>
                      )}
                      <button
                        className="ghost small"
                        disabled={busy}
                        title="Set a new password"
                        onClick={() => setPassword(u)}
                      >
                        Password
                      </button>
                      <button
                        className="ghost danger small"
                        disabled={busy}
                        title="Delete user"
                        onClick={() => remove(u)}
                      >
                        ✕
                      </button>
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <ProvidersPanel projectRef={projectRef} />
    </div>
  );
}
