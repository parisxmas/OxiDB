import { useState } from "react";
import { completePasswordReset } from "./api.ts";

/** Public /reset page — the landing target of password-reset emails
 *  (`/reset?ref=<project>&token=<one-time token>`). */
export function ResetPassword() {
  const params = new URLSearchParams(window.location.search);
  const ref = params.get("ref") ?? "";
  const token = params.get("token") ?? "";

  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [done, setDone] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (password.length < 8) return setError("password must be at least 8 characters");
    if (password !== confirm) return setError("passwords do not match");
    setBusy(true);
    setError(null);
    try {
      await completePasswordReset(ref, token, password);
      setDone(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  const invalid = !ref || !token;

  return (
    <div className="auth">
      <div className="card auth-card">
        <div className="brand center">
          <img src="/logo.svg" alt="OxiBase" className="auth-logo" />
        </div>
        {invalid ? (
          <p className="muted center">
            This reset link is incomplete — request a new one from the app you signed up in.
          </p>
        ) : done ? (
          <>
            <h3 className="center" style={{ margin: "4px 0" }}>
              Password updated ✓
            </h3>
            <p className="muted center">You can sign in to the app with your new password now.</p>
          </>
        ) : (
          <form onSubmit={submit} style={{ display: "grid", gap: 12 }}>
            <p className="muted center" style={{ margin: 0 }}>
              Choose a new password
            </p>
            <input
              type="password"
              placeholder="new password"
              value={password}
              autoFocus
              onChange={(e) => setPassword(e.target.value)}
            />
            <input
              type="password"
              placeholder="repeat password"
              value={confirm}
              onChange={(e) => setConfirm(e.target.value)}
            />
            {error && <div className="error">{error}</div>}
            <button className="primary" disabled={busy}>
              {busy ? "Saving…" : "Set password"}
            </button>
          </form>
        )}
      </div>
    </div>
  );
}
