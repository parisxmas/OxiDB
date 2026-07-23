import { useState } from "react";
import { login, signup } from "./api.ts";

export function Auth({ onAuthed }: { onAuthed: () => void }) {
  const [mode, setMode] = useState<"login" | "signup">("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      if (mode === "signup") await signup(email, password);
      else await login(email, password);
      onAuthed();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="auth">
      <form className="card auth-card" onSubmit={submit}>
        <div className="brand center">
          <span className="logo">◇</span> OxiBase
        </div>
        <p className="muted center">
          {mode === "login" ? "Sign in to your control plane" : "Create a developer account"}
        </p>
        <label>
          Email
          <input
            type="email"
            autoComplete="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            required
          />
        </label>
        <label>
          Password
          <input
            type="password"
            autoComplete={mode === "signup" ? "new-password" : "current-password"}
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            minLength={mode === "signup" ? 8 : undefined}
            required
          />
        </label>
        {error && <div className="error">{error}</div>}
        <button className="primary" disabled={busy}>
          {busy ? "…" : mode === "login" ? "Sign in" : "Sign up"}
        </button>
        <div className="switch">
          {mode === "login" ? (
            <>
              No account?{" "}
              <a onClick={() => { setMode("signup"); setError(null); }}>Sign up</a>
            </>
          ) : (
            <>
              Have an account?{" "}
              <a onClick={() => { setMode("login"); setError(null); }}>Sign in</a>
            </>
          )}
        </div>
      </form>
    </div>
  );
}
