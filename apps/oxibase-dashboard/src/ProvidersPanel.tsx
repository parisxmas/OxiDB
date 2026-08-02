import { useEffect, useState } from "react";
import { type AuthProviders, type ProviderConfig, getAuthProviders, setAuthProviders } from "./api.ts";

type Name = "google" | "github";

const HELP: Record<Name, { label: string; where: string }> = {
  google: {
    label: "Google",
    where: "Google Cloud Console → APIs & Services → Credentials → OAuth client ID (Web application)",
  },
  github: {
    label: "GitHub",
    where: "GitHub → Settings → Developer settings → OAuth Apps → New OAuth App",
  },
};

const fieldStyle: React.CSSProperties = {
  display: "flex",
  flexDirection: "column",
  gap: 4,
  marginTop: 8,
};

/**
 * Social sign-in configuration for a project's *end users*. The client secret
 * is write-only: the control plane seals it and never sends it back, so the
 * field shows a placeholder once one is stored and stays empty on re-save
 * unless the developer types a new one.
 */
export function ProvidersPanel({ projectRef }: { projectRef: string }) {
  const [cfg, setCfg] = useState<AuthProviders | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [saved, setSaved] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const [ids, setIds] = useState<Record<Name, string>>({ google: "", github: "" });
  const [secrets, setSecrets] = useState<Record<Name, string>>({ google: "", github: "" });
  const [urls, setUrls] = useState("");

  function adopt(c: AuthProviders) {
    setCfg(c);
    setIds({ google: c.google.client_id ?? "", github: c.github.client_id ?? "" });
    setSecrets({ google: "", github: "" });
    setUrls(c.redirect_urls.join("\n"));
  }

  useEffect(() => {
    getAuthProviders(projectRef)
      .then((c) => {
        adopt(c);
        setError(null);
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, [projectRef]);

  async function apply(patch: Parameters<typeof setAuthProviders>[1], message: string) {
    setBusy(true);
    setError(null);
    setSaved(null);
    try {
      adopt(await setAuthProviders(projectRef, patch));
      setSaved(message);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  function save(name: Name) {
    if (!ids[name].trim()) {
      setError(`${HELP[name].label}: a client ID is required`);
      return;
    }
    const entry: { client_id: string; client_secret?: string } = { client_id: ids[name].trim() };
    if (secrets[name].trim()) entry.client_secret = secrets[name].trim();
    apply({ [name]: entry }, `${HELP[name].label} saved`);
  }

  function clear(name: Name) {
    if (
      !confirm(
        `Remove the ${HELP[name].label} configuration? Users will no longer be able to sign in with it.`,
      )
    )
      return;
    apply({ [name]: null }, `${HELP[name].label} removed`);
  }

  function saveUrls() {
    apply(
      {
        redirect_urls: urls
          .split("\n")
          .map((s) => s.trim())
          .filter(Boolean),
      },
      "Redirect URLs saved",
    );
  }

  function card(name: Name, p: ProviderConfig) {
    return (
      <div className="card" style={{ margin: "12px 0", padding: 14 }} key={name}>
        <div className="row between" style={{ marginBottom: 6 }}>
          <strong className="small">
            {HELP[name].label}{" "}
            {p.secret_set ? (
              <span className="badge on">enabled</span>
            ) : (
              <span className="badge off">off</span>
            )}
          </strong>
          {p.secret_set && (
            <button className="ghost small" disabled={busy} onClick={() => clear(name)}>
              Remove
            </button>
          )}
        </div>
        <p className="muted small" style={{ margin: 0 }}>
          Create the OAuth app in {HELP[name].where}.
        </p>
        <label className="small" style={fieldStyle}>
          Client ID
          <input
            value={ids[name]}
            placeholder="client id"
            spellCheck={false}
            onChange={(e) => setIds({ ...ids, [name]: e.target.value })}
          />
        </label>
        <label className="small" style={fieldStyle}>
          Client secret
          <input
            type="password"
            value={secrets[name]}
            placeholder={p.secret_set ? "•••••••• stored — type to replace" : "client secret"}
            onChange={(e) => setSecrets({ ...secrets, [name]: e.target.value })}
          />
        </label>
        <label className="small" style={fieldStyle}>
          Authorized redirect URI — paste this into the provider
          <input readOnly value={p.callback_url} onFocus={(e) => e.currentTarget.select()} />
        </label>
        <button className="primary small" style={{ marginTop: 10 }} disabled={busy} onClick={() => save(name)}>
          Save {HELP[name].label}
        </button>
      </div>
    );
  }

  return (
    <div style={{ marginTop: 28 }}>
      <h3 style={{ margin: "4px 0" }}>Sign-in methods</h3>
      <p className="muted small">
        Beyond email + password: social sign-in with an account your users already have (
        <code>oxibase.auth.signInWithOAuth({"{ provider: 'github' }"})</code>) and passwordless{" "}
        <strong>magic links</strong> (<code>oxibase.auth.signInWithMagicLink({"{ email }"})</code>,
        which needs no configuration beyond a redirect URL below). Someone signing in a new way is
        matched to an existing account by verified email, so they land in one account rather than
        two.
      </p>

      {error && <div className="error small">{error}</div>}
      {saved && <div className="muted small">{saved} ✓</div>}

      {!cfg ? (
        <p className="muted">Loading…</p>
      ) : (
        <>
          {card("google", cfg.google)}
          {card("github", cfg.github)}

          <div className="card" style={{ margin: "12px 0", padding: 14 }}>
            <strong className="small">Allowed redirect URLs</strong>
            <p className="muted small">
              Where a completed sign-in may send the user, one per line. The session is handed over
              in the URL, so only these destinations are accepted. A trailing <code>*</code> allows
              any deeper path (<code>https://app.example.com/auth/*</code>) but can never widen the
              host. <strong>Magic links use this list too</strong> — passwordless sign-in needs at
              least one entry.
            </p>
            <textarea
              className="sql-input"
              rows={4}
              value={urls}
              spellCheck={false}
              placeholder={"https://app.example.com/callback\nhttp://localhost:5173/*"}
              onChange={(e) => setUrls(e.target.value)}
              style={{ width: "100%" }}
            />
            <button className="primary small" style={{ marginTop: 10 }} disabled={busy} onClick={saveUrls}>
              Save redirect URLs
            </button>
          </div>
        </>
      )}
    </div>
  );
}
