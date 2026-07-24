import { useEffect, useRef, useState } from "react";
import { authGoogle, fetchConfig } from "./api.ts";

// Minimal typing for the Google Identity Services global we use.
declare global {
  interface Window {
    google?: {
      accounts: {
        id: {
          initialize: (cfg: {
            client_id: string;
            callback: (r: { credential: string }) => void;
          }) => void;
          renderButton: (el: HTMLElement, opts: Record<string, unknown>) => void;
        };
      };
    };
  }
}

const GSI_SRC = "https://accounts.google.com/gsi/client";

function loadGsi(): Promise<void> {
  return new Promise((resolve, reject) => {
    if (window.google?.accounts?.id) return resolve();
    const existing = document.getElementById("gsi-script") as HTMLScriptElement | null;
    if (existing) {
      existing.addEventListener("load", () => resolve());
      existing.addEventListener("error", () => reject(new Error("failed to load Google sign-in")));
      return;
    }
    const s = document.createElement("script");
    s.id = "gsi-script";
    s.src = GSI_SRC;
    s.async = true;
    s.defer = true;
    s.onload = () => resolve();
    s.onerror = () => reject(new Error("failed to load Google sign-in"));
    document.head.appendChild(s);
  });
}

export function Auth({ onAuthed, onDocs }: { onAuthed: () => void; onDocs: () => void }) {
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [showInfo, setShowInfo] = useState(false);
  const btnRef = useRef<HTMLDivElement>(null);
  // Keep the latest onAuthed without re-running the effect.
  const onAuthedRef = useRef(onAuthed);
  onAuthedRef.current = onAuthed;

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const cfg = await fetchConfig();
        if (cancelled) return;
        if (!cfg.google_client_id) {
          setError("Google sign-in is not configured on this server.");
          setLoading(false);
          return;
        }
        await loadGsi();
        if (cancelled || !window.google) return;
        window.google.accounts.id.initialize({
          client_id: cfg.google_client_id,
          callback: (r) => {
            authGoogle(r.credential)
              .then(() => onAuthedRef.current())
              .catch((e) => setError(e instanceof Error ? e.message : String(e)));
          },
        });
        if (btnRef.current) {
          window.google.accounts.id.renderButton(btnRef.current, {
            theme: "outline",
            size: "large",
            width: 260,
            text: "continue_with",
            shape: "pill",
          });
        }
        setLoading(false);
      } catch (e) {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : String(e));
          setLoading(false);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <div className="auth">
      <div className="card auth-card">
        <div className="brand center">
          <img src="/logo.svg" alt="OxiBase" className="auth-logo" />
        </div>
        <p className="muted center">Sign in to your control plane</p>
        <div
          ref={btnRef}
          style={{ display: "flex", justifyContent: "center", minHeight: 44, margin: "8px 0" }}
        />
        {loading && !error && <p className="muted center">Loading…</p>}
        {error && <div className="error">{error}</div>}
        <p className="muted center" style={{ fontSize: 12 }}>
          Developer accounts use Google sign-in.
        </p>
        <div className="center" style={{ marginTop: 6, display: "flex", gap: 8, justifyContent: "center" }}>
          <button
            className="ghost small"
            onClick={() => setShowInfo((s) => !s)}
            aria-expanded={showInfo}
          >
            {showInfo ? "Hide" : "What is OxiBase?"}
          </button>
          <button className="ghost small" onClick={onDocs}>
            JavaScript tutorial
          </button>
        </div>
      </div>

      {showInfo && (
        <div className="card auth-card info-panel">
          <h3 style={{ marginTop: 0 }}>What is OxiBase?</h3>
          <p className="muted small">
            A multi-tenant <strong>backend-as-a-service built on OxiDB</strong> — the
            Supabase model, with OxiDB as the engine instead of Postgres.
          </p>
          <ul className="info-list">
            <li>
              <strong>A database per tenant.</strong> Every project gets its own isolated
              OxiDB database, provisioned instantly. They share one process, so thousands
              of tenants stay cheap (OxiDB's density advantage).
            </li>
            <li>
              <strong>Instant REST API.</strong> Each collection and SQL table is served at{" "}
              <code>/rest/v1/…</code> with a <strong>PostgREST-compatible</strong> grammar
              (filters, ordering, pagination, resource embeds), so{" "}
              <code>@supabase/postgrest-js</code> and PostgREST clients work unmodified.
            </li>
            <li>
              <strong>Asymmetric per-project keys.</strong> Each project signs tokens with
              its own <strong>ES256</strong> key (published as JWKS); the data plane verifies
              with the public key alone — no shared secret, so it scales to many nodes.
              Two keys: <code>anon</code> (browser-safe, rules apply) and{" "}
              <code>service_role</code> (server-side, bypasses rules).
            </li>
            <li>
              <strong>Row-level security.</strong> Per-collection rules like{" "}
              <code>auth.username == doc.owner</code> filter reads <em>per row</em> and gate
              writes — enforced inside the engine, not the client.
            </li>
            <li>
              <strong>End-user auth.</strong> Your app's users sign up / sign in against a
              project (access + rotating refresh tokens); rules then see their identity.
            </li>
            <li>
              <strong>Quotas.</strong> Per-project caps on collections, SQL tables and total
              documents, enforced at the data plane.
            </li>
            <li>
              <strong>Multi-model underneath.</strong> OxiDB is document + SQL + time-series
              + Redis + S3 in one binary, so a project isn't limited to a single data model.
            </li>
          </ul>
          <p className="muted small" style={{ marginBottom: 0 }}>
            Addressing is path-based (<code>&lt;host&gt;/&lt;project&gt;/rest/v1/…</code>), so
            no per-tenant certificate is needed.
          </p>
        </div>
      )}
    </div>
  );
}
