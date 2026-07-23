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

export function Auth({ onAuthed }: { onAuthed: () => void }) {
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
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
          <span className="logo">◇</span> OxiBase
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
      </div>
    </div>
  );
}
