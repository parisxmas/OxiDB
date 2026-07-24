import { useState } from "react";
import { isAuthed, logout, currentEmail } from "./api.ts";
import { Auth } from "./Auth.tsx";
import { Console } from "./Console.tsx";
import { Docs } from "./Docs.tsx";
import { ResetPassword } from "./ResetPassword.tsx";

export default function App() {
  const [authed, setAuthed] = useState(isAuthed());
  // Tiny router: /docs is the public JavaScript tutorial (no sign-in needed);
  // everything else is the console. nginx serves index.html for any path.
  const [path, setPath] = useState(window.location.pathname);

  const go = (to: string) => {
    window.history.pushState({}, "", to);
    setPath(to);
  };

  if (path.startsWith("/docs")) {
    return <Docs onOpenConsole={() => go("/")} />;
  }

  // Public landing page of password-reset emails — no sign-in.
  if (path.startsWith("/reset")) {
    return <ResetPassword />;
  }

  if (!authed) {
    return <Auth onAuthed={() => setAuthed(true)} onDocs={() => go("/docs")} />;
  }

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <img src="/logo-horizontal.svg" alt="OxiBase" className="brand-logo" />
        </div>
        <div className="who">
          <button className="ghost" onClick={() => go("/docs")}>
            Docs
          </button>
          <span className="email">{currentEmail()}</span>
          <button
            className="ghost"
            onClick={() => {
              logout();
              setAuthed(false);
            }}
          >
            Sign out
          </button>
        </div>
      </header>
      <main className="main">
        <Console />
      </main>
    </div>
  );
}
