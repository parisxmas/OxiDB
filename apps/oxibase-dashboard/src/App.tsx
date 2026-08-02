import { useState } from "react";
import { isAuthed, logout, currentEmail } from "./api.ts";
import { navigate, parseRoute, usePath } from "./router.ts";
import { Auth } from "./Auth.tsx";
import { Console } from "./Console.tsx";
import { Docs } from "./Docs.tsx";
import { ResetPassword } from "./ResetPassword.tsx";

export default function App() {
  const [authed, setAuthed] = useState(isAuthed());
  // /docs is the public JavaScript tutorial (no sign-in needed); /reset is where
  // password-reset emails land. Everything else is the console, whose own view
  // also comes from the path — see router.ts.
  const route = parseRoute(usePath());
  const go = navigate;

  if (route.view === "docs") {
    return <Docs onOpenConsole={() => go("/")} />;
  }

  if (route.view === "reset") {
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
