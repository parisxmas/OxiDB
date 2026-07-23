import { useState } from "react";
import { isAuthed, logout, currentEmail } from "./api.ts";
import { Auth } from "./Auth.tsx";
import { Projects } from "./Projects.tsx";

export default function App() {
  const [authed, setAuthed] = useState(isAuthed());

  if (!authed) {
    return <Auth onAuthed={() => setAuthed(true)} />;
  }

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo">◇</span> OxiBase
        </div>
        <div className="who">
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
        <Projects />
      </main>
    </div>
  );
}
