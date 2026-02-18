import { useConnection } from "../../context/ConnectionContext";
import { useTheme } from "../../context/ThemeContext";
import { disconnect as apiDisconnect } from "../../api/tauri";
import { useNavigate } from "react-router-dom";

export function Header() {
  const { status, setStatus } = useConnection();
  const { theme, toggle } = useTheme();
  const navigate = useNavigate();

  const handleDisconnect = async () => {
    try {
      await apiDisconnect();
    } catch {
      // ignore
    }
    setStatus({ connected: false, mode: "disconnected", detail: "" });
    navigate("/");
  };

  return (
    <header className="header">
      <div className="header-left">
        {status.connected ? (
          <>
            <span className={`badge badge-success`}>
              <span className="status-dot green" />
              {status.mode === "embedded" ? "Embedded" : "Client"}
            </span>
            <span style={{ color: "var(--text-secondary)", fontSize: 13 }}>
              {status.detail}
            </span>
          </>
        ) : (
          <span className="badge badge-muted">
            <span className="status-dot gray" />
            Disconnected
          </span>
        )}
      </div>
      <div className="header-right">
        <button className="btn btn-secondary btn-sm" onClick={toggle}>
          {theme === "dark" ? "Light" : "Dark"}
        </button>
        {status.connected && (
          <button
            className="btn btn-secondary btn-sm"
            onClick={handleDisconnect}
          >
            Disconnect
          </button>
        )}
      </div>
    </header>
  );
}
