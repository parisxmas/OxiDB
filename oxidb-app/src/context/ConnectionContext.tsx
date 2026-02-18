import { createContext, useContext, useState, type ReactNode } from "react";
import type { ConnectionStatus } from "../api/types";

interface ConnectionCtx {
  status: ConnectionStatus;
  setStatus: (s: ConnectionStatus) => void;
}

const defaultStatus: ConnectionStatus = {
  connected: false,
  mode: "disconnected",
  detail: "",
};

const ConnectionContext = createContext<ConnectionCtx>({
  status: defaultStatus,
  setStatus: () => {},
});

export function ConnectionProvider({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<ConnectionStatus>(defaultStatus);
  return (
    <ConnectionContext.Provider value={{ status, setStatus }}>
      {children}
    </ConnectionContext.Provider>
  );
}

export const useConnection = () => useContext(ConnectionContext);
