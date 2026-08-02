import {
  createContext,
  useContext,
  useState,
  useEffect,
  useCallback,
  type ReactNode,
} from "react";
import {
  listDatabases,
  createDatabase,
  dropDatabase,
  setCurrentDb,
} from "../api/tauri";
import { useConnection } from "./ConnectionContext";

interface DatabaseCtx {
  db: string;
  databases: string[];
  setDb: (name: string) => void;
  reload: () => Promise<void>;
  createDb: (name: string) => Promise<void>;
  dropDb: (name: string) => Promise<void>;
}

const Ctx = createContext<DatabaseCtx>({
  db: "",
  databases: [],
  setDb: () => {},
  reload: async () => {},
  createDb: async () => {},
  dropDb: async () => {},
});

/**
 * Current database (ADR-0012), shared across every db-aware page. Selecting a
 * database updates the api-layer default (setCurrentDb) so SQL and document
 * commands scope to it, and bumps a value all consumers can react to.
 */
export function DatabaseProvider({ children }: { children: ReactNode }) {
  const { status } = useConnection();
  const [databases, setDatabases] = useState<string[]>([]);
  const [db, setDbState] = useState<string>("");

  const setDb = useCallback((name: string) => {
    setDbState(name);
    setCurrentDb(name || undefined);
  }, []);

  const reload = useCallback(async () => {
    try {
      const dbs = await listDatabases();
      setDatabases(dbs);
      setDbState((cur) => {
        if (cur && dbs.includes(cur)) return cur;
        const def = dbs.includes("oxidb") ? "oxidb" : dbs[0] || "";
        setCurrentDb(def || undefined);
        return def;
      });
    } catch {
      setDatabases([]);
    }
  }, []);

  // (Re)load the database list whenever a connection becomes active.
  useEffect(() => {
    if (status.connected) reload();
    else {
      setDatabases([]);
      setDbState("");
      setCurrentDb(undefined);
    }
  }, [status.connected, status.detail, reload]);

  const createDb = useCallback(
    async (name: string) => {
      await createDatabase(name);
      await reload();
      setDb(name);
    },
    [reload, setDb]
  );

  const dropDb = useCallback(
    async (name: string) => {
      await dropDatabase(name);
      await reload();
    },
    [reload]
  );

  return (
    <Ctx.Provider value={{ db, databases, setDb, reload, createDb, dropDb }}>
      {children}
    </Ctx.Provider>
  );
}

export const useDatabase = () => useContext(Ctx);
