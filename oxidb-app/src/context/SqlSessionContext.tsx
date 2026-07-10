import { createContext, useContext, useState, useRef, useCallback } from "react";
import type { JsonValue } from "../api/types";

// The SQL page's working state lives here — above the router — so it survives
// navigating away to another view and back (SqlPage unmounts on route change).

export interface StmtResult {
  columns?: string[];
  types?: (string | null)[];
  rows?: JsonValue[][];
  affected?: number;
  last_insert_id?: number;
  ddl?: boolean;
  transaction?: boolean;
  /** print() output from a stored procedure (like a psql NOTICE). */
  notices?: string[];
}

/** One query tab — its own editor text, results and view state. */
export interface QueryTab {
  id: number;
  name: string;
  sql: string;
  results: StmtResult[] | null;
  error: string | null;
  elapsed: number | null;
  loading: boolean;
  active: number; // focused statement index
  browseTable: string | null;
  resultTab: "query" | "data";
  page: number;
  pageSize: number;
}

export function newTab(id: number): QueryTab {
  return {
    id,
    name: `Query ${id}`,
    sql: "SELECT name FROM sqlite_schema;\n-- ⌘/Ctrl+Enter to run",
    results: null,
    error: null,
    elapsed: null,
    loading: false,
    active: 0,
    browseTable: null,
    resultTab: "query",
    page: 0,
    pageSize: 100,
  };
}

const HISTORY_KEY = "oxidb-sql-history";
function loadHistory(): string[] {
  try {
    return JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]");
  } catch {
    return [];
  }
}

interface SqlSession {
  tabs: QueryTab[];
  setTabs: React.Dispatch<React.SetStateAction<QueryTab[]>>;
  activeIdx: number;
  setActiveIdx: React.Dispatch<React.SetStateAction<number>>;
  /** Allocate a monotonic tab id. */
  allocId: () => number;
  history: string[];
  setHistory: React.Dispatch<React.SetStateAction<string[]>>;
}

const Ctx = createContext<SqlSession | null>(null);

export function SqlSessionProvider({ children }: { children: React.ReactNode }) {
  const [tabs, setTabs] = useState<QueryTab[]>([newTab(1)]);
  const [activeIdx, setActiveIdx] = useState(0);
  const nextId = useRef(2);
  const [history, setHistory] = useState<string[]>(loadHistory);
  const allocId = useCallback(() => nextId.current++, []);

  return (
    <Ctx.Provider value={{ tabs, setTabs, activeIdx, setActiveIdx, allocId, history, setHistory }}>
      {children}
    </Ctx.Provider>
  );
}

export function useSqlSession(): SqlSession {
  const v = useContext(Ctx);
  if (!v) throw new Error("useSqlSession must be used within SqlSessionProvider");
  return v;
}
