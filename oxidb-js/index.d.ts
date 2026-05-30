export interface OxiDBOptions {
  token?: string;
  wsUrl?: string;
}

/** Per-collection storage options for {@link OxiDB.createCollection}. Omitted
 *  fields fall back to the server defaults (in-RAM, compressed, auto-compaction). */
export interface StorageOptions {
  /** Store documents on disk (mmap'd) keeping only the offset index resident. */
  disk_first?: boolean;
  /** zstd-compress on-disk records (ignored unless disk_first). */
  compress?: boolean;
  /** Reclaim dead space automatically (disk-first only). */
  auto_compact?: boolean;
  /** Never auto-compact a data file smaller than this many bytes. */
  compact_min_bytes?: number;
  /** Dead-space fraction (0..1) that triggers compaction. */
  compact_dead_ratio?: number;
}

export interface FindOptions {
  sort?: Record<string, 1 | -1>;
  skip?: number;
  limit?: number;
}

export interface IndexOptions {
  type?: "field" | "unique" | "composite" | "ttl";
  unique?: boolean;
  fields?: string[];
  expireAfterSeconds?: number;
}

export interface SecurityRules {
  read?: string;
  create?: string;
  update?: string;
  delete?: string;
}

export interface ChangeEvent {
  op: "insert" | "update" | "delete";
  collection: string;
  docId: number;
  doc: any;
  token: number;
}

export interface AuthResult {
  user?: { username: string; role: string };
  token: string;
}

export interface VerifyResult {
  valid: boolean;
  username: string;
  role: string;
  exp: number;
}

export class OxiDBError extends Error {
  status: number;
  constructor(message: string, status: number);
}

export class OxiDBAuth {
  signup(username: string, password: string, role?: string): Promise<AuthResult>;
  login(username: string, password: string): Promise<AuthResult>;
  verify(): Promise<VerifyResult>;
  setToken(token: string): void;
  getToken(): string | null;
}

export class OxiDBCollection {
  insert(doc: Record<string, any>): Promise<{ id: number }>;
  insertMany(docs: Record<string, any>[]): Promise<{ ids: number[] }>;
  find(query?: Record<string, any>, options?: FindOptions): Promise<any[]>;
  findOne(query?: Record<string, any>): Promise<any | null>;
  update(query: Record<string, any>, update: Record<string, any>, options?: { one?: boolean }): Promise<{ modified: number }>;
  updateOne(query: Record<string, any>, update: Record<string, any>): Promise<{ modified: number }>;
  delete(query: Record<string, any>): Promise<{ deleted: number }>;
  deleteOne(query: Record<string, any>): Promise<{ deleted: number }>;
  count(query?: Record<string, any>): Promise<number>;
  aggregate(pipeline: Record<string, any>[]): Promise<any[]>;

  createIndex(field: string, options?: IndexOptions): Promise<any>;
  listIndexes(): Promise<any[]>;
  dropIndex(name: string): Promise<any>;

  setRules(rules: SecurityRules): Promise<any>;
  getRules(): Promise<SecurityRules>;
  deleteRules(): Promise<any>;

  /** Subscribe to real-time changes. Returns an unsubscribe function. */
  onSnapshot(callback: (event: ChangeEvent) => void): () => void;
  onSnapshot(query: Record<string, any>, callback: (event: ChangeEvent) => void): () => void;
}

export class OxiDB {
  auth: OxiDBAuth;

  constructor(url: string, options?: OxiDBOptions);

  collection(name: string): OxiDBCollection;
  ping(): Promise<{ status: string; data: string }>;
  listCollections(): Promise<string[]>;
  createCollection(name: string, options?: StorageOptions): Promise<any>;
  dropCollection(name: string): Promise<any>;
  sql(query: string): Promise<any>;

  createProcedure(scriptOrDef: string | Record<string, any>): Promise<any>;
  callProcedure(name: string, params?: Record<string, any>): Promise<any>;
  listProcedures(): Promise<string[]>;
  deleteProcedure(name: string): Promise<any>;

  connectWebSocket(url: string): void;
  closeWebSocket(): void;
}
