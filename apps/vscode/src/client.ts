import * as net from 'net';

interface Pending {
  resolve: (value: any) => void;
  reject: (reason: any) => void;
}

/** One statement's result from the SQL engine. */
export type SqlResult =
  | { columns: string[]; rows: any[][] }
  | { affected: number }
  | { ddl: boolean }
  | { transaction: boolean };

export class OxiDBClient {
  private socket: net.Socket | null = null;
  private host: string;
  private port: number;
  private connected = false;
  // The server answers requests on a connection in order, so a FIFO of
  // pending promises pipelines safely.
  private pending: Pending[] = [];
  private recvBuf = Buffer.alloc(0);
  private keepAlive: ReturnType<typeof setInterval> | null = null;
  /** Invoked once when an established connection drops (not on disconnect()). */
  onClose: (() => void) | null = null;

  constructor(host: string, port: number) {
    this.host = host;
    this.port = port;
  }

  get address(): string {
    return `${this.host}:${this.port}`;
  }

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.socket = new net.Socket();
      this.socket.connect(this.port, this.host, () => {
        this.connected = true;
        // The server closes idle connections (OXIDB_IDLE_TIMEOUT, default
        // 30s); a periodic ping keeps this one alive.
        this.keepAlive = setInterval(() => {
          this.ping().catch(() => {});
        }, 15000);
        resolve();
      });
      this.socket.on('data', (data) => this.onData(data));
      this.socket.on('error', (err) => {
        this.connected = false;
        this.failAll(err);
        reject(err);
      });
      this.socket.on('close', () => {
        const wasConnected = this.connected;
        this.stop();
        this.failAll(new Error('Connection closed'));
        if (wasConnected && this.onClose) {
          const cb = this.onClose;
          this.onClose = null;
          cb();
        }
      });
    });
  }

  disconnect(): void {
    this.onClose = null; // deliberate: don't report it as a drop
    this.stop();
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
    this.failAll(new Error('Disconnected'));
  }

  private stop(): void {
    this.connected = false;
    if (this.keepAlive) {
      clearInterval(this.keepAlive);
      this.keepAlive = null;
    }
  }

  isConnected(): boolean {
    return this.connected;
  }

  private failAll(err: any): void {
    const waiting = this.pending;
    this.pending = [];
    waiting.forEach((p) => p.reject(err));
  }

  private onData(data: Buffer): void {
    this.recvBuf = Buffer.concat([this.recvBuf, data]);
    // Drain every complete frame in the buffer.
    while (this.recvBuf.length >= 4) {
      const len = this.recvBuf.readUInt32LE(0);
      if (this.recvBuf.length < 4 + len) { return; }
      const payload = this.recvBuf.subarray(4, 4 + len);
      this.recvBuf = this.recvBuf.subarray(4 + len);
      const waiter = this.pending.shift();
      if (!waiter) { continue; }
      try {
        waiter.resolve(JSON.parse(payload.toString()));
      } catch (e) {
        waiter.reject(e);
      }
    }
  }

  async send(payload: any): Promise<any> {
    if (!this.socket || !this.connected) {
      throw new Error('Not connected');
    }
    return new Promise((resolve, reject) => {
      this.pending.push({ resolve, reject });
      const json = JSON.stringify(payload);
      const buf = Buffer.alloc(4 + Buffer.byteLength(json));
      buf.writeUInt32LE(Buffer.byteLength(json), 0);
      buf.write(json, 4);
      this.socket!.write(buf);
    });
  }

  /** Send and throw on an error response, returning `data`. */
  private async call(payload: any): Promise<any> {
    const r = await this.send(payload);
    if (r && r.ok === false) {
      throw new Error(r.error || 'OxiDB error');
    }
    return r?.data;
  }

  // ─── Document engine ─────────────────────────────────────

  async ping(): Promise<string> {
    return this.call({ cmd: 'ping' });
  }

  async listCollections(): Promise<string[]> {
    return (await this.call({ cmd: 'list_collections' })) || [];
  }

  async count(collection: string): Promise<number> {
    const d = await this.call({ cmd: 'count', collection });
    return d?.count ?? 0;
  }

  async find(collection: string, query: any = {}, limit = 50): Promise<any[]> {
    const d = await this.call({ cmd: 'find', collection, query, limit });
    return Array.isArray(d) ? d : (d?.docs ?? []);
  }

  async insert(collection: string, doc: any): Promise<any> {
    return this.call({ cmd: 'insert', collection, doc });
  }

  async update(collection: string, query: any, update: any): Promise<any> {
    return this.call({ cmd: 'update', collection, query, update });
  }

  async deleteMany(collection: string, query: any): Promise<any> {
    return this.call({ cmd: 'delete', collection, query });
  }

  async dropCollection(collection: string): Promise<any> {
    return this.call({ cmd: 'drop_collection', collection });
  }

  async createIndex(collection: string, field: string): Promise<any> {
    return this.call({ cmd: 'create_index', collection, field });
  }

  async listIndexes(collection: string): Promise<any[]> {
    return (await this.call({ cmd: 'list_indexes', collection })) || [];
  }

  async aggregate(collection: string, pipeline: any[]): Promise<any[]> {
    const d = await this.call({ cmd: 'aggregate', collection, pipeline });
    return Array.isArray(d) ? d : (d?.docs ?? []);
  }

  // ─── SQL engine ──────────────────────────────────────────

  /**
   * Execute SQL against the second engine. Returns one result per statement.
   * `params` binds `?` / `$N` placeholders left-to-right.
   */
  async sql(sql: string, params?: any[]): Promise<SqlResult[]> {
    const payload: any = { engine: 'sql', cmd: 'sql', sql };
    if (params && params.length) { payload.params = params; }
    const d = await this.call(payload);
    return Array.isArray(d) ? d : [d];
  }

  /** Whether the server has the SQL engine enabled (OXIDB_SQL=1). */
  async sqlEnabled(): Promise<boolean> {
    try {
      await this.sql('SHOW TABLES');
      return true;
    } catch (e: any) {
      if (String(e.message).includes('not enabled')) { return false; }
      throw e;
    }
  }

  /** Table names + row counts via SHOW TABLES. */
  async sqlTables(): Promise<{ name: string; rows: number | null }[]> {
    const [r] = await this.sql('SHOW TABLES');
    if (!('columns' in r)) { return []; }
    return r.rows.map((row) => ({ name: String(row[0]), rows: row[1] === null ? null : Number(row[1]) }));
  }

  /** Views as (name, definition) pairs via SHOW VIEWS. */
  async sqlViews(): Promise<{ name: string; definition: string }[]> {
    const [r] = await this.sql('SHOW VIEWS');
    if (!('columns' in r)) { return []; }
    return r.rows.map((row) => ({ name: String(row[0]), definition: String(row[1]) }));
  }

  /** Columns of a table via DESCRIBE. */
  async sqlColumns(table: string): Promise<{ name: string; type: string; nullable: boolean; primaryKey: boolean }[]> {
    const [r] = await this.sql(`DESCRIBE ${quoteIdent(table)}`);
    if (!('columns' in r)) { return []; }
    return r.rows.map((row) => ({
      name: String(row[0]),
      type: String(row[1]),
      nullable: Boolean(row[2]),
      primaryKey: Boolean(row[3]),
    }));
  }

  /** Indexes, optionally of a single table, via SHOW INDEXES. */
  async sqlIndexes(table?: string): Promise<{ name: string; table: string; columns: string }[]> {
    const [r] = await this.sql(table ? `SHOW INDEXES FROM ${quoteIdent(table)}` : 'SHOW INDEXES');
    if (!('columns' in r)) { return []; }
    return r.rows.map((row) => ({ name: String(row[0]), table: String(row[1]), columns: String(row[2]) }));
  }
}

/** Quote an identifier for interpolation into introspection statements. */
function quoteIdent(name: string): string {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name) ? name : `"${name.replace(/"/g, '""')}"`;
}
