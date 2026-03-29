import * as net from 'net';

export class OxiDBClient {
  private socket: net.Socket | null = null;
  private host: string;
  private port: number;
  private connected = false;
  private pendingResolve: ((value: any) => void) | null = null;
  private pendingReject: ((reason: any) => void) | null = null;
  private recvBuf = Buffer.alloc(0);

  constructor(host: string, port: number) {
    this.host = host;
    this.port = port;
  }

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.socket = new net.Socket();
      this.socket.connect(this.port, this.host, () => {
        this.connected = true;
        this.socket!.on('data', (data) => this.onData(data));
        resolve();
      });
      this.socket.on('error', (err) => {
        this.connected = false;
        reject(err);
      });
      this.socket.on('close', () => {
        this.connected = false;
      });
    });
  }

  disconnect(): void {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
      this.connected = false;
    }
  }

  isConnected(): boolean {
    return this.connected;
  }

  private onData(data: Buffer): void {
    this.recvBuf = Buffer.concat([this.recvBuf, data]);
    this.tryParseResponse();
  }

  private tryParseResponse(): void {
    if (this.recvBuf.length < 4) { return; }
    const len = this.recvBuf.readUInt32LE(0);
    if (this.recvBuf.length < 4 + len) { return; }
    const payload = this.recvBuf.subarray(4, 4 + len);
    this.recvBuf = this.recvBuf.subarray(4 + len);
    try {
      const resp = JSON.parse(payload.toString());
      if (this.pendingResolve) {
        this.pendingResolve(resp);
        this.pendingResolve = null;
        this.pendingReject = null;
      }
    } catch (e) {
      if (this.pendingReject) {
        this.pendingReject(e);
        this.pendingResolve = null;
        this.pendingReject = null;
      }
    }
  }

  async send(payload: any): Promise<any> {
    if (!this.socket || !this.connected) {
      throw new Error('Not connected');
    }
    return new Promise((resolve, reject) => {
      this.pendingResolve = resolve;
      this.pendingReject = reject;
      const json = JSON.stringify(payload);
      const buf = Buffer.alloc(4 + Buffer.byteLength(json));
      buf.writeUInt32LE(Buffer.byteLength(json), 0);
      buf.write(json, 4);
      this.socket!.write(buf);
    });
  }

  // ─── Convenience methods ─────────────────────────────────

  async ping(): Promise<string> {
    const r = await this.send({ cmd: 'ping' });
    return r.data;
  }

  async listCollections(): Promise<string[]> {
    const r = await this.send({ cmd: 'list_collections' });
    return r.data || [];
  }

  async count(collection: string): Promise<number> {
    const r = await this.send({ cmd: 'count', collection });
    return r.data?.count ?? 0;
  }

  async find(collection: string, query: any = {}, limit = 50): Promise<any[]> {
    const r = await this.send({ cmd: 'find', collection, query, limit });
    return Array.isArray(r.data) ? r.data : (r.data?.docs ?? []);
  }

  async findOne(collection: string, query: any): Promise<any> {
    const r = await this.send({ cmd: 'find_one', collection, query });
    return r.data;
  }

  async insert(collection: string, doc: any): Promise<any> {
    const r = await this.send({ cmd: 'insert', collection, doc });
    return r.data;
  }

  async insertMany(collection: string, docs: any[]): Promise<any> {
    const r = await this.send({ cmd: 'insert_many', collection, docs });
    return r.data;
  }

  async update(collection: string, query: any, update: any): Promise<any> {
    const r = await this.send({ cmd: 'update', collection, query, update });
    return r.data;
  }

  async deleteMany(collection: string, query: any): Promise<any> {
    const r = await this.send({ cmd: 'delete', collection, query });
    return r.data;
  }

  async dropCollection(collection: string): Promise<any> {
    const r = await this.send({ cmd: 'drop_collection', collection });
    return r.data;
  }

  async createIndex(collection: string, field: string): Promise<any> {
    const r = await this.send({ cmd: 'create_index', collection, field });
    return r.data;
  }

  async listIndexes(collection: string): Promise<any[]> {
    const r = await this.send({ cmd: 'list_indexes', collection });
    return r.data || [];
  }

  async aggregate(collection: string, pipeline: any[]): Promise<any[]> {
    const r = await this.send({ cmd: 'aggregate', collection, pipeline });
    return Array.isArray(r.data) ? r.data : (r.data?.docs ?? []);
  }

  async sql(query: string): Promise<any> {
    const r = await this.send({ cmd: 'sql', query });
    return r.data;
  }
}
