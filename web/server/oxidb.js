const net = require('net');

// ─── Single Connection ─────────────────────────────────────
class OxiDBConnection {
  constructor(host, port) {
    this.host = host;
    this.port = port;
    this.socket = null;
    this.buffer = Buffer.alloc(0);
    this.queue = [];
    this.connected = false;
    this.busy = false; // true while a request is in-flight
  }

  connect() {
    if (this.connected) return Promise.resolve();
    return new Promise((resolve, reject) => {
      this.socket = net.createConnection({ host: this.host, port: this.port }, () => {
        this.connected = true;
        resolve();
      });
      this.socket.on('data', (chunk) => this._onData(chunk));
      this.socket.on('error', (err) => {
        if (!this.connected) return reject(err);
        this._drainError(err);
      });
      this.socket.on('close', () => {
        this.connected = false;
        this._drainError(new Error('Connection closed'));
      });
    });
  }

  _drainError(err) {
    this.busy = false;
    const pending = this.queue.splice(0);
    for (const item of pending) item.reject(err);
  }

  _onData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    while (this.buffer.length >= 4) {
      const msgLen = this.buffer.readUInt32LE(0);
      if (this.buffer.length < 4 + msgLen) break;
      const jsonBuf = this.buffer.subarray(4, 4 + msgLen);
      this.buffer = this.buffer.subarray(4 + msgLen);
      const response = JSON.parse(jsonBuf.toString('utf8'));
      const item = this.queue.shift();
      this.busy = false;
      if (item) {
        if (response.ok) item.resolve(response.data);
        else item.reject(new Error(response.error || 'Unknown error'));
      }
    }
  }

  send(msg) {
    return new Promise((resolve, reject) => {
      const json = JSON.stringify(msg);
      const payload = Buffer.from(json, 'utf8');
      const header = Buffer.alloc(4);
      header.writeUInt32LE(payload.length, 0);
      this.queue.push({ resolve, reject });
      this.busy = true;
      this.socket.write(Buffer.concat([header, payload]));
    });
  }

  destroy() {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
      this.connected = false;
    }
  }
}

// ─── Connection Pool ───────────────────────────────────────
class OxiDBPool {
  constructor(host = '127.0.0.1', port = 4444, size = 3) {
    this.host = host;
    this.port = port;
    this.size = size;
    this.connections = [];
    this.waiters = [];
  }

  async connect() {
    for (let i = 0; i < this.size; i++) {
      const conn = new OxiDBConnection(this.host, this.port);
      await conn.connect();
      this.connections.push(conn);
    }
  }

  // Get the least-busy connection (shortest queue), or wait if all saturated
  _acquire() {
    // Find connection with shortest queue
    let best = this.connections[0];
    for (const c of this.connections) {
      if (c.queue.length < best.queue.length) best = c;
    }
    return best;
  }

  _send(msg) {
    const conn = this._acquire();
    return conn.send(msg);
  }

  async ping() { return this._send({ cmd: 'ping' }); }

  // Collection operations
  async createCollection(collection) { return this._send({ cmd: 'create_collection', collection }); }
  async listCollections() { return this._send({ cmd: 'list_collections' }); }
  async dropCollection(collection) { return this._send({ cmd: 'drop_collection', collection }); }

  // Index operations
  async createIndex(collection, field) { return this._send({ cmd: 'create_index', collection, field }); }
  async createUniqueIndex(collection, field) { return this._send({ cmd: 'create_unique_index', collection, field }); }
  async createCompositeIndex(collection, fields) { return this._send({ cmd: 'create_composite_index', collection, fields }); }

  // CRUD operations
  async insert(collection, doc) { return this._send({ cmd: 'insert', collection, doc }); }

  async find(collection, query = {}, options = {}) {
    const msg = { cmd: 'find', collection, query };
    if (options.sort) msg.sort = options.sort;
    if (options.limit != null) msg.limit = options.limit;
    if (options.skip != null) msg.skip = options.skip;
    return this._send(msg);
  }

  async findOne(collection, query = {}) { return this._send({ cmd: 'find_one', collection, query }); }
  async update(collection, query, update) { return this._send({ cmd: 'update', collection, query, update }); }
  async delete(collection, query) { return this._send({ cmd: 'delete', collection, query }); }
  async count(collection, query = {}) { return this._send({ cmd: 'count', collection, query }); }

  // Blob storage
  async createBucket(bucket) { return this._send({ cmd: 'create_bucket', bucket }); }
  async deleteBucket(bucket) { return this._send({ cmd: 'delete_bucket', bucket }); }
  async putObject(bucket, key, data, contentType, metadata) {
    const msg = { cmd: 'put_object', bucket, key, data, content_type: contentType };
    if (metadata) msg.metadata = metadata;
    return this._send(msg);
  }
  async getObject(bucket, key) { return this._send({ cmd: 'get_object', bucket, key }); }
  async deleteObject(bucket, key) { return this._send({ cmd: 'delete_object', bucket, key }); }
  async listObjects(bucket, prefix, limit) {
    const msg = { cmd: 'list_objects', bucket };
    if (prefix) msg.prefix = prefix;
    if (limit != null) msg.limit = limit;
    return this._send(msg);
  }

  // Full-text search
  async search(query, bucket, limit) {
    const msg = { cmd: 'search', query };
    if (bucket) msg.bucket = bucket;
    if (limit != null) msg.limit = limit;
    return this._send(msg);
  }

  async close() {
    for (const c of this.connections) c.destroy();
    this.connections = [];
  }
}

module.exports = OxiDBPool;
