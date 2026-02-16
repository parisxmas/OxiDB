const net = require('net');

class OxiDBClient {
  constructor(host = '127.0.0.1', port = 4444) {
    this.host = host;
    this.port = port;
    this.socket = null;
    this.buffer = Buffer.alloc(0);
    this.queue = [];
    this.connected = false;
    this.connecting = false;
  }

  connect() {
    if (this.connected) return Promise.resolve();
    if (this.connecting) {
      return new Promise((resolve, reject) => {
        this._connectWaiters = this._connectWaiters || [];
        this._connectWaiters.push({ resolve, reject });
      });
    }

    this.connecting = true;
    return new Promise((resolve, reject) => {
      this.socket = net.createConnection({ host: this.host, port: this.port }, () => {
        this.connected = true;
        this.connecting = false;
        resolve();
        if (this._connectWaiters) {
          this._connectWaiters.forEach(w => w.resolve());
          this._connectWaiters = null;
        }
      });

      this.socket.on('data', (chunk) => this._onData(chunk));

      this.socket.on('error', (err) => {
        if (!this.connected) {
          this.connecting = false;
          reject(err);
          if (this._connectWaiters) {
            this._connectWaiters.forEach(w => w.reject(err));
            this._connectWaiters = null;
          }
        }
        this._drainError(err);
      });

      this.socket.on('close', () => {
        this.connected = false;
        this._drainError(new Error('Connection closed'));
      });
    });
  }

  _drainError(err) {
    const pending = this.queue.splice(0);
    for (const item of pending) {
      item.reject(err);
    }
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
      if (item) {
        if (response.ok) {
          item.resolve(response.data);
        } else {
          item.reject(new Error(response.error || 'Unknown error'));
        }
      }
    }
  }

  _send(msg) {
    return new Promise((resolve, reject) => {
      const json = JSON.stringify(msg);
      const payload = Buffer.from(json, 'utf8');
      const header = Buffer.alloc(4);
      header.writeUInt32LE(payload.length, 0);

      this.queue.push({ resolve, reject });
      this.socket.write(Buffer.concat([header, payload]));
    });
  }

  async ping() { return this._send({ cmd: 'ping' }); }
  async createCollection(collection) { return this._send({ cmd: 'create_collection', collection }); }
  async listCollections() { return this._send({ cmd: 'list_collections' }); }
  async dropCollection(collection) { return this._send({ cmd: 'drop_collection', collection }); }
  async createIndex(collection, field) { return this._send({ cmd: 'create_index', collection, field }); }
  async createUniqueIndex(collection, field) { return this._send({ cmd: 'create_unique_index', collection, field }); }
  async insert(collection, doc) { return this._send({ cmd: 'insert', collection, doc }); }
  async find(collection, query = {}, options = {}) {
    const msg = { cmd: 'find', collection, query };
    if (options.sort) msg.sort = options.sort;
    if (options.limit != null) msg.limit = options.limit;
    if (options.skip != null) msg.skip = options.skip;
    return this._send(msg);
  }
  async findOne(collection, query = {}) { return this._send({ cmd: 'find_one', collection, query }); }
  async delete(collection, query) { return this._send({ cmd: 'delete', collection, query }); }
  async createBucket(bucket) { return this._send({ cmd: 'create_bucket', bucket }); }
  async putObject(bucket, key, data, contentType, metadata) {
    const msg = { cmd: 'put_object', bucket, key, data, content_type: contentType };
    if (metadata) msg.metadata = metadata;
    return this._send(msg);
  }
  async search(query, bucket, limit) {
    const msg = { cmd: 'search', query };
    if (bucket) msg.bucket = bucket;
    if (limit != null) msg.limit = limit;
    return this._send(msg);
  }

  async close() {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
      this.connected = false;
    }
  }
}

module.exports = OxiDBClient;
