"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.OxiDBClient = void 0;
const net = require("net");
class OxiDBClient {
    constructor(host, port) {
        this.socket = null;
        this.connected = false;
        this.pendingResolve = null;
        this.pendingReject = null;
        this.recvBuf = Buffer.alloc(0);
        this.host = host;
        this.port = port;
    }
    async connect() {
        return new Promise((resolve, reject) => {
            this.socket = new net.Socket();
            this.socket.connect(this.port, this.host, () => {
                this.connected = true;
                this.socket.on('data', (data) => this.onData(data));
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
    disconnect() {
        if (this.socket) {
            this.socket.destroy();
            this.socket = null;
            this.connected = false;
        }
    }
    isConnected() {
        return this.connected;
    }
    onData(data) {
        this.recvBuf = Buffer.concat([this.recvBuf, data]);
        this.tryParseResponse();
    }
    tryParseResponse() {
        if (this.recvBuf.length < 4) {
            return;
        }
        const len = this.recvBuf.readUInt32LE(0);
        if (this.recvBuf.length < 4 + len) {
            return;
        }
        const payload = this.recvBuf.subarray(4, 4 + len);
        this.recvBuf = this.recvBuf.subarray(4 + len);
        try {
            const resp = JSON.parse(payload.toString());
            if (this.pendingResolve) {
                this.pendingResolve(resp);
                this.pendingResolve = null;
                this.pendingReject = null;
            }
        }
        catch (e) {
            if (this.pendingReject) {
                this.pendingReject(e);
                this.pendingResolve = null;
                this.pendingReject = null;
            }
        }
    }
    async send(payload) {
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
            this.socket.write(buf);
        });
    }
    // ─── Convenience methods ─────────────────────────────────
    async ping() {
        const r = await this.send({ cmd: 'ping' });
        return r.data;
    }
    async listCollections() {
        const r = await this.send({ cmd: 'list_collections' });
        return r.data || [];
    }
    async count(collection) {
        const r = await this.send({ cmd: 'count', collection });
        return r.data?.count ?? 0;
    }
    async find(collection, query = {}, limit = 50) {
        const r = await this.send({ cmd: 'find', collection, query, limit });
        return Array.isArray(r.data) ? r.data : (r.data?.docs ?? []);
    }
    async findOne(collection, query) {
        const r = await this.send({ cmd: 'find_one', collection, query });
        return r.data;
    }
    async insert(collection, doc) {
        const r = await this.send({ cmd: 'insert', collection, doc });
        return r.data;
    }
    async insertMany(collection, docs) {
        const r = await this.send({ cmd: 'insert_many', collection, docs });
        return r.data;
    }
    async update(collection, query, update) {
        const r = await this.send({ cmd: 'update', collection, query, update });
        return r.data;
    }
    async deleteMany(collection, query) {
        const r = await this.send({ cmd: 'delete', collection, query });
        return r.data;
    }
    async dropCollection(collection) {
        const r = await this.send({ cmd: 'drop_collection', collection });
        return r.data;
    }
    async createIndex(collection, field) {
        const r = await this.send({ cmd: 'create_index', collection, field });
        return r.data;
    }
    async listIndexes(collection) {
        const r = await this.send({ cmd: 'list_indexes', collection });
        return r.data || [];
    }
    async aggregate(collection, pipeline) {
        const r = await this.send({ cmd: 'aggregate', collection, pipeline });
        return Array.isArray(r.data) ? r.data : (r.data?.docs ?? []);
    }
    async sql(query) {
        const r = await this.send({ cmd: 'sql', query });
        return r.data;
    }
}
exports.OxiDBClient = OxiDBClient;
//# sourceMappingURL=client.js.map