"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.OxiDBClient = void 0;
const net = require("net");
class OxiDBClient {
    constructor(host, port) {
        this.socket = null;
        this.connected = false;
        // The server answers requests on a connection in order, so a FIFO of
        // pending promises pipelines safely.
        this.pending = [];
        this.recvBuf = Buffer.alloc(0);
        this.keepAlive = null;
        /** Invoked once when an established connection drops (not on disconnect()). */
        this.onClose = null;
        this.host = host;
        this.port = port;
    }
    get address() {
        return `${this.host}:${this.port}`;
    }
    async connect() {
        return new Promise((resolve, reject) => {
            this.socket = new net.Socket();
            this.socket.connect(this.port, this.host, () => {
                this.connected = true;
                // The server closes idle connections (OXIDB_IDLE_TIMEOUT, default
                // 30s); a periodic ping keeps this one alive.
                this.keepAlive = setInterval(() => {
                    this.ping().catch(() => { });
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
    disconnect() {
        this.onClose = null; // deliberate: don't report it as a drop
        this.stop();
        if (this.socket) {
            this.socket.destroy();
            this.socket = null;
        }
        this.failAll(new Error('Disconnected'));
    }
    stop() {
        this.connected = false;
        if (this.keepAlive) {
            clearInterval(this.keepAlive);
            this.keepAlive = null;
        }
    }
    isConnected() {
        return this.connected;
    }
    failAll(err) {
        const waiting = this.pending;
        this.pending = [];
        waiting.forEach((p) => p.reject(err));
    }
    onData(data) {
        this.recvBuf = Buffer.concat([this.recvBuf, data]);
        // Drain every complete frame in the buffer.
        while (this.recvBuf.length >= 4) {
            const len = this.recvBuf.readUInt32LE(0);
            if (this.recvBuf.length < 4 + len) {
                return;
            }
            const payload = this.recvBuf.subarray(4, 4 + len);
            this.recvBuf = this.recvBuf.subarray(4 + len);
            const waiter = this.pending.shift();
            if (!waiter) {
                continue;
            }
            try {
                waiter.resolve(JSON.parse(payload.toString()));
            }
            catch (e) {
                waiter.reject(e);
            }
        }
    }
    async send(payload) {
        if (!this.socket || !this.connected) {
            throw new Error('Not connected');
        }
        return new Promise((resolve, reject) => {
            this.pending.push({ resolve, reject });
            const json = JSON.stringify(payload);
            const buf = Buffer.alloc(4 + Buffer.byteLength(json));
            buf.writeUInt32LE(Buffer.byteLength(json), 0);
            buf.write(json, 4);
            this.socket.write(buf);
        });
    }
    /** Send and throw on an error response, returning `data`. */
    async call(payload) {
        const r = await this.send(payload);
        if (r && r.ok === false) {
            throw new Error(r.error || 'OxiDB error');
        }
        return r?.data;
    }
    // ─── Document engine ─────────────────────────────────────
    async ping() {
        return this.call({ cmd: 'ping' });
    }
    async listCollections() {
        return (await this.call({ cmd: 'list_collections' })) || [];
    }
    async count(collection) {
        const d = await this.call({ cmd: 'count', collection });
        return d?.count ?? 0;
    }
    async find(collection, query = {}, limit = 50) {
        const d = await this.call({ cmd: 'find', collection, query, limit });
        return Array.isArray(d) ? d : (d?.docs ?? []);
    }
    async insert(collection, doc) {
        return this.call({ cmd: 'insert', collection, doc });
    }
    async update(collection, query, update) {
        return this.call({ cmd: 'update', collection, query, update });
    }
    async deleteMany(collection, query) {
        return this.call({ cmd: 'delete', collection, query });
    }
    async dropCollection(collection) {
        return this.call({ cmd: 'drop_collection', collection });
    }
    async createIndex(collection, field) {
        return this.call({ cmd: 'create_index', collection, field });
    }
    async listIndexes(collection) {
        return (await this.call({ cmd: 'list_indexes', collection })) || [];
    }
    async aggregate(collection, pipeline) {
        const d = await this.call({ cmd: 'aggregate', collection, pipeline });
        return Array.isArray(d) ? d : (d?.docs ?? []);
    }
    // ─── SQL engine ──────────────────────────────────────────
    /**
     * Execute SQL against the second engine. Returns one result per statement.
     * `params` binds `?` / `$N` placeholders left-to-right.
     */
    async sql(sql, params) {
        const payload = { engine: 'sql', cmd: 'sql', sql };
        if (params && params.length) {
            payload.params = params;
        }
        const d = await this.call(payload);
        return Array.isArray(d) ? d : [d];
    }
    /** Whether the server has the SQL engine enabled (OXIDB_SQL=1). */
    async sqlEnabled() {
        try {
            await this.sql('SHOW TABLES');
            return true;
        }
        catch (e) {
            if (String(e.message).includes('not enabled')) {
                return false;
            }
            throw e;
        }
    }
    /** Table names + row counts via SHOW TABLES. */
    async sqlTables() {
        const [r] = await this.sql('SHOW TABLES');
        if (!('columns' in r)) {
            return [];
        }
        return r.rows.map((row) => ({ name: String(row[0]), rows: row[1] === null ? null : Number(row[1]) }));
    }
    /** Views as (name, definition) pairs via SHOW VIEWS. */
    async sqlViews() {
        const [r] = await this.sql('SHOW VIEWS');
        if (!('columns' in r)) {
            return [];
        }
        return r.rows.map((row) => ({ name: String(row[0]), definition: String(row[1]) }));
    }
    /** Columns of a table via DESCRIBE. */
    async sqlColumns(table) {
        const [r] = await this.sql(`DESCRIBE ${quoteIdent(table)}`);
        if (!('columns' in r)) {
            return [];
        }
        return r.rows.map((row) => ({
            name: String(row[0]),
            type: String(row[1]),
            nullable: Boolean(row[2]),
            primaryKey: Boolean(row[3]),
        }));
    }
    /** Indexes, optionally of a single table, via SHOW INDEXES. */
    async sqlIndexes(table) {
        const [r] = await this.sql(table ? `SHOW INDEXES FROM ${quoteIdent(table)}` : 'SHOW INDEXES');
        if (!('columns' in r)) {
            return [];
        }
        return r.rows.map((row) => ({ name: String(row[0]), table: String(row[1]), columns: String(row[2]) }));
    }
}
exports.OxiDBClient = OxiDBClient;
/** Quote an identifier for interpolation into introspection statements. */
function quoteIdent(name) {
    return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name) ? name : `"${name.replace(/"/g, '""')}"`;
}
//# sourceMappingURL=client.js.map