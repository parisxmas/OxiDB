/**
 * OxiDB JavaScript/TypeScript Client
 *
 * Works in Node.js (22+, for the global WebSocket) and browsers. Zero dependencies.
 * Supports REST API (CRUD, SQL, aggregation) and WebSocket (real-time subscriptions).
 *
 * @example
 * const { OxiDB } = require('oxidb')
 * const db = new OxiDB('http://localhost:8080')
 * await db.auth.login('alice', 'secret123')
 * await db.collection('users').insert({ name: 'Alice', age: 30 })
 * const docs = await db.collection('users').find({ age: { $gt: 21 } })
 */

"use strict";

class OxiDBError extends Error {
  constructor(message, status) {
    super(message);
    this.name = "OxiDBError";
    this.status = status;
  }
}

class OxiDBAuth {
  constructor(client) {
    this._client = client;
  }

  async signup(username, password, role = "readwrite") {
    const res = await this._client._post("/api/auth/signup", { username, password, role }, true);
    if (res.token) {
      this._client._token = res.token;
    }
    return res;
  }

  async login(username, password) {
    const res = await this._client._post("/api/auth/login", { username, password }, true);
    if (res.token) {
      this._client._token = res.token;
    }
    return res;
  }

  async verify() {
    return this._client._get("/api/auth/verify");
  }

  setToken(token) {
    this._client._token = token;
  }

  getToken() {
    return this._client._token;
  }
}

class OxiDBCollection {
  constructor(client, name) {
    this._client = client;
    this._name = name;
  }

  async insert(doc) {
    return this._client._post(`/api/${this._name}/documents`, { doc });
  }

  async insertMany(docs) {
    return this._client._post(`/api/${this._name}/documents`, { docs });
  }

  async find(query = {}, options = {}) {
    const params = new URLSearchParams();
    params.set("q", JSON.stringify(query));
    if (options.sort) params.set("sort", JSON.stringify(options.sort));
    if (options.skip != null) params.set("skip", String(options.skip));
    if (options.limit != null) params.set("limit", String(options.limit));
    return this._client._get(`/api/${this._name}/documents?${params}`);
  }

  async findOne(query = {}) {
    const docs = await this.find(query, { limit: 1 });
    return Array.isArray(docs) && docs.length > 0 ? docs[0] : null;
  }

  async update(query, update, options = {}) {
    return this._client._patch(`/api/${this._name}/documents`, {
      query,
      update,
      one: options.one || false,
    });
  }

  async updateOne(query, update) {
    return this.update(query, update, { one: true });
  }

  async delete(query) {
    return this._client._delete(`/api/${this._name}/documents`, { query });
  }

  async deleteOne(query) {
    const docs = await this.find(query, { limit: 1 });
    if (Array.isArray(docs) && docs.length > 0) {
      return this._client._delete(`/api/${this._name}/documents`, {
        query: { _id: docs[0]._id },
      });
    }
    return { deleted: 0 };
  }

  async count(query = {}) {
    const params = new URLSearchParams();
    params.set("q", JSON.stringify(query));
    const res = await this._client._get(`/api/${this._name}/count?${params}`);
    return res.count;
  }

  async aggregate(pipeline) {
    return this._client._post(`/api/${this._name}/aggregate`, { pipeline });
  }

  // -- Indexes -------------------------------------------------

  async createIndex(field, options = {}) {
    const body = { field, type: options.type || "field" };
    if (options.expireAfterSeconds != null) {
      body.type = "ttl";
      body.expireAfterSeconds = options.expireAfterSeconds;
    }
    if (options.unique) body.type = "unique";
    if (options.fields) {
      body.fields = options.fields;
      body.type = "composite";
      delete body.field;
    }
    return this._client._post(`/api/${this._name}/indexes`, body);
  }

  async listIndexes() {
    return this._client._get(`/api/${this._name}/indexes`);
  }

  async dropIndex(name) {
    return this._client._deleteNoBody(`/api/${this._name}/indexes/${name}`);
  }

  // -- Security rules ------------------------------------------

  async setRules(rules) {
    return this._client._post(`/api/rules/${this._name}`, rules);
  }

  async getRules() {
    return this._client._get(`/api/rules/${this._name}`);
  }

  async deleteRules() {
    return this._client._deleteNoBody(`/api/rules/${this._name}`);
  }

  // -- Real-time subscriptions (WebSocket) ----------------------

  onSnapshot(queryOrCallback, callbackOrUndefined) {
    let query = {};
    let callback;
    if (typeof queryOrCallback === "function") {
      callback = queryOrCallback;
    } else {
      query = queryOrCallback;
      callback = callbackOrUndefined;
    }
    if (!callback) throw new Error("callback required");

    const subId = `sub_${this._name}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const ws = this._client._ensureWebSocket();

    const handler = (event) => {
      if (event.subscription === subId) {
        callback({
          op: event.op,
          collection: event.collection,
          docId: event.doc_id,
          doc: event.doc,
          token: event.token,
        });
      }
    };

    this._client._wsHandlers.set(subId, handler);
    this._client._wsSend({
      cmd: "subscribe",
      id: subId,
      collection: this._name,
      query,
    });

    // Return unsubscribe function
    return () => {
      this._client._wsHandlers.delete(subId);
      this._client._wsSend({ cmd: "unsubscribe", id: subId });
    };
  }
}

class OxiDB {
  constructor(url, options = {}) {
    this._baseUrl = url.replace(/\/$/, "");
    this._token = options.token || null;
    this._wsUrl = options.wsUrl || null;
    this._ws = null;
    this._wsHandlers = new Map();
    this._wsReady = null;
    this._wsQueue = [];
    this.auth = new OxiDBAuth(this);
  }

  collection(name) {
    return new OxiDBCollection(this, name);
  }

  async ping() {
    return this._get("/api/ping");
  }

  async listCollections() {
    const res = await this._get("/api/collections");
    return res.collections;
  }

  /**
   * Create a collection. Optionally pass per-collection storage options
   * (disk-first, compression, compaction policy); omitted fields fall back to
   * the server defaults (in-RAM, compressed, auto-compaction). The chosen shape
   * is persisted, so the collection reopens the same way regardless of the
   * server's environment.
   *
   * @param {string} name
   * @param {{disk_first?: boolean, compress?: boolean, auto_compact?: boolean,
   *          compact_min_bytes?: number, compact_dead_ratio?: number}} [options]
   * @example db.createCollection("events", { disk_first: true, compress: false })
   */
  async createCollection(name, options) {
    const body = { name };
    if (options && Object.keys(options).length > 0) body.options = options;
    return this._post("/api/collections", body);
  }

  async dropCollection(name) {
    return this._deleteNoBody(`/api/collections/${name}`);
  }

  /**
   * Execute SQL against the server's standalone SQL engine.
   *
   * The SQL engine is separate from document collections (own tables, own
   * files) and must be enabled on the server with OXIDB_SQL=1. `params`
   * optionally binds `?` / `$N` placeholders left-to-right.
   *
   * Resolves to an array with one result per statement:
   * - SELECT: {columns: [...], rows: [[...], ...]}
   * - INSERT/UPDATE/DELETE: {affected: N}
   * - CREATE/DROP: {ddl: true}
   * - BEGIN/COMMIT/ROLLBACK: {transaction: true}
   *
   * @example
   * await db.sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)");
   * await db.sql("INSERT INTO users VALUES (?, ?)", [1, "ada"]);
   * const [res] = await db.sql("SELECT name FROM users WHERE id = $1", [1]);
   * // res.columns == ["name"]; res.rows == [["ada"]]
   */
  async sql(sql, params) {
    const body = { sql };
    if (params != null) body.params = params;
    const res = await this._post("/api/sql", body);
    return res.results;
  }

  // -- Procedures -----------------------------------------------

  async createProcedure(scriptOrDef) {
    if (typeof scriptOrDef === "string") {
      return this._post("/api/procedures", { script: scriptOrDef });
    }
    return this._post("/api/procedures", scriptOrDef);
  }

  async callProcedure(name, params = {}) {
    return this._post(`/api/procedures/${name}/call`, params);
  }

  async listProcedures() {
    return this._get("/api/procedures");
  }

  async deleteProcedure(name) {
    return this._deleteNoBody(`/api/procedures/${name}`);
  }

  // -- WebSocket ------------------------------------------------

  connectWebSocket(url) {
    this._wsUrl = url;
    return this._ensureWebSocket();
  }

  _ensureWebSocket() {
    // CONNECTING (0) counts as "already ensured": opening a second socket while
    // the first is mid-handshake orphans it, and its onopen would then flush the
    // queue into a socket that is not open yet.
    if (this._ws && (this._ws.readyState === 0 || this._ws.readyState === 1) && this._wsReady) {
      return this._wsReady;
    }

    if (!this._wsUrl) {
      const u = new URL(this._baseUrl);
      const wsProto = u.protocol === "https:" ? "wss:" : "ws:";
      const wsPort = parseInt(u.port || "8080") + 1;
      this._wsUrl = `${wsProto}//${u.hostname}:${wsPort}`;
    }

    // Native WebSocket everywhere: browsers, and Node.js 22+ (global WebSocket).
    if (typeof globalThis.WebSocket === "undefined") {
      throw new OxiDBError(
        "No WebSocket implementation available — Node.js 22+ or a browser is required for subscriptions",
      );
    }
    this._ws = new globalThis.WebSocket(this._wsUrl);
    this._wsAuthenticated = false;

    this._wsReady = new Promise((resolve, reject) => {
      this._ws.onopen = () => {
        if (this._token) {
          // Send auth and wait for response before flushing queue
          this._ws.send(JSON.stringify({ cmd: "auth", token: this._token }));
          this._wsAuthResolve = resolve;
        } else {
          this._wsAuthenticated = true;
          this._flushWsQueue();
          resolve();
        }
      };
      this._ws.onerror = (err) => {
        reject(err);
      };
    });

    this._ws.onmessage = (event) => {
      try {
        const data = JSON.parse(typeof event.data === "string" ? event.data : event.data.toString());

        // Handle auth response
        if (!this._wsAuthenticated && data.ok !== undefined) {
          if (data.ok) {
            this._wsAuthenticated = true;
            this._flushWsQueue();
            if (this._wsAuthResolve) { this._wsAuthResolve(); this._wsAuthResolve = null; }
          } else {
            if (this._wsAuthResolve) { this._wsAuthResolve(); this._wsAuthResolve = null; }
          }
          return;
        }

        // Handle change events
        if (data.event === "change") {
          for (const [subId, handler] of this._wsHandlers) {
            handler(data);
          }
        }
      } catch {}
    };

    this._ws.onclose = () => {
      this._ws = null;
      this._wsAuthenticated = false;
    };

    return this._wsReady;
  }

  _flushWsQueue() {
    for (const msg of this._wsQueue) {
      this._ws.send(JSON.stringify(msg));
    }
    this._wsQueue = [];
  }

  async _wsSend(msg) {
    if (this._ws && this._ws.readyState === 1 && this._wsAuthenticated) {
      this._ws.send(JSON.stringify(msg));
    } else {
      this._wsQueue.push(msg);
      await this._ensureWebSocket();
    }
  }

  closeWebSocket() {
    if (this._ws) {
      this._ws.close();
      this._ws = null;
      this._wsAuthenticated = false;
    }
  }

  // -- HTTP helpers ---------------------------------------------

  _headers(skipAuth = false) {
    const h = { "Content-Type": "application/json" };
    if (this._token && !skipAuth) {
      h["Authorization"] = `Bearer ${this._token}`;
    }
    return h;
  }

  async _request(method, path, body, skipAuth = false) {
    const url = `${this._baseUrl}${path}`;
    const opts = {
      method,
      headers: this._headers(skipAuth),
    };
    if (body != null) {
      opts.body = JSON.stringify(body);
    }

    const res = await fetch(url, opts);
    const json = await res.json();

    if (!res.ok) {
      throw new OxiDBError(json.error || `HTTP ${res.status}`, res.status);
    }
    return json;
  }

  async _get(path, skipAuth = false) {
    return this._request("GET", path, null, skipAuth);
  }

  async _post(path, body, skipAuth = false) {
    return this._request("POST", path, body, skipAuth);
  }

  async _patch(path, body) {
    return this._request("PATCH", path, body);
  }

  async _delete(path, body) {
    return this._request("DELETE", path, body);
  }

  async _deleteNoBody(path) {
    return this._request("DELETE", path, null);
  }
}

// Export for both CommonJS and ESM
if (typeof module !== "undefined" && module.exports) {
  module.exports = { OxiDB, OxiDBCollection, OxiDBAuth, OxiDBError };
}
if (typeof globalThis !== "undefined") {
  globalThis.OxiDB = OxiDB;
}
