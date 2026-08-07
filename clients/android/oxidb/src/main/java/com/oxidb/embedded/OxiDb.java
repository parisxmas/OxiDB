package com.oxidb.embedded;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * OxiDB Embedded — run the database in-process on Android via native FFI.
 *
 * <pre>
 * OxiDb db = new OxiDb(context.getFilesDir() + "/oxidb_data");
 * db.insert("users", new JSONObject().put("name", "Alice").put("age", 30));
 * JSONArray results = db.find("users", new JSONObject().put("name", "Alice"));
 * db.close();
 * </pre>
 *
 * Every response from the native layer is an envelope
 * {@code {"ok": bool, "data": ..., "error": ...}}; the typed methods below
 * unwrap it and throw {@link OxiDbException} on {@code ok == false}.
 */
public class OxiDb implements AutoCloseable {

    static {
        System.loadLibrary("oxidb_embedded_ffi");
    }

    // Native methods (C FFI via JNI bridge)
    private static native long nativeOpen(String path);
    private static native long nativeOpenEncrypted(String path, String keyPath);
    private static native long nativeOpenEncryptedBytes(String path, byte[] key);
    private static native void nativeClose(long handle);
    private static native String nativeExecute(long handle, String commandJson);

    private long handle;
    private boolean closed = false;

    /**
     * Open or create a database at the given path.
     */
    public OxiDb(String path) {
        this.handle = nativeOpen(path);
        if (this.handle == 0) {
            throw new RuntimeException("Failed to open OxiDB at: " + path);
        }
    }

    /**
     * Open an encrypted database. keyPath points to a 32-byte AES-256 key file.
     */
    public OxiDb(String path, String keyPath) {
        this.handle = nativeOpenEncrypted(path, keyPath);
        if (this.handle == 0) {
            throw new RuntimeException("Failed to open encrypted OxiDB at: " + path);
        }
    }

    /**
     * Open an encrypted database with the 32-byte AES-256 key passed as raw
     * bytes — the mobile-idiomatic variant: keep (or wrap) the key in the
     * Android Keystore, never in a file inside the app sandbox.
     */
    public OxiDb(String path, byte[] key) {
        if (key == null || key.length != 32) {
            throw new IllegalArgumentException("encryption key must be exactly 32 bytes");
        }
        this.handle = nativeOpenEncryptedBytes(path, key);
        if (this.handle == 0) {
            throw new RuntimeException("Failed to open encrypted OxiDB at: " + path);
        }
    }

    @Override
    public void close() {
        if (!closed && handle != 0) {
            nativeClose(handle);
            closed = true;
            handle = 0;
        }
    }

    // ── Low-level ────────────────────────────────────────────────────

    /**
     * Execute a raw JSON command and return the full response envelope
     * ({@code {"ok": ..., "data"/"error": ...}}), unchecked.
     */
    public JSONObject execute(JSONObject command) {
        ensureOpen();
        String result = nativeExecute(handle, command.toString());
        try {
            return new JSONObject(result);
        } catch (JSONException e) {
            throw new OxiDbException("Invalid response: " + result, e);
        }
    }

    // ── Collection Operations ────────────────────────────────────────

    public void createCollection(String name) {
        exec("create_collection", obj("collection", name));
    }

    /** Collection names, as a JSON array of strings. */
    public JSONArray listCollections() {
        Object data = exec("list_collections", obj());
        return data instanceof JSONArray ? (JSONArray) data : new JSONArray();
    }

    public void dropCollection(String name) {
        exec("drop_collection", obj("collection", name));
    }

    // ── CRUD ─────────────────────────────────────────────────────────

    /**
     * Insert a document. Returns the new document id.
     */
    public long insert(String collection, JSONObject doc) {
        JSONObject r = execObj("insert", obj("collection", collection, "doc", doc));
        return r.optLong("id", 0);
    }

    /**
     * Insert multiple documents. Returns the new document ids.
     */
    public JSONArray insertMany(String collection, JSONArray docs) {
        Object data = exec("insert_many", obj("collection", collection, "docs", docs));
        return data instanceof JSONArray ? (JSONArray) data : new JSONArray();
    }

    /**
     * Find documents matching a query.
     */
    public JSONArray find(String collection, JSONObject query) {
        Object data = exec("find", obj("collection", collection, "query", query));
        return data instanceof JSONArray ? (JSONArray) data : new JSONArray();
    }

    /**
     * Find with sort, skip, limit.
     */
    public JSONArray find(String collection, JSONObject query,
                          JSONObject sort, int skip, int limit) {
        JSONObject cmd = obj("collection", collection, "query", query);
        if (sort != null) {
            put(cmd, "sort", sort);
        }
        if (skip > 0) {
            put(cmd, "skip", skip);
        }
        if (limit > 0) {
            put(cmd, "limit", limit);
        }
        Object data = exec("find", cmd);
        return data instanceof JSONArray ? (JSONArray) data : new JSONArray();
    }

    /**
     * Find one document matching a query, or null.
     */
    public JSONObject findOne(String collection, JSONObject query) {
        Object data = exec("find_one", obj("collection", collection, "query", query));
        return data instanceof JSONObject ? (JSONObject) data : null;
    }

    /**
     * Update documents matching query. Returns {@code {"modified": n}}.
     */
    public JSONObject update(String collection, JSONObject query, JSONObject update) {
        return update(collection, query, update, false);
    }

    /**
     * Update one document. Returns {@code {"modified": n}}.
     */
    public JSONObject updateOne(String collection, JSONObject query, JSONObject update) {
        return updateOne(collection, query, update, false);
    }

    /**
     * Update with MongoDB upsert semantics: when {@code upsert} is true and
     * nothing matches, a document synthesized from the query's equality
     * conditions with the update applied is inserted; the result then
     * carries {@code "upserted": <id>}.
     */
    public JSONObject update(String collection, JSONObject query, JSONObject update, boolean upsert) {
        JSONObject cmd = obj("collection", collection, "query", query, "update", update);
        if (upsert) {
            put(cmd, "upsert", true);
        }
        return execObj("update", cmd);
    }

    /**
     * Update one document, with upsert (see {@link #update(String, JSONObject, JSONObject, boolean)}).
     */
    public JSONObject updateOne(String collection, JSONObject query, JSONObject update, boolean upsert) {
        JSONObject cmd = obj("collection", collection, "query", query, "update", update);
        if (upsert) {
            put(cmd, "upsert", true);
        }
        return execObj("update_one", cmd);
    }

    /**
     * Delete documents matching query. Returns {@code {"deleted": n}}.
     */
    public JSONObject delete(String collection, JSONObject query) {
        return execObj("delete", obj("collection", collection, "query", query));
    }

    /**
     * Delete one document. Returns {@code {"deleted": n}}.
     */
    public JSONObject deleteOne(String collection, JSONObject query) {
        return execObj("delete_one", obj("collection", collection, "query", query));
    }

    /**
     * Count documents matching query.
     */
    public long count(String collection, JSONObject query) {
        JSONObject r = execObj("count",
                obj("collection", collection, "query", query != null ? query : obj()));
        return r.optLong("count", 0);
    }

    // ── Indexes ──────────────────────────────────────────────────────

    public void createIndex(String collection, String field) {
        exec("create_index", obj("collection", collection, "field", field));
    }

    public void createUniqueIndex(String collection, String field) {
        exec("create_unique_index", obj("collection", collection, "field", field));
    }

    public void createCompositeIndex(String collection, JSONArray fields) {
        exec("create_composite_index", obj("collection", collection, "fields", fields));
    }

    public void createTextIndex(String collection, JSONArray fields) {
        exec("create_text_index", obj("collection", collection, "fields", fields));
    }

    public void dropIndex(String collection, String field) {
        exec("drop_index", obj("collection", collection, "field", field));
    }

    // ── Aggregation ──────────────────────────────────────────────────

    public JSONArray aggregate(String collection, JSONArray pipeline) {
        Object data = exec("aggregate", obj("collection", collection, "pipeline", pipeline));
        return data instanceof JSONArray ? (JSONArray) data : new JSONArray();
    }

    // ── Full-Text Search ─────────────────────────────────────────────

    public JSONArray textSearch(String collection, String query) {
        Object data = exec("text_search", obj("collection", collection, "query", query));
        return data instanceof JSONArray ? (JSONArray) data : new JSONArray();
    }

    // ── Transactions ─────────────────────────────────────────────────

    public void beginTransaction() {
        exec("begin_tx", obj());
    }

    public void commitTransaction() {
        exec("commit_tx", obj());
    }

    public void rollbackTransaction() {
        exec("rollback_tx", obj());
    }

    // ── Blob Storage ─────────────────────────────────────────────────

    public void createBucket(String name) {
        exec("create_bucket", obj("bucket", name));
    }

    public void putObject(String bucket, String key, byte[] data, String contentType) {
        String b64 = android.util.Base64.encodeToString(data, android.util.Base64.NO_WRAP);
        exec("put_object", obj(
                "bucket", bucket,
                "key", key,
                "data", b64,
                "content_type", contentType));
    }

    public byte[] getObject(String bucket, String key) {
        JSONObject r = execObj("get_object", obj("bucket", bucket, "key", key));
        String b64 = r.optString("content", "");
        return android.util.Base64.decode(b64, android.util.Base64.NO_WRAP);
    }

    public void deleteObject(String bucket, String key) {
        exec("delete_object", obj("bucket", bucket, "key", key));
    }

    // ── Internals ────────────────────────────────────────────────────

    /**
     * Build {@code {k1: v1, k2: v2, ...}} without org.json's checked
     * exception — a JSONException here means a non-JSON value was passed,
     * which is a programming error, not an I/O condition.
     */
    static JSONObject obj(Object... kv) {
        JSONObject o = new JSONObject();
        try {
            for (int i = 0; i + 1 < kv.length; i += 2) {
                o.put((String) kv[i], kv[i + 1]);
            }
        } catch (JSONException e) {
            throw new OxiDbException("invalid JSON key/value", e);
        }
        return o;
    }

    static JSONObject put(JSONObject o, String key, Object value) {
        try {
            return o.put(key, value);
        } catch (JSONException e) {
            throw new OxiDbException("invalid JSON key/value", e);
        }
    }

    /** Run a command, unwrap the envelope, return the {@code data} value. */
    private Object exec(String cmd, JSONObject params) {
        put(params, "cmd", cmd);
        JSONObject result = execute(params);
        if (!result.optBoolean("ok", false)) {
            throw new OxiDbException(result.optString("error", "unknown error"));
        }
        return result.opt("data");
    }

    /** Like {@link #exec} for commands whose {@code data} is an object. */
    private JSONObject execObj(String cmd, JSONObject params) {
        Object data = exec(cmd, params);
        return data instanceof JSONObject ? (JSONObject) data : new JSONObject();
    }

    private void ensureOpen() {
        if (closed || handle == 0) {
            throw new IllegalStateException("OxiDb is closed");
        }
    }

    // ── Exception ────────────────────────────────────────────────────

    public static class OxiDbException extends RuntimeException {
        public OxiDbException(String message) { super(message); }
        public OxiDbException(String message, Throwable cause) { super(message, cause); }
    }
}
