package com.oxidb.embedded;

import org.json.JSONArray;
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
 */
public class OxiDb implements AutoCloseable {

    static {
        System.loadLibrary("oxidb_embedded_ffi");
    }

    // Native methods (C FFI via JNI bridge)
    private static native long nativeOpen(String path);
    private static native long nativeOpenEncrypted(String path, String keyPath);
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
     * Execute a raw JSON command and return the parsed response.
     */
    public JSONObject execute(JSONObject command) {
        ensureOpen();
        String result = nativeExecute(handle, command.toString());
        try {
            return new JSONObject(result);
        } catch (Exception e) {
            throw new OxiDbException("Invalid response: " + result, e);
        }
    }

    // ── Collection Operations ────────────────────────────────────────

    public void createCollection(String name) {
        exec("create_collection", new JSONObject().put("collection", name));
    }

    public JSONArray listCollections() {
        JSONObject r = exec("list_collections", new JSONObject());
        return r.optJSONArray("collections");
    }

    public void dropCollection(String name) {
        exec("drop_collection", new JSONObject().put("collection", name));
    }

    // ── CRUD ─────────────────────────────────────────────────────────

    /**
     * Insert a document. Returns the inserted document with _id.
     */
    public JSONObject insert(String collection, JSONObject doc) {
        JSONObject cmd = new JSONObject()
                .put("collection", collection)
                .put("doc", doc);
        return exec("insert", cmd);
    }

    /**
     * Insert multiple documents. Returns {"inserted": count}.
     */
    public JSONObject insertMany(String collection, JSONArray docs) {
        JSONObject cmd = new JSONObject()
                .put("collection", collection)
                .put("docs", docs);
        return exec("insert_many", cmd);
    }

    /**
     * Find documents matching a query.
     */
    public JSONArray find(String collection, JSONObject query) {
        JSONObject cmd = new JSONObject()
                .put("collection", collection)
                .put("query", query);
        JSONObject r = exec("find", cmd);
        return r.optJSONArray("docs");
    }

    /**
     * Find with sort, skip, limit.
     */
    public JSONArray find(String collection, JSONObject query,
                          JSONObject sort, int skip, int limit) {
        JSONObject cmd = new JSONObject()
                .put("collection", collection)
                .put("query", query);
        if (sort != null) cmd.put("sort", sort);
        if (skip > 0) cmd.put("skip", skip);
        if (limit > 0) cmd.put("limit", limit);
        JSONObject r = exec("find", cmd);
        return r.optJSONArray("docs");
    }

    /**
     * Find one document matching a query.
     */
    public JSONObject findOne(String collection, JSONObject query) {
        JSONObject cmd = new JSONObject()
                .put("collection", collection)
                .put("query", query);
        JSONObject r = exec("find_one", cmd);
        return r.optJSONObject("doc");
    }

    /**
     * Update documents matching query with the given update operators.
     */
    public JSONObject update(String collection, JSONObject query, JSONObject update) {
        JSONObject cmd = new JSONObject()
                .put("collection", collection)
                .put("query", query)
                .put("update", update);
        return exec("update", cmd);
    }

    /**
     * Update one document.
     */
    public JSONObject updateOne(String collection, JSONObject query, JSONObject update) {
        JSONObject cmd = new JSONObject()
                .put("collection", collection)
                .put("query", query)
                .put("update", update);
        return exec("update_one", cmd);
    }

    /**
     * Delete documents matching query.
     */
    public JSONObject delete(String collection, JSONObject query) {
        JSONObject cmd = new JSONObject()
                .put("collection", collection)
                .put("query", query);
        return exec("delete", cmd);
    }

    /**
     * Delete one document.
     */
    public JSONObject deleteOne(String collection, JSONObject query) {
        JSONObject cmd = new JSONObject()
                .put("collection", collection)
                .put("query", query);
        return exec("delete_one", cmd);
    }

    /**
     * Count documents matching query.
     */
    public long count(String collection, JSONObject query) {
        JSONObject cmd = new JSONObject()
                .put("collection", collection)
                .put("query", query != null ? query : new JSONObject());
        JSONObject r = exec("count", cmd);
        return r.optLong("count", 0);
    }

    // ── Indexes ──────────────────────────────────────────────────────

    public void createIndex(String collection, String field) {
        exec("create_index", new JSONObject()
                .put("collection", collection)
                .put("field", field));
    }

    public void createUniqueIndex(String collection, String field) {
        exec("create_unique_index", new JSONObject()
                .put("collection", collection)
                .put("field", field));
    }

    public void createCompositeIndex(String collection, JSONArray fields) {
        exec("create_composite_index", new JSONObject()
                .put("collection", collection)
                .put("fields", fields));
    }

    public void createTextIndex(String collection, JSONArray fields) {
        exec("create_text_index", new JSONObject()
                .put("collection", collection)
                .put("fields", fields));
    }

    public void dropIndex(String collection, String field) {
        exec("drop_index", new JSONObject()
                .put("collection", collection)
                .put("field", field));
    }

    // ── Aggregation ──────────────────────────────────────────────────

    public JSONArray aggregate(String collection, JSONArray pipeline) {
        JSONObject cmd = new JSONObject()
                .put("collection", collection)
                .put("pipeline", pipeline);
        JSONObject r = exec("aggregate", cmd);
        return r.optJSONArray("results");
    }

    // ── Full-Text Search ─────────────────────────────────────────────

    public JSONArray textSearch(String collection, String query) {
        JSONObject cmd = new JSONObject()
                .put("collection", collection)
                .put("query", query);
        JSONObject r = exec("text_search", cmd);
        return r.optJSONArray("results");
    }

    // ── Transactions ─────────────────────────────────────────────────

    public void beginTransaction() {
        exec("begin_tx", new JSONObject());
    }

    public void commitTransaction() {
        exec("commit_tx", new JSONObject());
    }

    public void rollbackTransaction() {
        exec("rollback_tx", new JSONObject());
    }

    // ── Blob Storage ─────────────────────────────────────────────────

    public void createBucket(String name) {
        exec("create_bucket", new JSONObject().put("bucket", name));
    }

    public void putObject(String bucket, String key, byte[] data, String contentType) {
        String b64 = android.util.Base64.encodeToString(data, android.util.Base64.NO_WRAP);
        exec("put_object", new JSONObject()
                .put("bucket", bucket)
                .put("key", key)
                .put("data_base64", b64)
                .put("content_type", contentType));
    }

    public byte[] getObject(String bucket, String key) {
        JSONObject r = exec("get_object", new JSONObject()
                .put("bucket", bucket)
                .put("key", key));
        String b64 = r.optString("data_base64", "");
        return android.util.Base64.decode(b64, android.util.Base64.NO_WRAP);
    }

    public void deleteObject(String bucket, String key) {
        exec("delete_object", new JSONObject()
                .put("bucket", bucket)
                .put("key", key));
    }

    // ── Internals ────────────────────────────────────────────────────

    private JSONObject exec(String cmd, JSONObject params) {
        params.put("cmd", cmd);
        JSONObject result = execute(params);
        if (result.has("error")) {
            throw new OxiDbException(result.optString("error"));
        }
        return result;
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
