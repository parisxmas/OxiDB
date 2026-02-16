package com.oxidb.spring;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * TCP client for oxidb-server.
 * <p>
 * Protocol: each message is [4-byte little-endian length][JSON payload].
 * Server responds with {"ok": true, "data": ...} or {"ok": false, "error": "..."}.
 * <p>
 * Thread-safe: all send/receive operations are synchronized.
 * Implements AutoCloseable for try-with-resources and Spring lifecycle.
 */
public class OxiDbClient implements AutoCloseable {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Socket socket;
    private final DataOutputStream out;
    private final DataInputStream in;

    public OxiDbClient(String host, int port, int timeoutMs) {
        try {
            this.socket = new Socket();
            this.socket.setSoTimeout(timeoutMs);
            this.socket.connect(new InetSocketAddress(host, port), timeoutMs);
            this.out = new DataOutputStream(socket.getOutputStream());
            this.in = new DataInputStream(socket.getInputStream());
        } catch (IOException e) {
            throw new OxiDbException("Failed to connect to OxiDB at " + host + ":" + port, e);
        }
    }

    // ------------------------------------------------------------------
    // Low-level protocol
    // ------------------------------------------------------------------

    private synchronized void sendRaw(byte[] data) throws IOException {
        byte[] lenBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(data.length).array();
        out.write(lenBytes);
        out.write(data);
        out.flush();
    }

    private synchronized byte[] recvRaw() throws IOException {
        byte[] lenBytes = in.readNBytes(4);
        if (lenBytes.length < 4) {
            throw new IOException("Connection closed by server");
        }
        int length = ByteBuffer.wrap(lenBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
        byte[] payload = in.readNBytes(length);
        if (payload.length < length) {
            throw new IOException("Connection closed by server");
        }
        return payload;
    }

    private synchronized JsonNode request(ObjectNode payload) {
        try {
            byte[] jsonBytes = mapper.writeValueAsBytes(payload);
            sendRaw(jsonBytes);
            byte[] respBytes = recvRaw();
            return mapper.readTree(respBytes);
        } catch (IOException e) {
            throw new OxiDbException("Communication error", e);
        }
    }

    private JsonNode checked(ObjectNode payload) {
        JsonNode resp = request(payload);
        if (!resp.path("ok").asBoolean(false)) {
            String error = resp.path("error").asText("unknown error");
            if (error.toLowerCase().contains("conflict")) {
                throw new TransactionConflictException(error);
            }
            throw new OxiDbException(error);
        }
        return resp.get("data");
    }

    private ObjectNode cmd(String command) {
        ObjectNode node = mapper.createObjectNode();
        node.put("cmd", command);
        return node;
    }

    // ------------------------------------------------------------------
    // Utility
    // ------------------------------------------------------------------

    /** Ping the server. Returns "pong". */
    public JsonNode ping() {
        return checked(cmd("ping"));
    }

    // ------------------------------------------------------------------
    // Collection management
    // ------------------------------------------------------------------

    /** Explicitly create a collection. Collections are also auto-created on insert. */
    public JsonNode createCollection(String name) {
        ObjectNode p = cmd("create_collection");
        p.put("collection", name);
        return checked(p);
    }

    /** Return a list of collection names. */
    public JsonNode listCollections() {
        return checked(cmd("list_collections"));
    }

    /** Drop a collection and its data. */
    public JsonNode dropCollection(String name) {
        ObjectNode p = cmd("drop_collection");
        p.put("collection", name);
        return checked(p);
    }

    // ------------------------------------------------------------------
    // CRUD
    // ------------------------------------------------------------------

    /** Insert a single document. Returns {"id": ...} outside tx, "buffered" inside tx. */
    public JsonNode insert(String collection, Map<String, Object> doc) {
        ObjectNode p = cmd("insert");
        p.put("collection", collection);
        p.set("doc", mapper.valueToTree(doc));
        return checked(p);
    }

    /** Insert a single document from a JSON string. */
    public JsonNode insert(String collection, String docJson) {
        try {
            ObjectNode p = cmd("insert");
            p.put("collection", collection);
            p.set("doc", mapper.readTree(docJson));
            return checked(p);
        } catch (JsonProcessingException e) {
            throw new OxiDbException("Invalid JSON: " + e.getMessage(), e);
        }
    }

    /** Insert multiple documents. */
    public JsonNode insertMany(String collection, List<Map<String, Object>> docs) {
        ObjectNode p = cmd("insert_many");
        p.put("collection", collection);
        p.set("docs", mapper.valueToTree(docs));
        return checked(p);
    }

    /** Find documents matching a query. */
    public JsonNode find(String collection, Map<String, Object> query) {
        return find(collection, query, null, null, null);
    }

    /** Find documents with sort, skip, and limit options. */
    public JsonNode find(String collection, Map<String, Object> query,
                         Map<String, Object> sort, Integer skip, Integer limit) {
        ObjectNode p = cmd("find");
        p.put("collection", collection);
        p.set("query", query != null ? mapper.valueToTree(query) : mapper.createObjectNode());
        if (sort != null) p.set("sort", mapper.valueToTree(sort));
        if (skip != null) p.put("skip", skip);
        if (limit != null) p.put("limit", limit);
        return checked(p);
    }

    /** Find documents using a JSON query string. */
    public JsonNode find(String collection, String queryJson) {
        try {
            ObjectNode p = cmd("find");
            p.put("collection", collection);
            p.set("query", mapper.readTree(queryJson));
            return checked(p);
        } catch (JsonProcessingException e) {
            throw new OxiDbException("Invalid JSON: " + e.getMessage(), e);
        }
    }

    /** Find a single document matching a query. Returns the document or null. */
    public JsonNode findOne(String collection, Map<String, Object> query) {
        ObjectNode p = cmd("find_one");
        p.put("collection", collection);
        p.set("query", query != null ? mapper.valueToTree(query) : mapper.createObjectNode());
        return checked(p);
    }

    /** Update documents matching a query. Returns {"modified": n} outside tx. */
    public JsonNode update(String collection, Map<String, Object> query, Map<String, Object> update) {
        ObjectNode p = cmd("update");
        p.put("collection", collection);
        p.set("query", mapper.valueToTree(query));
        p.set("update", mapper.valueToTree(update));
        return checked(p);
    }

    /** Update documents using JSON strings for query and update. */
    public JsonNode update(String collection, String queryJson, String updateJson) {
        try {
            ObjectNode p = cmd("update");
            p.put("collection", collection);
            p.set("query", mapper.readTree(queryJson));
            p.set("update", mapper.readTree(updateJson));
            return checked(p);
        } catch (JsonProcessingException e) {
            throw new OxiDbException("Invalid JSON: " + e.getMessage(), e);
        }
    }

    /** Delete documents matching a query. Returns {"deleted": n} outside tx. */
    public JsonNode delete(String collection, Map<String, Object> query) {
        ObjectNode p = cmd("delete");
        p.put("collection", collection);
        p.set("query", mapper.valueToTree(query));
        return checked(p);
    }

    /** Delete documents using a JSON query string. */
    public JsonNode delete(String collection, String queryJson) {
        try {
            ObjectNode p = cmd("delete");
            p.put("collection", collection);
            p.set("query", mapper.readTree(queryJson));
            return checked(p);
        } catch (JsonProcessingException e) {
            throw new OxiDbException("Invalid JSON: " + e.getMessage(), e);
        }
    }

    /** Count documents matching a query. */
    public int count(String collection, Map<String, Object> query) {
        ObjectNode p = cmd("count");
        p.put("collection", collection);
        p.set("query", query != null ? mapper.valueToTree(query) : mapper.createObjectNode());
        JsonNode result = checked(p);
        return result.path("count").asInt();
    }

    /** Count all documents in a collection. */
    public int count(String collection) {
        return count(collection, null);
    }

    // ------------------------------------------------------------------
    // Indexes
    // ------------------------------------------------------------------

    /** Create a non-unique index on a field. */
    public JsonNode createIndex(String collection, String field) {
        ObjectNode p = cmd("create_index");
        p.put("collection", collection);
        p.put("field", field);
        return checked(p);
    }

    /** Create a unique index on a field. */
    public JsonNode createUniqueIndex(String collection, String field) {
        ObjectNode p = cmd("create_unique_index");
        p.put("collection", collection);
        p.put("field", field);
        return checked(p);
    }

    /** Create a composite index on multiple fields. */
    public JsonNode createCompositeIndex(String collection, List<String> fields) {
        ObjectNode p = cmd("create_composite_index");
        p.put("collection", collection);
        p.set("fields", mapper.valueToTree(fields));
        return checked(p);
    }

    // ------------------------------------------------------------------
    // Aggregation
    // ------------------------------------------------------------------

    /** Run an aggregation pipeline. Returns list of result documents. */
    public JsonNode aggregate(String collection, List<Map<String, Object>> pipeline) {
        ObjectNode p = cmd("aggregate");
        p.put("collection", collection);
        p.set("pipeline", mapper.valueToTree(pipeline));
        return checked(p);
    }

    /** Run an aggregation pipeline from a JSON string. */
    public JsonNode aggregate(String collection, String pipelineJson) {
        try {
            ObjectNode p = cmd("aggregate");
            p.put("collection", collection);
            p.set("pipeline", mapper.readTree(pipelineJson));
            return checked(p);
        } catch (JsonProcessingException e) {
            throw new OxiDbException("Invalid JSON: " + e.getMessage(), e);
        }
    }

    // ------------------------------------------------------------------
    // Compaction
    // ------------------------------------------------------------------

    /** Compact a collection. Returns {old_size, new_size, docs_kept}. */
    public JsonNode compact(String collection) {
        ObjectNode p = cmd("compact");
        p.put("collection", collection);
        return checked(p);
    }

    // ------------------------------------------------------------------
    // Transactions
    // ------------------------------------------------------------------

    /** Begin a transaction on this connection. Returns {"tx_id": ...}. */
    public JsonNode beginTx() {
        return checked(cmd("begin_tx"));
    }

    /** Commit the active transaction. Throws TransactionConflictException on OCC conflict. */
    public JsonNode commitTx() {
        return checked(cmd("commit_tx"));
    }

    /** Rollback the active transaction. */
    public JsonNode rollbackTx() {
        return checked(cmd("rollback_tx"));
    }

    /**
     * Execute a block within a transaction. Auto-commits on success, auto-rolls back on exception.
     *
     * @param action the operations to perform within the transaction
     */
    public void withTransaction(Runnable action) {
        beginTx();
        try {
            action.run();
            commitTx();
        } catch (Exception e) {
            try {
                rollbackTx();
            } catch (OxiDbException ignored) {
                // rollback may fail if commit already failed
            }
            throw e;
        }
    }

    // ------------------------------------------------------------------
    // Blob storage
    // ------------------------------------------------------------------

    /** Create a blob storage bucket. */
    public JsonNode createBucket(String bucket) {
        ObjectNode p = cmd("create_bucket");
        p.put("bucket", bucket);
        return checked(p);
    }

    /** List all blob storage buckets. */
    public JsonNode listBuckets() {
        return checked(cmd("list_buckets"));
    }

    /** Delete a blob storage bucket. */
    public JsonNode deleteBucket(String bucket) {
        ObjectNode p = cmd("delete_bucket");
        p.put("bucket", bucket);
        return checked(p);
    }

    /** Upload a blob object. Data is base64-encoded automatically. */
    public JsonNode putObject(String bucket, String key, byte[] data,
                              String contentType, Map<String, String> metadata) {
        ObjectNode p = cmd("put_object");
        p.put("bucket", bucket);
        p.put("key", key);
        p.put("data", Base64.getEncoder().encodeToString(data));
        p.put("content_type", contentType != null ? contentType : "application/octet-stream");
        if (metadata != null && !metadata.isEmpty()) {
            p.set("metadata", mapper.valueToTree(metadata));
        }
        return checked(p);
    }

    /** Upload a blob object with default content type. */
    public JsonNode putObject(String bucket, String key, byte[] data) {
        return putObject(bucket, key, data, null, null);
    }

    /** Download a blob object. Returns the response with "content" (base64) and "metadata". */
    public JsonNode getObject(String bucket, String key) {
        ObjectNode p = cmd("get_object");
        p.put("bucket", bucket);
        p.put("key", key);
        return checked(p);
    }

    /** Decode the base64 content from a getObject response. */
    public byte[] decodeObjectContent(JsonNode getObjectResult) {
        String b64 = getObjectResult.path("content").asText();
        return Base64.getDecoder().decode(b64);
    }

    /** Get blob object metadata without downloading the content. */
    public JsonNode headObject(String bucket, String key) {
        ObjectNode p = cmd("head_object");
        p.put("bucket", bucket);
        p.put("key", key);
        return checked(p);
    }

    /** Delete a blob object. */
    public JsonNode deleteObject(String bucket, String key) {
        ObjectNode p = cmd("delete_object");
        p.put("bucket", bucket);
        p.put("key", key);
        return checked(p);
    }

    /** List objects in a bucket. */
    public JsonNode listObjects(String bucket, String prefix, Integer limit) {
        ObjectNode p = cmd("list_objects");
        p.put("bucket", bucket);
        if (prefix != null) p.put("prefix", prefix);
        if (limit != null) p.put("limit", limit);
        return checked(p);
    }

    /** List all objects in a bucket. */
    public JsonNode listObjects(String bucket) {
        return listObjects(bucket, null, null);
    }

    // ------------------------------------------------------------------
    // Full-text search
    // ------------------------------------------------------------------

    /** Full-text search across blobs. Returns [{bucket, key, score}, ...]. */
    public JsonNode search(String query, String bucket, int limit) {
        ObjectNode p = cmd("search");
        p.put("query", query);
        if (bucket != null) p.put("bucket", bucket);
        p.put("limit", limit);
        return checked(p);
    }

    /** Full-text search with default limit of 10. */
    public JsonNode search(String query) {
        return search(query, null, 10);
    }

    // ------------------------------------------------------------------
    // AutoCloseable
    // ------------------------------------------------------------------

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }
}
