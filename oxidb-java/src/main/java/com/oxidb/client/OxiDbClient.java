package com.oxidb.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Pure-Java TCP client for the OxiDB document database server.
 *
 * <p>Uses the OxiWire binary protocol over TCP. Thread-safe: an internal
 * lock serialises requests on the single socket. For higher throughput,
 * use a connection pool of {@code OxiDbClient} instances.</p>
 *
 * <p>Typical usage:
 * <pre>{@code
 * try (OxiDbClient client = OxiDbClient.connect("127.0.0.1", 4444)) {
 *     HelloResponse hello = client.hello("myapp/1.0");
 *     System.out.println("Server " + hello.version());
 *
 *     long id = client.insertReturningId("users",
 *         Map.of("name", "Alice", "age", 30));
 *
 *     List<Map<String, Object>> adults = client.find(
 *         "users", Query.gte("age", 18));
 * }
 * }</pre></p>
 */
public final class OxiDbClient implements AutoCloseable {

    // Tolerant ObjectMapper: the server auto-injects engine fields (_id,
    // _version) on every doc. User-defined records / classes typically
    // don't model _version, so we ignore unknown properties by default.
    // Users can supply their own ObjectMapper later if they want stricter
    // validation.
    private static final ObjectMapper JSON = new ObjectMapper()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private final ReentrantLock lock = new ReentrantLock();
    private volatile boolean closed = false;

    private OxiDbClient(Socket socket) throws IOException {
        this.socket = socket;
        this.in = socket.getInputStream();
        this.out = socket.getOutputStream();
    }

    // ── Connection ──────────────────────────────────────────────────────

    /** Connect to an OxiDB server on the given host/port. */
    public static OxiDbClient connect(String host, int port) throws IOException {
        return connect(host, port, Duration.ofSeconds(30));
    }

    public static OxiDbClient connect(String host, int port, Duration timeout) throws IOException {
        Socket s = new Socket();
        try {
            s.connect(new java.net.InetSocketAddress(host, port), (int) timeout.toMillis());
            s.setTcpNoDelay(true);
        } catch (IOException e) {
            try { s.close(); } catch (IOException ignored) {}
            throw new OxiDbException.OxiDbConnectionException(
                "Failed to connect to " + host + ":" + port + ": " + e.getMessage(), e);
        }
        return new OxiDbClient(s);
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        socket.close();
    }

    // ── Core request/response ───────────────────────────────────────────

    /**
     * Send a raw command and return the {@code data} field of the response.
     * Escape hatch — prefer the typed methods below where they exist.
     */
    public Object execRaw(Map<String, Object> payload) throws IOException {
        Map<String, Object> envelope = sendCommand(payload, false);
        // OxiWire path: envelope IS the full response map → unwrap "data".
        // JSON path: same shape, server returns {ok, data}; also unwrap.
        // For HELLO (no "data"), callers should use execRawEnvelope instead.
        return envelope.containsKey("data") ? envelope.get("data") : envelope;
    }

    /**
     * Send a raw command and return the entire response envelope (not
     * just {@code data}). Use this for commands whose response shape
     * isn't {@code {ok: true, data: ...}} — notably HELLO.
     */
    public Map<String, Object> execRawEnvelope(Map<String, Object> payload) throws IOException {
        return sendCommand(payload, true);
    }

    private static final com.fasterxml.jackson.databind.ObjectMapper RESP_JSON =
        new com.fasterxml.jackson.databind.ObjectMapper();

    @SuppressWarnings("unchecked")
    private Map<String, Object> sendCommand(Map<String, Object> payload, boolean wantEnvelope) throws IOException {
        if (closed) throw new OxiDbException("Client is closed");
        lock.lock();
        try {
            byte[] reqBytes = OxiWireCodec.encodeRequest(payload);
            OxiWireCodec.writeFrame(out, reqBytes);
            byte[] respBytes = OxiWireCodec.readFrame(in);

            // Server may respond in either OxiWire (0xDB magic) or JSON.
            // HELLO in particular always returns JSON regardless of the
            // request's wire format. Sniff the first byte to dispatch.
            if (respBytes.length > 0 && (respBytes[0] & 0xFF) == (OxiWireCodec.MAGIC & 0xFF)) {
                OxiWireCodec.OxiWireResponse resp = OxiWireCodec.decodeResponse(respBytes);
                if (!resp.ok()) {
                    throw OxiDbException.fromServerMessage(resp.errorMessage());
                }
                if (resp.value() instanceof Map<?, ?> map) {
                    return (Map<String, Object>) map;
                }
                Map<String, Object> wrapped = new LinkedHashMap<>();
                wrapped.put("data", resp.value());
                return wrapped;
            }

            // JSON envelope: {"ok": bool, "data" | "server" | "error": ...}
            Map<String, Object> root = RESP_JSON.readValue(respBytes, Map.class);
            Object okFlag = root.get("ok");
            if (Boolean.FALSE.equals(okFlag)) {
                Object errObj = root.get("error");
                String errMsg = errObj != null ? errObj.toString() : "unknown error";
                throw OxiDbException.fromServerMessage(errMsg);
            }
            return root;
        } finally {
            lock.unlock();
        }
    }

    // ── HELLO handshake ─────────────────────────────────────────────────

    /**
     * Invoke the wire-level HELLO handshake. Returns the server's version,
     * supported wire versions, feature sets, and auth methods. Pre-auth:
     * safe on a fresh connection. Idempotent.
     */
    public HelloResponse hello() throws IOException {
        return hello(null, List.of(1L));
    }

    public HelloResponse hello(String clientId) throws IOException {
        return hello(clientId, List.of(1L));
    }

    @SuppressWarnings("unchecked")
    public HelloResponse hello(String clientId, List<Long> wireVersions) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("cmd", "hello");
        if (clientId != null) payload.put("client", clientId);
        if (wireVersions != null) payload.put("wire_versions", wireVersions);

        Map<String, Object> envelope = execRawEnvelope(payload);
        Object server = envelope.get("server");
        if (!(server instanceof Map<?, ?> serverMap)) {
            throw new OxiDbException("HELLO response missing 'server' field");
        }
        return HelloResponse.fromMap((Map<String, Object>) serverMap);
    }

    // ── CRUD ────────────────────────────────────────────────────────────

    public Object ping() throws IOException {
        return execRaw(Map.of("cmd", "ping"));
    }

    public long insertReturningId(String collection, Map<String, Object> doc) throws IOException {
        Object data = execRaw(Map.of("cmd", "insert", "collection", collection, "doc", doc));
        if (data instanceof Map<?, ?> map) {
            Object id = map.get("id");
            if (id instanceof Number n) return n.longValue();
        }
        if (data instanceof Number n) return n.longValue(); // some servers return id scalar
        throw new OxiDbException("Insert response did not include a numeric 'id': " + data);
    }

    @SuppressWarnings("unchecked")
    public long[] insertManyReturningIds(String collection, List<Map<String, Object>> docs) throws IOException {
        Object data = execRaw(Map.of("cmd", "insert_many", "collection", collection, "docs", docs));
        List<?> list = null;
        if (data instanceof Map<?, ?> map && map.get("ids") instanceof List<?> idsList) {
            list = idsList;
        } else if (data instanceof List<?> direct) {
            list = direct;
        }
        if (list == null) {
            throw new OxiDbException("InsertMany response missing 'ids' array: " + data);
        }
        long[] out = new long[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Object id = list.get(i);
            if (id instanceof Number n) out[i] = n.longValue();
            else throw new OxiDbException("Non-numeric id at index " + i + ": " + id);
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> find(String collection, Map<String, Object> query) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("cmd", "find");
        payload.put("collection", collection);
        if (query != null) payload.put("query", query);
        Object data = execRaw(payload);
        return data instanceof List<?> list ? (List<Map<String, Object>>) list : new ArrayList<>();
    }

    /** Find documents and deserialize each match to {@code T} via Jackson. */
    public <T> List<T> find(String collection, Map<String, Object> query, Class<T> type) throws IOException {
        List<Map<String, Object>> raw = find(collection, query);
        List<T> result = new ArrayList<>(raw.size());
        for (Map<String, Object> doc : raw) {
            result.add(JSON.convertValue(doc, type));
        }
        return result;
    }

    /** Find at most one document matching the query, deserialized to {@code T}. */
    public <T> T findOne(String collection, Map<String, Object> query, Class<T> type) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("cmd", "find_one");
        payload.put("collection", collection);
        payload.put("query", query);
        Object data = execRaw(payload);
        if (data == null) return null;
        return JSON.convertValue(data, type);
    }

    public int count(String collection, Map<String, Object> query) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("cmd", "count");
        payload.put("collection", collection);
        if (query != null) payload.put("query", query);
        Object data = execRaw(payload);
        if (data instanceof Map<?, ?> map) {
            Object n = map.get("count");
            if (n instanceof Number num) return num.intValue();
        }
        if (data instanceof Number n) return n.intValue();
        throw new OxiDbException("Count response missing numeric count: " + data);
    }

    public int update(String collection, Map<String, Object> query, Map<String, Object> update) throws IOException {
        Object data = execRaw(Map.of(
            "cmd", "update",
            "collection", collection,
            "query", query,
            "update", update
        ));
        return countFrom(data, "modified");
    }

    public int delete(String collection, Map<String, Object> query) throws IOException {
        Object data = execRaw(Map.of(
            "cmd", "delete",
            "collection", collection,
            "query", query
        ));
        return countFrom(data, "deleted");
    }

    private static int countFrom(Object data, String field) {
        if (data instanceof Map<?, ?> map) {
            Object n = map.get(field);
            if (n instanceof Number num) return num.intValue();
        }
        if (data instanceof Number n) return n.intValue();
        return 0;
    }

    // ── Async variants (CompletableFuture) ──────────────────────────────

    /** Async variant of {@link #find(String, Map, Class)}. */
    public <T> CompletableFuture<List<T>> findAsync(String collection, Map<String, Object> query, Class<T> type) {
        return CompletableFuture.supplyAsync(() -> {
            try { return find(collection, query, type); }
            catch (IOException e) { throw new OxiDbException("find failed: " + e.getMessage(), e); }
        });
    }

    /** Async variant of {@link #insertReturningId(String, Map)}. */
    public CompletableFuture<Long> insertReturningIdAsync(String collection, Map<String, Object> doc) {
        return CompletableFuture.supplyAsync(() -> {
            try { return insertReturningId(collection, doc); }
            catch (IOException e) { throw new OxiDbException("insert failed: " + e.getMessage(), e); }
        });
    }

    /** Async variant of {@link #hello(String, List)}. */
    public CompletableFuture<HelloResponse> helloAsync(String clientId) {
        return CompletableFuture.supplyAsync(() -> {
            try { return hello(clientId); }
            catch (IOException e) { throw new OxiDbException("hello failed: " + e.getMessage(), e); }
        });
    }

    // ── Iterable streaming ──────────────────────────────────────────────

    /**
     * Stream a query's result set as an {@link Iterable} fetched in
     * batches via LIMIT/SKIP pagination. Useful for large result sets
     * that would blow up memory if loaded via {@link #find}.
     *
     * <p><strong>Caveat:</strong> pass a {@code sort} spec for a stable
     * iteration order across batches.</p>
     */
    public <T> Iterable<T> stream(
            String collection,
            Map<String, Object> query,
            Map<String, Object> sort,
            int batchSize,
            Class<T> type) {
        if (batchSize <= 0) throw new IllegalArgumentException("batchSize must be positive");

        return () -> new java.util.Iterator<>() {
            int skip = 0;
            java.util.Iterator<T> currentBatch = null;
            boolean exhausted = false;

            @Override
            public boolean hasNext() {
                if (exhausted) return false;
                while (currentBatch == null || !currentBatch.hasNext()) {
                    try {
                        Map<String, Object> payload = new LinkedHashMap<>();
                        payload.put("cmd", "find");
                        payload.put("collection", collection);
                        if (query != null) payload.put("query", query);
                        if (sort != null) payload.put("sort", sort);
                        payload.put("skip", (long) skip);
                        payload.put("limit", (long) batchSize);
                        Object data = execRaw(payload);
                        List<Map<String, Object>> raw = data instanceof List<?> list
                            ? (List<Map<String, Object>>) list
                            : new ArrayList<>();
                        if (raw.isEmpty()) { exhausted = true; return false; }
                        List<T> typed = new ArrayList<>(raw.size());
                        for (Map<String, Object> doc : raw) typed.add(JSON.convertValue(doc, type));
                        skip += raw.size();
                        currentBatch = typed.iterator();
                        if (raw.size() < batchSize) exhausted = !currentBatch.hasNext();
                    } catch (IOException e) {
                        throw new OxiDbException.OxiDbConnectionException("stream failed", e);
                    }
                }
                return currentBatch.hasNext();
            }

            @Override
            public T next() {
                if (!hasNext()) throw new java.util.NoSuchElementException();
                return currentBatch.next();
            }
        };
    }
}
