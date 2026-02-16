package com.oxidb.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.oxidb.spring.OxiDbClient;
import com.oxidb.spring.OxiDbException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@RestController
public class OxiDbController {

    private final OxiDbClient db;

    public OxiDbController(OxiDbClient db) {
        this.db = db;
    }

    // ---- Ping ----

    @GetMapping("/ping")
    public ResponseEntity<?> ping() {
        return ok(db.ping());
    }

    // ---- CRUD ----

    @PostMapping("/{collection}")
    public ResponseEntity<?> insert(@PathVariable String collection,
                                    @RequestBody String docJson) {
        return ok(db.insert(collection, docJson));
    }

    @GetMapping("/{collection}")
    public ResponseEntity<?> find(@PathVariable String collection,
                                  @RequestParam(defaultValue = "{}") String query,
                                  @RequestParam(required = false) Integer skip,
                                  @RequestParam(required = false) Integer limit) {
        return ok(db.find(collection, query != null ? parseJson(query) : null, null, skip, limit));
    }

    @PutMapping("/{collection}")
    public ResponseEntity<?> update(@PathVariable String collection,
                                    @RequestBody Map<String, Object> body) {
        @SuppressWarnings("unchecked")
        Map<String, Object> query = (Map<String, Object>) body.get("query");
        @SuppressWarnings("unchecked")
        Map<String, Object> update = (Map<String, Object>) body.get("update");
        return ok(db.update(collection, query, update));
    }

    @DeleteMapping("/{collection}")
    public ResponseEntity<?> delete(@PathVariable String collection,
                                    @RequestBody String queryJson) {
        return ok(db.delete(collection, queryJson));
    }

    @GetMapping("/{collection}/count")
    public ResponseEntity<?> count(@PathVariable String collection) {
        return ok(Map.of("count", db.count(collection)));
    }

    // ---- Transaction demo ----

    @PostMapping("/tx/demo")
    public ResponseEntity<?> txDemo() {
        db.withTransaction(() -> {
            db.insert("tx_log", Map.of("action", "debit", "amount", 100));
            db.insert("tx_log", Map.of("action", "credit", "amount", 100));
        });
        return ok(Map.of("status", "committed", "description", "Inserted debit + credit atomically"));
    }

    // ---- Blob storage ----

    @PostMapping("/blobs/{bucket}")
    public ResponseEntity<?> createBucket(@PathVariable String bucket) {
        return ok(db.createBucket(bucket));
    }

    @PutMapping("/blobs/{bucket}/{key}")
    public ResponseEntity<?> putObject(@PathVariable String bucket,
                                       @PathVariable String key,
                                       @RequestBody byte[] data) {
        return ok(db.putObject(bucket, key, data));
    }

    @GetMapping("/blobs/{bucket}/{key}")
    public ResponseEntity<byte[]> getObject(@PathVariable String bucket,
                                            @PathVariable String key) {
        JsonNode result = db.getObject(bucket, key);
        byte[] content = db.decodeObjectContent(result);
        return ResponseEntity.ok(content);
    }

    // ---- Full-text search ----

    @GetMapping("/search")
    public ResponseEntity<?> search(@RequestParam String q,
                                    @RequestParam(required = false) String bucket,
                                    @RequestParam(defaultValue = "10") int limit) {
        return ok(db.search(q, bucket, limit));
    }

    // ---- Helpers ----

    private ResponseEntity<?> ok(Object data) {
        return ResponseEntity.ok(data);
    }

    private Map<String, Object> parseJson(String json) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = new com.fasterxml.jackson.databind.ObjectMapper()
                    .readValue(json, Map.class);
            return map;
        } catch (Exception e) {
            throw new OxiDbException("Invalid JSON query: " + e.getMessage(), e);
        }
    }

    @ExceptionHandler(OxiDbException.class)
    public ResponseEntity<?> handleOxiDbError(OxiDbException e) {
        return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
    }
}
