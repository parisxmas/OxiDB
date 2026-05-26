package com.oxidb.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oxidb.client.OxiDbClient;
import com.oxidb.client.OxiDbException;
import com.oxidb.client.Query;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * REST API translating the standard CRUD shape into OxiDB calls.
 * Each route is a thin wrapper around an {@link OxiDbClient} method —
 * the goal is to show the API surface side-by-side with the routes a
 * typical Spring developer would build on top.
 *
 * <p>Routes:
 * <ul>
 *   <li>GET    /ping                       — server liveness</li>
 *   <li>GET    /hello                      — server version + features</li>
 *   <li>POST   /{collection}               — insert (body: JSON doc)</li>
 *   <li>GET    /{collection}?query=...     — find (query is a JSON string)</li>
 *   <li>GET    /{collection}/count         — count all in the collection</li>
 *   <li>PATCH  /{collection}               — update (body: {"query": ..., "update": ...})</li>
 *   <li>DELETE /{collection}               — delete (body: JSON query)</li>
 *   <li>GET    /{collection}/typed/{name}  — typed findOne example</li>
 * </ul>
 */
@RestController
public class OxiDbController {

    private static final ObjectMapper JSON = new ObjectMapper();
    private final OxiDbClient db;

    public OxiDbController(OxiDbClient db) {
        this.db = db;
    }

    /** Demo entity for the typed example below. */
    public record User(long _id, String name, int age, boolean active) {}

    // ── Liveness / handshake ────────────────────────────────────────────

    @GetMapping("/ping")
    public Object ping() throws IOException {
        return Map.of("data", db.ping());
    }

    @GetMapping("/hello")
    public Object hello() throws IOException {
        var h = db.hello("spring-boot-example/1.0");
        return Map.of(
            "name", h.name(),
            "version", h.version(),
            "wire_version", h.wireVersion(),
            "stable_surface_version", h.stableSurfaceVersion(),
            "features", h.features(),
            "experimental_features", h.experimentalFeatures(),
            "auth_methods", h.authMethods()
        );
    }

    // ── CRUD ────────────────────────────────────────────────────────────

    @PostMapping("/{collection}")
    public Object insert(@PathVariable String collection,
                         @RequestBody Map<String, Object> doc) throws IOException {
        long id = db.insertReturningId(collection, doc);
        return Map.of("id", id);
    }

    @GetMapping("/{collection}")
    public List<Map<String, Object>> find(@PathVariable String collection,
                                          @RequestParam(defaultValue = "{}") String query)
            throws IOException {
        return db.find(collection, parseJson(query));
    }

    @GetMapping("/{collection}/count")
    public Object count(@PathVariable String collection) throws IOException {
        return Map.of("count", db.count(collection, null));
    }

    @PatchMapping("/{collection}")
    public Object update(@PathVariable String collection,
                         @RequestBody Map<String, Object> body) throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, Object> query = (Map<String, Object>) body.get("query");
        @SuppressWarnings("unchecked")
        Map<String, Object> update = (Map<String, Object>) body.get("update");
        int modified = db.update(collection, query, update);
        return Map.of("modified", modified);
    }

    @DeleteMapping("/{collection}")
    public Object delete(@PathVariable String collection,
                         @RequestBody(required = false) Map<String, Object> query)
            throws IOException {
        int deleted = db.delete(collection, query != null ? query : Map.of());
        return Map.of("deleted", deleted);
    }

    // ── Typed example ───────────────────────────────────────────────────

    /**
     * Shows {@code findOne} with typed deserialization to a record.
     * Returns 404 if the user isn't found.
     */
    @GetMapping("/{collection}/typed/{name}")
    public ResponseEntity<User> findUserByName(@PathVariable String collection,
                                               @PathVariable String name) throws IOException {
        User user = db.findOne(collection, Query.eq("name", name), User.class);
        return user != null ? ResponseEntity.ok(user) : ResponseEntity.notFound().build();
    }

    // ── Error handling ──────────────────────────────────────────────────

    @ExceptionHandler(OxiDbException.OxiDbNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleNotFound(OxiDbException.OxiDbNotFoundException e) {
        return ResponseEntity.status(404).body(Map.of("error", e.getServerMessage()));
    }

    @ExceptionHandler(OxiDbException.OxiDbDuplicateKeyException.class)
    public ResponseEntity<Map<String, Object>> handleDuplicate(OxiDbException.OxiDbDuplicateKeyException e) {
        return ResponseEntity.status(409).body(Map.of("error", e.getServerMessage()));
    }

    @ExceptionHandler(OxiDbException.class)
    public ResponseEntity<Map<String, Object>> handleAny(OxiDbException e) {
        return ResponseEntity.internalServerError().body(Map.of("error", e.getServerMessage()));
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJson(String json) {
        try {
            return JSON.readValue(json, Map.class);
        } catch (Exception e) {
            throw new OxiDbException("Invalid JSON query: " + e.getMessage(), e);
        }
    }
}
