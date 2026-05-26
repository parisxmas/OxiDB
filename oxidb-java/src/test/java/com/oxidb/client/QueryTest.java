package com.oxidb.client;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the {@link Query} static builder.
 *
 * <p>These tests are intentionally pure-Java (no socket, no server) — they
 * pin the shape of the maps that go on the wire. The actual round-trip
 * against a running server lives in the integration test suite that
 * isn't part of <code>mvn test</code> by default.</p>
 */
class QueryTest {

    @Test
    void eqProducesPlainKeyValuePair() {
        Map<String, Object> q = Query.eq("name", "Alice");
        assertEquals(1, q.size());
        assertEquals("Alice", q.get("name"));
    }

    @Test
    void gteWrapsInOperator() {
        Map<String, Object> q = Query.gte("age", 18);
        Map<?, ?> spec = (Map<?, ?>) q.get("age");
        assertEquals(18, spec.get("$gte"));
    }

    @Test
    void inAcceptsAnyListType() {
        Map<String, Object> q = Query.in("country", List.of("US", "UK", "JP"));
        Map<?, ?> spec = (Map<?, ?>) q.get("country");
        assertEquals(List.of("US", "UK", "JP"), spec.get("$in"));
    }

    @Test
    void andComposesSubQueries() {
        Map<String, Object> q = Query.and(
            Query.eq("status", "active"),
            Query.gte("age", 18)
        );
        List<?> subs = (List<?>) q.get("$and");
        assertEquals(2, subs.size());
    }

    @Test
    void orComposesSubQueries() {
        Map<String, Object> q = Query.or(
            Query.eq("city", "Tokyo"),
            Query.eq("city", "Paris")
        );
        List<?> subs = (List<?>) q.get("$or");
        assertEquals(2, subs.size());
    }

    @Test
    void rangeBuildsHalfOpenInterval() {
        Map<String, Object> q = Query.range("salary", 50_000, 100_000);
        Map<?, ?> spec = (Map<?, ?>) q.get("salary");
        assertEquals(50_000, spec.get("$gte"));
        assertEquals(100_000, spec.get("$lt"));
    }

    @Test
    void existsTakesBoolean() {
        Map<String, Object> q1 = Query.exists("email", true);
        Map<?, ?> spec1 = (Map<?, ?>) q1.get("email");
        assertEquals(Boolean.TRUE, spec1.get("$exists"));

        Map<String, Object> q2 = Query.exists("deleted_at", false);
        Map<?, ?> spec2 = (Map<?, ?>) q2.get("deleted_at");
        assertEquals(Boolean.FALSE, spec2.get("$exists"));
    }
}
