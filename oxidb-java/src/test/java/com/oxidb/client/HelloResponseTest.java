package com.oxidb.client;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HelloResponseTest {

    @Test
    void fromMapBuildsTypedRecord() {
        Map<String, Object> serverMap = Map.of(
            "name", "oxidb-server",
            "version", "0.28.18",
            "wire_version", 1L,
            "supported_wire_versions", List.of(1L),
            "stable_surface_version", "1.0",
            "features", List.of("fts", "blobs", "txn"),
            "experimental_features", List.of("raft", "pitr"),
            "auth_methods", List.of("scram-sha-256")
        );

        HelloResponse hello = HelloResponse.fromMap(serverMap);

        assertEquals("oxidb-server", hello.name());
        assertEquals("0.28.18", hello.version());
        assertEquals(1L, hello.wireVersion());
        assertEquals("1.0", hello.stableSurfaceVersion());
        assertTrue(hello.features().contains("blobs"));
        assertTrue(hello.experimentalFeatures().contains("raft"));
    }

    @Test
    void hasFeatureChecksBothLists() {
        HelloResponse hello = new HelloResponse(
            "oxidb-server", "0.28.18", 1L, List.of(1L), "1.0",
            List.of("fts"), List.of("raft"), List.of("scram-sha-256"));

        assertTrue(hello.hasFeature("fts"));            // in stable
        assertTrue(hello.hasFeature("raft"));           // in experimental
        assertFalse(hello.hasFeature("nonexistent"));
        assertTrue(hello.hasStableFeature("fts"));
        assertFalse(hello.hasStableFeature("raft"));    // experimental, not stable
    }

    @Test
    void supportsWireVersionWorks() {
        HelloResponse hello = new HelloResponse(
            "oxidb-server", "0.28.18", 1L, List.of(1L, 2L), "1.0",
            List.of(), List.of(), List.of());

        assertTrue(hello.supportsWireVersion(1L));
        assertTrue(hello.supportsWireVersion(2L));
        assertFalse(hello.supportsWireVersion(3L));
    }
}
