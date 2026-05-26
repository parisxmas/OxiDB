package com.oxidb.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Strongly-typed view of the server's response to a {@code hello} command.
 * Returned by {@link OxiDbClient#hello(String, List)}.
 *
 * <p>HELLO is the OxiWire wire-protocol handshake landed in OxiDB v0.28.13
 * (ADR-0003 Phase 2). It's pre-auth, idempotent, and free — call it once
 * at connection time to discover what the server supports.</p>
 */
public record HelloResponse(
    String name,
    String version,
    long wireVersion,
    List<Long> supportedWireVersions,
    String stableSurfaceVersion,
    List<String> features,
    List<String> experimentalFeatures,
    List<String> authMethods
) {

    /** Returns true if {@code feature} is in either feature list. */
    public boolean hasFeature(String feature) {
        return features.contains(feature) || experimentalFeatures.contains(feature);
    }

    /** Returns true if {@code feature} is in the 1.0-stable feature list. */
    public boolean hasStableFeature(String feature) {
        return features.contains(feature);
    }

    /** Returns true if the server supports the given OxiWire wire version. */
    public boolean supportsWireVersion(long version) {
        return supportedWireVersions.contains(version);
    }

    @SuppressWarnings("unchecked")
    static HelloResponse fromMap(Map<String, Object> map) {
        return new HelloResponse(
            asString(map, "name"),
            asString(map, "version"),
            asLong(map, "wire_version"),
            asLongList(map, "supported_wire_versions"),
            asString(map, "stable_surface_version"),
            asStringList(map, "features"),
            asStringList(map, "experimental_features"),
            asStringList(map, "auth_methods")
        );
    }

    private static String asString(Map<String, Object> map, String key) {
        Object v = map.get(key);
        return v == null ? "" : v.toString();
    }

    private static long asLong(Map<String, Object> map, String key) {
        Object v = map.get(key);
        return v instanceof Number n ? n.longValue() : 0L;
    }

    private static List<Long> asLongList(Map<String, Object> map, String key) {
        Object v = map.get(key);
        if (!(v instanceof List<?> list)) return Collections.emptyList();
        List<Long> out = new ArrayList<>(list.size());
        for (Object item : list) {
            if (item instanceof Number n) out.add(n.longValue());
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static List<String> asStringList(Map<String, Object> map, String key) {
        Object v = map.get(key);
        if (!(v instanceof List<?> list)) return Collections.emptyList();
        List<String> out = new ArrayList<>(list.size());
        for (Object item : list) {
            if (item != null) out.add(item.toString());
        }
        return out;
    }
}
