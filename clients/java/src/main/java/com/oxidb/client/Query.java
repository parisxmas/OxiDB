package com.oxidb.client;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Type-safe query builder. Each helper returns a {@link Map} that the
 * wire protocol accepts directly — anywhere {@link OxiDbClient}
 * takes a query map, you can pass a {@code Query.*} result.
 *
 * <p>Shapes match the JSON operators documented at
 * <a href="https://oxidb.baltavista.com/docs/">oxidb.baltavista.com/docs</a>:
 * {@code $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $exists, $regex,
 * $and, $or, $nor}.</p>
 */
public final class Query {

    private Query() {}

    /** Match docs where {@code field} equals {@code value}. */
    public static Map<String, Object> eq(String field, Object value) {
        return Map.of(field, value);
    }

    /** Match docs where {@code field} is not equal to {@code value}. */
    public static Map<String, Object> ne(String field, Object value) {
        return wrap(field, "$ne", value);
    }

    /** Match docs where {@code field > value}. */
    public static Map<String, Object> gt(String field, Object value) {
        return wrap(field, "$gt", value);
    }

    /** Match docs where {@code field >= value}. */
    public static Map<String, Object> gte(String field, Object value) {
        return wrap(field, "$gte", value);
    }

    /** Match docs where {@code field < value}. */
    public static Map<String, Object> lt(String field, Object value) {
        return wrap(field, "$lt", value);
    }

    /** Match docs where {@code field <= value}. */
    public static Map<String, Object> lte(String field, Object value) {
        return wrap(field, "$lte", value);
    }

    /** Match docs where {@code field}'s value is in the given set. */
    public static Map<String, Object> in(String field, List<?> values) {
        return wrap(field, "$in", values);
    }

    /** Match docs where {@code field}'s value is NOT in the given set. */
    public static Map<String, Object> nin(String field, List<?> values) {
        return wrap(field, "$nin", values);
    }

    /** Match docs where {@code field} exists ({@code exists=false} for the inverse). */
    public static Map<String, Object> exists(String field, boolean exists) {
        return wrap(field, "$exists", exists);
    }

    /** Match docs where {@code field}'s string value matches the regex pattern. */
    public static Map<String, Object> regex(String field, String pattern) {
        return wrap(field, "$regex", pattern);
    }

    /** Match docs satisfying ALL sub-queries. */
    @SafeVarargs
    public static Map<String, Object> and(Map<String, Object>... subQueries) {
        return Map.of("$and", Arrays.asList(subQueries));
    }

    /** Match docs satisfying AT LEAST ONE sub-query. */
    @SafeVarargs
    public static Map<String, Object> or(Map<String, Object>... subQueries) {
        return Map.of("$or", Arrays.asList(subQueries));
    }

    /** Match docs satisfying NONE of the sub-queries. */
    @SafeVarargs
    public static Map<String, Object> nor(Map<String, Object>... subQueries) {
        return Map.of("$nor", Arrays.asList(subQueries));
    }

    /** Half-open interval {@code [low, high)} on a single field. */
    public static Map<String, Object> range(String field, Object low, Object high) {
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("$gte", low);
        spec.put("$lt", high);
        return Map.of(field, spec);
    }

    private static Map<String, Object> wrap(String field, String op, Object value) {
        return Map.of(field, Map.of(op, value));
    }
}
