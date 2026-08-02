package com.oxidb.client;

/**
 * Base class for all OxiDB client exceptions. Catch this when you don't
 * care about distinguishing failure modes; catch the derived subclasses
 * when you do.
 *
 * <p>Server error strings are mapped to specific subclasses via
 * {@link #fromServerMessage(String)}. The mapping is heuristic — server
 * error messages aren't stable across versions (see <code>docs/SEMVER.md</code>
 * "Greyish, with explicit rules"). For matching that must be reliable,
 * inspect {@link #getServerMessage()} directly.</p>
 */
public class OxiDbException extends RuntimeException {

    private final String serverMessage;

    public OxiDbException(String message) {
        super(message);
        this.serverMessage = message;
    }

    public OxiDbException(String message, Throwable cause) {
        super(message, cause);
        this.serverMessage = message;
    }

    /** The raw error string returned by the server, exactly as on the wire. */
    public String getServerMessage() {
        return serverMessage;
    }

    /**
     * Classify a server-reported error message into the most specific
     * subclass available. Falls back to {@link OxiDbException} for
     * errors the client doesn't have a dedicated mapping for yet.
     */
    public static OxiDbException fromServerMessage(String message) {
        if (message == null) return new OxiDbException("unknown error");
        String lower = message.toLowerCase();

        if (lower.contains("duplicate") || lower.contains("unique constraint"))
            return new OxiDbDuplicateKeyException(message);
        if (lower.contains("transaction conflict") || lower.contains("write-write conflict")
            || lower.contains("occ retry"))
            return new OxiDbTransactionConflictException(message);
        if (lower.contains("permission denied") || lower.contains("authentication required")
            || lower.contains("not authorized") || lower.contains("invalid credentials"))
            return new OxiDbAuthenticationException(message);
        if (lower.contains("no such collection") || lower.contains("not found")
            || lower.contains("does not exist"))
            return new OxiDbNotFoundException(message);
        if (lower.contains("worm") || lower.contains("immutable") || lower.contains("locked until"))
            return new OxiDbImmutableException(message);
        if (lower.contains("connection closed") || lower.contains("broken pipe")
            || lower.contains("eof"))
            return new OxiDbConnectionException(message);

        return new OxiDbException(message);
    }

    // ── Specific subclasses ─────────────────────────────────────────────

    /** Write violated a unique index constraint. */
    public static final class OxiDbDuplicateKeyException extends OxiDbException {
        public OxiDbDuplicateKeyException(String message) { super(message); }
    }

    /** OCC commit-time validation failed; retry the whole transaction. */
    public static final class OxiDbTransactionConflictException extends OxiDbException {
        public OxiDbTransactionConflictException(String message) { super(message); }
    }

    /** SCRAM / RBAC auth failure. */
    public static final class OxiDbAuthenticationException extends OxiDbException {
        public OxiDbAuthenticationException(String message) { super(message); }
    }

    /** Requested collection / index / user doesn't exist. */
    public static final class OxiDbNotFoundException extends OxiDbException {
        public OxiDbNotFoundException(String message) { super(message); }
    }

    /** Write targeted a document under a WORM retention lock. */
    public static final class OxiDbImmutableException extends OxiDbException {
        public OxiDbImmutableException(String message) { super(message); }
    }

    /** Wire-level failure — connection closed, broken pipe, EOF, etc. */
    public static final class OxiDbConnectionException extends OxiDbException {
        public OxiDbConnectionException(String message) { super(message); }
        public OxiDbConnectionException(String message, Throwable cause) { super(message, cause); }
    }

    /** OxiWire binary decoder rejected the wire bytes (almost always a
     *  client/server version mismatch — run {@code hello()} on connect). */
    public static final class OxiDbProtocolException extends OxiDbException {
        public OxiDbProtocolException(String message) { super(message); }
    }
}
