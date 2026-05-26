namespace OxiDb.Client.Tcp;

/// <summary>
/// Base class for all OxiDB client exceptions. Catch this if you don't care
/// about distinguishing between specific failure modes; catch the derived
/// types below when you do (duplicate keys, transaction conflicts, etc.).
/// </summary>
public class OxiDbException : Exception
{
    /// <summary>The raw error string returned by the server, exactly as
    /// shipped over the wire. Useful for logging or pattern-matching when
    /// the structured subclass doesn't cover your case.</summary>
    public string ServerMessage { get; }

    public OxiDbException(string message) : base(message)
    {
        ServerMessage = message;
    }

    public OxiDbException(string message, Exception inner) : base(message, inner)
    {
        ServerMessage = message;
    }

    /// <summary>
    /// Classify a server-reported error message into the most specific
    /// subclass available. Falls back to <see cref="OxiDbException"/> for
    /// errors the client doesn't have a dedicated mapping for yet.
    /// </summary>
    public static OxiDbException FromServerMessage(string message)
    {
        // Heuristic match — server error strings aren't stable across
        // versions (see docs/SEMVER.md "Greyish, with explicit rules"),
        // so this is best-effort. The base class always carries the raw
        // message for callers that need to do their own matching.
        var lower = message.ToLowerInvariant();

        if (lower.Contains("duplicate") || lower.Contains("unique constraint"))
            return new OxiDbDuplicateKeyException(message);

        if (lower.Contains("transaction conflict") || lower.Contains("write-write conflict")
            || lower.Contains("occ retry"))
            return new OxiDbTransactionConflictException(message);

        if (lower.Contains("permission denied") || lower.Contains("authentication required")
            || lower.Contains("not authorized") || lower.Contains("invalid credentials"))
            return new OxiDbAuthenticationException(message);

        if (lower.Contains("no such collection") || lower.Contains("not found")
            || lower.Contains("does not exist"))
            return new OxiDbNotFoundException(message);

        if (lower.Contains("worm") || lower.Contains("immutable") || lower.Contains("locked until"))
            return new OxiDbImmutableException(message);

        if (lower.Contains("connection closed") || lower.Contains("broken pipe")
            || lower.Contains("eof"))
            return new OxiDbConnectionException(message);

        return new OxiDbException(message);
    }
}

/// <summary>
/// Thrown when a write violates a unique index constraint.
/// Mongo equivalent: E11000 duplicate key error.
/// </summary>
public sealed class OxiDbDuplicateKeyException : OxiDbException
{
    public OxiDbDuplicateKeyException(string message) : base(message) { }
}

/// <summary>
/// Thrown when an OCC transaction's commit-time validation fails because
/// another transaction wrote a document this one read. Retrying the whole
/// transaction is the standard remedy.
/// </summary>
public sealed class OxiDbTransactionConflictException : OxiDbException
{
    public OxiDbTransactionConflictException(string message) : base(message) { }
}

/// <summary>Thrown for SCRAM/RBAC auth failures.</summary>
public sealed class OxiDbAuthenticationException : OxiDbException
{
    public OxiDbAuthenticationException(string message) : base(message) { }
}

/// <summary>Thrown when a requested collection / index / user doesn't exist.</summary>
public sealed class OxiDbNotFoundException : OxiDbException
{
    public OxiDbNotFoundException(string message) : base(message) { }
}

/// <summary>
/// Thrown when a write targets a document under a WORM (write-once read-
/// many) retention lock. Surfaces from the engine's worm module.
/// </summary>
public sealed class OxiDbImmutableException : OxiDbException
{
    public OxiDbImmutableException(string message) : base(message) { }
}

/// <summary>
/// Wire-level failure — connection closed by server, broken pipe, EOF
/// before response, etc. Distinct from logical errors so callers can
/// retry connection-level failures without re-running the whole query.
/// </summary>
public sealed class OxiDbConnectionException : OxiDbException
{
    public OxiDbConnectionException(string message) : base(message) { }
}

/// <summary>
/// Thrown by the OxiWire binary decoder when the bytes on the wire
/// don't match the protocol (bad magic byte, unknown type tag,
/// truncated payload, ...). Almost always a sign of a version
/// mismatch between client and server — run <c>HelloAsync</c> on
/// connect to verify wire versions intersect.
/// </summary>
public sealed class OxiDbProtocolException : OxiDbException
{
    public OxiDbProtocolException(string message) : base(message) { }
}

/// <summary>
/// Legacy alias retained for binary compatibility with code compiled
/// against the pre-rework client. New code should catch
/// <see cref="OxiDbException"/> or one of its specific subclasses.
/// </summary>
[Obsolete("Use OxiDbException or one of its specific subclasses (OxiDbDuplicateKeyException, " +
          "OxiDbTransactionConflictException, ...). This alias will be removed in 2.0.")]
public class OxiDbTcpException : OxiDbException
{
    public OxiDbTcpException(string message) : base(message) { }
}
