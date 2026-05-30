namespace OxiDb.Client.Tcp;

/// <summary>
/// Per-collection storage options for
/// <see cref="IOxiDbClient.CreateCollectionWithOptionsAsync"/>. Any property
/// left <c>null</c> is omitted from the request and falls back to the server
/// default (in-RAM, compressed, auto-compaction). The chosen storage shape is
/// persisted, so the collection reopens the same way regardless of the server's
/// environment.
/// </summary>
public sealed class StorageOptions
{
    /// <summary>Store documents on disk (an mmap'd data file) keeping only the
    /// offset index resident, instead of the default in-RAM store.</summary>
    public bool? DiskFirst { get; set; }

    /// <summary>zstd-compress on-disk records. Ignored unless <see cref="DiskFirst"/>.</summary>
    public bool? Compress { get; set; }

    /// <summary>Reclaim dead space automatically (disk-first only).</summary>
    public bool? AutoCompact { get; set; }

    /// <summary>Never auto-compact a data file smaller than this many bytes.</summary>
    public long? CompactMinBytes { get; set; }

    /// <summary>Dead-space fraction (0..1) that triggers compaction.</summary>
    public double? CompactDeadRatio { get; set; }

    /// <summary>Build the wire-format <c>options</c> map, omitting unset fields so
    /// the server applies its defaults for them. A string-keyed dictionary
    /// serializes correctly over both the JSON and OxiWire transports.</summary>
    public Dictionary<string, object?> ToWire()
    {
        var d = new Dictionary<string, object?>();
        if (DiskFirst.HasValue) d["disk_first"] = DiskFirst.Value;
        if (Compress.HasValue) d["compress"] = Compress.Value;
        if (AutoCompact.HasValue) d["auto_compact"] = AutoCompact.Value;
        if (CompactMinBytes.HasValue) d["compact_min_bytes"] = CompactMinBytes.Value;
        if (CompactDeadRatio.HasValue) d["compact_dead_ratio"] = CompactDeadRatio.Value;
        return d;
    }
}
