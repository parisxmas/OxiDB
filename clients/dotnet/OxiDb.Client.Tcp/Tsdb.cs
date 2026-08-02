using System.Globalization;
using System.Text.Json;

namespace OxiDb.Client.Tcp;

/// <summary>
/// An aggregation over a time-series field.
/// </summary>
/// <remarks>
/// A closed set of names plus one parameterised case (percentile), which is
/// why this is a struct with factories rather than an enum: the engine takes
/// <c>agg</c> as a string and percentile carries a <c>p</c> alongside it.
/// Passing the string yourself works — this exists so a typo is a compile
/// error instead of <c>unknown agg: "maximum"</c> at runtime.
/// </remarks>
public readonly record struct TsdbAgg
{
    private TsdbAgg(string name, double? p = null) { Name = name; P = p; }

    /// <summary>The wire name (<c>mean</c>, <c>max</c>, <c>percentile</c>, …).</summary>
    public string Name { get; }

    /// <summary>The percentile, 0..100, when <see cref="Name"/> is <c>percentile</c>.</summary>
    public double? P { get; }

    /// <summary>Arithmetic mean. The engine's default.</summary>
    public static TsdbAgg Mean => new("mean");
    public static TsdbAgg Sum => new("sum");
    public static TsdbAgg Min => new("min");
    public static TsdbAgg Max => new("max");
    public static TsdbAgg Count => new("count");
    /// <summary>Number of distinct values in the bucket.</summary>
    public static TsdbAgg Distinct => new("distinct");
    public static TsdbAgg First => new("first");
    public static TsdbAgg Last => new("last");
    /// <summary>Per-second rate of change. For counters, which only ever climb.</summary>
    public static TsdbAgg Rate => new("rate");

    /// <summary>Linearly-interpolated percentile, e.g. <c>Percentile(95)</c>.</summary>
    /// <exception cref="ArgumentOutOfRangeException">If <paramref name="p"/> is outside 0..100.</exception>
    public static TsdbAgg Percentile(double p) =>
        p is >= 0 and <= 100
            ? new("percentile", p)
            : throw new ArgumentOutOfRangeException(nameof(p), p, "percentile must be 0..100");

    /// <summary>An aggregation the engine knows but this client does not name yet.</summary>
    public static TsdbAgg Raw(string name) => new(name);

    public override string ToString() => P is { } p
        ? "p" + p.ToString(CultureInfo.InvariantCulture)
        : Name;
}

/// <summary>One reading: a measurement, its tags, its fields, and when.</summary>
/// <param name="Measurement">The series family, e.g. <c>temperature</c>.</param>
/// <param name="Tags">Indexed dimensions (device, region). They identify the series.</param>
/// <param name="Fields">The measured values. <c>bool</c> stores as boolean, integral
/// types as integer (exact to 2^53), everything else numeric as float; strings take a
/// separate text path.</param>
/// <param name="TimestampUtc">When it was measured. Treated as UTC.</param>
public sealed record TsdbPoint(
    string Measurement,
    IReadOnlyDictionary<string, string>? Tags,
    IReadOnlyDictionary<string, object?> Fields,
    DateTime TimestampUtc);

/// <summary>One sample. Numeric series fill <see cref="Value"/>; text series may fill either.</summary>
/// <param name="At">Sample time (UTC).</param>
/// <param name="Value">The number, when the sample is numeric.</param>
/// <param name="Text">The text, when the sample is a string.</param>
public sealed record TsdbSample(DateTime At, double? Value, string? Text);

/// <summary>One series returned by a query: a distinct tag-set and its samples.</summary>
/// <param name="Tags">The tag-set identifying this series.</param>
/// <param name="Type">The field's stored type: <c>float</c>, <c>integer</c>, <c>boolean</c> or <c>string</c>.</param>
/// <param name="Points">Samples, oldest first.</param>
public sealed record TsdbSeries(
    IReadOnlyDictionary<string, string> Tags,
    string Type,
    IReadOnlyList<TsdbSample> Points);

/// <summary>What the engine is holding.</summary>
/// <param name="Series">Distinct series (measurement × tag-set × field).</param>
/// <param name="Points">Samples across all of them.</param>
/// <param name="Bytes">Compressed size on disk.</param>
public sealed record TsdbStats(long Series, long Points, long Bytes);

/// <summary>A registered continuous-aggregate rule.</summary>
/// <param name="Measurement">The raw measurement being rolled up.</param>
/// <param name="Label">The suffix of the derived measurement, e.g. <c>1m</c> in <c>cpu@1m</c>.</param>
/// <param name="Interval">The bucket width.</param>
public sealed record TsdbRollup(string Measurement, string Label, TimeSpan Interval);

/// <summary>
/// The time-series engine, typed.
/// </summary>
/// <remarks>
/// <para>
/// These sit on <see cref="IOxiDbClient"/> next to <c>SqlAsync</c>, but the TSDB
/// is a server-side engine, so they need a connected <see cref="OxiDbTcpClient"/>
/// and throw <see cref="NotSupportedException"/> otherwise.
/// </para>
/// <para>
/// The engine is off unless the server runs with <c>OXIDB_TSDB=1</c>, and it is
/// per-database, like SQL.
/// </para>
/// </remarks>
public static class OxiDbTsdbExtensions
{
    /// <summary>
    /// Epoch milliseconds, UTC — how the engine stores every timestamp.
    /// A <see cref="DateTimeKind.Unspecified"/> value is taken as UTC (the same
    /// rule OxiDb.Data uses), a local one is converted.
    /// </summary>
    /// <remarks>Public because line protocol makes the caller format its own
    /// timestamps, and getting this wrong shifts data by an offset silently.</remarks>
    public static long ToEpochMs(DateTime t) =>
        new DateTimeOffset(t.Kind == DateTimeKind.Unspecified
            ? DateTime.SpecifyKind(t, DateTimeKind.Utc)
            : t.ToUniversalTime()).ToUnixTimeMilliseconds();

    /// <summary>The inverse of <see cref="ToEpochMs"/>.</summary>
    public static DateTime FromEpochMs(long ms) =>
        DateTimeOffset.FromUnixTimeMilliseconds(ms).UtcDateTime;

    private static OxiDbTcpClient Tcp(IOxiDbClient client) =>
        client as OxiDbTcpClient
        ?? throw new NotSupportedException(
            "The time-series engine is server-side; it needs an OxiDbTcpClient.");

    private static Dictionary<string, object?> Op(string op) => new()
    {
        ["engine"] = "tsdb",
        ["cmd"] = "tsdb",
        ["op"] = op,
    };

    // ── Writing ─────────────────────────────────────────────────────────

    /// <summary>Append points. Returns how many the engine stored.</summary>
    /// <exception cref="ArgumentException">If <paramref name="points"/> is empty.</exception>
    public static async Task<int> TsdbWriteAsync(
        this IOxiDbClient client,
        IEnumerable<TsdbPoint> points,
        CancellationToken ct = default)
    {
        var wire = points.Select(p => new Dictionary<string, object?>
        {
            ["measurement"] = p.Measurement,
            ["tags"] = p.Tags ?? new Dictionary<string, string>(),
            ["fields"] = p.Fields,
            ["ts"] = ToEpochMs(p.TimestampUtc),
        }).ToList();

        if (wire.Count == 0)
            throw new ArgumentException("no points to write", nameof(points));

        var req = Op("write");
        req["points"] = wire;
        var r = await Tcp(client).ExecRawAsync(req, ct);
        return r.TryGetProperty("written", out var w) ? w.GetInt32() : 0;
    }

    /// <summary>Append a single reading.</summary>
    /// <param name="atUtc">When it was measured; defaults to now.</param>
    public static Task<int> TsdbWriteAsync(
        this IOxiDbClient client,
        string measurement,
        IReadOnlyDictionary<string, string>? tags,
        IReadOnlyDictionary<string, object?> fields,
        DateTime? atUtc = null,
        CancellationToken ct = default) =>
        client.TsdbWriteAsync(
            [new TsdbPoint(measurement, tags, fields, atUtc ?? DateTime.UtcNow)], ct);

    /// <summary>
    /// Write InfluxDB line protocol, so existing collectors (Telegraf and
    /// friends) can point here unmodified.
    /// </summary>
    /// <param name="lineProtocol">One or more lines,
    /// <c>measurement,tag=v field=1 1700000000000</c>. Timestamps are ms;
    /// a line without one is stamped now.</param>
    public static async Task<int> TsdbWriteLineProtocolAsync(
        this IOxiDbClient client,
        string lineProtocol,
        CancellationToken ct = default)
    {
        var req = Op("write_lp");
        req["lp"] = lineProtocol;
        var r = await Tcp(client).ExecRawAsync(req, ct);
        return r.TryGetProperty("written", out var w) ? w.GetInt32() : 0;
    }

    // ── Reading ─────────────────────────────────────────────────────────

    /// <summary>
    /// Read a field back, optionally downsampled — the reason to have the
    /// engine at all: a month of 10-second samples answers as a chart without
    /// walking the raw data.
    /// </summary>
    /// <param name="measurement">e.g. <c>temperature</c>. For a rollup, <c>temperature@1m</c>.</param>
    /// <param name="field">The field within it. For a rollup, <c>celsius_max</c>.</param>
    /// <param name="tags">Exact-match tag filters. Null matches every series.</param>
    /// <param name="start">Inclusive lower bound; null for the beginning of time.</param>
    /// <param name="end">Exclusive upper bound; null for the end of it.</param>
    /// <param name="interval">Bucket width. With no interval the whole range
    /// collapses into ONE bucket — there is no unaggregated read; see
    /// <see cref="TsdbReadRawAsync"/>.</param>
    /// <param name="agg">How to combine each bucket. Defaults to mean, engine-side,
    /// so omitting it does not mean "don't aggregate" — it means "take the mean".
    /// Without an <paramref name="interval"/> this reduces the whole range to a
    /// single value, which is how you ask for a maximum over a journey.</param>
    /// <param name="groupBy">Split the result by these tags instead of merging.</param>
    /// <returns>One entry per distinct tag-set, each with its samples oldest-first.</returns>
    public static async Task<IReadOnlyList<TsdbSeries>> TsdbQueryAsync(
        this IOxiDbClient client,
        string measurement,
        string field,
        IReadOnlyDictionary<string, string>? tags = null,
        DateTime? start = null,
        DateTime? end = null,
        TimeSpan? interval = null,
        TsdbAgg? agg = null,
        IEnumerable<string>? groupBy = null,
        CancellationToken ct = default)
    {
        var req = Op("query");
        req["measurement"] = measurement;
        req["field"] = field;
        if (tags is { Count: > 0 }) req["tags"] = tags;
        if (start is { } s) req["start"] = ToEpochMs(s);
        if (end is { } e) req["end"] = ToEpochMs(e);
        if (groupBy is { } g)
        {
            var names = g.ToList();
            if (names.Count > 0) req["group_by"] = names;
        }

        // `agg` and `interval` are independent, and this is the whole reason
        // this helper exists rather than each caller assembling the dictionary:
        // sending agg only alongside an interval silently downgrades "give me
        // the max" to the engine's default (mean). A plausible wrong number is
        // worse than an error.
        if (agg is { } a)
        {
            req["agg"] = a.Name;
            if (a.P is { } p) req["p"] = p;
        }
        if (interval is { } iv)
        {
            var ms = (long)iv.TotalMilliseconds;
            if (ms <= 0) throw new ArgumentOutOfRangeException(nameof(interval), iv, "interval must be positive");
            req["interval"] = ms;
        }

        var data = await Tcp(client).ExecRawAsync(req, ct);
        if (data.ValueKind != JsonValueKind.Array) return [];

        var series = new List<TsdbSeries>();
        foreach (var item in data.EnumerateArray())
        {
            var seriesTags = new Dictionary<string, string>();
            if (item.TryGetProperty("tags", out var tg) && tg.ValueKind == JsonValueKind.Object)
                foreach (var t in tg.EnumerateObject())
                    seriesTags[t.Name] = t.Value.GetString() ?? "";

            var samples = new List<TsdbSample>();
            if (item.TryGetProperty("points", out var ps) && ps.ValueKind == JsonValueKind.Array)
                foreach (var p in ps.EnumerateArray())
                {
                    if (!p.TryGetProperty("ts", out var ts) || ts.ValueKind != JsonValueKind.Number) continue;
                    p.TryGetProperty("value", out var v);
                    // A text series can still hand back a number (a count, a
                    // distinct tally), so key off the JSON, not the declared type.
                    samples.Add(new TsdbSample(
                        FromEpochMs(ts.GetInt64()),
                        v.ValueKind == JsonValueKind.Number ? v.GetDouble() : null,
                        v.ValueKind == JsonValueKind.String ? v.GetString() : null));
                }

            samples.Sort((x, y) => x.At.CompareTo(y.At));
            series.Add(new TsdbSeries(
                seriesTags,
                item.TryGetProperty("type", out var ty) ? ty.GetString() ?? "float" : "float",
                samples));
        }
        return series;
    }

    /// <summary>
    /// The samples of a single series, oldest first — the common case, when the
    /// tags pin one series down and you want a chart, not a shape.
    /// </summary>
    /// <returns>The first series' samples, or empty if nothing matched.</returns>
    public static async Task<IReadOnlyList<TsdbSample>> TsdbQuerySeriesAsync(
        this IOxiDbClient client,
        string measurement,
        string field,
        IReadOnlyDictionary<string, string>? tags = null,
        DateTime? start = null,
        DateTime? end = null,
        TimeSpan? interval = null,
        TsdbAgg? agg = null,
        CancellationToken ct = default)
    {
        var all = await client.TsdbQueryAsync(
            measurement, field, tags, start, end, interval, agg, null, ct);
        return all.Count > 0 ? all[0].Points : [];
    }

    /// <summary>
    /// The individual samples, unaggregated.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The engine has no raw mode: a query with no interval reduces the range to
    /// one bucket, and <c>agg</c> defaults to mean rather than to "leave it
    /// alone". So this asks for 1ms buckets, which is one bucket per sample for
    /// any series not sampled faster than a kilohertz.
    /// </para>
    /// <para>
    /// The caveat that implies: <b>samples sharing a millisecond collapse into
    /// one</b>, because the engine buckets on the timestamp and cannot tell them
    /// apart. If your series can have two samples in the same millisecond, this
    /// is lossy and you want an explicit interval and aggregation instead.
    /// </para>
    /// </remarks>
    public static Task<IReadOnlyList<TsdbSample>> TsdbReadRawAsync(
        this IOxiDbClient client,
        string measurement,
        string field,
        IReadOnlyDictionary<string, string>? tags = null,
        DateTime? start = null,
        DateTime? end = null,
        CancellationToken ct = default) =>
        client.TsdbQuerySeriesAsync(measurement, field, tags, start, end,
            interval: TimeSpan.FromMilliseconds(1), agg: TsdbAgg.Last, ct: ct);

    /// <summary>Series, point and byte counts.</summary>
    public static async Task<TsdbStats> TsdbStatsAsync(
        this IOxiDbClient client, CancellationToken ct = default)
    {
        var r = await Tcp(client).ExecRawAsync(Op("stats"), ct);
        long L(string n) => r.TryGetProperty(n, out var v) && v.ValueKind == JsonValueKind.Number
            ? v.GetInt64() : 0;
        return new TsdbStats(L("series"), L("points"), L("bytes"));
    }

    // ── Keeping it bounded ──────────────────────────────────────────────

    /// <summary>
    /// Drop everything older than <paramref name="cutoffUtc"/>. Retention drops
    /// whole sealed blocks, so this is cheap and does not rewrite anything.
    /// </summary>
    /// <returns>How many blocks went.</returns>
    public static async Task<int> TsdbEnforceRetentionAsync(
        this IOxiDbClient client, DateTime cutoffUtc, CancellationToken ct = default)
    {
        var req = Op("retention");
        req["cutoff"] = ToEpochMs(cutoffUtc);
        var r = await Tcp(client).ExecRawAsync(req, ct);
        return r.TryGetProperty("removed", out var v) ? v.GetInt32() : 0;
    }

    /// <summary>
    /// Force a checkpoint. The engine checkpoints itself past 8 MiB of WAL;
    /// this is for tests and for taking a known-good snapshot on demand.
    /// </summary>
    public static Task TsdbCheckpointAsync(
        this IOxiDbClient client, CancellationToken ct = default) =>
        Tcp(client).ExecRawAsync(Op("checkpoint"), ct);

    // ── Continuous aggregates ───────────────────────────────────────────

    /// <summary>
    /// Materialise completed buckets of every numeric series of a measurement
    /// into a derived one, <c>&lt;measurement&gt;@&lt;label&gt;</c>, with fields
    /// named <c>&lt;field&gt;_&lt;agg&gt;</c>.
    /// </summary>
    /// <remarks>
    /// This is how a series stays affordable forever: keep <c>temperature@1m</c>
    /// with min/max/mean and drop the raw stream on a retention rule. A max over
    /// a minute cannot hide a spike, so the rollup still answers the question.
    /// The rule persists; it survives restarts and does not double-count.
    /// </remarks>
    /// <param name="label">Suffix for the derived measurement. Defaults to the interval, e.g. <c>1m</c>.</param>
    /// <param name="aggs">Which aggregations to materialise per field.</param>
    public static Task TsdbAddRollupAsync(
        this IOxiDbClient client,
        string measurement,
        TimeSpan interval,
        IEnumerable<TsdbAgg> aggs,
        string? label = null,
        CancellationToken ct = default)
    {
        var ms = (long)interval.TotalMilliseconds;
        if (ms <= 0) throw new ArgumentOutOfRangeException(nameof(interval), interval, "interval must be positive");

        var names = aggs.Select(a => a.ToString()).ToList();
        if (names.Count == 0) throw new ArgumentException("a rollup needs at least one aggregation", nameof(aggs));

        var req = Op("rollup_add");
        req["measurement"] = measurement;
        req["interval"] = ms;
        req["aggs"] = names;
        if (label is not null) req["label"] = label;
        return Tcp(client).ExecRawAsync(req, ct);
    }

    /// <summary>
    /// Fold every bucket that has closed since the last refresh. Only complete
    /// buckets are folded, so calling it often is safe and cheap.
    /// </summary>
    /// <param name="now">Override "now" — for testing.</param>
    /// <returns>How many rollup points were written.</returns>
    public static async Task<int> TsdbRefreshRollupsAsync(
        this IOxiDbClient client, DateTime? now = null, CancellationToken ct = default)
    {
        var req = Op("rollup_refresh");
        if (now is { } n) req["now"] = ToEpochMs(n);
        var r = await Tcp(client).ExecRawAsync(req, ct);
        return r.TryGetProperty("written", out var v) ? v.GetInt32() : 0;
    }

    /// <summary>The registered rollup rules.</summary>
    public static async Task<IReadOnlyList<TsdbRollup>> TsdbRollupsAsync(
        this IOxiDbClient client, CancellationToken ct = default)
    {
        var r = await Tcp(client).ExecRawAsync(Op("rollups"), ct);
        if (r.ValueKind != JsonValueKind.Array) return [];
        return [.. r.EnumerateArray().Select(x => new TsdbRollup(
            x.TryGetProperty("measurement", out var m) ? m.GetString() ?? "" : "",
            x.TryGetProperty("label", out var l) ? l.GetString() ?? "" : "",
            TimeSpan.FromMilliseconds(
                x.TryGetProperty("interval", out var i) ? i.GetInt64() : 0)))];
    }
}
