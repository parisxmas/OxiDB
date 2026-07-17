using System.Text.Json;
using OxiDb.Client.Tcp;

namespace ColdChain;

/// A typed shim over the time-series engine.
///
/// The .NET packages cover the document and SQL engines but have no TSDB
/// helper yet, so this wraps `ExecRawAsync` — the client's escape hatch — in
/// something callers can read. It is deliberately small: this demo needs to
/// write points and read back downsampled series, nothing more.
public static class Tsdb
{
    public static long Ms(DateTime utc) =>
        new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc)).ToUnixTimeMilliseconds();

    /// Append one reading. `tags` are indexed dimensions (device, truck);
    /// `fields` are the measured values.
    public static Task WriteAsync(
        OxiDbTcpClient c, string measurement,
        Dictionary<string, string> tags, Dictionary<string, object> fields,
        DateTime atUtc) =>
        c.ExecRawAsync(new Dictionary<string, object?>
        {
            ["engine"] = "tsdb",
            ["cmd"] = "tsdb",
            ["op"] = "write",
            ["points"] = new[]
            {
                new Dictionary<string, object?>
                {
                    ["measurement"] = measurement,
                    ["tags"] = tags,
                    ["fields"] = fields,
                    ["ts"] = Ms(atUtc),
                }
            },
        });

    /// Read a series back, optionally downsampled into `interval` buckets.
    /// This is what makes the engine worth having: a month of 10-second
    /// readings answers as ~100 points without touching the raw data twice.
    public static async Task<List<(DateTime At, double Value)>> QueryAsync(
        OxiDbTcpClient c, string measurement, string field,
        Dictionary<string, string> tags, DateTime startUtc, DateTime endUtc,
        long? intervalMs = null, string agg = "mean")
    {
        var req = new Dictionary<string, object?>
        {
            ["engine"] = "tsdb",
            ["cmd"] = "tsdb",
            ["op"] = "query",
            ["measurement"] = measurement,
            ["field"] = field,
            ["tags"] = tags,
            ["start"] = Ms(startUtc),
            ["end"] = Ms(endUtc),
        };
        // `agg` and `interval` are INDEPENDENT. Sending agg only alongside an
        // interval silently downgraded "give me the max" to the engine's
        // default (mean) — a plausible-looking wrong number, which in a
        // compliance report is the worst kind.
        req["agg"] = agg;
        if (intervalMs is { } iv) req["interval"] = iv;

        // Verified against the engine: `data` is an ARRAY of series, each
        // { tags, type, points: [ {ts, value}, ... ] }.
        var data = await c.ExecRawAsync(req);
        var points = new List<(DateTime, double)>();
        if (data.ValueKind != JsonValueKind.Array) return points;

        foreach (var s in data.EnumerateArray())
        {
            if (!s.TryGetProperty("points", out var ps) || ps.ValueKind != JsonValueKind.Array) continue;
            foreach (var p in ps.EnumerateArray())
            {
                if (p.TryGetProperty("ts", out var t)
                    && p.TryGetProperty("value", out var v)
                    && v.ValueKind is JsonValueKind.Number)
                {
                    points.Add((DateTimeOffset.FromUnixTimeMilliseconds(t.GetInt64()).UtcDateTime,
                                v.GetDouble()));
                }
            }
        }
        points.Sort((a, b) => a.Item1.CompareTo(b.Item1));
        return points;
    }

    public static Task<JsonElement> StatsAsync(OxiDbTcpClient c) =>
        c.ExecRawAsync(new Dictionary<string, object?>
        { ["engine"] = "tsdb", ["cmd"] = "tsdb", ["op"] = "stats" });
}
