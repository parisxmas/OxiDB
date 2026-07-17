using OxiDb.Client.Tcp;

// Exercises the typed time-series surface in OxiDb.Client.Tcp against a real
// server. Needs one running with OXIDB_TSDB=1:
//
//   OXIDB_TSDB=1 OXIDB_ADDR=127.0.0.1:14555 OXIDB_DATA=$(mktemp -d) oxidb-server &
//   dotnet run --project tests/tsdb-dotnet 14555

Console.WriteLine("=== OxiDB TSDB .NET Client Test ===\n");

var port = args.Length > 0 ? int.Parse(args[0]) : 14555;
int passed = 0, failed = 0;

void Assert(bool cond, string name, string detail = "")
{
    if (cond) { passed++; Console.WriteLine($"  PASS: {name}"); }
    else { failed++; Console.WriteLine($"  FAIL: {name} {detail}"); }
}

await using var db = await OxiDbTcpClient.ConnectAsync("127.0.0.1", port);

// A distinct measurement per run: the engine is persistent and these assert on
// exact counts.
var m = $"probe_{Guid.NewGuid():N}"[..20];
var t0 = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
var tags = new Dictionary<string, string> { ["device"] = "p1" };

// ── 1. Write and read back ─────────────────────────────────────────────
var written = await db.TsdbWriteAsync(
    Enumerable.Range(0, 10).Select(i => new TsdbPoint(
        m, tags,
        new Dictionary<string, object?> { ["celsius"] = 10.0 + i },
        t0.AddSeconds(i))));
Assert(written == 10, "write returns the count stored", $"got {written}");

// There is no unaggregated read: with no interval the engine collapses the
// range into one bucket, and `agg` defaults to mean rather than to "leave it
// alone". TsdbReadRawAsync asks for 1ms buckets to get the samples back.
var collapsed = await db.TsdbQuerySeriesAsync(m, "celsius", tags, t0, t0.AddMinutes(1));
Assert(collapsed.Count == 1 && collapsed[0].Value == 14.5,
    "no interval and no agg collapses the range to its mean — NOT a raw read",
    $"got {collapsed.Count} point(s) = {collapsed.FirstOrDefault()?.Value}");

var raw = await db.TsdbReadRawAsync(m, "celsius", tags, t0, t0.AddMinutes(1));
Assert(raw.Count == 10, "TsdbReadRawAsync returns every sample", $"got {raw.Count}");
Assert(raw[0].Value == 10.0 && raw[9].Value == 19.0, "points come back oldest-first, values intact",
    $"got {raw[0].Value}..{raw[9].Value}");
Assert(raw[0].At == t0, "timestamps round-trip as UTC", $"got {raw[0].At:O}");

// ── 2. The bug this API exists to prevent ──────────────────────────────
// `agg` and `interval` are independent. The demo's hand-rolled shim sent agg
// only alongside an interval, so "give me the max" silently became the
// engine's default (mean) — a plausible wrong number, the worst kind.
var max = await db.TsdbQuerySeriesAsync(m, "celsius", tags, t0, t0.AddMinutes(1), agg: TsdbAgg.Max);
Assert(max.Count == 1 && max[0].Value == 19.0, "agg WITHOUT interval reduces the range to one value",
    $"got {max.Count} point(s), value {max.FirstOrDefault()?.Value}");

var mean = await db.TsdbQuerySeriesAsync(m, "celsius", tags, t0, t0.AddMinutes(1), agg: TsdbAgg.Mean);
Assert(mean[0].Value == 14.5, "mean over the same range differs from max — agg is really applied",
    $"got {mean[0].Value}");

var min = await db.TsdbQuerySeriesAsync(m, "celsius", tags, t0, t0.AddMinutes(1), agg: TsdbAgg.Min);
Assert(min[0].Value == 10.0, "min", $"got {min[0].Value}");

// ── 3. Downsampling ────────────────────────────────────────────────────
var buckets = await db.TsdbQuerySeriesAsync(m, "celsius", tags, t0, t0.AddMinutes(1),
    interval: TimeSpan.FromSeconds(5), agg: TsdbAgg.Max);
Assert(buckets.Count == 2, "10s of points into 5s buckets = 2 buckets", $"got {buckets.Count}");
Assert(buckets[0].Value == 14.0 && buckets[1].Value == 19.0, "each bucket carries its own max",
    $"got {buckets[0].Value}, {buckets[1].Value}");

// ── 4. Percentiles, both spellings ─────────────────────────────────────
var p90 = await db.TsdbQuerySeriesAsync(m, "celsius", tags, t0, t0.AddMinutes(1),
    agg: TsdbAgg.Percentile(90));
Assert(p90.Count == 1 && p90[0].Value is > 17.9 and < 19.1, "percentile(90)", $"got {p90.FirstOrDefault()?.Value}");
var p50 = await db.TsdbQuerySeriesAsync(m, "celsius", tags, t0, t0.AddMinutes(1),
    agg: TsdbAgg.Raw("p50"));
Assert(p50.Count == 1 && p50[0].Value is > 13.9 and < 15.6, "the pNN shorthand works too",
    $"got {p50.FirstOrDefault()?.Value}");
Assert(TsdbAgg.Percentile(95).ToString() == "p95", "Percentile renders as the shorthand the engine takes",
    $"got {TsdbAgg.Percentile(95)}");
try { TsdbAgg.Percentile(101); Assert(false, "percentile > 100 is rejected client-side"); }
catch (ArgumentOutOfRangeException) { Assert(true, "percentile > 100 is rejected client-side"); }

// ── 5. Typed fields ────────────────────────────────────────────────────
// The engine remembers a field's type and reports it back.
var mt = $"typed_{Guid.NewGuid():N}"[..20];
await db.TsdbWriteAsync(mt, tags, new Dictionary<string, object?>
{
    ["count"] = 42,          // integer
    ["ok"] = true,           // boolean
    ["ratio"] = 0.5,         // float
    ["state"] = "cooling",   // string
}, t0);

var ints = await db.TsdbQueryAsync(mt, "count", tags, t0, t0.AddMinutes(1));
Assert(ints.Count == 1 && ints[0].Type == "integer", "integer fields report as integer",
    $"got {ints.FirstOrDefault()?.Type}");
Assert(ints[0].Points[0].Value == 42, "integer value", $"got {ints[0].Points[0].Value}");

var bools = await db.TsdbQueryAsync(mt, "ok", tags, t0, t0.AddMinutes(1));
Assert(bools[0].Type == "boolean" && bools[0].Points[0].Value == 1, "boolean fields store as 0/1",
    $"got {bools[0].Type} = {bools[0].Points[0].Value}");

var text = await db.TsdbQueryAsync(mt, "state", tags, t0, t0.AddMinutes(1), agg: TsdbAgg.Last);
Assert(text[0].Type == "string", "string fields report as string", $"got {text[0].Type}");
Assert(text[0].Points[0].Text == "cooling" && text[0].Points[0].Value is null,
    "a text sample fills Text, not Value", $"got Text={text[0].Points[0].Text}, Value={text[0].Points[0].Value}");

// A text field aggregated to a count comes back as a NUMBER even though the
// series' declared type is string — which is why the sample keys off the JSON.
var tcount = await db.TsdbQueryAsync(mt, "state", tags, t0, t0.AddMinutes(1), agg: TsdbAgg.Count);
Assert(tcount[0].Points[0].Value == 1, "counting a text field yields a number, not text",
    $"got Value={tcount[0].Points[0].Value}, Text={tcount[0].Points[0].Text}");

// ── 6. Line protocol ───────────────────────────────────────────────────
var ml = $"lp_{Guid.NewGuid():N}"[..20];
var lpWritten = await db.TsdbWriteLineProtocolAsync(
    $"{ml},host=a load=0.5 {OxiDbTsdbExtensions.ToEpochMs(t0)}\n" +
    $"{ml},host=a load=1.5 {OxiDbTsdbExtensions.ToEpochMs(t0.AddSeconds(1))}");
Assert(lpWritten == 2, "line protocol writes both lines", $"got {lpWritten}");
var lp = await db.TsdbQuerySeriesAsync(ml, "load",
    new Dictionary<string, string> { ["host"] = "a" }, t0, t0.AddMinutes(1), agg: TsdbAgg.Sum);
Assert(lp[0].Value == 2.0, "line-protocol points read back", $"got {lp[0].Value}");

// ── 7. group_by splits instead of merging ──────────────────────────────
var mg = $"grp_{Guid.NewGuid():N}"[..20];
foreach (var (dev, val) in new[] { ("p1", 1.0), ("p2", 5.0) })
    await db.TsdbWriteAsync(mg, new Dictionary<string, string> { ["device"] = dev },
        new Dictionary<string, object?> { ["v"] = val }, t0);

var grouped = await db.TsdbQueryAsync(mg, "v", null, t0, t0.AddMinutes(1),
    agg: TsdbAgg.Max, groupBy: ["device"]);
Assert(grouped.Count == 2, "group_by returns one series per tag value", $"got {grouped.Count}");
Assert(grouped.All(s => s.Tags.ContainsKey("device")), "each grouped series carries its tags");
Assert(grouped.Sum(s => s.Points[0].Value) == 6.0, "grouped series keep their own values",
    $"got {string.Join(",", grouped.Select(s => s.Points[0].Value))}");

// ── 8. Stats ───────────────────────────────────────────────────────────
var stats = await db.TsdbStatsAsync();
Assert(stats.Series > 0 && stats.Points > 0, "stats reports series and points",
    $"got {stats.Series} series, {stats.Points} points");

// ── 9. Rollups ─────────────────────────────────────────────────────────
// The reason a series stays affordable forever: keep the 1m rollup, drop raw.
var mr = $"roll_{Guid.NewGuid():N}"[..20];
await db.TsdbAddRollupAsync(mr, TimeSpan.FromMinutes(1), [TsdbAgg.Mean, TsdbAgg.Max], label: "1m");
var rules = await db.TsdbRollupsAsync();
Assert(rules.Any(r => r.Measurement == mr && r.Label == "1m" && r.Interval == TimeSpan.FromMinutes(1)),
    "the rollup rule is registered and readable back");

await db.TsdbWriteAsync(
    Enumerable.Range(0, 6).Select(i => new TsdbPoint(
        mr, tags, new Dictionary<string, object?> { ["v"] = (double)i }, t0.AddSeconds(i * 10))));
// "now" is well past the bucket, so the minute is closed and foldable.
var folded = await db.TsdbRefreshRollupsAsync(t0.AddMinutes(5));
Assert(folded > 0, "refresh folds the closed bucket", $"wrote {folded}");

var rolled = await db.TsdbQuerySeriesAsync($"{mr}@1m", "v_max", tags, t0, t0.AddMinutes(5));
Assert(rolled.Count == 1 && rolled[0].Value == 5.0, "the rollup's max is the raw max — a spike cannot hide",
    $"got {rolled.Count} point(s), {rolled.FirstOrDefault()?.Value}");

// ── 10. Retention and checkpoint ───────────────────────────────────────
await db.TsdbCheckpointAsync();
Assert(true, "checkpoint on demand");
var removed = await db.TsdbEnforceRetentionAsync(new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc));
Assert(removed == 0, "retention with a cutoff before all data drops nothing", $"got {removed}");

// ── 11. Time handling ──────────────────────────────────────────────────
// OxiDb.Data already treats Unspecified as UTC; be consistent, or a caller
// gets silently shifted by their offset.
var unspec = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
Assert(OxiDbTsdbExtensions.ToEpochMs(unspec) == OxiDbTsdbExtensions.ToEpochMs(t0),
    "DateTimeKind.Unspecified is taken as UTC, not local");
Assert(OxiDbTsdbExtensions.ToEpochMs(t0.ToLocalTime()) == OxiDbTsdbExtensions.ToEpochMs(t0),
    "a local DateTime is converted, not reinterpreted");

// ── 12. Client-side guards ─────────────────────────────────────────────
try { await db.TsdbWriteAsync([]); Assert(false, "writing no points is an ArgumentException"); }
catch (ArgumentException) { Assert(true, "writing no points is an ArgumentException"); }
try
{
    await db.TsdbQuerySeriesAsync(m, "celsius", tags, t0, t0.AddMinutes(1), interval: TimeSpan.Zero);
    Assert(false, "a zero interval is rejected client-side");
}
catch (ArgumentOutOfRangeException) { Assert(true, "a zero interval is rejected client-side"); }

Console.WriteLine($"\n=== {passed} passed, {failed} failed ===");
return failed == 0 ? 0 : 1;
