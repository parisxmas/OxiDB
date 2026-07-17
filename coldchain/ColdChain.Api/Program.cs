using System.Text;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using ColdChain;
using ColdChain.Api;
using Microsoft.EntityFrameworkCore;
using OxiDb.Client.Tcp;
using OxiDb.EntityFrameworkCore;
using StackExchange.Redis;

if (args.Contains("seed")) { await Seed.RunAsync(); return; }

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<ColdChainDb>(o => o.UseOxiDb(Endpoints.SqlConnectionString));
builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
    ConnectionMultiplexer.Connect(Endpoints.RedisConfiguration));
builder.Services.AddSingleton<IAmazonS3>(_ => new AmazonS3Client(
    new Amazon.Runtime.BasicAWSCredentials("demo", "demo"),
    new AmazonS3Config
    {
        ServiceURL = Endpoints.S3ServiceUrl,
        ForcePathStyle = true,          // OxiDB is path-style only
        AuthenticationRegion = "us-east-1",
    }));
builder.Services.AddSingleton<OxiConnection>();
builder.Services.AddHostedService<Retention>();
builder.Services.AddHostedService<Paperwork>();

const string CertBucket = Paperwork.Bucket;

var app = builder.Build();

// Seed on startup — there is no shell to run it from in a container, and it is
// idempotent, so a restart is harmless.
try { await Seed.RunAsync(); }
catch (Exception e) { app.Logger.LogWarning("seed skipped: {msg}", e.Message); }

app.MapGet("/api", () => Results.Ok(new
{
    demo = "ColdChain — one OxiDB process behind every engine",
    endpoints = new[]
    {
        "GET  /shipments            SQL (EF Core)  — shipments joined to customers + breach cost",
        "GET  /live                 OxiMem         — what every probe is doing right now",
        "GET  /history/{device}     time-series    — downsampled temperature history",
        "GET  /audit/{shipmentId}   all of them    — the compliance packet",
        "POST /certificate/{id}     S3             — file a signed certificate",
    },
}));

// ── SQL: this is relational, so it is asked relationally. ───────────────────
app.MapGet("/shipments", async (ColdChainDb db) =>
    await db.Shipments
        .Include(s => s.Customer)
        .Select(s => new
        {
            s.Id, s.Reference, s.DeviceId,
            Customer = s.Customer!.Name,
            Contract = $"{s.MinCelsius}..{s.MaxCelsius}°C",
            Breaches = s.Excursions.Count,
            // The join that makes it worth being relational: what the
            // breaches actually cost, per the customer's contract.
            Penalty = s.Excursions.Count * s.Customer!.PenaltyPerBreach,
        })
        .OrderByDescending(x => x.Penalty)
        .ToListAsync());

// ── OxiMem: "right now" is a cache question, not a database question. ───────
app.MapGet("/live", async (IConnectionMultiplexer redis) =>
{
    var db = redis.GetDatabase();
    var server = redis.GetServer(redis.GetEndPoints()[0]);
    var live = new List<JsonElement>();
    foreach (var key in server.Keys(pattern: "live:*"))
    {
        var v = await db.StringGetAsync(key);
        if (v.HasValue) live.Add(JsonDocument.Parse((string)v!).RootElement.Clone());
    }
    return Results.Ok(live);
});

// ── Time-series: a month of readings, answered as a chart. ──────────────────
app.MapGet("/history/{device}", async (string device, OxiConnection oxi, int? minutes, long? bucketMs) =>
{
    var end = DateTime.UtcNow;
    var start = end.AddMinutes(-(minutes ?? 60));
    var pts = await oxi.ReadAsync(c => c.TsdbQuerySeriesAsync("temperature", "celsius",
        new Dictionary<string, string> { ["device"] = device }, start, end,
        TimeSpan.FromMilliseconds(bucketMs ?? 10_000), TsdbAgg.Mean));
    return Results.Ok(pts.Where(p => p.Value is not null)
                         .Select(p => new { at = p.At, celsius = Math.Round(p.Value!.Value, 2) }));
});

// ── Document: the events as sent, whatever shape that was. ────────────────
//
// The engine the other three cannot replace. A fleet is three vendors and four
// firmwares, so no fixed schema fits: one probe reports only a temperature,
// another adds humidity, a door switch and a GPS fix, a third calls its fields
// sensor_id/temp_c and carries an alarm list. The time-series engine holds the
// number; only this holds what the device actually SAID — including the
// `door_open` that explains WHY the number moved.
app.MapGet("/events/{device}", async (string device, OxiConnection oxi, int? limit) =>
{
    var docs = await oxi.ReadAsync(c => c.FindAsync("readings", new { _device = device },
        sort: new Dictionary<string, int> { ["_at"] = -1 }, limit: limit ?? 6));
    return Results.Ok(docs);
});

// ── S3: the paperwork. Blobs belong in a blob store, not in a row. ─────────
app.MapPost("/certificate/{shipmentId:int}", async (
    int shipmentId, ColdChainDb db, IAmazonS3 s3, HttpRequest req) =>
{
    var shipment = await db.Shipments
        .Include(s => s.Customer).Include(s => s.Excursions)
        .FirstOrDefaultAsync(s => s.Id == shipmentId);
    if (shipment is null) return Results.NotFound();

    using var body = new MemoryStream();
    await req.Body.CopyToAsync(body);
    if (body.Length == 0)
        body.Write(Encoding.UTF8.GetBytes(Certificate.For(shipment)));
    body.Position = 0;
    var bytes = body.Length; // read it before the SDK disposes the stream

    const string bucket = CertBucket;
    try { await s3.PutBucketAsync(bucket); } catch (AmazonS3Exception) { /* exists */ }

    var key = $"{shipment.Reference}/certificate.txt";
    await s3.PutObjectAsync(new PutObjectRequest
    {
        BucketName = bucket, Key = key, InputStream = body, ContentType = "text/plain",
    });
    return Results.Ok(new { bucket, key, bytes });
});

// ── The compliance packet: every engine, one answer. ───────────────────────
//
// This is the question the whole system exists to answer, and the reason the
// engines are not interchangeable. "Prove shipment SHP-1004 stayed in range"
// needs the contract (SQL), the readings (time-series), the breaches and what
// they cost (SQL), the raw events as sent (document), and the signed paperwork
// (S3). One request, one process, five stores' worth of work.
app.MapGet("/audit/{shipmentId:int}", async (
    int shipmentId, ColdChainDb db, OxiConnection oxi, IAmazonS3 s3) =>
{
    var shipment = await db.Shipments
        .Include(s => s.Customer).Include(s => s.Excursions)
        .FirstOrDefaultAsync(s => s.Id == shipmentId);
    if (shipment is null) return Results.NotFound();

    var end = DateTime.UtcNow;
    var probe = new Dictionary<string, string> { ["device"] = shipment.DeviceId };
    var readings = await oxi.ReadAsync(c => c.TsdbQuerySeriesAsync("temperature", "celsius",
        probe, shipment.DepartedUtc, end, TimeSpan.FromMinutes(1), TsdbAgg.Mean));
    // No interval: the whole journey reduced to its single hottest moment.
    var peak = await oxi.ReadAsync(c => c.TsdbQuerySeriesAsync("temperature", "celsius",
        probe, shipment.DepartedUtc, end, agg: TsdbAgg.Max));

    // The raw events, verbatim — what the device actually sent.
    var raw = await oxi.ReadAsync(c => c.CountAsync("readings", new { device = shipment.DeviceId }));

    string? certificate = null;
    try
    {
        var o = await s3.GetObjectMetadataAsync("coldchain-certificates", $"{shipment.Reference}/certificate.txt");
        certificate = $"{o.ContentLength} bytes, {o.LastModified:O}";
    }
    catch (AmazonS3Exception) { /* not filed yet */ }

    return Results.Ok(new
    {
        shipment.Reference,
        Customer = shipment.Customer!.Name,
        Contract = new { shipment.MinCelsius, shipment.MaxCelsius },
        Verdict = shipment.Excursions.Count == 0 ? "IN RANGE — no breach recorded" : "BREACHED",
        Breaches = shipment.Excursions
            .Select(e => new { e.AtUtc, e.Celsius, e.LimitCelsius })
            .OrderBy(e => e.AtUtc),
        Cost = shipment.Excursions.Count * shipment.Customer!.PenaltyPerBreach,
        Evidence = new
        {
            RawEventsKept = raw,
            // A sample's value is nullable because a text series carries text
            // instead — the compiler now makes that impossible to ignore, which
            // on a compliance report is exactly right: no reading is not 0°C.
            PeakCelsius = peak.FirstOrDefault()?.Value is { } pk ? Math.Round(pk, 2) : (double?)null,
            Chart = readings.Where(p => p.Value is not null)
                            .Select(p => new { at = p.At, celsius = Math.Round(p.Value!.Value, 2) }),
            Certificate = certificate ?? "not filed",
        },
    });
});

// ── S3 + full-text search: the paperwork, and finding it again. ────────────
//
// The engine indexes an object's TEXT when it is PUT — nothing here asks it to.
// So the same binary that holds the readings can answer "which certificates
// mention Nordfresh", over documents that were only ever handed to an S3 client.
// That is the half of compliance that is not numbers.
app.MapGet("/documents", async (IAmazonS3 s3) =>
{
    try
    {
        var r = await s3.ListObjectsV2Async(new ListObjectsV2Request { BucketName = CertBucket });
        return Results.Ok(r.S3Objects
            .OrderBy(o => o.Key)
            .Select(o => new { key = o.Key, size = o.Size, at = o.LastModified, etag = o.ETag?.Trim('"') }));
    }
    catch (AmazonS3Exception) { return Results.Ok(Array.Empty<object>()); }
});

app.MapGet("/documents/search", async (string q, OxiConnection oxi) =>
{
    if (string.IsNullOrWhiteSpace(q)) return Results.Ok(Array.Empty<object>());

    // The engine's own index — TF-IDF, with snippets around the hit. Extracting
    // text is expensive, so highlight is opt-in and we opt in: a compliance
    // search that returns filenames is not an answer.
    var r = await oxi.ReadAsync(c => c.ExecRawAsync(new Dictionary<string, object?>
    {
        ["cmd"] = "search",
        ["query"] = q,
        ["bucket"] = CertBucket,
        ["limit"] = 10,
        ["highlight"] = true,
    }));

    if (r.ValueKind != JsonValueKind.Array) return Results.Ok(Array.Empty<object>());
    return Results.Ok(r.EnumerateArray().Select(h => new
    {
        key = h.TryGetProperty("key", out var k) ? k.GetString() : null,
        score = h.TryGetProperty("score", out var sc) ? Math.Round(sc.GetDouble(), 3) : 0,
        // The engine calls them "highlights", and wraps the matched terms in
        // <mark>. Guessing "snippets" got a silently empty list — the search
        // worked and looked resultless.
        snippets = h.TryGetProperty("highlights", out var sn) && sn.ValueKind == JsonValueKind.Array
            ? sn.EnumerateArray().Select(x => x.GetString()).ToArray()
            : [],
    }));
});

app.MapGet("/documents/{*key}", async (string key, IAmazonS3 s3) =>
{
    try
    {
        using var o = await s3.GetObjectAsync(CertBucket, key);
        using var sr = new StreamReader(o.ResponseStream);
        return Results.Text(await sr.ReadToEndAsync());
    }
    catch (AmazonS3Exception) { return Results.NotFound(); }
});

// ── What the two processes cost. ───────────────────────────────────────────
//
// The whole claim of this demo is "one small binary instead of six systems", so
// the number belongs on the page rather than in the prose. The engine measures
// itself — `proc_status` is the same source its Prometheus endpoint uses — so we
// ask it instead of guessing from the outside.
app.MapGet("/resources", async (OxiConnection oxi) =>
{
    var engine = await oxi.ReadAsync(c => c.ExecRawAsync(
        new Dictionary<string, object?> { ["cmd"] = "proc_status" }));

    double D(string n) => engine.TryGetProperty(n, out var v) && v.ValueKind == JsonValueKind.Number
        ? v.GetDouble() : 0;
    int I(string n) => engine.TryGetProperty(n, out var v) && v.ValueKind == JsonValueKind.Number
        ? v.GetInt32() : 0;

    var api = SelfUsage.Sample();
    return Results.Ok(new
    {
        Oxidb = new
        {
            CpuPercent = D("cpu_percent"),
            MemoryMb = D("mem_rss_mb"),
            Threads = I("threads"),
            UptimeSeconds = I("uptime_s"),
        },
        Api = api,
    });
});

// ── Live feed: OxiMem pub/sub → the browser. ───────────────────────────────
//
// Ingest PUBLISHes every reading to OxiMem; this relays that channel to the
// page as Server-Sent Events. Nothing polls: the dashboard updates because the
// sensor published, one hop away.
app.MapGet("/stream", async (HttpContext ctx, IConnectionMultiplexer redis, ILoggerFactory lf, CancellationToken ct) =>
{
    var log = lf.CreateLogger("stream");
    ctx.Response.Headers.ContentType = "text/event-stream";
    ctx.Response.Headers.CacheControl = "no-cache";
    // Tell nginx not to buffer even if the vhost forgets to.
    ctx.Response.Headers["X-Accel-Buffering"] = "no";

    // Send the headers NOW. Without this the response does not begin until the
    // first WriteAsync, and the first write waits on the first reading — so the
    // browser sees no 200 and fires no `onopen` until a probe happens to tick.
    // That is where the ~2.6s "connect" time came from: not latency, just the
    // gap to the next reading. The connection was ready the whole time.
    await ctx.Response.Body.FlushAsync(ct);

    // Then paint what is already true. A dashboard that opens into an empty
    // grid and fills in over the next two seconds looks like it is loading;
    // OxiMem already knows every probe's last reading, so say so at once.
    // This is the same state /live serves, pushed instead of polled.
    try
    {
        var kv = redis.GetDatabase();
        foreach (var key in redis.GetServer(redis.GetEndPoints()[0]).Keys(pattern: "live:*"))
        {
            var v = await kv.StringGetAsync(key);
            if (v.HasValue) await ctx.Response.WriteAsync($"data: {v}\n\n", ct);
        }
        await ctx.Response.Body.FlushAsync(ct);
    }
    catch (Exception e) { log.LogDebug("snapshot skipped: {m}", e.Message); }

    var sub = redis.GetSubscriber();
    ChannelMessageQueue? queue = null;
    try
    {
        queue = await sub.SubscribeAsync(RedisChannel.Literal("live.readings"));
        while (!ct.IsCancellationRequested)
        {
            string payload;
            try
            {
                payload = (await queue.ReadAsync(ct).AsTask().WaitAsync(TimeSpan.FromSeconds(20), ct)).Message.ToString();
            }
            catch (TimeoutException)
            {
                // An SSE comment: ignored by EventSource, but it is bytes on the
                // wire. Cloudflare and nginx both drop a stream that goes quiet,
                // and a fleet can legitimately go quiet, so never let it.
                payload = null!;
            }
            await ctx.Response.WriteAsync(payload is null ? ": ping\n\n" : $"data: {payload}\n\n", ct);
            await ctx.Response.Body.FlushAsync(ct);
        }
    }
    catch (OperationCanceledException) { /* client went away — normal */ }
    catch (Exception e)
    {
        // The 200 and the headers are already on the wire, so an exception
        // escaping this handler leaves Kestrel one option: RESET the stream.
        // The browser then reports ERR_HTTP2_PROTOCOL_ERROR against a request
        // it already saw succeed, and EventSource treats a reset as a hard
        // error rather than a stream that ended. Ending the body cleanly makes
        // it reconnect on its own instead.
        log.LogWarning("stream ended early: {t}: {m}", e.GetType().Name, e.Message);
    }
    finally
    {
        // OxiMem may be the very thing that just died; unsubscribing must not
        // throw on the way out and re-raise the problem it is cleaning up after.
        if (queue is not null)
            try { await queue.UnsubscribeAsync(); } catch (Exception e) { log.LogDebug("unsubscribe: {m}", e.Message); }
    }
});

app.UseDefaultFiles();
app.UseStaticFiles();

app.Run();
