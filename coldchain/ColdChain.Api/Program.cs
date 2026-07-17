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
builder.Services.AddSingleton(_ => OxiDbTcpClient.ConnectAsync(Endpoints.Host, Endpoints.Tcp).Result);

var app = builder.Build();

app.MapGet("/", () => Results.Ok(new
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
app.MapGet("/history/{device}", async (string device, OxiDbTcpClient oxi, int? minutes, long? bucketMs) =>
{
    var end = DateTime.UtcNow;
    var start = end.AddMinutes(-(minutes ?? 60));
    var pts = await Tsdb.QueryAsync(oxi, "temperature", "celsius",
        new() { ["device"] = device }, start, end, bucketMs ?? 10_000, "mean");
    return Results.Ok(pts.Select(p => new { at = p.At, celsius = Math.Round(p.Value, 2) }));
});

// ── S3: the paperwork. Blobs belong in a blob store, not in a row. ─────────
app.MapPost("/certificate/{shipmentId:int}", async (
    int shipmentId, ColdChainDb db, IAmazonS3 s3, HttpRequest req) =>
{
    var shipment = await db.Shipments.FindAsync(shipmentId);
    if (shipment is null) return Results.NotFound();

    using var body = new MemoryStream();
    await req.Body.CopyToAsync(body);
    if (body.Length == 0)
        body.Write(Encoding.UTF8.GetBytes($"CERTIFICATE OF CONFORMITY\nShipment {shipment.Reference}\nIssued {DateTime.UtcNow:O}\n"));
    body.Position = 0;
    var bytes = body.Length; // read it before the SDK disposes the stream

    const string bucket = "coldchain-certificates";
    try { await s3.PutBucketAsync(bucket); } catch (AmazonS3Exception) { /* exists */ }

    var key = $"{shipment.Reference}/certificate.txt";
    await s3.PutObjectAsync(new PutObjectRequest
    {
        BucketName = bucket, Key = key, InputStream = body, ContentType = "text/plain",
        // The AWS .NET SDK verifies the returned ETag as an MD5 of what it
        // sent. OxiDB's ETag is deliberately not an MD5 — it is the first 16
        // bytes of the payload's SHA-256 — so the check fails even though the
        // object stored fine. aws-cli and boto3 don't do this; the .NET SDK
        // does, and this is the knob for it.
        DisableMD5Stream = true,
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
    int shipmentId, ColdChainDb db, OxiDbTcpClient oxi, IAmazonS3 s3) =>
{
    var shipment = await db.Shipments
        .Include(s => s.Customer).Include(s => s.Excursions)
        .FirstOrDefaultAsync(s => s.Id == shipmentId);
    if (shipment is null) return Results.NotFound();

    var end = DateTime.UtcNow;
    var readings = await Tsdb.QueryAsync(oxi, "temperature", "celsius",
        new() { ["device"] = shipment.DeviceId }, shipment.DepartedUtc, end, 60_000, "mean");
    var peak = await Tsdb.QueryAsync(oxi, "temperature", "celsius",
        new() { ["device"] = shipment.DeviceId }, shipment.DepartedUtc, end, null, "max");

    // The raw events, verbatim — what the device actually sent.
    var raw = await oxi.CountAsync("readings", new { device = shipment.DeviceId });

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
            PeakCelsius = peak.Count == 0 ? (double?)null : Math.Round(peak.Max(p => p.Value), 2),
            Chart = readings.Select(p => new { at = p.At, celsius = Math.Round(p.Value, 2) }),
            Certificate = certificate ?? "not filed",
        },
    });
});

app.Run();
