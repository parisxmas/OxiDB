using System.Text.Json;
using ColdChain;
using Microsoft.EntityFrameworkCore;
using MQTTnet;
using MQTTnet.Client;
using OxiDb.Client.Tcp;
using OxiDb.EntityFrameworkCore;
using StackExchange.Redis;

// The seam where every engine meets. One reading arrives over MQTT and fans
// out to the store that actually suits it:
//
//   time-series  ← the reading itself (millions of them, compressed, rolled up)
//   OxiMem       ← "what is this probe doing right now" + a live pub/sub feed
//   document     ← the raw event, kept verbatim for the auditor
//   SQL          ← an excursion, because a breach has money attached
//
// Six stores' worth of jobs. One server process.

var mqttFactory = new MqttFactory();
using var mqtt = mqttFactory.CreateMqttClient();
// The engine restarts (a deploy, a checkpoint, a crash) and this socket dies
// with it, so dial it on demand rather than once at startup.
await using var oxi = new OxiConnection();
var redis = await ConnectionMultiplexer.ConnectAsync(Endpoints.RedisConfiguration);
var live = redis.GetDatabase();

var sql = new DbContextOptionsBuilder<ColdChainDb>()
    .UseOxiDb(Endpoints.SqlConnectionString).Options;

// Contracted limits, read once from the relational side and cached.
Dictionary<string, (int ShipmentId, double Min, double Max)> limits;
await using (var db = new ColdChainDb(sql))
{
    limits = await db.Shipments
        .Where(s => s.DeliveredUtc == null)
        .ToDictionaryAsync(s => s.DeviceId, s => (s.Id, s.MinCelsius, s.MaxCelsius));
}
Console.WriteLine($"ingest: watching {limits.Count} in-flight devices");

var readings = 0;
var breaches = 0;

mqtt.ApplicationMessageReceivedAsync += async e =>
{
    try { await Handle(e); }
    catch (Exception ex)
    {
        // Say so, and drop the connection so the next reading rebuilds it.
        Console.WriteLine($"  ingest error: {ex.GetType().Name}: {ex.Message}");
        oxi.Drop();
    }
};

async Task Handle(MqttApplicationMessageReceivedEventArgs e)
{
    var topic = e.ApplicationMessage.Topic;
    if (topic.StartsWith("fleet/gateway/"))
    {
        Console.WriteLine($"  gateway → {e.ApplicationMessage.ConvertPayloadToString()}");
        return;
    }

    var json = e.ApplicationMessage.ConvertPayloadToString();
    // Three vendors, three dialects. Normalise what the numeric engines need —
    // and keep the original, because we cannot know today which of tomorrow's
    // fields will matter, and an auditor asks what the device SAID, not what we
    // decided to keep.
    var r = Normalise(json);
    if (r is null) { Console.WriteLine($"  unparsable payload on {topic}"); return; }
    var at = DateTimeOffset.FromUnixTimeMilliseconds(r.ts).UtcDateTime;

    // 1. TIME-SERIES — the reading. Tags are the dimensions we slice by.
    // The v1 probe reports no battery. Absent is not zero — writing a 0 would
    // invent a dead battery that nobody measured.
    var fields = new Dictionary<string, object> { ["celsius"] = r.celsius };
    if (!double.IsNaN(r.battery)) fields["battery"] = r.battery;
    await (await oxi.GetAsync()).TsdbWriteAsync("temperature",
        new Dictionary<string, string> { ["device"] = r.device, ["truck"] = r.truck }, fields, at);

    // 2. OXIMEM — current state, and a live feed for any dashboard. Expires on
    //    its own: a probe silent for 5 minutes should read as unknown, not as
    //    its last-known temperature. TTL is the honest default here.
    // Live state is the NORMALISED view — a dashboard should not have to learn
    // three vendors' field names. The original is not lost; it is in the
    // document engine, which is the half that promises to keep it.
    var norm = JsonSerializer.Serialize(new
    {
        device = r.device, truck = r.truck, celsius = r.celsius,
        battery = double.IsNaN(r.battery) ? (double?)null : r.battery,
        ts = r.ts,
    });
    await live.StringSetAsync($"live:{r.device}", norm, TimeSpan.FromMinutes(5));
    await live.PublishAsync(RedisChannel.Literal("live.readings"), norm);

    // 3. DOCUMENT — the event exactly as it arrived. This is the engine that
    //    earns its place here: no fixed schema fits all three dialects, and the
    //    extra fields are not noise — `door_open` names the CAUSE of the breach
    //    the time-series can only show as a number going up. Flattening it into
    //    columns we chose today throws that away forever.
    await (await oxi.GetAsync()).ExecRawAsync(new Dictionary<string, object?>
    {
        ["cmd"] = "insert",
        ["collection"] = "readings",
        // Merge, don't wrap: the payload's own fields stay top-level and
        // queryable, and we add only what routing needs.
        ["doc"] = MergeEnvelope(json, r.device, at),
    });

    readings++;

    // 4. SQL — a breach. This is relational: it joins to a shipment, which
    //    joins to a customer, which has a contracted penalty.
    if (limits.TryGetValue(r.device, out var lim)
        && (r.celsius < lim.Min || r.celsius > lim.Max))
    {
        // Don't record the same breach every second — one row per device per
        // minute is enough to prove it, and OxiMem is the natural place to
        // hold that "already reported" flag.
        if (await live.StringSetAsync($"breach:{r.device}", "1", TimeSpan.FromMinutes(1), When.NotExists))
        {
            await using var db = new ColdChainDb(sql);
            db.Excursions.Add(new Excursion
            {
                ShipmentId = lim.ShipmentId,
                AtUtc = at,
                Celsius = r.celsius,
                LimitCelsius = r.celsius > lim.Max ? lim.Max : lim.Min,
            });
            await db.SaveChangesAsync();
            breaches++;
            Console.WriteLine($"  BREACH {r.device} {r.celsius}°C (limit {lim.Min}..{lim.Max})");
        }
    }
};

// A broker restart must not end the service. Without this the client stays
// "up" forever with a dead socket, silently consuming nothing — which is
// exactly what happened the first time the engine container was restarted:
// the simulator came back, the ingest didn't, and the dashboard went empty
// while every container still reported healthy.
var mqttOptions = new MqttClientOptionsBuilder()
    .WithTcpServer(Endpoints.Host, Endpoints.Mqtt)
    .WithClientId("coldchain-ingest")
    .WithCleanSession(false)
    .Build();

mqtt.DisconnectedAsync += async e =>
{
    Console.WriteLine($"  mqtt disconnected ({e.Reason}) — reconnecting");
    while (true)
    {
        await Task.Delay(TimeSpan.FromSeconds(2));
        try
        {
            await mqtt.ConnectAsync(mqttOptions);
            // Subscriptions do not survive the broker; re-establish them or we
            // reconnect into silence, which looks identical to being down.
            await mqtt.SubscribeAsync("sensors/+/+/temperature");
            await mqtt.SubscribeAsync("fleet/gateway/#");
            Console.WriteLine("  mqtt reconnected");
            return;
        }
        catch (Exception ex) { Console.WriteLine($"  mqtt reconnect failed: {ex.Message}"); }
    }
};

await mqtt.ConnectAsync(mqttOptions);
// '+' is one level, '#' is the rest — every probe on every truck.
await mqtt.SubscribeAsync("sensors/+/+/temperature");
await mqtt.SubscribeAsync("fleet/gateway/#");
Console.WriteLine($"ingest ← mqtt://{Endpoints.Host}:{Endpoints.Mqtt}  (sensors/+/+/temperature)");

// A service runs until it is stopped. `Task.Delay` cannot express "a year"
// anyway — it caps at ~24.8 days — so waiting on the shutdown signal is both
// correct and simpler. INGEST_SECONDS stays for the scripted local demo.
var seconds = int.TryParse(Environment.GetEnvironmentVariable("INGEST_SECONDS"), out var s) ? s : 0;
using var stopping = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; stopping.Cancel(); };
AppDomain.CurrentDomain.ProcessExit += (_, _) => stopping.Cancel();
try
{
    if (seconds > 0) await Task.Delay(TimeSpan.FromSeconds(seconds), stopping.Token);
    else await Task.Delay(Timeout.InfiniteTimeSpan, stopping.Token);
}
catch (OperationCanceledException) { /* SIGTERM / Ctrl-C */ }
Console.WriteLine($"ingest: {readings} readings, {breaches} excursions recorded");

record Reading(string device, string truck, double celsius, double battery, long ts);

partial class Program
{
    /// Map any vendor's dialect onto the one shape the time-series and SQL
    /// sides need. Only this function knows the dialects exist.
    static Reading? Normalise(string json)
    {
        try
        {
            var e = JsonDocument.Parse(json).RootElement;

            // v1 / v2 speak `device`/`truck`/`celsius`; acme speaks
            // `sensor_id`/`vehicle`/`temp_c`. Same reading, different words.
            var device = Str(e, "device") ?? Str(e, "sensor_id");
            var truck = Str(e, "truck") ?? Str(e, "vehicle");
            var celsius = Num(e, "celsius") ?? Num(e, "temp_c");
            var ts = e.TryGetProperty("ts", out var t) && t.TryGetInt64(out var ms) ? ms : 0;
            if (device is null || truck is null || celsius is null || ts == 0) return null;

            // v1 doesn't report battery at all — absent is not zero.
            var battery = Num(e, "battery") ?? Num(e, "batt_pct") ?? double.NaN;
            return new Reading(device, truck, celsius.Value, battery, ts);
        }
        catch (JsonException) { return null; }

        static string? Str(JsonElement e, string p) =>
            e.TryGetProperty(p, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;
        static double? Num(JsonElement e, string p) =>
            e.TryGetProperty(p, out var v) && v.ValueKind == JsonValueKind.Number ? v.GetDouble() : null;
    }

    /// Keep every field the device sent, top-level, and add the two the
    /// platform needs to find it again.
    static Dictionary<string, object?> MergeEnvelope(string json, string device, DateTime at)
    {
        var doc = new Dictionary<string, object?>();
        foreach (var p in JsonDocument.Parse(json).RootElement.EnumerateObject())
            doc[p.Name] = p.Value.Clone();
        doc["_device"] = device;           // normalised, whatever the dialect called it
        doc["_at"] = at.ToString("O");
        return doc;
    }
}
