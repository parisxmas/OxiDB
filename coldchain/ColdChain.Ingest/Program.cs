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
await using var oxi = await OxiDbTcpClient.ConnectAsync(Endpoints.Host, Endpoints.Tcp);
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
    var topic = e.ApplicationMessage.Topic;
    if (topic.StartsWith("fleet/gateway/"))
    {
        Console.WriteLine($"  gateway → {e.ApplicationMessage.ConvertPayloadToString()}");
        return;
    }

    var json = e.ApplicationMessage.ConvertPayloadToString();
    var r = JsonSerializer.Deserialize<Reading>(json);
    if (r is null) return;
    var at = DateTimeOffset.FromUnixTimeMilliseconds(r.ts).UtcDateTime;

    // 1. TIME-SERIES — the reading. Tags are the dimensions we slice by.
    await Tsdb.WriteAsync(oxi, "temperature",
        new() { ["device"] = r.device, ["truck"] = r.truck },
        new() { ["celsius"] = r.celsius, ["battery"] = r.battery },
        at);

    // 2. OXIMEM — current state, and a live feed for any dashboard. Expires on
    //    its own: a probe silent for 5 minutes should read as unknown, not as
    //    its last-known temperature. TTL is the honest default here.
    await live.StringSetAsync($"live:{r.device}", json, TimeSpan.FromMinutes(5));
    await live.PublishAsync(RedisChannel.Literal("live.readings"), json);

    // 3. DOCUMENT — the raw event, verbatim, schemaless. Different probe models
    //    send different extra fields; the auditor wants what was actually sent,
    //    not our interpretation of it.
    await oxi.InsertAsync("readings", new
    {
        device = r.device, truck = r.truck, celsius = r.celsius,
        battery = r.battery, at = at.ToString("O"), raw = json,
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

await mqtt.ConnectAsync(new MqttClientOptionsBuilder()
    .WithTcpServer(Endpoints.Host, Endpoints.Mqtt)
    .WithClientId("coldchain-ingest")
    .Build());
// '+' is one level, '#' is the rest — every probe on every truck.
await mqtt.SubscribeAsync("sensors/+/+/temperature");
await mqtt.SubscribeAsync("fleet/gateway/#");
Console.WriteLine($"ingest ← mqtt://{Endpoints.Host}:{Endpoints.Mqtt}  (sensors/+/+/temperature)");

var seconds = int.TryParse(Environment.GetEnvironmentVariable("INGEST_SECONDS"), out var s) ? s : 75;
await Task.Delay(TimeSpan.FromSeconds(seconds));
Console.WriteLine($"ingest: {readings} readings, {breaches} excursions recorded");

record Reading(string device, string truck, double celsius, double battery, long ts);
