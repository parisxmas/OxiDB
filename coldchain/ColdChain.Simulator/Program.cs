using System.Text.Json;
using ColdChain;
using MQTTnet;
using MQTTnet.Client;

// Sensors in trucks and cold rooms, publishing over MQTT. This is the one
// piece of the demo that is NOT a database client — it is the fleet.
//
// Two of the six devices are told to misbehave, because a cold-chain demo
// where nothing ever breaches proves nothing.

// Three vendors, three dialects. This is not a contrivance — a real fleet is
// whatever was cheapest each year, and their firmwares do not agree on field
// names, let alone on which extra fields exist. It is the reason the raw event
// is kept verbatim instead of being flattened into a schema we chose today.
//
//   v1        — an old probe: temperature and nothing else
//   v2        — newer: humidity, a door switch, a nested GPS fix
//   acme      — a different vendor entirely: its own field NAMES, plus alarms
var devices = new (string Id, string Truck, double Target, bool Faulty, string Model)[]
{
    ("probe-01", "TR-34-ABC",   4.0, false, "v1"),
    ("probe-02", "TR-34-ABC",   4.0, false, "v2"),
    ("probe-03", "TR-06-XYZ", -18.0, false, "acme"),
    ("probe-04", "TR-06-XYZ", -18.0, true,  "v2"),    // freezer door left ajar
    ("probe-05", "TR-35-DEF",   4.0, false, "acme"),
    ("probe-06", "TR-35-DEF",   4.0, true,  "v1"),    // failing compressor
};

var factory = new MqttFactory();
using var client = factory.CreateMqttClient();
await client.ConnectAsync(new MqttClientOptionsBuilder()
    .WithTcpServer(Endpoints.Host, Endpoints.Mqtt)
    .WithClientId("coldchain-simulator")
    // If the fleet gateway dies, the broker announces it for us.
    .WithWillTopic("fleet/gateway/status")
    .WithWillPayload("offline")
    .WithWillRetain(true)
    .Build());

Console.WriteLine($"simulator → mqtt://{Endpoints.Host}:{Endpoints.Mqtt}");
await client.PublishStringAsync("fleet/gateway/status", "online", retain: true);

var rng = new Random(42);
// 0 (the container default) means: keep publishing until stopped.
var seconds = int.TryParse(Environment.GetEnvironmentVariable("SIM_SECONDS"), out var s) ? s : 60;
var deadline = seconds > 0 ? DateTime.UtcNow.AddSeconds(seconds) : DateTime.MaxValue;
var tick = 0;

while (DateTime.UtcNow < deadline)
{
    foreach (var d in devices)
    {
        // Normal drift, plus a slow ramp on the faulty ones so they breach
        // partway through the run rather than immediately.
        // A faulty unit fails in EPISODES: it drifts out of range for a couple
        // of minutes, then someone shuts the door / the compressor catches up.
        // A monotonic ramp would read +1255°C after an hour, and this demo is
        // meant to run for months.
        var drift = (rng.NextDouble() - 0.5) * 0.4;
        var fault = 0.0;
        if (d.Faulty)
        {
            var phase = tick % 150;                 // ~5 min at 2s/tick
            if (phase is > 30 and < 75) fault = (phase - 30) * 0.35;   // going wrong
            else if (phase is >= 75 and < 105) fault = (105 - phase) * 0.5; // recovering
        }
        var celsius = Math.Round(d.Target + drift + fault, 2);

        var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var battery = Math.Round(100 - tick * 0.05, 1);

        // Each model serialises its OWN shape. Nothing normalises here — the
        // fleet has no incentive to agree, and the platform has to cope.
        object body = d.Model switch
        {
            "v1" => new { device = d.Id, truck = d.Truck, celsius, ts },

            "v2" => new
            {
                device = d.Id, truck = d.Truck, celsius, battery, ts,
                humidity = Math.Round(55 + rng.NextDouble() * 10, 1),
                door_open = fault > 0,                  // the door that caused it
                gps = new { lat = Math.Round(41.0 + rng.NextDouble() * 0.1, 5),
                            lon = Math.Round(29.0 + rng.NextDouble() * 0.1, 5) },
            },

            // Different vendor, different names: sensor_id/vehicle/temp_c.
            _ => new
            {
                sensor_id = d.Id, vehicle = d.Truck, temp_c = celsius, ts,
                batt_pct = battery,
                alarms = celsius > 8 || celsius < -20 ? new[] { "TEMP_HIGH" } : Array.Empty<string>(),
            },
        };
        var payload = JsonSerializer.Serialize(body);

        await client.PublishStringAsync($"sensors/{d.Truck}/{d.Id}/temperature", payload);
    }
    tick++;
    if (tick % 10 == 0) Console.WriteLine($"  t={tick,3}s  published {tick * devices.Length} readings");
    await Task.Delay(2000);
}

await client.PublishStringAsync("fleet/gateway/status", "offline", retain: true);
Console.WriteLine($"done — {tick * devices.Length} readings from {devices.Length} devices");
