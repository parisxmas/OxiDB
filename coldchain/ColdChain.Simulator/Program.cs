using System.Text.Json;
using ColdChain;
using MQTTnet;
using MQTTnet.Client;

// Sensors in trucks and cold rooms, publishing over MQTT. This is the one
// piece of the demo that is NOT a database client — it is the fleet.
//
// Two of the six devices are told to misbehave, because a cold-chain demo
// where nothing ever breaches proves nothing.

var devices = new (string Id, string Truck, double Target, bool Faulty)[]
{
    ("probe-01", "TR-34-ABC", 4.0, false),
    ("probe-02", "TR-34-ABC", 4.0, false),
    ("probe-03", "TR-06-XYZ", -18.0, false),
    ("probe-04", "TR-06-XYZ", -18.0, true),   // freezer door left ajar
    ("probe-05", "TR-35-DEF", 4.0, false),
    ("probe-06", "TR-35-DEF", 4.0, true),     // failing compressor
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
var seconds = int.TryParse(Environment.GetEnvironmentVariable("SIM_SECONDS"), out var s) ? s : 60;
var deadline = DateTime.UtcNow.AddSeconds(seconds);
var tick = 0;

while (DateTime.UtcNow < deadline)
{
    foreach (var d in devices)
    {
        // Normal drift, plus a slow ramp on the faulty ones so they breach
        // partway through the run rather than immediately.
        var drift = (rng.NextDouble() - 0.5) * 0.4;
        var fault = d.Faulty ? Math.Max(0, tick - 15) * 0.35 : 0;
        var celsius = Math.Round(d.Target + drift + fault, 2);

        var payload = JsonSerializer.Serialize(new
        {
            device = d.Id,
            truck = d.Truck,
            celsius,
            battery = Math.Round(100 - tick * 0.05, 1),
            ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        });
        // Topic carries the routing: sensors/<truck>/<device>/temperature
        await client.PublishStringAsync($"sensors/{d.Truck}/{d.Id}/temperature", payload);
    }
    tick++;
    if (tick % 10 == 0) Console.WriteLine($"  t={tick,3}s  published {tick * devices.Length} readings");
    await Task.Delay(1000);
}

await client.PublishStringAsync("fleet/gateway/status", "offline", retain: true);
Console.WriteLine($"done — {tick * devices.Length} readings from {devices.Length} devices");
