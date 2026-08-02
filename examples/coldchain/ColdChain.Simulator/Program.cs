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
// Every probe is its own unit in its own place, so none of them reads like any
// other. That is not decoration: three probes sitting at exactly 4.0°C would
// draw one line on the chart and imply a fleet that does not exist.
//
//   Target   where in its contracted band this unit actually sits — a probe by
//            the door runs warmer than one deep in the load, and both are fine
//   Amp/Per  refrigeration duty cycle: compressors cycle, so temperature waves
//   Phase    they were switched on at different times and never resynchronise
//   Fault    the failure this unit has, and how often it happens
var devices = new (string Id, string Truck, double Target, string Model,
                   double Amp, int Period, int Phase, double Noise,
                   Fault Fault, int FaultPeriod, double Battery)[]
{
    // TR-34-ABC — chilled pharma. Two healthy units, deliberately not twins.
    ("probe-01", "TR-34-ABC",   3.4, "v1",   0.5, 47,  0, 0.10, Fault.None,       0, 96),
    ("probe-02", "TR-34-ABC",   5.2, "v2",   0.9, 61, 19, 0.16, Fault.None,       0, 71),

    // TR-06-XYZ — frozen. One healthy, one with a door that does not seal.
    ("probe-03", "TR-06-XYZ", -18.6, "acme", 0.6, 71,  8, 0.12, Fault.None,       0, 88),
    ("probe-04", "TR-06-XYZ", -17.4, "v2",   0.5, 43, 31, 0.09, Fault.DoorAjar, 173, 54),

    // TR-35-DEF — chilled groceries. One healthy, one failing compressor.
    ("probe-05", "TR-35-DEF",   6.6, "acme", 0.7, 53, 12, 0.14, Fault.None,       0, 79),
    ("probe-06", "TR-35-DEF",   5.0, "v1",   1.1, 37, 25, 0.20, Fault.Compressor, 211, 63),
};

var factory = new MqttFactory();
using var client = factory.CreateMqttClient();
var options = new MqttClientOptionsBuilder()
    .WithTcpServer(Endpoints.Host, Endpoints.Mqtt)
    .WithClientId("coldchain-simulator")
    // If the fleet gateway dies, the broker announces it for us.
    .WithWillTopic("fleet/gateway/status")
    .WithWillPayload("offline")
    .WithWillRetain(true)
    .Build();

// The fleet keeps reporting across a broker restart; sensors in the real world
// do not give up because the far end blinked.
client.DisconnectedAsync += async _ =>
{
    while (true)
    {
        await Task.Delay(TimeSpan.FromSeconds(2));
        try { await client.ConnectAsync(options); Console.WriteLine("  reconnected"); return; }
        catch { /* keep trying */ }
    }
};
await client.ConnectAsync(options);

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
        // A compressor cycles: it cools until it overshoots, stops, drifts
        // back, starts again. So a healthy probe is a slow wave, not a flat
        // line. Each unit has its own amplitude, period and phase — they were
        // switched on at different times and nothing resynchronises them, so
        // the traces drift in and out with each other forever instead of
        // repeating.
        var duty = d.Amp * Math.Sin(2 * Math.PI * (tick + d.Phase) / d.Period);
        var noise = (rng.NextDouble() - 0.5) * 2 * d.Noise;

        // A faulty unit fails in EPISODES: it goes wrong, then someone shuts
        // the door or the compressor catches up. A monotonic ramp would read
        // +1255°C after an hour, and this demo is meant to run for months.
        var fault = 0.0;
        var doorOpen = false;
        if (d.Fault != Fault.None)
        {
            var phase = tick % d.FaultPeriod;
            switch (d.Fault)
            {
                // A door left ajar: warm air arrives at once, and the unit
                // claws it back slowly after the door is shut. Sharp up,
                // long tail down — and it only just breaches, which is the
                // kind of excursion that is easy to argue about later.
                case Fault.DoorAjar when phase < 60:
                    doorOpen = phase < 18;
                    fault = phase < 18
                        ? phase * 0.30                        // door open
                        : Math.Max(0, 5.4 - (phase - 18) * 0.13); // recovering
                    break;

                // A failing compressor: a slow climb while it loses the fight,
                // a plateau where it is simply not cooling, then a fast drop
                // when it finally catches. Rounder, longer, and much worse.
                case Fault.Compressor when phase < 95:
                    fault = phase switch
                    {
                        < 40 => phase * 0.42,
                        < 65 => 16.8,
                        _ => Math.Max(0, 16.8 - (phase - 65) * 0.62),
                    };
                    break;
            }
        }
        var celsius = Math.Round(d.Target + duty + noise + fault, 2);

        var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        // This drained without a floor: after a few hours the tiles were
        // reporting -103% battery, which is not a reading any device has ever
        // sent. A real pack drains slowly from wherever it happens to be and
        // gets swapped long before zero.
        var battery = Math.Round(Math.Max(18, d.Battery - tick * 0.0004), 1);

        // Each model serialises its OWN shape. Nothing normalises here — the
        // fleet has no incentive to agree, and the platform has to cope.
        object body = d.Model switch
        {
            "v1" => new { device = d.Id, truck = d.Truck, celsius, ts },

            "v2" => new
            {
                device = d.Id, truck = d.Truck, celsius, battery, ts,
                humidity = Math.Round(55 + rng.NextDouble() * 10, 1),
                door_open = doorOpen,                   // the door that caused it
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

/// What is wrong with a unit, if anything. Each fails in its own shape, because
/// "the door is ajar" and "the compressor is dying" do not look alike on a chart
/// — and telling them apart from the trace is the entire job of the person
/// reading it.
enum Fault { None, DoorAjar, Compressor }
