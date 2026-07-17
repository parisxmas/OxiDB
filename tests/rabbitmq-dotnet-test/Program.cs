// The official RabbitMQ .NET client (RabbitMQ.Client 7.x), unmodified,
// against OxiDB's AMQP listener — ADR-0016's claim tested from the third
// mainstream client (pika covers Python in oxidb-server/tests/amqp_e2e.rs).
//
// Self-contained: spawns its own oxidb-server (target/debug, or
// OXIDB_SERVER_BIN), runs every scenario, exits nonzero on any failure.
// The durability scenario kills the server hard (SIGKILL, no graceful
// shutdown) and restarts it — the publisher confirm is the only promise
// being tested.

using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

int passed = 0, failed = 0;

await Run("hello world roundtrip with publisher confirms", HelloWorld);
await Run("competing consumers split the work exactly once", CompetingConsumers);
await Run("prefetch caps a slow consumer, the rest flows on", Prefetch);
await Run("topic exchange routes on wildcards", TopicExchange);
await Run("fanout copies to every bound queue", Fanout);
await Run("mandatory unroutable publish comes back as Basic.Return", MandatoryReturn);
await Run("nack with requeue redelivers, flagged", NackRequeue);
await Run("durable persistent messages survive a hard kill", DurableSurvivesKill);

Console.WriteLine($"\n{passed} passed, {failed} failed");
return failed == 0 ? 0 : 1;

async Task Run(string name, Func<Task> test)
{
    try
    {
        await test();
        passed++;
        Console.WriteLine($"PASS  {name}");
    }
    catch (Exception e)
    {
        failed++;
        Console.WriteLine($"FAIL  {name}\n      {e.GetType().Name}: {e.Message}");
    }
}

static void Check(bool cond, string what)
{
    if (!cond) throw new Exception(what);
}

// ── Scenarios ───────────────────────────────────────────────────────────

async Task HelloWorld()
{
    using var srv = Server.Start();
    await using var conn = await srv.ConnectAsync();
    await using var ch = await conn.CreateChannelAsync(ConfirmOptions());

    await ch.QueueDeclareAsync("hello", durable: false, exclusive: false, autoDelete: false);
    await ch.BasicPublishAsync("", "hello", Encoding.UTF8.GetBytes("Hello OxiDB!"));

    var got = await ch.BasicGetAsync("hello", autoAck: true);
    Check(got is not null, "queue was empty after a confirmed publish");
    Check(Encoding.UTF8.GetString(got!.Body.ToArray()) == "Hello OxiDB!", "body mismatch");
    Check(await ch.BasicGetAsync("hello", autoAck: true) is null, "queue must be drained");
}

async Task CompetingConsumers()
{
    using var srv = Server.Start();
    await using var c1 = await srv.ConnectAsync();
    await using var c2 = await srv.ConnectAsync();
    await using var ch1 = await c1.CreateChannelAsync();
    await using var ch2 = await c2.CreateChannelAsync();

    await ch1.QueueDeclareAsync("work", durable: false, exclusive: false, autoDelete: false);
    var got1 = new List<string>();
    var got2 = new List<string>();
    await ch1.BasicConsumeAsync("work", autoAck: false, AckingConsumer(ch1, got1));
    await ch2.BasicConsumeAsync("work", autoAck: false, AckingConsumer(ch2, got2));

    await using var pub = await srv.ConnectAsync();
    await using var chp = await pub.CreateChannelAsync(ConfirmOptions());
    for (var i = 0; i < 10; i++)
        await chp.BasicPublishAsync("", "work", Encoding.UTF8.GetBytes($"{i}"));

    await Until(() => got1.Count + got2.Count >= 10, "10 deliveries");
    var all = got1.Concat(got2).ToList();
    Check(all.Count == 10, $"expected 10 total, got {all.Count}");
    Check(all.Distinct().Count() == 10, "a message was delivered twice");
    Check(got1.Count == 5 && got2.Count == 5,
        $"round-robin must split evenly, got {got1.Count}/{got2.Count}");
}

async Task Prefetch()
{
    using var srv = Server.Start();
    await using var c1 = await srv.ConnectAsync();
    await using var c2 = await srv.ConnectAsync();
    await using var ch1 = await c1.CreateChannelAsync();
    await using var ch2 = await c2.CreateChannelAsync();

    await ch1.QueueDeclareAsync("work", durable: false, exclusive: false, autoDelete: false);
    // ch1 never acks and has prefetch 1: it must hold exactly one delivery
    // while its skipped turns pass to ch2 — the work-queue pattern Basic.Qos
    // exists for.
    await ch1.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);
    var stuck = new List<string>();
    var flowed = new List<string>();
    var hoard = new AsyncEventingBasicConsumer(ch1);
    hoard.ReceivedAsync += (_, ea) =>
    {
        lock (stuck) stuck.Add(Encoding.UTF8.GetString(ea.Body.ToArray()));
        return Task.CompletedTask; // no ack, ever
    };
    await ch1.BasicConsumeAsync("work", autoAck: false, hoard);
    await ch2.BasicConsumeAsync("work", autoAck: false, AckingConsumer(ch2, flowed));

    await using var pub = await srv.ConnectAsync();
    await using var chp = await pub.CreateChannelAsync(ConfirmOptions());
    for (var i = 0; i < 6; i++)
        await chp.BasicPublishAsync("", "work", Encoding.UTF8.GetBytes($"{i}"));

    await Until(() => stuck.Count + flowed.Count >= 6, "6 deliveries");
    Check(stuck.Count == 1, $"prefetch=1 with no ack must hold at 1, held {stuck.Count}");
    Check(flowed.Count == 5, $"the capped consumer's turns must flow on, got {flowed.Count}");
}

async Task TopicExchange()
{
    using var srv = Server.Start();
    await using var conn = await srv.ConnectAsync();
    await using var ch = await conn.CreateChannelAsync(ConfirmOptions());

    await ch.ExchangeDeclareAsync("logs", ExchangeType.Topic, durable: false, autoDelete: false);
    await ch.QueueDeclareAsync("kern", durable: false, exclusive: false, autoDelete: false);
    await ch.QueueDeclareAsync("all", durable: false, exclusive: false, autoDelete: false);
    await ch.QueueBindAsync("kern", "logs", "kern.*");
    await ch.QueueBindAsync("all", "logs", "#");

    await ch.BasicPublishAsync("logs", "kern.crit", Encoding.UTF8.GetBytes("kc"));
    await ch.BasicPublishAsync("logs", "app.info", Encoding.UTF8.GetBytes("ai"));

    var k = await ch.BasicGetAsync("kern", autoAck: true);
    Check(k is not null && Encoding.UTF8.GetString(k.Body.ToArray()) == "kc", "kern.* must match kern.crit");
    Check(await ch.BasicGetAsync("kern", autoAck: true) is null, "kern.* must not match app.info");
    var a1 = await ch.BasicGetAsync("all", autoAck: true);
    var a2 = await ch.BasicGetAsync("all", autoAck: true);
    Check(a1 is not null && a2 is not null, "# must match everything");
}

async Task Fanout()
{
    using var srv = Server.Start();
    await using var conn = await srv.ConnectAsync();
    await using var ch = await conn.CreateChannelAsync(ConfirmOptions());

    await ch.ExchangeDeclareAsync("bcast", ExchangeType.Fanout, durable: false, autoDelete: false);
    await ch.QueueDeclareAsync("f1", durable: false, exclusive: false, autoDelete: false);
    await ch.QueueDeclareAsync("f2", durable: false, exclusive: false, autoDelete: false);
    await ch.QueueBindAsync("f1", "bcast", "");
    await ch.QueueBindAsync("f2", "bcast", "");

    await ch.BasicPublishAsync("bcast", "ignored-key", Encoding.UTF8.GetBytes("copy"));

    foreach (var q in new[] { "f1", "f2" })
    {
        var got = await ch.BasicGetAsync(q, autoAck: true);
        Check(got is not null && Encoding.UTF8.GetString(got.Body.ToArray()) == "copy",
            $"fanout must copy to {q}");
    }
}

async Task MandatoryReturn()
{
    using var srv = Server.Start();
    await using var conn = await srv.ConnectAsync();
    // No confirm tracking on this channel: the Basic.Return event itself is
    // what is under test.
    await using var ch = await conn.CreateChannelAsync();

    var returned = new TaskCompletionSource<(ushort code, string body)>(
        TaskCreationOptions.RunContinuationsAsynchronously);
    ch.BasicReturnAsync += (_, ea) =>
    {
        returned.TrySetResult((ea.ReplyCode, Encoding.UTF8.GetString(ea.Body.ToArray())));
        return Task.CompletedTask;
    };

    await ch.BasicPublishAsync("", "no-such-queue", mandatory: true,
        basicProperties: new BasicProperties(), body: Encoding.UTF8.GetBytes("boomerang"));

    var done = await Task.WhenAny(returned.Task, Task.Delay(TimeSpan.FromSeconds(8)));
    Check(done == returned.Task, "no Basic.Return arrived for an unroutable mandatory publish");
    var (code, body) = await returned.Task;
    Check(code == 312, $"reply code must be 312 NO_ROUTE, got {code}");
    Check(body == "boomerang", "the returned body must be the published one");

    // A routable mandatory publish must NOT return.
    await ch.QueueDeclareAsync("exists", durable: false, exclusive: false, autoDelete: false);
    await ch.BasicPublishAsync("", "exists", mandatory: true,
        basicProperties: new BasicProperties(), body: Encoding.UTF8.GetBytes("lands"));
    await UntilAsync(async () => await ch.BasicGetAsync("exists", autoAck: true) is not null,
        "the routable mandatory publish to land");
}

async Task NackRequeue()
{
    using var srv = Server.Start();
    await using var conn = await srv.ConnectAsync();
    await using var ch = await conn.CreateChannelAsync(ConfirmOptions());

    await ch.QueueDeclareAsync("q", durable: false, exclusive: false, autoDelete: false);
    await ch.BasicPublishAsync("", "q", Encoding.UTF8.GetBytes("precious"));

    var first = await ch.BasicGetAsync("q", autoAck: false);
    Check(first is not null && !first.Redelivered, "first delivery must not be flagged redelivered");
    await ch.BasicNackAsync(first!.DeliveryTag, multiple: false, requeue: true);

    var second = await ch.BasicGetAsync("q", autoAck: false);
    Check(second is not null, "the nacked message must come back");
    Check(second!.Redelivered, "the requeued delivery must be flagged redelivered");
    Check(Encoding.UTF8.GetString(second.Body.ToArray()) == "precious", "body mismatch");

    // Nack WITHOUT requeue: gone for good.
    await ch.BasicNackAsync(second.DeliveryTag, multiple: false, requeue: false);
    Check(await ch.BasicGetAsync("q", autoAck: true) is null, "a discarded message must not return");
}

async Task DurableSurvivesKill()
{
    var dataDir = Directory.CreateTempSubdirectory("oxidb-amqp-net").FullName;
    var port = Server.FreePort();
    var srv = Server.Start(port, dataDir);
    try
    {
        await using (var conn = await srv.ConnectAsync())
        await using (var ch = await conn.CreateChannelAsync(ConfirmOptions()))
        {
            await ch.QueueDeclareAsync("dq", durable: true, exclusive: false, autoDelete: false);
            for (var i = 0; i < 3; i++)
                await ch.BasicPublishAsync("", "dq", mandatory: false,
                    basicProperties: new BasicProperties { Persistent = true },
                    body: Encoding.UTF8.GetBytes($"m{i}"));
            // BasicPublishAsync with confirms awaited: the broker has fsync'd.
        }

        // No graceful shutdown — the confirm is the only promise being tested.
        srv.Kill();
        srv = Server.Start(port, dataDir);

        await using (var conn = await srv.ConnectAsync())
        await using (var ch = await conn.CreateChannelAsync())
        {
            await ch.QueueDeclareAsync("dq", durable: true, exclusive: false, autoDelete: false);
            for (var i = 0; i < 3; i++)
            {
                var got = await ch.BasicGetAsync("dq", autoAck: true);
                Check(got is not null, $"message m{i} must survive the kill");
                Check(Encoding.UTF8.GetString(got!.Body.ToArray()) == $"m{i}", "order/body mismatch");
                Check(got.Redelivered, "a recovered message must be flagged redelivered");
            }
            Check(await ch.BasicGetAsync("dq", autoAck: true) is null,
                "exactly three messages — no resurrection");
        }

        // The drain above deleted the durable records; a second kill must not
        // bring anything back (at-least-once must not become
        // at-least-twice-after-every-crash).
        srv.Kill();
        srv = Server.Start(port, dataDir);

        await using (var conn = await srv.ConnectAsync())
        await using (var ch = await conn.CreateChannelAsync())
        {
            await ch.QueueDeclareAsync("dq", durable: true, exclusive: false, autoDelete: false);
            Check(await ch.BasicGetAsync("dq", autoAck: true) is null,
                "consumed messages must stay consumed across a crash");
        }
    }
    finally
    {
        srv.Dispose();
        try { Directory.Delete(dataDir, recursive: true); } catch { }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

static CreateChannelOptions ConfirmOptions() => new(
    publisherConfirmationsEnabled: true,
    publisherConfirmationTrackingEnabled: true);

static AsyncEventingBasicConsumer AckingConsumer(IChannel ch, List<string> into)
{
    var c = new AsyncEventingBasicConsumer(ch);
    c.ReceivedAsync += async (_, ea) =>
    {
        lock (into) into.Add(Encoding.UTF8.GetString(ea.Body.ToArray()));
        await ch.BasicAckAsync(ea.DeliveryTag, multiple: false);
    };
    return c;
}

static async Task Until(Func<bool> cond, string what)
{
    var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(8);
    while (!cond())
    {
        if (DateTime.UtcNow > deadline) throw new Exception($"timed out waiting for {what}");
        await Task.Delay(50);
    }
}

static async Task UntilAsync(Func<Task<bool>> cond, string what)
{
    var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(8);
    while (!await cond())
    {
        if (DateTime.UtcNow > deadline) throw new Exception($"timed out waiting for {what}");
        await Task.Delay(50);
    }
}

/// <summary>An oxidb-server that dies with its guard, AMQP listener up.</summary>
sealed class Server : IDisposable
{
    public int Port { get; private set; }
    Process _proc = null!;

    public static Server Start() => Start(FreePort(), Directory.CreateTempSubdirectory("oxidb-amqp-net").FullName);

    public static Server Start(int port, string dataDir)
    {
        var bin = FindServerBinary();
        var psi = new ProcessStartInfo(bin)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        psi.Environment["OXIDB_AMQP_PORT"] = port.ToString();
        psi.Environment["OXIDB_ADDR"] = $"127.0.0.1:{FreePort()}";
        psi.Environment["OXIDB_DATA"] = dataDir;
        var srv = new Server { Port = port, _proc = Process.Start(psi)! };

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (true)
        {
            try
            {
                using var probe = new TcpClient();
                probe.Connect("127.0.0.1", port);
                return srv;
            }
            catch (SocketException)
            {
                if (DateTime.UtcNow > deadline)
                {
                    srv.Dispose();
                    throw new Exception($"AMQP listener never came up on {port}");
                }
                Thread.Sleep(100);
            }
        }
    }

    public async Task<IConnection> ConnectAsync()
    {
        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = Port,
            // Deterministic tests: a killed server must fail fast, not
            // trigger background recovery.
            AutomaticRecoveryEnabled = false,
        };
        return await factory.CreateConnectionAsync();
    }

    /// <summary>SIGKILL — no graceful shutdown, that is the point.</summary>
    public void Kill()
    {
        _proc.Kill();
        _proc.WaitForExit();
    }

    public void Dispose()
    {
        try { if (!_proc.HasExited) _proc.Kill(); _proc.WaitForExit(); } catch { }
    }

    public static int FreePort()
    {
        using var l = new TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        return ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
    }

    static string FindServerBinary()
    {
        var env = Environment.GetEnvironmentVariable("OXIDB_SERVER_BIN");
        if (env is not null && File.Exists(env)) return env;
        // Walk up from the working directory to the repo root.
        for (var dir = new DirectoryInfo(Environment.CurrentDirectory); dir is not null; dir = dir.Parent)
        {
            foreach (var profile in new[] { "debug", "release" })
            {
                var p = Path.Combine(dir.FullName, "target", profile, "oxidb-server");
                if (File.Exists(p)) return p;
            }
        }
        throw new Exception(
            "oxidb-server binary not found — `cargo build -p oxidb-server` first, or set OXIDB_SERVER_BIN");
    }
}
