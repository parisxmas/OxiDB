// EF Core benchmark: the same LINQ queries against OxiDB and PostgreSQL.
//
// One shared model + query set, two providers. Each bench reuses a single
// DbContext (so we measure query execution, not connection setup), runs a
// warmup, then times N iterations. Reported per bench and provider:
// mean / p50 / p95 latency and managed allocations per operation. At the
// end: server-side RSS (oxidb-server process; sum of postgres processes —
// note PG counts shared buffers once per backend, so treat it as an upper
// bound).
//
// Usage: dotnet run -c Release [--seed-only] [--skip-seed]
//   env: OXIDB_PORT (default 4499), PG_PORT (default 5432), PG_USER,
//        SCALE_CUSTOMERS (default 5000), SCALE_ORDERS (default 50000)
using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using OxiDb.EntityFrameworkCore;

var oxiPort = Environment.GetEnvironmentVariable("OXIDB_PORT") ?? "4499";
var pgPort = Environment.GetEnvironmentVariable("PG_PORT") ?? "5432";
var pgUser = Environment.GetEnvironmentVariable("PG_USER") ?? Environment.UserName;
var customers = int.Parse(Environment.GetEnvironmentVariable("SCALE_CUSTOMERS") ?? "5000");
var orders = int.Parse(Environment.GetEnvironmentVariable("SCALE_ORDERS") ?? "50000");
var skipSeed = args.Contains("--skip-seed");

var oxiCs = $"Host=127.0.0.1;Port={oxiPort};Database=efbench";
var pgCs = $"Host=127.0.0.1;Port={pgPort};Database=efbench;Username={pgUser}";

// ── setup: fresh databases ──────────────────────────────────────────────────

if (!skipSeed)
{
    await using (var boot = await OxiDb.Client.Tcp.OxiDbTcpClient.ConnectAsync(
        "127.0.0.1", int.Parse(oxiPort)))
    {
        await boot.SqlAsync("DROP DATABASE IF EXISTS efbench");
        await boot.SqlAsync("CREATE DATABASE efbench");
    }
    using (var pgAdmin = new Bench(new DbContextOptionsBuilder<Bench>()
        .UseNpgsql(pgCs.Replace("Database=efbench", "Database=postgres")).Options))
    {
        pgAdmin.Database.ExecuteSqlRaw("DROP DATABASE IF EXISTS efbench WITH (FORCE)");
        pgAdmin.Database.ExecuteSqlRaw("CREATE DATABASE efbench");
    }
}

Bench Open(string provider) =>
    new(provider == "oxidb"
        ? new DbContextOptionsBuilder<Bench>().UseOxiDb(oxiCs).Options
        : new DbContextOptionsBuilder<Bench>().UseNpgsql(pgCs).Options);

// ── seed: identical deterministic data on both ──────────────────────────────

var cities = Enumerable.Range(0, 20).Select(i => $"City{i:D2}").ToArray();
DateTime Utc(int y, int mo, int d) => new(y, mo, d, 0, 0, 0, DateTimeKind.Utc);

var seedSeconds = new Dictionary<string, double>();
if (!skipSeed)
{
    foreach (var provider in new[] { "oxidb", "postgres" })
    {
        var sw = Stopwatch.StartNew();
        using (var db = Open(provider))
            db.Database.EnsureCreated();
        for (var lo = 1; lo <= customers; lo += 2000)
        {
            using var db = Open(provider);
            for (var i = lo; i < lo + 2000 && i <= customers; i++)
                db.Customers.Add(new Customer
                {
                    Id = i,
                    Name = $"Customer {i:D6}",
                    City = cities[i % cities.Length],
                    Segment = i % 5,
                    Joined = Utc(2024, 1, 1).AddMinutes(i),
                });
            db.SaveChanges();
        }
        for (var lo = 1; lo <= orders; lo += 2000)
        {
            using var db = Open(provider);
            for (var i = lo; i < lo + 2000 && i <= orders; i++)
                db.Orders.Add(new Order
                {
                    Id = i,
                    CustomerId = (i * 7919) % customers + 1,
                    Amount = (i * 2654435761L % 100000) / 100.0,
                    Status = i % 4,
                    Created = Utc(2025, 1, 1).AddSeconds(i),
                });
            db.SaveChanges();
        }
        sw.Stop();
        seedSeconds[provider] = sw.Elapsed.TotalSeconds;
        Console.WriteLine(
            $"seeded {provider,-8} {customers} customers + {orders} orders in {sw.Elapsed.TotalSeconds:F1}s");
    }
}

// ── bench harness ───────────────────────────────────────────────────────────

var results = new List<BenchResult>();

// Deterministic id sequence for point lookups / writes. Inserted ids live in
// a per-provider range so delete_point removes rows that actually exist for
// THAT provider (a shared counter would make one side's deletes no-ops).
int Rnd(int i, int mod) => (int)((i * 48271L) % mod) + 1;
var currentProvider = "oxidb";
int IdBase() => currentProvider == "oxidb" ? 10_000_000 : 20_000_000;
var insertSeq = new Dictionary<string, int> { ["oxidb"] = 0, ["postgres"] = 0 };
int NextId() => IdBase() + ++insertSeq[currentProvider];

void Run(string name, int iterations, Action<Bench, int> op, int warmup = 15)
{
    foreach (var provider in new[] { "oxidb", "postgres" })
    {
        currentProvider = provider;
        using var db = Open(provider);
        db.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;
        for (var i = 0; i < warmup; i++)
        {
            op(db, i);
            db.ChangeTracker.Clear();
        }
        var ticks = new long[iterations];
        var sw = new Stopwatch();
        var allocBefore = GC.GetTotalAllocatedBytes(precise: true);
        for (var i = 0; i < iterations; i++)
        {
            sw.Restart();
            op(db, warmup + i);
            sw.Stop();
            ticks[i] = sw.ElapsedTicks;
            db.ChangeTracker.Clear();
        }
        var allocPerOp = (GC.GetTotalAllocatedBytes(precise: true) - allocBefore) / iterations;
        Array.Sort(ticks);
        double Us(long t) => t * 1_000_000.0 / Stopwatch.Frequency;
        results.Add(new BenchResult(name, provider,
            Us((long)ticks.Average()), Us(ticks[iterations / 2]),
            Us(ticks[(int)(iterations * 0.95)]), allocPerOp));
        Console.WriteLine($"  {name,-28} {provider,-8} " +
            $"mean {Fmt(Us((long)ticks.Average())),10}  p50 {Fmt(Us(ticks[iterations / 2])),10}  " +
            $"p95 {Fmt(Us(ticks[(int)(iterations * 0.95)])),10}  alloc {allocPerOp / 1024.0,8:F1} KB");
    }
}

static string Fmt(double us) => us < 1000 ? $"{us:F0} µs" : $"{us / 1000.0:F2} ms";

Console.WriteLine("\n── simple benches ─────────────────────────────────────────");

Run("point_lookup_pk", 300, (db, i) =>
{
    var id = Rnd(i, orders);
    db.Orders.Single(o => o.Id == id);
});

Run("indexed_fk_filter", 300, (db, i) =>
{
    var cid = Rnd(i, customers);
    db.Orders.Where(o => o.CustomerId == cid).ToList();
});

Run("top20_orderby", 100, (db, i) =>
    db.Orders.OrderByDescending(o => o.Created).Take(20).ToList());

Run("count_predicate", 100, (db, i) =>
    db.Orders.Count(o => o.Status == 2));

Run("sum_filtered", 100, (db, i) =>
    db.Orders.Where(o => o.Status == 1).Sum(o => o.Amount));

Run("projection_filter", 100, (db, i) =>
    db.Customers.Where(c => c.City == "City07")
        .Select(c => new { c.Name, c.Joined }).ToList());

Run("string_contains", 50, (db, i) =>
    db.Customers.Count(c => c.Name.Contains("42")));

Run("insert_single", 200, (db, i) =>
{
    db.Orders.Add(new Order
    {
        Id = NextId(), CustomerId = Rnd(i, customers),
        Amount = 42.5, Status = 1, Created = Utc(2026, 1, 1),
    });
    db.SaveChanges();
});

Run("insert_batch_100", 30, (db, i) =>
{
    for (var k = 0; k < 100; k++)
        db.Orders.Add(new Order
        {
            Id = NextId(), CustomerId = Rnd(i * 100 + k, customers),
            Amount = 17.25, Status = 2, Created = Utc(2026, 1, 2),
        });
    db.SaveChanges();
});

Run("update_point", 200, (db, i) =>
{
    var id = Rnd(i, orders);
    db.Orders.Where(o => o.Id == id)
        .ExecuteUpdate(s => s.SetProperty(o => o.Status, 3));
});

// Deletes insert_single's rows for this provider (ids IdBase()+1..+215).
Run("delete_point", 200, (db, i) =>
{
    var id = IdBase() + i + 1;
    db.Orders.Where(o => o.Id == id).ExecuteDelete();
});

// ── report ──────────────────────────────────────────────────────────────────

Console.WriteLine("\n── results ────────────────────────────────────────────────");
Console.WriteLine($"{"bench",-28} {"OxiDB mean",12} {"PG mean",12} {"speedup",9}  " +
    $"{"OxiDB p95",12} {"PG p95",12}  {"alloc oxi",10} {"alloc pg",10}");
foreach (var group in results.GroupBy(r => r.Name))
{
    var oxi = group.First(r => r.Provider == "oxidb");
    var pg = group.First(r => r.Provider == "postgres");
    var speedup = pg.MeanUs / oxi.MeanUs;
    Console.WriteLine($"{group.Key,-28} {Fmt(oxi.MeanUs),12} {Fmt(pg.MeanUs),12} " +
        $"{speedup,8:F2}x  {Fmt(oxi.P95Us),12} {Fmt(pg.P95Us),12}  " +
        $"{oxi.AllocBytes / 1024.0,8:F1} KB {pg.AllocBytes / 1024.0,8:F1} KB");
}
if (seedSeconds.Count == 2)
    Console.WriteLine($"\nseed ({customers} customers + {orders} orders): " +
        $"oxidb {seedSeconds["oxidb"]:F1}s, postgres {seedSeconds["postgres"]:F1}s");

// Server memory: RSS of the oxidb-server process and of all postgres
// processes summed (PG's per-backend RSS re-counts shared buffers, so the
// sum is an upper bound).
long RssKb(string pattern)
{
    var pgrep = Process.Start(new ProcessStartInfo("pgrep", $"-f {pattern}")
    { RedirectStandardOutput = true })!;
    var pids = pgrep.StandardOutput.ReadToEnd()
        .Split('\n', StringSplitOptions.RemoveEmptyEntries)
        .Where(p => int.TryParse(p, out var pid) && pid != Environment.ProcessId);
    pgrep.WaitForExit();
    long total = 0;
    foreach (var pid in pids)
    {
        var ps = Process.Start(new ProcessStartInfo("ps", $"-o rss= -p {pid}")
        { RedirectStandardOutput = true })!;
        if (long.TryParse(ps.StandardOutput.ReadToEnd().Trim(), out var kb))
            total += kb;
        ps.WaitForExit();
    }
    return total;
}
Console.WriteLine($"\nserver RSS after run: oxidb-server {RssKb("oxidb-server") / 1024.0:F0} MB, " +
    $"postgres (all processes, upper bound) {RssKb("postgres") / 1024.0:F0} MB");

record BenchResult(string Name, string Provider, double MeanUs, double P50Us, double P95Us,
    long AllocBytes);

// ── model ───────────────────────────────────────────────────────────────────

class Customer
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public string City { get; set; } = "";
    public int Segment { get; set; }
    public DateTime Joined { get; set; }
    public List<Order> Orders { get; set; } = [];
}

class Order
{
    public int Id { get; set; }
    public int CustomerId { get; set; }
    public double Amount { get; set; }
    public int Status { get; set; }
    public DateTime Created { get; set; }
    public Customer? Customer { get; set; }
}

class Bench(DbContextOptions<Bench> options) : DbContext(options)
{
    public DbSet<Customer> Customers => Set<Customer>();
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Customer>().Property(c => c.Id).ValueGeneratedNever();
        b.Entity<Order>().Property(o => o.Id).ValueGeneratedNever();
        b.Entity<Order>().HasIndex(o => o.CustomerId);
        b.Entity<Order>()
            .HasOne(o => o.Customer).WithMany(c => c.Orders)
            .HasForeignKey(o => o.CustomerId);
    }
}
