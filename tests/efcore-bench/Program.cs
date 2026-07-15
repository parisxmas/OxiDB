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
        using (var db = Open(provider))
        {
            for (var p = 1; p <= 500; p++)
                db.Products.Add(new Product
                {
                    Id = p,
                    Sku = $"SKU-{p:D4}",
                    Category = p % 20,
                    Price = (p * 97 % 1000) / 10.0 + 1,
                });
            db.SaveChanges();
        }
        var lines = orders * 3;
        for (var lo = 1; lo <= lines; lo += 3000)
        {
            using var db = Open(provider);
            for (var i = lo; i < lo + 3000 && i <= lines; i++)
                db.OrderLines.Add(new OrderLine
                {
                    Id = i,
                    OrderId = (i - 1) / 3 + 1,
                    ProductId = (i * 31) % 500 + 1,
                    Qty = i % 5 + 1,
                });
            db.SaveChanges();
        }
        sw.Stop();
        seedSeconds[provider] = sw.Elapsed.TotalSeconds;
        Console.WriteLine(
            $"seeded {provider,-8} {customers} customers + {orders} orders + 500 products + {lines} lines in {sw.Elapsed.TotalSeconds:F1}s");
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
        try
        {
            op(db, 0); // translation smoke test — a failed shape is a finding, not a crash
        }
        catch (Exception e)
        {
            results.Add(new BenchResult(name, provider, double.NaN, double.NaN, double.NaN, 0));
            Console.WriteLine($"  {name,-28} {provider,-8} FAILED: {Head(e)}");
            continue;
        }
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

static string Fmt(double us) =>
    double.IsNaN(us) ? "FAIL" : us < 1000 ? $"{us:F0} µs" : $"{us / 1000.0:F2} ms";

static string Head(Exception e)
{
    var m = (e.GetBaseException().Message ?? "").ReplaceLineEndings(" ");
    return m.Length > 120 ? m[..120] + "…" : m;
}

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

// ── complex benches ─────────────────────────────────────────────────────────

Console.WriteLine("\n── complex benches ────────────────────────────────────────");

// Two-way join + GROUP BY + ORDER BY aggregate: revenue per city, top 5.
Run("join_groupby_top5", 30, (db, i) =>
    db.Orders.Join(db.Customers, o => o.CustomerId, c => c.Id, (o, c) => new { c.City, o.Amount })
        .GroupBy(x => x.City)
        .Select(g => new { City = g.Key, Total = g.Sum(x => x.Amount) })
        .OrderByDescending(x => x.Total)
        .Take(5)
        .ToList());

// Per-customer aggregates over the Orders navigation, top spenders.
Run("nav_aggregate_top10", 20, (db, i) =>
    db.Customers
        .Select(c => new { c.Name, Total = c.Orders.Sum(o => o.Amount), Cnt = c.Orders.Count() })
        .Where(x => x.Cnt > 0)
        .OrderByDescending(x => x.Total)
        .Take(10)
        .ToList());

// Include: parent rows with their full child collections rehydrated.
Run("include_collection", 30, (db, i) =>
    db.Customers.Include(c => c.Orders).Where(c => c.City == "City03").ToList());

// EXISTS over a filtered child collection.
Run("exists_any", 30, (db, i) =>
    db.Customers.Count(c => c.Orders.Any(o => o.Amount > 995)));

// Parameterized IN list (EF renders a VALUES table or IN).
Run("in_list_count", 50, (db, i) =>
{
    var wanted = new[] { "City01", "City05", "City09", "City13" };
    db.Customers.Count(c => wanted.Contains(c.City));
});

// Three-way join through navigations + arithmetic aggregate.
Run("three_way_join_sum", 15, (db, i) =>
    db.OrderLines
        .Where(l => l.Product.Category == 7 && l.Order.Status == 1)
        .Sum(l => l.Qty * l.Product.Price));

// Correlated top-1 per row (EF renders LATERAL / APPLY).
Run("top1_per_customer", 20, (db, i) =>
    db.Customers.Where(c => c.Id <= 100)
        .Select(c => new
        {
            c.Name,
            Best = c.Orders.OrderByDescending(o => o.Amount).Select(o => o.Amount).FirstOrDefault(),
        })
        .ToList());

// Deep OFFSET paging (past the engine's bounded top-N cutoff).
Run("deep_paging", 20, (db, i) =>
    db.Orders.OrderBy(o => o.Created).Skip(5000).Take(50).ToList());

// DISTINCT projection under a predicate.
Run("distinct_projection", 50, (db, i) =>
    db.Customers.Where(c => c.Segment == 2).Select(c => c.City).Distinct().Count());

// Set operation over two filtered projections.
Run("union_projection", 30, (db, i) =>
    db.Customers.Where(c => c.Segment == 0).Select(c => c.City)
        .Union(db.Customers.Where(c => c.Segment == 1).Select(c => c.City))
        .Count());

// GROUP BY a computed date part (EXTRACT) with two aggregates.
Run("month_histogram", 20, (db, i) =>
    db.Orders.GroupBy(o => o.Created.Month)
        .Select(g => new { Month = g.Key, Cnt = g.Count(), Total = g.Sum(x => x.Amount) })
        .OrderBy(x => x.Month)
        .ToList());

// GROUP BY + HAVING on the group aggregate.
Run("groupby_having", 20, (db, i) =>
    db.Orders.GroupBy(o => o.CustomerId).Where(g => g.Count() >= 15).Count());

// Combined string predicates (prefix + suffix).
Run("string_multi", 30, (db, i) =>
    db.Customers.Count(c => c.Name.StartsWith("Customer 00") && c.Name.EndsWith("7")));

// ── advanced benches (harder OLAP / correlated / window shapes) ──────────────

Console.WriteLine("\n── advanced benches ───────────────────────────────────────");

// Correlated scalar subquery in projection: each customer with the size of
// their largest single order (EF renders a correlated aggregate subquery).
Run("correlated_scalar", 20, (db, i) =>
    db.Customers.Where(c => c.Id <= 200)
        .Select(c => new
        {
            c.Name,
            MaxOrder = db.Orders.Where(o => o.CustomerId == c.Id).Max(o => (double?)o.Amount) ?? 0,
        })
        .ToList());

// Subquery in WHERE against a global aggregate: customers whose total spend
// exceeds the average customer's total spend.
Run("above_average_spenders", 15, (db, i) =>
{
    var avg = db.Orders.Average(o => o.Amount);
    db.Customers
        .Where(c => c.Orders.Sum(o => o.Amount) > avg * 10)
        .Select(c => c.Name)
        .ToList();
});

// Window function: rank customers by spend within their city, keep the top of
// each (ROW_NUMBER() OVER (PARTITION BY city ORDER BY spend DESC)).
Run("window_rank_per_city", 15, (db, i) =>
    db.Customers
        .Select(c => new { c.City, c.Name, Spend = c.Orders.Sum(o => o.Amount) })
        .GroupBy(x => x.City)
        .Select(g => g.OrderByDescending(x => x.Spend).First())
        .ToList());

// Conditional aggregation: per status, count and the sum of only the large
// orders (EF renders SUM(CASE WHEN ... THEN amount ELSE 0 END)).
Run("conditional_aggregate", 20, (db, i) =>
    db.Orders.GroupBy(o => o.Status)
        .Select(g => new
        {
            Status = g.Key,
            Total = g.Count(),
            BigSum = g.Sum(o => o.Amount > 500 ? o.Amount : 0),
            BigCount = g.Count(o => o.Amount > 500),
        })
        .OrderBy(x => x.Status)
        .ToList());

// Paged listing with a total count — the classic "page N of M" shape: two
// round trips (COUNT + a windowed/limited page) EF issues per grid.
Run("paged_with_total", 25, (db, i) =>
{
    var total = db.Orders.Count(o => o.Status == 1);
    db.Orders.Where(o => o.Status == 1)
        .OrderByDescending(o => o.Created)
        .Skip(40).Take(20)
        .Select(o => new { o.Id, o.Amount })
        .ToList();
    _ = total;
});

// Self-join: count customer pairs in the same city with the same segment
// (a join of a table to itself, filtered to avoid the trivial/self pairs).
Run("self_join_pairs", 15, (db, i) =>
    db.Customers.Where(c => c.City == "City05")
        .Join(db.Customers.Where(c => c.City == "City05"),
            a => a.Segment, b => b.Segment, (a, b) => new { a, b })
        .Count(p => p.a.Id < p.b.Id));

// Left join with null handling: every customer, with their order count —
// including the ones with zero orders (GroupJoin + DefaultIfEmpty).
Run("left_join_counts", 20, (db, i) =>
    db.Customers
        .GroupJoin(db.Orders, c => c.Id, o => o.CustomerId, (c, os) => new { c.Name, N = os.Count() })
        .Where(x => x.N < 5)
        .Count());

// Multi-key grouping with HAVING and an ordered top: revenue by (city,
// segment), only busy cells, top 10 by revenue.
Run("multikey_group_having", 15, (db, i) =>
    db.Orders.Join(db.Customers, o => o.CustomerId, c => c.Id, (o, c) => new { c.City, c.Segment, o.Amount })
        .GroupBy(x => new { x.City, x.Segment })
        .Select(g => new { g.Key.City, g.Key.Segment, Revenue = g.Sum(x => x.Amount), N = g.Count() })
        .Where(x => x.N >= 40)
        .OrderByDescending(x => x.Revenue)
        .Take(10)
        .ToList());

// Top-N per group via a correlated count: for each product category, the
// products priced above the category's own average (EF: correlated subquery
// in the predicate + join).
Run("top_per_group_correlated", 15, (db, i) =>
    db.Products
        .Where(p => p.Price > db.Products.Where(q => q.Category == p.Category).Average(q => q.Price))
        .Select(p => new { p.Sku, p.Category, p.Price })
        .ToList());

// Deep three-level navigation aggregate: revenue per product category, going
// OrderLine -> Product and OrderLine -> Order for the status filter.
Run("category_revenue", 12, (db, i) =>
    db.OrderLines
        .Where(l => l.Order.Status != 3)
        .GroupBy(l => l.Product.Category)
        .Select(g => new { Category = g.Key, Revenue = g.Sum(l => l.Qty * l.Product.Price), Units = g.Sum(l => l.Qty) })
        .OrderByDescending(x => x.Revenue)
        .Take(10)
        .ToList());

// EXCEPT: cities that have segment-0 customers but no segment-3 customers.
Run("except_sets", 25, (db, i) =>
    db.Customers.Where(c => c.Segment == 0).Select(c => c.City)
        .Except(db.Customers.Where(c => c.Segment == 3).Select(c => c.City))
        .Count());

// Any with a compound correlated predicate: customers with at least one
// large, recent order.
Run("any_compound", 25, (db, i) =>
    db.Customers.Count(c => c.Orders.Any(o => o.Amount > 900 && o.Status == 1)));

// Ordered join projection with a computed sort key across two tables.
Run("join_computed_sort", 20, (db, i) =>
    db.OrderLines
        .Where(l => l.Order.CustomerId <= 500)
        .Select(l => new { l.Id, Line = l.Qty * l.Product.Price })
        .OrderByDescending(x => x.Line)
        .Take(25)
        .ToList());

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
    public List<OrderLine> Lines { get; set; } = [];
}

class Product
{
    public int Id { get; set; }
    public string Sku { get; set; } = "";
    public int Category { get; set; }
    public double Price { get; set; }
}

class OrderLine
{
    public int Id { get; set; }
    public int OrderId { get; set; }
    public int ProductId { get; set; }
    public int Qty { get; set; }
    public Order? Order { get; set; }
    public Product? Product { get; set; }
}

class Bench(DbContextOptions<Bench> options) : DbContext(options)
{
    public DbSet<Customer> Customers => Set<Customer>();
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<Product> Products => Set<Product>();
    public DbSet<OrderLine> OrderLines => Set<OrderLine>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Customer>().Property(c => c.Id).ValueGeneratedNever();
        b.Entity<Order>().Property(o => o.Id).ValueGeneratedNever();
        b.Entity<Order>().HasIndex(o => o.CustomerId);
        b.Entity<Order>()
            .HasOne(o => o.Customer).WithMany(c => c.Orders)
            .HasForeignKey(o => o.CustomerId);
        b.Entity<Product>().Property(p => p.Id).ValueGeneratedNever();
        b.Entity<OrderLine>().Property(l => l.Id).ValueGeneratedNever();
        b.Entity<OrderLine>().HasIndex(l => l.OrderId);
        b.Entity<OrderLine>().HasIndex(l => l.ProductId);
        b.Entity<OrderLine>()
            .HasOne(l => l.Order).WithMany(o => o.Lines)
            .HasForeignKey(l => l.OrderId);
        b.Entity<OrderLine>()
            .HasOne(l => l.Product).WithMany()
            .HasForeignKey(l => l.ProductId);
    }
}
