// FOREIGN KEY benchmark: identical FK-enforced schema and workloads run against
// OxiDB (SQL engine, over its wire) and PostgreSQL, via raw ADO.NET — no ORM, so
// the numbers are the databases' own referential-integrity cost, not EF's.
//
// Needs: an oxidb-server with SQL enabled on OXIDB_PORT (default 4544) and a
// PostgreSQL server on PG_PORT (default 5432). Each provider gets its own
// `fkbench` database on its own server, so there is no cross-provider state.
//
// Scenarios (each timed mean / p50 / p95 over many iterations):
//   insert_fk       INSERT a child row that references an existing parent
//   insert_nofk     the same INSERT into an FK-free twin table (the baseline —
//                   the delta over insert_fk is the enforcement cost)
//   fk_reject       INSERT a child referencing a missing parent (both reject)
//   cascade_delete  DELETE a parent with K children (ON DELETE CASCADE)

using System.Data.Common;
using System.Diagnostics;
using Npgsql;
using OxiDb.Data;

const string Db = "fkbench";
const int Customers = 20_000; // parent rows
const int InsertIters = 500;
const int RejectIters = 300;
const int CascadeK = 20; // children per cascade-deleted parent
const int CascadeIters = 200;
const int Warmup = 40;
const int CascadeBase = 5_000_000; // parent-id range for cascade deletes
const int CascadeChildBase = 6_000_000;

int oxiPort = int.Parse(Environment.GetEnvironmentVariable("OXIDB_PORT") ?? "4544");
int pgPort = int.Parse(Environment.GetEnvironmentVariable("PG_PORT") ?? "5432");
string pgUser = Environment.GetEnvironmentVariable("PG_USER") ?? Environment.UserName;

Console.WriteLine("── FOREIGN KEY benchmark: OxiDB vs PostgreSQL (raw ADO.NET) ──");
Console.WriteLine($"  parents={Customers}  insert_iters={InsertIters}  cascade K={CascadeK}×{CascadeIters}");
Console.WriteLine("  NOTE: on macOS PostgreSQL's per-commit sync is not a true F_FULLFSYNC");
Console.WriteLine("  barrier. For a durability-matched comparison run oxidb-server with");
Console.WriteLine("  OXIDB_LAZY_SYNC=true; otherwise OxiDB's honest per-commit fsync (~ms)");
Console.WriteLine("  dominates the absolute numbers — the FK delta below is the fair signal.\n");

var results = new List<(string scenario, string provider, double mean, double p50, double p95)>();

foreach (var provider in new[] { "oxidb", "postgres" })
{
    using var db = Open(provider);
    SetupSchema(db);
    SeedCustomers(db, Customers);
    SeedCascade(db, CascadeIters + Warmup, CascadeK, CascadeBase, CascadeChildBase);

    // insert_fk — child INSERT with a live parent (FK existence check).
    long oid = 1_000_000;
    results.Add(Measure("insert_fk", provider, InsertIters, Warmup, i =>
        Exec(db, $"INSERT INTO orders VALUES ({oid++}, {Rnd(i, Customers)}, 'x')")));

    // insert_nofk — same shape, no FK constraint (the baseline).
    long noid = 2_000_000;
    results.Add(Measure("insert_nofk", provider, InsertIters, Warmup, i =>
        Exec(db, $"INSERT INTO orders_nofk VALUES ({noid++}, {Rnd(i, Customers)}, 'x')")));

    // fk_reject — child referencing a missing parent; both must reject.
    long rid = 3_000_000;
    results.Add(Measure("fk_reject", provider, RejectIters, Warmup, i =>
    {
        try { Exec(db, $"INSERT INTO orders VALUES ({rid++}, 999999999, 'x')"); }
        catch { /* expected FK violation */ }
    }));

    // cascade_delete — DELETE a parent with K children (ON DELETE CASCADE).
    int delId = CascadeBase;
    results.Add(Measure("cascade_delete", provider, CascadeIters, Warmup, _ =>
        Exec(db, $"DELETE FROM customers WHERE id = {delId++}")));

    Console.WriteLine();
}

// ── report ──────────────────────────────────────────────────────────────
Console.WriteLine("── results (mean · p50 · p95) ────────────────────────────────");
Console.WriteLine($"  {"scenario",-16} {"OxiDB",-28}{"PostgreSQL",-28}{"OxiDB/PG",8}");
foreach (var scenario in new[] { "insert_fk", "insert_nofk", "fk_reject", "cascade_delete" })
{
    var oxi = results.First(r => r.scenario == scenario && r.provider == "oxidb");
    var pg = results.First(r => r.scenario == scenario && r.provider == "postgres");
    var ratio = pg.mean > 0 ? oxi.mean / pg.mean : double.NaN;
    Console.WriteLine($"  {scenario,-16} {Cell(oxi),-28}{Cell(pg),-28}{ratio,7:P0}");
}

Console.WriteLine("\n── FK enforcement overhead (insert_fk − insert_nofk, mean) ───");
foreach (var provider in new[] { "oxidb", "postgres" })
{
    var fk = results.First(r => r.scenario == "insert_fk" && r.provider == provider).mean;
    var no = results.First(r => r.scenario == "insert_nofk" && r.provider == provider).mean;
    Console.WriteLine($"  {provider,-10} +{fk - no,6:F1} µs/insert  ({(fk - no) / no,6:P0} over the FK-free baseline)");
}

// ── helpers ──────────────────────────────────────────────────────────────
DbConnection Open(string provider)
{
    if (provider == "oxidb")
    {
        using (var boot = new OxiDbConnection($"Host=127.0.0.1;Port={oxiPort};Database=oxidb"))
        {
            boot.Open();
            try { Exec(boot, $"CREATE DATABASE {Db}"); } catch { /* exists */ }
        }
        var c = new OxiDbConnection($"Host=127.0.0.1;Port={oxiPort};Database={Db}");
        c.Open();
        return c;
    }
    else
    {
        using (var boot = new NpgsqlConnection($"Host=127.0.0.1;Port={pgPort};Database=postgres;Username={pgUser}"))
        {
            boot.Open();
            try { Exec(boot, $"CREATE DATABASE {Db}"); } catch { /* exists */ }
        }
        var c = new NpgsqlConnection($"Host=127.0.0.1;Port={pgPort};Database={Db};Username={pgUser}");
        c.Open();
        return c;
    }
}

static void SetupSchema(DbConnection db)
{
    Exec(db, "DROP TABLE IF EXISTS orders");
    Exec(db, "DROP TABLE IF EXISTS orders_nofk");
    Exec(db, "DROP TABLE IF EXISTS customers");
    Exec(db, "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)");
    Exec(db, "CREATE TABLE orders (id INT PRIMARY KEY, " +
             "customer_id INT REFERENCES customers(id) ON DELETE CASCADE, note TEXT)");
    Exec(db, "CREATE TABLE orders_nofk (id INT PRIMARY KEY, customer_id INT, note TEXT)");
    // Index the FK column on BOTH tables: realistic (a child FK is normally
    // indexed), lets the parent-side check (cascade/reject) use the index
    // instead of a scan, and — being present on the FK-free twin too — keeps
    // the insert_fk vs insert_nofk delta a pure FK-check cost, not index upkeep.
    Exec(db, "CREATE INDEX ix_orders_cust ON orders(customer_id)");
    Exec(db, "CREATE INDEX ix_ordersnofk_cust ON orders_nofk(customer_id)");
}

static void SeedCustomers(DbConnection db, int n)
{
    for (var start = 1; start <= n; start += 1000)
    {
        var end = Math.Min(start + 999, n);
        var sb = new System.Text.StringBuilder("INSERT INTO customers VALUES ");
        for (var id = start; id <= end; id++)
        {
            if (id > start) sb.Append(',');
            sb.Append($"({id},'c{id}')");
        }
        Exec(db, sb.ToString());
    }
}

static void SeedCascade(DbConnection db, int parents, int k, int cascadeBase, int childBase)
{
    long childId = childBase;
    for (var p = 0; p < parents; p++)
    {
        var pid = cascadeBase + p;
        Exec(db, $"INSERT INTO customers VALUES ({pid},'p{pid}')");
        var sb = new System.Text.StringBuilder("INSERT INTO orders VALUES ");
        for (var j = 0; j < k; j++)
        {
            if (j > 0) sb.Append(',');
            sb.Append($"({childId++},{pid},'x')");
        }
        Exec(db, sb.ToString());
    }
}

static int Exec(DbConnection c, string sql)
{
    using var cmd = c.CreateCommand();
    cmd.CommandText = sql;
    return cmd.ExecuteNonQuery();
}

static int Rnd(int i, int mod) => (int)((i * 48271L) % mod) + 1;

static (string, string, double, double, double) Measure(
    string name, string provider, int iters, int warmup, Action<int> op)
{
    var sw = new Stopwatch();
    for (var i = 0; i < warmup; i++) op(i);
    var ticks = new long[iters];
    for (var i = 0; i < iters; i++)
    {
        sw.Restart();
        op(warmup + i);
        sw.Stop();
        ticks[i] = sw.ElapsedTicks;
    }
    Array.Sort(ticks);
    double Us(long t) => t * 1_000_000.0 / Stopwatch.Frequency;
    double mean = Us((long)ticks.Average()), p50 = Us(ticks[iters / 2]), p95 = Us(ticks[(int)(iters * 0.95)]);
    Console.WriteLine($"  {name,-16} {provider,-10} mean {Fmt(mean),9}  p50 {Fmt(p50),9}  p95 {Fmt(p95),9}");
    return (name, provider, mean, p50, p95);
}

static string Fmt(double us) => us < 1000 ? $"{us:F0} µs" : $"{us / 1000.0:F2} ms";
static string Cell((string, string, double mean, double p50, double p95) r) =>
    $"{Fmt(r.mean)} · {Fmt(r.p50)} · {Fmt(r.p95)}";
