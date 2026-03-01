using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using OxiDb.EntityFrameworkCore.Extensions;
using OxiDb.Tests.EFCore.Models;
using Xunit;
using Xunit.Abstractions;

namespace OxiDb.Tests.EFCore;

// ═══════════════════════════════════════════════════════════════════════════
// Timing infrastructure
// ═══════════════════════════════════════════════════════════════════════════

public record TimingEntry(
    string Category,
    string Name,
    TimeSpan OxiDur,
    TimeSpan PgDur,
    int OxiCount = 0,
    int PgCount = 0,
    string? OxiErr = null,
    string? PgErr = null);

// ═══════════════════════════════════════════════════════════════════════════
// Shared fixture: manages contexts and collects timings across all tests
// ═══════════════════════════════════════════════════════════════════════════

public class ComparisonFixture : IAsyncLifetime
{
    private readonly List<TimingEntry> _timings = new();
    private readonly object _lock = new();

    public BenchmarkDbContext OxiContext { get; private set; } = null!;
    public BenchmarkDbContext PgContext { get; private set; } = null!;

    private static readonly string[] Departments = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Support", "Legal", "Operations"];
    private static readonly string[] Cities = ["New York", "London", "Tokyo", "Berlin", "Paris", "Sydney", "Toronto", "Mumbai"];
    private static readonly string[] Countries = ["US", "UK", "JP", "DE", "FR", "AU", "CA", "IN"];
    private static readonly string[] Statuses = ["active", "inactive", "pending"];
    private static readonly string[][] TagSets =
    [
        ["senior", "lead"],
        ["junior"],
        ["remote", "contractor"],
        ["full-time"],
        ["intern"],
        ["senior", "remote"],
        ["part-time", "contractor"]
    ];

    public async Task InitializeAsync()
    {
        // OxiDB context
        var oxiOptions = new DbContextOptionsBuilder<BenchmarkDbContext>()
            .UseOxiDb("127.0.0.1", 4444)
            .Options;
        OxiContext = new BenchmarkDbContext(oxiOptions);

        // PostgreSQL context
        var pgOptions = new DbContextOptionsBuilder<BenchmarkDbContext>()
            .UseNpgsql("Host=127.0.0.1;Port=5433;Database=bench;Username=bench;Password=bench")
            .Options;
        PgContext = new BenchmarkDbContext(pgOptions);

        // Ensure databases/collections exist
        await OxiContext.Database.EnsureCreatedAsync();
        await PgContext.Database.EnsureCreatedAsync();
    }

    public async Task DisposeAsync()
    {
        GenerateReports();

        await OxiContext.DisposeAsync();
        await PgContext.DisposeAsync();
    }

    public void RecordTiming(string category, string name, TimeSpan oxiDur, TimeSpan pgDur,
        int oxiCount = 0, int pgCount = 0,
        string? oxiErr = null, string? pgErr = null)
    {
        lock (_lock)
        {
            _timings.Add(new TimingEntry(category, name, oxiDur, pgDur,
                oxiCount, pgCount, oxiErr, pgErr));
        }
    }

    public Employee GenerateEmployee(int seq)
    {
        var rng = new Random(seq);
        return new Employee
        {
            Id = Guid.NewGuid().ToString("N"),
            FirstName = $"First{seq}",
            LastName = $"Last{seq}",
            Email = $"user{seq}@example.com",
            Department = Departments[rng.Next(Departments.Length)],
            City = Cities[rng.Next(Cities.Length)],
            Country = Countries[rng.Next(Countries.Length)],
            Status = Statuses[rng.Next(Statuses.Length)],
            Age = 22 + rng.Next(40),
            Seq = seq,
            Salary = 40000 + rng.NextDouble() * 160000,
            Score = Math.Round(rng.NextDouble() * 100, 2),
            Verified = rng.Next(2) == 1,
            Rating = 1 + rng.Next(5),
            Tags = TagSets[rng.Next(TagSets.Length)].ToList()
        };
    }

    public Employee CloneEmployee(Employee src)
    {
        return new Employee
        {
            Id = src.Id,
            FirstName = src.FirstName,
            LastName = src.LastName,
            Email = src.Email,
            Department = src.Department,
            City = src.City,
            Country = src.Country,
            Status = src.Status,
            Age = src.Age,
            Seq = src.Seq,
            Salary = src.Salary,
            Score = src.Score,
            Verified = src.Verified,
            Rating = src.Rating,
            Tags = src.Tags.ToList()
        };
    }

    // ── Report generation ─────────────────────────────────────────────

    private void GenerateReports()
    {
        List<TimingEntry> snapshot;
        lock (_lock)
        {
            snapshot = _timings.ToList();
        }

        if (snapshot.Count == 0) return;

        GenerateHTMLReport(snapshot);
        GenerateJSONReport(snapshot);
    }

    private static void GenerateHTMLReport(List<TimingEntry> timings)
    {
        // Group by category preserving order
        var catMap = new Dictionary<string, List<TimingEntry>>();
        var catOrder = new List<string>();
        foreach (var e in timings)
        {
            if (!catMap.ContainsKey(e.Category))
            {
                catMap[e.Category] = new();
                catOrder.Add(e.Category);
            }
            catMap[e.Category].Add(e);
        }

        // Compute summary
        int oxiWins = 0, pgWins = 0, ties = 0;
        foreach (var e in timings)
        {
            var durations = new (TimeSpan dur, string name)[]
            {
                (e.OxiDur, "OxiDB"), (e.PgDur, "PostgreSQL")
            };
            var valid = durations.Where(d => d.dur > TimeSpan.Zero).ToList();
            if (valid.Count < 2) { ties++; continue; }
            var min = valid.MinBy(d => d.dur);
            var minCount = valid.Count(d => d.dur == min.dur);
            if (minCount > 1) { ties++; continue; }
            switch (min.name)
            {
                case "OxiDB": oxiWins++; break;
                case "PostgreSQL": pgWins++; break;
            }
        }

        var sb = new StringBuilder();
        sb.Append($$"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>OxiDB vs PostgreSQL — EF Core Comparison Report</title>
            <style>
              :root { --bg: #0d1117; --card: #161b22; --border: #30363d; --text: #e6edf3; --dim: #8b949e; --green: #3fb950; --red: #f85149; --blue: #58a6ff; --yellow: #d29922; }
              * { margin: 0; padding: 0; box-sizing: border-box; }
              body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif; background: var(--bg); color: var(--text); padding: 24px; }
              .container { max-width: 1200px; margin: 0 auto; }
              h1 { font-size: 28px; font-weight: 700; margin-bottom: 8px; }
              .subtitle { color: var(--dim); font-size: 14px; margin-bottom: 32px; }
              .summary { display: flex; gap: 16px; margin-bottom: 32px; flex-wrap: wrap; }
              .summary-card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 20px 28px; flex: 1; min-width: 160px; text-align: center; }
              .summary-card .label { font-size: 13px; color: var(--dim); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; }
              .summary-card .value { font-size: 36px; font-weight: 700; }
              .summary-card .value.green { color: var(--green); }
              .summary-card .value.blue { color: var(--blue); }
              .summary-card .value.yellow { color: var(--yellow); }
              .category { background: var(--card); border: 1px solid var(--border); border-radius: 12px; margin-bottom: 24px; overflow: hidden; }
              .category h2 { font-size: 18px; font-weight: 600; padding: 16px 20px; border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 8px; }
              .category h2 .badge { font-size: 12px; background: var(--border); color: var(--dim); padding: 2px 8px; border-radius: 10px; font-weight: 400; }
              table { width: 100%; border-collapse: collapse; font-size: 14px; }
              th { text-align: left; padding: 10px 16px; color: var(--dim); font-weight: 500; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; border-bottom: 1px solid var(--border); }
              td { padding: 10px 16px; border-bottom: 1px solid var(--border); }
              tr:last-child td { border-bottom: none; }
              tr:hover { background: rgba(88,166,255,0.04); }
              .time { font-family: 'SF Mono', 'Fira Code', monospace; font-size: 13px; }
              .winner { font-weight: 600; }
              .oxi-win { color: var(--green); }
              .pg-win { color: var(--blue); }
              .tie { color: var(--dim); }
              .speedup { font-family: 'SF Mono', 'Fira Code', monospace; font-size: 12px; color: var(--yellow); }
              .bar-cell { width: 260px; }
              .bar-container { display: flex; height: 20px; border-radius: 4px; overflow: hidden; background: var(--border); }
              .bar-oxi { background: var(--green); min-width: 2px; }
              .bar-pg { background: var(--blue); min-width: 2px; }
              .footer { margin-top: 32px; text-align: center; color: var(--dim); font-size: 12px; padding: 16px; border-top: 1px solid var(--border); }
            </style>
            </head>
            <body>
            <div class="container">
            <h1>OxiDB vs PostgreSQL</h1>
            <p class="subtitle">EF Core comparison report &mdash; {{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}} UTC &mdash; {{timings.Count}} tests</p>

            <div class="summary">
              <div class="summary-card"><div class="label">Total Tests</div><div class="value blue">{{timings.Count}}</div></div>
              <div class="summary-card"><div class="label">OxiDB Wins</div><div class="value green">{{oxiWins}}</div></div>
              <div class="summary-card"><div class="label">PostgreSQL Wins</div><div class="value blue">{{pgWins}}</div></div>
              <div class="summary-card"><div class="label">Ties / N/A</div><div class="value yellow">{{ties}}</div></div>
            </div>

            """);

        foreach (var catName in catOrder)
        {
            var entries = catMap[catName];
            sb.AppendLine($"""
                <div class="category">
                <h2>{catName} <span class="badge">{entries.Count} tests</span></h2>
                <table>
                <tr><th>Operation</th><th>OxiDB</th><th>PostgreSQL</th><th>Speedup</th><th>Winner</th><th class="bar-cell">Comparison</th></tr>
                """);

            foreach (var e in entries)
            {
                var oxiStr = FmtDur(e.OxiDur);
                var pgStr = FmtDur(e.PgDur);

                var durations = new (TimeSpan dur, string name, string cls)[]
                {
                    (e.OxiDur, "OxiDB", "oxi-win"),
                    (e.PgDur, "PostgreSQL", "pg-win")
                };
                var valid = durations.Where(d => d.dur > TimeSpan.Zero).ToList();

                string winClass = "tie", winLabel = "-", speedupStr = "-";
                if (valid.Count >= 2)
                {
                    var min = valid.MinBy(d => d.dur);
                    var second = valid.Where(d => d.name != min.name).MinBy(d => d.dur);
                    var factor = (double)second.dur.Ticks / min.dur.Ticks;
                    speedupStr = $"{factor:F1}x";
                    winClass = min.cls;
                    winLabel = min.name;
                }

                // Bar chart
                var barHtml = "";
                if (valid.Count >= 2)
                {
                    var total = (double)(e.OxiDur.Ticks + e.PgDur.Ticks);
                    if (total > 0)
                    {
                        var oxiPct = e.OxiDur.Ticks / total * 100;
                        var pgPct = e.PgDur.Ticks / total * 100;
                        barHtml = $"""<div class="bar-container"><div class="bar-oxi" style="width:{oxiPct:F1}%" title="OxiDB: {oxiStr}"></div><div class="bar-pg" style="width:{pgPct:F1}%" title="PostgreSQL: {pgStr}"></div></div>""";
                    }
                }

                sb.AppendLine($"""
                    <tr>
                      <td>{e.Name}</td>
                      <td class="time">{oxiStr}</td>
                      <td class="time">{pgStr}</td>
                      <td class="speedup">{speedupStr}</td>
                      <td class="winner {winClass}">{winLabel}</td>
                      <td class="bar-cell">{barHtml}</td>
                    </tr>
                    """);
            }

            sb.AppendLine("</table>\n</div>");
        }

        sb.AppendLine($"""
            <div class="footer">Generated by OxiDB vs PostgreSQL EF Core Comparison Test Suite &mdash; {timings.Count} tests across {catOrder.Count} categories</div>
            </div>
            </body>
            </html>
            """);

        File.WriteAllText("report.html", sb.ToString());
        Console.WriteLine($"\n  HTML Report: report.html");
        Console.WriteLine($"  OxiDB wins: {oxiWins} | PostgreSQL wins: {pgWins} | Ties/N-A: {ties}");
    }

    private static void GenerateJSONReport(List<TimingEntry> timings)
    {
        var entries = timings.Select(e =>
        {
            string winner = "tie", speedup = "1.0x";
            var durations = new (TimeSpan dur, string name)[]
            {
                (e.OxiDur, "oxidb"), (e.PgDur, "postgresql")
            };
            var valid = durations.Where(d => d.dur > TimeSpan.Zero).ToList();
            if (valid.Count >= 2)
            {
                var min = valid.MinBy(d => d.dur);
                var second = valid.Where(d => d.name != min.name).MinBy(d => d.dur);
                winner = min.name;
                speedup = $"{(double)second.dur.Ticks / min.dur.Ticks:F1}x";
            }
            else if (valid.Count < 2)
            {
                winner = "n/a";
                speedup = "n/a";
            }

            return new
            {
                category = e.Category,
                name = e.Name,
                oxi_ms = e.OxiDur.TotalMilliseconds,
                pg_ms = e.PgDur.TotalMilliseconds,
                winner,
                speedup,
                oxi_count = e.OxiCount,
                pg_count = e.PgCount
            };
        });

        var report = new
        {
            total = timings.Count,
            oxi_wins = timings.Count(e => IsWinner(e, "oxi")),
            pg_wins = timings.Count(e => IsWinner(e, "pg")),
            timings = entries
        };

        var json = JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText("report.json", json);
    }

    private static bool IsWinner(TimingEntry e, string engine)
    {
        var durations = new (TimeSpan dur, string tag)[]
        {
            (e.OxiDur, "oxi"), (e.PgDur, "pg")
        };
        var valid = durations.Where(d => d.dur > TimeSpan.Zero).ToList();
        if (valid.Count < 2) return false;
        var min = valid.MinBy(d => d.dur);
        return min.tag == engine && valid.Count(d => d.dur == min.dur) == 1;
    }

    private static string FmtDur(TimeSpan d)
    {
        if (d <= TimeSpan.Zero) return "-";
        if (d.TotalMilliseconds < 1) return $"{d.TotalMicroseconds:F0}us";
        if (d.TotalSeconds < 1) return $"{d.TotalMilliseconds:F1}ms";
        return $"{d.TotalSeconds:F2}s";
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Test collection — ensures all tests share the fixture
// ═══════════════════════════════════════════════════════════════════════════

[CollectionDefinition("Comparison")]
public class ComparisonCollection : ICollectionFixture<ComparisonFixture> { }

// ═══════════════════════════════════════════════════════════════════════════
// Comparison tests
// ═══════════════════════════════════════════════════════════════════════════

[Collection("Comparison")]
public class ComparisonTests
{
    private readonly ComparisonFixture _f;
    private readonly ITestOutputHelper _output;

    public ComparisonTests(ComparisonFixture fixture, ITestOutputHelper output)
    {
        _f = fixture;
        _output = output;
    }

    // ── Helper ──────────────────────────────────────────────────────

    private async Task<(TimeSpan oxi, TimeSpan pg)> Time2(
        Func<BenchmarkDbContext, Task> action)
    {
        var sw = Stopwatch.StartNew();

        sw.Restart();
        await action(_f.OxiContext);
        var oxiDur = sw.Elapsed;

        sw.Restart();
        await action(_f.PgContext);
        var pgDur = sw.Elapsed;

        return (oxiDur, pgDur);
    }

    private void Log(string category, string name, TimeSpan oxi, TimeSpan pg,
        int oxiCount = 0, int pgCount = 0)
    {
        _f.RecordTiming(category, name, oxi, pg, oxiCount, pgCount);
        _output.WriteLine($"[{category}] {name}: OxiDB={oxi.TotalMilliseconds:F2}ms PG={pg.TotalMilliseconds:F2}ms");
    }

    // ═════════════════════════════════════════════════════════════════
    // 1. CRUD Tests
    // ═════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CRUD_01_Insert()
    {
        var emp = _f.GenerateEmployee(100_001);
        var (oxi, pg) = await Time2(async ctx =>
        {
            var e = _f.CloneEmployee(emp);
            e.Id = Guid.NewGuid().ToString("N");
            ctx.Employees.Add(e);
            await ctx.SaveChangesAsync();
            ctx.ChangeTracker.Clear();
        });
        Log("CRUD", "Insert single", oxi, pg);
    }

    [Fact]
    public async Task CRUD_02_InsertMany()
    {
        const int count = 500;
        var emps = Enumerable.Range(200_000, count).Select(i => _f.GenerateEmployee(i)).ToList();

        var (oxi, pg) = await Time2(async ctx =>
        {
            foreach (var e in emps)
            {
                var clone = _f.CloneEmployee(e);
                clone.Id = Guid.NewGuid().ToString("N");
                ctx.Employees.Add(clone);
            }
            await ctx.SaveChangesAsync();
            ctx.ChangeTracker.Clear();
        });
        Log("CRUD", $"InsertMany ({count})", oxi, pg, count, count);
    }

    [Fact]
    public async Task CRUD_03_Count()
    {
        int oxiCount = 0, pgCount = 0;
        var sw = Stopwatch.StartNew();

        sw.Restart();
        oxiCount = await _f.OxiContext.Employees.CountAsync();
        var oxiDur = sw.Elapsed;

        sw.Restart();
        pgCount = await _f.PgContext.Employees.CountAsync();
        var pgDur = sw.Elapsed;

        Log("CRUD", "Count", oxiDur, pgDur, oxiCount, pgCount);
    }

    [Fact]
    public async Task CRUD_04_FindById()
    {
        // Insert a known employee to both
        var emp = _f.GenerateEmployee(300_001);
        var id = emp.Id;

        foreach (var ctx in new[] { _f.OxiContext, _f.PgContext })
        {
            var clone = _f.CloneEmployee(emp);
            ctx.Employees.Add(clone);
            await ctx.SaveChangesAsync();
            ctx.ChangeTracker.Clear();
        }

        var sw = Stopwatch.StartNew();

        sw.Restart();
        var oxiEmp = await _f.OxiContext.Employees.FindAsync(id);
        var oxiDur = sw.Elapsed;

        sw.Restart();
        var pgEmp = await _f.PgContext.Employees.FindAsync(id);
        var pgDur = sw.Elapsed;

        _f.OxiContext.ChangeTracker.Clear();
        _f.PgContext.ChangeTracker.Clear();

        Log("CRUD", "FindById", oxiDur, pgDur);
    }

    [Fact]
    public async Task CRUD_05_Update()
    {
        // Insert employee to update
        var emp = _f.GenerateEmployee(400_001);
        foreach (var ctx in new[] { _f.OxiContext, _f.PgContext })
        {
            ctx.Employees.Add(_f.CloneEmployee(emp));
            await ctx.SaveChangesAsync();
            ctx.ChangeTracker.Clear();
        }

        var sw = Stopwatch.StartNew();

        sw.Restart();
        var oxiEmp = await _f.OxiContext.Employees.FindAsync(emp.Id);
        if (oxiEmp != null) { oxiEmp.Salary = 150000; await _f.OxiContext.SaveChangesAsync(); }
        var oxiDur = sw.Elapsed;

        sw.Restart();
        var pgEmp = await _f.PgContext.Employees.FindAsync(emp.Id);
        if (pgEmp != null) { pgEmp.Salary = 150000; await _f.PgContext.SaveChangesAsync(); }
        var pgDur = sw.Elapsed;

        _f.OxiContext.ChangeTracker.Clear();
        _f.PgContext.ChangeTracker.Clear();

        Log("CRUD", "Update", oxiDur, pgDur);
    }

    [Fact]
    public async Task CRUD_06_Delete()
    {
        var emp = _f.GenerateEmployee(500_001);
        foreach (var ctx in new[] { _f.OxiContext, _f.PgContext })
        {
            ctx.Employees.Add(_f.CloneEmployee(emp));
            await ctx.SaveChangesAsync();
            ctx.ChangeTracker.Clear();
        }

        var sw = Stopwatch.StartNew();

        sw.Restart();
        var oxiEmp = await _f.OxiContext.Employees.FindAsync(emp.Id);
        if (oxiEmp != null) { _f.OxiContext.Employees.Remove(oxiEmp); await _f.OxiContext.SaveChangesAsync(); }
        var oxiDur = sw.Elapsed;

        sw.Restart();
        var pgEmp = await _f.PgContext.Employees.FindAsync(emp.Id);
        if (pgEmp != null) { _f.PgContext.Employees.Remove(pgEmp); await _f.PgContext.SaveChangesAsync(); }
        var pgDur = sw.Elapsed;

        _f.OxiContext.ChangeTracker.Clear();
        _f.PgContext.ChangeTracker.Clear();

        Log("CRUD", "Delete", oxiDur, pgDur);
    }

    // ═════════════════════════════════════════════════════════════════
    // 2. Query Pattern Tests
    // ═════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_01_ExactMatch()
    {
        int oxiCount = 0, pgCount = 0;
        var sw = Stopwatch.StartNew();

        sw.Restart();
        oxiCount = await _f.OxiContext.Employees.Where(e => e.Department == "Engineering").CountAsync();
        var oxiDur = sw.Elapsed;

        sw.Restart();
        pgCount = await _f.PgContext.Employees.Where(e => e.Department == "Engineering").CountAsync();
        var pgDur = sw.Elapsed;

        Log("Query", "Exact match (Department=Engineering)", oxiDur, pgDur, oxiCount, pgCount);
    }

    [Fact]
    public async Task Query_02_CompoundFilter()
    {
        int oxiCount = 0, pgCount = 0;
        var sw = Stopwatch.StartNew();

        sw.Restart();
        oxiCount = await _f.OxiContext.Employees
            .Where(e => e.Department == "Engineering" && e.Status == "active")
            .CountAsync();
        var oxiDur = sw.Elapsed;

        sw.Restart();
        pgCount = await _f.PgContext.Employees
            .Where(e => e.Department == "Engineering" && e.Status == "active")
            .CountAsync();
        var pgDur = sw.Elapsed;

        Log("Query", "Compound (Dept=Eng AND Status=active)", oxiDur, pgDur, oxiCount, pgCount);
    }

    [Fact]
    public async Task Query_03_RangeQuery()
    {
        int oxiCount = 0, pgCount = 0;
        var sw = Stopwatch.StartNew();

        sw.Restart();
        oxiCount = await _f.OxiContext.Employees
            .Where(e => e.Age >= 30 && e.Age <= 40)
            .CountAsync();
        var oxiDur = sw.Elapsed;

        sw.Restart();
        pgCount = await _f.PgContext.Employees
            .Where(e => e.Age >= 30 && e.Age <= 40)
            .CountAsync();
        var pgDur = sw.Elapsed;

        Log("Query", "Range (30 <= Age <= 40)", oxiDur, pgDur, oxiCount, pgCount);
    }

    [Fact]
    public async Task Query_04_OrQuery()
    {
        int oxiCount = 0, pgCount = 0;
        var sw = Stopwatch.StartNew();

        sw.Restart();
        oxiCount = await _f.OxiContext.Employees
            .Where(e => e.City == "New York" || e.City == "London")
            .CountAsync();
        var oxiDur = sw.Elapsed;

        sw.Restart();
        pgCount = await _f.PgContext.Employees
            .Where(e => e.City == "New York" || e.City == "London")
            .CountAsync();
        var pgDur = sw.Elapsed;

        Log("Query", "OR (City=NY or London)", oxiDur, pgDur, oxiCount, pgCount);
    }

    [Fact]
    public async Task Query_05_ContainsIn()
    {
        var cities = new List<string> { "Tokyo", "Berlin", "Paris" };
        int oxiCount = 0, pgCount = 0;
        var sw = Stopwatch.StartNew();

        sw.Restart();
        oxiCount = await _f.OxiContext.Employees
            .Where(e => cities.Contains(e.City))
            .CountAsync();
        var oxiDur = sw.Elapsed;

        sw.Restart();
        pgCount = await _f.PgContext.Employees
            .Where(e => cities.Contains(e.City))
            .CountAsync();
        var pgDur = sw.Elapsed;

        Log("Query", "Contains/In (3 cities)", oxiDur, pgDur, oxiCount, pgCount);
    }

    [Fact]
    public async Task Query_06_NotEqual()
    {
        int oxiCount = 0, pgCount = 0;
        var sw = Stopwatch.StartNew();

        sw.Restart();
        oxiCount = await _f.OxiContext.Employees
            .Where(e => e.Status != "inactive")
            .CountAsync();
        var oxiDur = sw.Elapsed;

        sw.Restart();
        pgCount = await _f.PgContext.Employees
            .Where(e => e.Status != "inactive")
            .CountAsync();
        var pgDur = sw.Elapsed;

        Log("Query", "Not-equal (Status != inactive)", oxiDur, pgDur, oxiCount, pgCount);
    }

    [Fact]
    public async Task Query_07_SortedQuery()
    {
        var sw = Stopwatch.StartNew();

        sw.Restart();
        var oxiResult = await _f.OxiContext.Employees
            .OrderByDescending(e => e.Salary)
            .Take(10)
            .ToListAsync();
        var oxiDur = sw.Elapsed;

        sw.Restart();
        var pgResult = await _f.PgContext.Employees
            .OrderByDescending(e => e.Salary)
            .Take(10)
            .ToListAsync();
        var pgDur = sw.Elapsed;

        Log("Query", "Sort desc + Take(10)", oxiDur, pgDur, oxiResult.Count, pgResult.Count);
    }

    [Fact]
    public async Task Query_08_BoolFilter()
    {
        int oxiCount = 0, pgCount = 0;
        var sw = Stopwatch.StartNew();

        sw.Restart();
        oxiCount = await _f.OxiContext.Employees
            .Where(e => e.Verified)
            .CountAsync();
        var oxiDur = sw.Elapsed;

        sw.Restart();
        pgCount = await _f.PgContext.Employees
            .Where(e => e.Verified)
            .CountAsync();
        var pgDur = sw.Elapsed;

        Log("Query", "Bool filter (Verified=true)", oxiDur, pgDur, oxiCount, pgCount);
    }

    [Fact]
    public async Task Query_09_GreaterThan()
    {
        int oxiCount = 0, pgCount = 0;
        var sw = Stopwatch.StartNew();

        sw.Restart();
        oxiCount = await _f.OxiContext.Employees
            .Where(e => e.Salary > 100000)
            .CountAsync();
        var oxiDur = sw.Elapsed;

        sw.Restart();
        pgCount = await _f.PgContext.Employees
            .Where(e => e.Salary > 100000)
            .CountAsync();
        var pgDur = sw.Elapsed;

        Log("Query", "Greater than (Salary > 100K)", oxiDur, pgDur, oxiCount, pgCount);
    }

    [Fact]
    public async Task Query_10_SkipTake()
    {
        var sw = Stopwatch.StartNew();

        sw.Restart();
        var oxiResult = await _f.OxiContext.Employees
            .OrderBy(e => e.Seq)
            .Skip(10)
            .Take(20)
            .ToListAsync();
        var oxiDur = sw.Elapsed;

        sw.Restart();
        var pgResult = await _f.PgContext.Employees
            .OrderBy(e => e.Seq)
            .Skip(10)
            .Take(20)
            .ToListAsync();
        var pgDur = sw.Elapsed;

        Log("Query", "Skip(10) + Take(20)", oxiDur, pgDur, oxiResult.Count, pgResult.Count);
    }

    // ═════════════════════════════════════════════════════════════════
    // 3. Aggregation Tests
    // ═════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Agg_01_GroupByCount()
    {
        var sw = Stopwatch.StartNew();

        sw.Restart();
        var oxiResult = await _f.OxiContext.Employees
            .GroupBy(e => e.Department)
            .Select(g => new { Dept = g.Key, Count = g.Count() })
            .ToListAsync();
        var oxiDur = sw.Elapsed;

        sw.Restart();
        var pgResult = await _f.PgContext.Employees
            .GroupBy(e => e.Department)
            .Select(g => new { Dept = g.Key, Count = g.Count() })
            .ToListAsync();
        var pgDur = sw.Elapsed;

        Log("Aggregation", "GroupBy Department + Count", oxiDur, pgDur,
            oxiResult.Count, pgResult.Count);
    }

    [Fact]
    public async Task Agg_02_Average()
    {
        var sw = Stopwatch.StartNew();

        sw.Restart();
        var oxiAvg = await _f.OxiContext.Employees
            .Select(e => (double?)e.Salary)
            .AverageAsync();
        var oxiDur = sw.Elapsed;

        sw.Restart();
        var pgAvg = await _f.PgContext.Employees
            .Select(e => (double?)e.Salary)
            .AverageAsync();
        var pgDur = sw.Elapsed;

        Log("Aggregation", "Average salary", oxiDur, pgDur);
    }

    [Fact]
    public async Task Agg_03_MinMax()
    {
        var sw = Stopwatch.StartNew();

        sw.Restart();
        var oxiMin = await _f.OxiContext.Employees.MinAsync(e => e.Age);
        var oxiMax = await _f.OxiContext.Employees.MaxAsync(e => e.Age);
        var oxiDur = sw.Elapsed;

        sw.Restart();
        var pgMin = await _f.PgContext.Employees.MinAsync(e => e.Age);
        var pgMax = await _f.PgContext.Employees.MaxAsync(e => e.Age);
        var pgDur = sw.Elapsed;

        Log("Aggregation", "Min/Max age", oxiDur, pgDur);
    }

    [Fact]
    public async Task Agg_04_TopN()
    {
        var sw = Stopwatch.StartNew();

        sw.Restart();
        var oxiResult = await _f.OxiContext.Employees
            .OrderByDescending(e => e.Score)
            .Take(5)
            .ToListAsync();
        var oxiDur = sw.Elapsed;

        sw.Restart();
        var pgResult = await _f.PgContext.Employees
            .OrderByDescending(e => e.Score)
            .Take(5)
            .ToListAsync();
        var pgDur = sw.Elapsed;

        Log("Aggregation", "Top 5 by score", oxiDur, pgDur,
            oxiResult.Count, pgResult.Count);
    }

    [Fact]
    public async Task Agg_05_FilteredCount()
    {
        int oxiCount = 0, pgCount = 0;
        var sw = Stopwatch.StartNew();

        sw.Restart();
        oxiCount = await _f.OxiContext.Employees
            .Where(e => e.Rating >= 4 && e.Verified)
            .CountAsync();
        var oxiDur = sw.Elapsed;

        sw.Restart();
        pgCount = await _f.PgContext.Employees
            .Where(e => e.Rating >= 4 && e.Verified)
            .CountAsync();
        var pgDur = sw.Elapsed;

        Log("Aggregation", "Filtered count (Rating>=4 AND Verified)", oxiDur, pgDur, oxiCount, pgCount);
    }

    // ═════════════════════════════════════════════════════════════════
    // 4. Bulk Operations
    // ═════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Bulk_01_Insert10K()
    {
        const int count = 10_000;
        var emps = Enumerable.Range(600_000, count).Select(i => _f.GenerateEmployee(i)).ToList();

        var (oxi, pg) = await Time2(async ctx =>
        {
            foreach (var e in emps)
            {
                var clone = _f.CloneEmployee(e);
                clone.Id = Guid.NewGuid().ToString("N");
                ctx.Employees.Add(clone);
            }
            await ctx.SaveChangesAsync();
            ctx.ChangeTracker.Clear();
        });
        Log("Bulk", $"Insert {count}", oxi, pg, count, count);
    }

    [Fact]
    public async Task Bulk_02_BulkDelete()
    {
        // Insert 500, then delete all with Status=inactive
        var emps = Enumerable.Range(700_000, 500).Select(i =>
        {
            var e = _f.GenerateEmployee(i);
            e.Status = "inactive";
            return e;
        }).ToList();

        foreach (var ctx in new[] { _f.OxiContext, _f.PgContext })
        {
            foreach (var e in emps)
            {
                var clone = _f.CloneEmployee(e);
                clone.Id = Guid.NewGuid().ToString("N");
                ctx.Employees.Add(clone);
            }
            await ctx.SaveChangesAsync();
            ctx.ChangeTracker.Clear();
        }

        int oxiCount = 0, pgCount = 0;
        var sw = Stopwatch.StartNew();

        sw.Restart();
        var oxiToDelete = await _f.OxiContext.Employees.Where(e => e.Status == "inactive").ToListAsync();
        oxiCount = oxiToDelete.Count;
        _f.OxiContext.Employees.RemoveRange(oxiToDelete);
        await _f.OxiContext.SaveChangesAsync();
        _f.OxiContext.ChangeTracker.Clear();
        var oxiDur = sw.Elapsed;

        sw.Restart();
        pgCount = await _f.PgContext.Employees.Where(e => e.Status == "inactive").ExecuteDeleteAsync();
        var pgDur = sw.Elapsed;

        Log("Bulk", "Bulk delete (Status=inactive)", oxiDur, pgDur, oxiCount, pgCount);
    }

    [Fact]
    public async Task Bulk_03_BulkUpdate()
    {
        int oxiCount = 0, pgCount = 0;
        var sw = Stopwatch.StartNew();

        sw.Restart();
        var oxiToUpdate = await _f.OxiContext.Employees.Where(e => e.Department == "Sales").ToListAsync();
        oxiCount = oxiToUpdate.Count;
        foreach (var e in oxiToUpdate) e.Rating = 5;
        await _f.OxiContext.SaveChangesAsync();
        _f.OxiContext.ChangeTracker.Clear();
        var oxiDur = sw.Elapsed;

        sw.Restart();
        pgCount = await _f.PgContext.Employees
            .Where(e => e.Department == "Sales")
            .ExecuteUpdateAsync(s => s.SetProperty(e => e.Rating, 5));
        var pgDur = sw.Elapsed;

        Log("Bulk", "Bulk update (Sales.Rating=5)", oxiDur, pgDur, oxiCount, pgCount);
    }

    // ═════════════════════════════════════════════════════════════════
    // 5. Transaction Tests
    // ═════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Tx_01_Commit()
    {
        var sw = Stopwatch.StartNew();

        sw.Restart();
        using (var txn = await _f.PgContext.Database.BeginTransactionAsync())
        {
            var pgEmp = _f.GenerateEmployee(800_001);
            pgEmp.Id = Guid.NewGuid().ToString("N");
            _f.PgContext.Employees.Add(pgEmp);
            await _f.PgContext.SaveChangesAsync();
            await txn.CommitAsync();
        }
        var pgDur = sw.Elapsed;
        _f.PgContext.ChangeTracker.Clear();

        // OxiDB — transactions through EF Core not yet supported, record as N/A
        sw.Restart();
        var oxiDur = sw.Elapsed;
        Log("Transactions", "Commit (insert in tx)", oxiDur, pgDur);
    }

    [Fact]
    public async Task Tx_02_Rollback()
    {
        var initialPgCount = await _f.PgContext.Employees.CountAsync();

        var sw = Stopwatch.StartNew();

        sw.Restart();
        using (var txn = await _f.PgContext.Database.BeginTransactionAsync())
        {
            var pgEmp = _f.GenerateEmployee(900_001);
            pgEmp.Id = Guid.NewGuid().ToString("N");
            _f.PgContext.Employees.Add(pgEmp);
            await _f.PgContext.SaveChangesAsync();
            await txn.RollbackAsync();
        }
        var pgDur = sw.Elapsed;
        _f.PgContext.ChangeTracker.Clear();

        sw.Restart();
        var oxiDur = sw.Elapsed; // placeholder
        Log("Transactions", "Rollback (insert + rollback)", oxiDur, pgDur);

        // Verify rollback
        var afterPgCount = await _f.PgContext.Employees.CountAsync();
        Assert.Equal(initialPgCount, afterPgCount);
    }

    // ═════════════════════════════════════════════════════════════════
    // 6. Concurrent Tests
    // ═════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Concurrent_01_ParallelReads()
    {
        const int parallelism = 20;
        var sw = Stopwatch.StartNew();

        // PostgreSQL parallel reads
        sw.Restart();
        await Parallel.ForAsync(0, parallelism, async (i, ct) =>
        {
            await using var ctx = CreatePgContext();
            await ctx.Employees.CountAsync(ct);
        });
        var pgDur = sw.Elapsed;

        // OxiDB: single connection, sequential for now
        sw.Restart();
        for (int i = 0; i < parallelism; i++)
        {
            await _f.OxiContext.Employees.CountAsync();
        }
        var oxiDur = sw.Elapsed;

        Log("Concurrent", $"{parallelism} parallel reads", oxiDur, pgDur, parallelism, parallelism);
    }

    [Fact]
    public async Task Concurrent_02_MixedReadWrite()
    {
        const int ops = 100;
        var sw = Stopwatch.StartNew();

        // PostgreSQL mixed R/W
        sw.Restart();
        await Parallel.ForAsync(0, ops, async (i, ct) =>
        {
            await using var ctx = CreatePgContext();
            if (i % 3 == 0)
            {
                var emp = _f.GenerateEmployee(1_000_000 + i);
                emp.Id = Guid.NewGuid().ToString("N");
                ctx.Employees.Add(emp);
                await ctx.SaveChangesAsync(ct);
            }
            else
            {
                await ctx.Employees.CountAsync(ct);
            }
        });
        var pgDur = sw.Elapsed;

        // OxiDB: sequential mixed
        sw.Restart();
        for (int i = 0; i < ops; i++)
        {
            if (i % 3 == 0)
            {
                var emp = _f.GenerateEmployee(1_200_000 + i);
                emp.Id = Guid.NewGuid().ToString("N");
                _f.OxiContext.Employees.Add(emp);
                await _f.OxiContext.SaveChangesAsync();
                _f.OxiContext.ChangeTracker.Clear();
            }
            else
            {
                await _f.OxiContext.Employees.CountAsync();
            }
        }
        var oxiDur = sw.Elapsed;

        Log("Concurrent", $"{ops} mixed R/W", oxiDur, pgDur, ops, ops);
    }

    // ── Context factory for parallel tests ────────────────────────

    private static BenchmarkDbContext CreatePgContext()
    {
        var options = new DbContextOptionsBuilder<BenchmarkDbContext>()
            .UseNpgsql("Host=127.0.0.1;Port=5433;Database=bench;Username=bench;Password=bench")
            .Options;
        return new BenchmarkDbContext(options);
    }
}
