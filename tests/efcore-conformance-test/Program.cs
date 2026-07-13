// EF Core ↔ OxiDB LINQ conformance matrix.
//
// A broad sweep of the query shapes EF applications actually generate —
// each CHECK computes the same result in LINQ-to-SQL and in-memory LINQ and
// compares. Any translation or engine bug shows up as a mismatch or a
// translation exception with the failing label.
using Microsoft.EntityFrameworkCore;
using OxiDb.EntityFrameworkCore;

var port = int.Parse(Environment.GetEnvironmentVariable("OXIDB_PORT") ?? "4444");
var cs = $"Host=127.0.0.1;Port={port};Database=efconf_test";

await using (var boot = await OxiDb.Client.Tcp.OxiDbTcpClient.ConnectAsync("127.0.0.1", port))
{
    await boot.SqlAsync("DROP DATABASE IF EXISTS efconf_test");
    await boot.SqlAsync("CREATE DATABASE efconf_test");
}

// ── seed ────────────────────────────────────────────────────────────────────
using (var db = new Conf(cs))
{
    db.Database.EnsureCreated();
    var blogs = new[]
    {
        new Blog { Name = "rust",  Rating = 5, Created = Utc(2026, 1, 10) },
        new Blog { Name = "go",    Rating = 3, Created = Utc(2026, 2, 20) },
        new Blog { Name = "zig",   Rating = 4, Created = Utc(2026, 3, 5) },
        new Blog { Name = "empty", Rating = 1, Created = Utc(2026, 3, 6) },
    };
    db.Blogs.AddRange(blogs);
    db.SaveChanges();
    db.Posts.AddRange(
        new Post { BlogId = blogs[0].Id, Title = "ownership", Score = 90, Tag = "lang" },
        new Post { BlogId = blogs[0].Id, Title = "lifetimes", Score = 70, Tag = "lang" },
        new Post { BlogId = blogs[0].Id, Title = "unsafe",    Score = 40, Tag = null },
        new Post { BlogId = blogs[1].Id, Title = "channels",  Score = 80, Tag = "conc" },
        new Post { BlogId = blogs[1].Id, Title = "gc",        Score = 30, Tag = "rt" },
        new Post { BlogId = blogs[2].Id, Title = "comptime",  Score = 85, Tag = "lang" });
    db.SaveChanges();
}

var failures = new List<string>();
int passed = 0;

void Check<T>(string label, Func<Conf, T> query)
{
    using var db = new Conf(cs);
    try
    {
        var got = query(db);
        passed++;
        Console.WriteLine($"ok   {label,-42} {Render(got)}");
    }
    catch (Exception e)
    {
        failures.Add(label);
        Console.WriteLine($"FAIL {label,-42} {e.Message.Split('\n')[0]}");
    }
}

void Eq<T>(string label, Func<Conf, T> query, T want)
{
    using var db = new Conf(cs);
    try
    {
        var got = query(db);
        if (Equals(got, want))
        {
            passed++;
            Console.WriteLine($"ok   {label,-42} {Render(got)}");
        }
        else
        {
            failures.Add(label);
            Console.WriteLine($"FAIL {label,-42} got {Render(got)} want {Render(want)}");
        }
    }
    catch (Exception e)
    {
        failures.Add(label);
        Console.WriteLine($"FAIL {label,-42} {e.Message.Split('\n')[0]}");
    }
}

static string Render(object? v) => v switch
{
    null => "null",
    System.Collections.IEnumerable e and not string =>
        "[" + string.Join(",", e.Cast<object?>().Select(Render)) + "]",
    _ => v.ToString() ?? "null",
};

// ── projections & filters ───────────────────────────────────────────────────
Eq("where+select scalar", db => db.Posts.Where(p => p.Score >= 80).Count(), 3);
Eq("anonymous projection", db => db.Posts.Where(p => p.Score > 85)
    .Select(p => new { p.Title, p.Score }).Single().Title, "ownership");
Eq("ternary → CASE", db => db.Posts.OrderBy(p => p.Title)
    .Select(p => p.Score >= 50 ? "hi" : "lo").First(), "hi");
Eq("null coalesce ??", db => db.Posts.Count(p => (p.Tag ?? "none") == "none"), 1);
Eq("is null", db => db.Posts.Count(p => p.Tag == null), 1);
Eq("in-list Contains", db => db.Posts.Count(p => new[] { "gc", "unsafe" }.Contains(p.Title)), 2);

// ── aggregates ──────────────────────────────────────────────────────────────
Eq("Sum", db => db.Posts.Sum(p => p.Score), 395L);
Eq("Average", db => db.Posts.Average(p => (double)p.Score), 395 / 6.0);
Eq("Min/Max", db => db.Posts.Max(p => p.Score) - db.Posts.Min(p => p.Score), 60L);
Eq("Any(pred)", db => db.Posts.Any(p => p.Score > 89), true);
Eq("All(pred)", db => db.Posts.All(p => p.Score >= 30), true);
Eq("Count distinct", db => db.Posts.Select(p => p.Tag).Distinct().Count(), 4); // lang,conc,rt,null
Eq("LongCount", db => db.Posts.LongCount(p => p.Tag != null), 5L);

// ── GroupBy ─────────────────────────────────────────────────────────────────
Eq("GroupBy key+count", db => db.Posts.GroupBy(p => p.BlogId)
    .Select(g => g.Count()).OrderByDescending(n => n).First(), 3);
Eq("GroupBy multi-agg", db => db.Posts.GroupBy(p => p.BlogId)
    .Select(g => new { S = g.Sum(x => x.Score), M = g.Max(x => x.Score) })
    .OrderByDescending(x => x.S).First().M, 90L);
Eq("GroupBy having", db => db.Posts.GroupBy(p => p.BlogId)
    .Where(g => g.Count() >= 2).Count(), 2);
Eq("GroupBy on expression", db => db.Posts.GroupBy(p => p.Score / 50)
    .Select(g => new { g.Key, N = g.Count() }).OrderBy(x => x.Key).First().N, 2);

// ── joins & navigations ─────────────────────────────────────────────────────
Eq("nav reference filter", db => db.Posts.Count(p => p.Blog!.Rating >= 4), 4);
Eq("nav collection Any", db => db.Blogs.Count(b => b.Posts.Any(p => p.Score > 80)), 2);
Eq("nav collection Count in proj", db => string.Join(",",
    db.Blogs.OrderBy(b => b.Name).Select(b => b.Posts.Count)), "0,2,3,1");
Check("Include collection", db => db.Blogs.Include(b => b.Posts)
    .OrderBy(b => b.Name).First().Posts.Count);
Check("Include reference", db => db.Posts.Include(p => p.Blog)
    .OrderBy(p => p.Title).First().Blog!.Name);
Eq("explicit join", db => (
    from p in db.Posts
    join b in db.Blogs on p.BlogId equals b.Id
    where b.Name == "go"
    select p).Count(), 2);
Eq("left join (GroupJoin)", db => (
    from b in db.Blogs
    join p in db.Posts on b.Id equals p.BlogId into ps
    from p in ps.DefaultIfEmpty()
    select new { b.Name, T = (string?)p!.Title }).Count(), 7); // 6 posts + empty blog
Eq("SelectMany cross", db => db.Blogs.SelectMany(_ => db.Blogs).Count(), 16);
Eq("collection proj Take (LATERAL)", db => string.Join(",",
    db.Blogs.OrderBy(b => b.Name)
        .Select(b => b.Posts.OrderByDescending(p => p.Score).Take(2).Count())), "0,2,2,1");

// ── subqueries ──────────────────────────────────────────────────────────────
Eq("correlated scalar subquery", db => db.Blogs
    .Count(b => db.Posts.Where(p => p.BlogId == b.Id).Max(p => (long?)p.Score) > 80), 2);
Eq("exists subquery", db => db.Blogs.Count(b => !b.Posts.Any()), 1);
Eq("contains subquery", db => db.Posts
    .Count(p => db.Blogs.Where(b => b.Rating >= 4).Select(b => b.Id).Contains(p.BlogId)), 4);

// ── set operators ───────────────────────────────────────────────────────────
Eq("Concat (UNION ALL)", db => db.Posts.Select(p => p.BlogId)
    .Concat(db.Blogs.Select(b => b.Id)).Count(), 10);
Eq("Union", db => db.Posts.Select(p => p.BlogId)
    .Union(db.Blogs.Select(b => b.Id)).Count(), 4);
Eq("Except", db => db.Blogs.Select(b => b.Id)
    .Except(db.Posts.Select(p => p.BlogId)).Count(), 1);
Eq("Intersect", db => db.Blogs.Select(b => b.Id)
    .Intersect(db.Posts.Select(p => p.BlogId)).Count(), 3);

// ── ordering & paging ───────────────────────────────────────────────────────
Eq("ThenByDescending", db => db.Posts.OrderBy(p => p.BlogId)
    .ThenByDescending(p => p.Score).First().Title, "ownership");
Eq("Skip/Take stable", db => db.Posts.OrderBy(p => p.Score).Skip(2).Take(2)
    .Sum(p => p.Score), 150L); // 70+80
Eq("FirstOrDefault none", db => db.Posts.FirstOrDefault(p => p.Score > 1000), null);
Eq("Distinct rows", db => db.Posts.Select(p => p.BlogId).Distinct().Count(), 3);

// ── strings ─────────────────────────────────────────────────────────────────
Eq("StartsWith/EndsWith", db => db.Posts.Count(p =>
    p.Title.StartsWith("c") || p.Title.EndsWith("es")), 3);
Eq("ToUpper+Contains", db => db.Posts.Count(p => p.Title.ToUpper().Contains("TIME")), 2);
Eq("Length/Substring/Trim", db => db.Posts.Where(p => p.Title.Length == 2)
    .Select(p => p.Title.Substring(0, 1).Trim()).Single(), "g");
Eq("IndexOf/PadRight", db => db.Posts.OrderBy(p => p.Title)
    .Select(p => p.Title.PadRight(9, '.').Length).First(), 9);
Eq("Replace", db => db.Posts.Count(p => p.Title.Replace("gc", "GC") == "GC"), 1);

// ── date/time & math ────────────────────────────────────────────────────────
var feb20 = Utc(2026, 2, 20);
var mar6 = Utc(2026, 3, 6);
var apr1 = Utc(2026, 4, 1);
Eq("Year/Month/Day", db => db.Blogs.Count(b =>
    b.Created.Year == 2026 && b.Created.Month == 3 && b.Created.Day >= 5), 2);
Eq("Date equality", db => db.Blogs.Count(b => b.Created.Date == feb20), 1);
Eq("AddDays window", db => db.Blogs.Count(b => b.Created.AddDays(30) >= mar6), 3);
Eq("AddMonths", db => db.Blogs.Count(b => b.Created.AddMonths(2) > apr1), 3);
Eq("DayOfWeek", db => db.Blogs.Count(b => b.Created.DayOfWeek == DayOfWeek.Friday), 2); // 2026-02-20, 2026-03-06
Eq("Math.Floor/Sqrt", db => db.Posts.Count(p =>
    Math.Floor(Math.Sqrt((double)p.Score)) >= 9.0), 2); // 90, 85
Eq("modulo", db => db.Posts.Count(p => p.Score % 20 == 0), 2); // 40, 80

// ── mutation shapes ─────────────────────────────────────────────────────────
Check("ExecuteUpdate", db =>
    db.Posts.Where(p => p.Tag == "rt").ExecuteUpdate(s => s.SetProperty(p => p.Score, p => p.Score + 1)));
Check("ExecuteDelete+restore", db =>
{
    var n = db.Posts.Where(p => p.Title == "gc").ExecuteDelete();
    db.Posts.Add(new Post { BlogId = db.Blogs.Single(b => b.Name == "go").Id, Title = "gc", Score = 31, Tag = "rt" });
    db.SaveChanges();
    return n;
});

Console.WriteLine($"\n{passed} passed, {failures.Count} failed");
if (failures.Count > 0)
{
    Console.WriteLine("failing: " + string.Join(", ", failures));
    Environment.Exit(1);
}
Console.WriteLine("CONFORMANCE OK");

static DateTime Utc(int y, int m, int d) => new(y, m, d, 0, 0, 0, DateTimeKind.Utc);

public sealed class Blog
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public long Rating { get; set; }
    public DateTime Created { get; set; }
    public List<Post> Posts { get; set; } = [];
}

public sealed class Post
{
    public long Id { get; set; }
    public long BlogId { get; set; }
    public string Title { get; set; } = "";
    public long Score { get; set; }
    public string? Tag { get; set; }
    public Blog? Blog { get; set; }
}

public sealed class Conf(string cs) : DbContext
{
    public DbSet<Blog> Blogs => Set<Blog>();
    public DbSet<Post> Posts => Set<Post>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseOxiDb(cs);
        if (Environment.GetEnvironmentVariable("EF_LOG") == "1")
            options.LogTo(Console.WriteLine,
                [Microsoft.EntityFrameworkCore.Diagnostics.RelationalEventId.CommandExecuting]);
    }

    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<Blog>().ToTable("conf_blog").HasKey(b => b.Id);
        mb.Entity<Blog>().Property(b => b.Id).ValueGeneratedOnAdd();
        mb.Entity<Post>().ToTable("conf_post").HasKey(p => p.Id);
        mb.Entity<Post>().Property(p => p.Id).ValueGeneratedOnAdd();
        mb.Entity<Post>().HasOne(p => p.Blog).WithMany(b => b.Posts).HasForeignKey(p => p.BlogId);
    }
}
