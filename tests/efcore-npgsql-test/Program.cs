// EF Core through the REAL Npgsql PostgreSQL provider, against OxiDB's PG
// wire listener (ADR-0023). Mirrors tests/efcore-oxidb-test scenario for
// scenario, but every phase is a pass/fail step: the point is a compatibility
// matrix, not an early exit.
using Microsoft.EntityFrameworkCore;

var pgPort = int.Parse(Environment.GetEnvironmentVariable("OXIDB_PG_PORT") ?? "5442");
var wirePort = int.Parse(Environment.GetEnvironmentVariable("OXIDB_PORT") ?? "4544");
var cs = $"Host=127.0.0.1;Port={pgPort};Database=efpg_test;Username=admin;Password=x";

int pass = 0, fail = 0;
void Step(string name, Action body)
{
    try
    {
        body();
        pass++;
        Console.WriteLine($"ok   {name}");
    }
    catch (Exception e)
    {
        fail++;
        var msg = e.InnerException?.Message ?? e.Message;
        Console.WriteLine($"FAIL {name}: {msg.ReplaceLineEndings(" ").TrimEnd()}");
    }
}

// A dedicated database, provisioned over OxiWire (CREATE DATABASE is a
// wire-admin verb, not engine SQL).
await using (var boot = await OxiDb.Client.Tcp.OxiDbTcpClient.ConnectAsync("127.0.0.1", wirePort))
{
    await boot.SqlAsync("DROP DATABASE IF EXISTS efpg_test");
    await boot.SqlAsync("CREATE DATABASE efpg_test");
}

long aliId = 0, ayseId = 0;

Step("EnsureCreated (Npgsql DDL)", () =>
{
    using var db = new ShopContext(cs);
    db.Database.EnsureCreated();
});

// Fallback DDL in the engine's own dialect through the SAME PG wire, so the
// rest of the matrix runs even if EnsureCreated's PostgreSQL DDL is refused.
using (var db = new ShopContext(cs))
{
    try { db.Musteriler.Any(); }
    catch
    {
        db.Database.ExecuteSqlRaw(
            "CREATE TABLE ef_musteri (\"Id\" BIGINT PRIMARY KEY AUTO_INCREMENT, \"Ad\" TEXT NOT NULL, \"Puan\" BIGINT NOT NULL, \"Kayit\" TIMESTAMP NOT NULL)");
        db.Database.ExecuteSqlRaw(
            "CREATE TABLE ef_siparis (\"Id\" BIGINT PRIMARY KEY AUTO_INCREMENT, \"MusteriId\" BIGINT NOT NULL, \"Tutar\" DOUBLE NOT NULL, \"Not\" TEXT)");
    }
}

Step("insert + generated keys (RETURNING)", () =>
{
    using var db = new ShopContext(cs);
    var ali = new Musteri { Ad = "ali", Puan = 10, Kayit = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc) };
    var ayse = new Musteri { Ad = "ayse", Puan = 25, Kayit = new DateTime(2026, 2, 1, 0, 0, 0, DateTimeKind.Utc) };
    db.Musteriler.AddRange(ali, ayse);
    db.SaveChanges();
    if (ali.Id <= 0 || ayse.Id <= 0 || ali.Id == ayse.Id) throw new Exception($"keys ali={ali.Id} ayse={ayse.Id}");
    aliId = ali.Id; ayseId = ayse.Id;
    db.Siparisler.AddRange(
        new Siparis { MusteriId = ali.Id, Tutar = 12.5, Not = null },
        new Siparis { MusteriId = ali.Id, Tutar = 7.5, Not = "hizli" },
        new Siparis { MusteriId = ayse.Id, Tutar = 100, Not = null });
    db.SaveChanges();
});

Step("where + order + ToUpper projection", () =>
{
    using var db = new ShopContext(cs);
    var adlar = db.Musteriler.Where(m => m.Puan >= 10).OrderBy(m => m.Ad)
        .Select(m => m.Ad.ToUpper()).ToList();
    if (string.Join(",", adlar) != "ALI,AYSE") throw new Exception(string.Join(",", adlar));
});

Step("join + group + sum", () =>
{
    using var db = new ShopContext(cs);
    var toplamlar = (
        from s in db.Siparisler
        join m in db.Musteriler on s.MusteriId equals m.Id
        group s by m.Ad into g
        orderby g.Key
        select new { Ad = g.Key, Toplam = g.Sum(x => x.Tutar) }).ToList();
    var got = string.Join(",", toplamlar.Select(t => $"{t.Ad}={t.Toplam}"));
    if (got != "ali=20,ayse=100") throw new Exception(got);
});

Step("Contains/Skip/Take/First", () =>
{
    using var db = new ShopContext(cs);
    var likeCount = db.Musteriler.Count(m => m.Ad.Contains("a"));
    var sayfa = db.Siparisler.OrderBy(s => s.Id).Skip(1).Take(1).Single();
    var ilk = db.Musteriler.OrderByDescending(m => m.Puan).FirstOrDefault();
    var got = $"like={likeCount} sayfa={sayfa.Tutar} ilk={ilk?.Ad}";
    if (got != "like=2 sayfa=7.5 ilk=ayse") throw new Exception(got);
});

Step("update in transaction + delete", () =>
{
    using var db = new ShopContext(cs);
    using (var tx = db.Database.BeginTransaction())
    {
        var m = db.Musteriler.Single(x => x.Ad == "ali");
        m.Puan = 99;
        db.SaveChanges();
        tx.Commit();
    }
    var deleted = db.Siparisler.Single(s => s.Not == "hizli");
    db.Siparisler.Remove(deleted);
    db.SaveChanges();
    var got = $"puan={db.Musteriler.Single(x => x.Ad == "ali").Puan} siparis={db.Siparisler.Count()}";
    if (got != "puan=99 siparis=2") throw new Exception(got);
});

Step("rollback preserves rows", () =>
{
    using var db = new ShopContext(cs);
    using (var tx = db.Database.BeginTransaction())
    {
        db.Musteriler.RemoveRange(db.Musteriler.ToList());
        db.SaveChanges();
        tx.Rollback();
    }
    db.ChangeTracker.Clear();
    if (db.Musteriler.Count() != 2) throw new Exception($"count={db.Musteriler.Count()}");
});

Step("datetime members (Year/Month/</Date/compare)", () =>
{
    using var db = new ShopContext(cs);
    var subat = db.Musteriler.Count(m => m.Kayit.Year == 2026 && m.Kayit.Month == 2);
    var eski = db.Musteriler.Count(m => m.Kayit < DateTime.UtcNow.AddDays(-1));
    var gun = db.Musteriler.Count(m => m.Kayit.Date == new DateTime(2026, 2, 1, 0, 0, 0, DateTimeKind.Utc));
    var got = $"subat={subat} eski={eski} gun={gun}";
    if (got != "subat=1 eski=2 gun=1") throw new Exception(got);
});

Step("distinct/math/string scalars", () =>
{
    using var db = new ShopContext(cs);
    var farkli = db.Siparisler.Select(s => s.MusteriId).Distinct().Count();
    var kok = db.Siparisler.Count(s => Math.Sqrt(s.Tutar) > 3);
    var tek = db.Musteriler.Count(m => m.Puan % 2 == 1);
    var idx = db.Musteriler.Count(m => m.Ad.IndexOf("y") == 1);
    var pad = db.Musteriler.OrderBy(m => m.Ad).Select(m => m.Ad.PadLeft(6, '.')).First();
    var got = $"farkli={farkli} kok={kok} tek={tek} idx={idx} pad={pad}";
    // kok=2: the 7.5 order is deleted by now (sqrt(12.5) and sqrt(100) both
    // clear 3); tek=2: ali's Puan is 99 after the update step, ayse's is 25.
    if (got != "farkli=2 kok=2 tek=2 idx=1 pad=...ali") throw new Exception(got);
});

Step("calendar/DayOfWeek/Contains(char)", () =>
{
    using var db = new ShopContext(cs);
    var ayEkle = db.Musteriler.Count(m => m.Kayit.AddMonths(1) <= new DateTime(2026, 3, 1, 0, 0, 0, DateTimeKind.Utc));
    var yilEkle = db.Musteriler.Count(m => m.Kayit.AddYears(1) >= new DateTime(2027, 1, 1, 0, 0, 0, DateTimeKind.Utc));
    var pazar = db.Musteriler.Count(m => m.Kayit.DayOfWeek == DayOfWeek.Sunday);
    // string overload: Npgsql's EF provider does not translate Contains(char)
    // at all (a provider limitation, not a server one).
    var harf = db.Musteriler.Count(m => m.Ad.Contains("y"));
    var got = $"ayekle={ayEkle} yilekle={yilEkle} pazar={pazar} harf={harf}";
    if (got != "ayekle=2 yilekle=2 pazar=1 harf=1") throw new Exception(got);
});

Step("correlated collection (APPLY/LATERAL)", () =>
{
    using var db = new ShopContext(cs);
    var enBuyuk = db.Musteriler.OrderBy(m => m.Ad)
        .Select(m => new
        {
            m.Ad,
            Max = db.Siparisler.Where(s => s.MusteriId == m.Id)
                .OrderByDescending(s => s.Tutar)
                .Select(s => (double?)s.Tutar)
                .FirstOrDefault(),
        })
        .ToList();
    var got = string.Join(",", enBuyuk.Select(x => $"{x.Ad}={x.Max?.ToString() ?? "yok"}"));
    if (got != "ali=12.5,ayse=100") throw new Exception(got);
});

Step("ExecuteUpdate / ExecuteDelete", () =>
{
    using var db = new ShopContext(cs);
    db.Siparisler.Where(s => s.MusteriId == aliId).ExecuteUpdate(u => u.SetProperty(s => s.Tutar, s => s.Tutar + 1));
    var n = db.Siparisler.Where(s => s.Tutar > 1000).ExecuteDelete();
    if (n != 0) throw new Exception($"deleted={n}");
});

Console.WriteLine($"\n{pass} passed, {fail} failed");
Console.WriteLine(fail == 0 ? "NPGSQL-EF OK" : "NPGSQL-EF PARTIAL");

public sealed class ShopContext(string cs) : DbContext
{
    public DbSet<Musteri> Musteriler => Set<Musteri>();
    public DbSet<Siparis> Siparisler => Set<Siparis>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseNpgsql(cs);
        if (Environment.GetEnvironmentVariable("EF_LOG") == "1")
            options.LogTo(Console.WriteLine,
                [Microsoft.EntityFrameworkCore.Diagnostics.RelationalEventId.CommandExecuting]);
    }

    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<Musteri>().ToTable("ef_musteri").HasKey(m => m.Id);
        mb.Entity<Musteri>().Property(m => m.Id).ValueGeneratedOnAdd();
        mb.Entity<Siparis>().ToTable("ef_siparis").HasKey(s => s.Id);
        mb.Entity<Siparis>().Property(s => s.Id).ValueGeneratedOnAdd();
    }
}

public sealed class Musteri
{
    public long Id { get; set; }
    public string Ad { get; set; } = "";
    public long Puan { get; set; }
    public DateTime Kayit { get; set; }
}

public sealed class Siparis
{
    public long Id { get; set; }
    public long MusteriId { get; set; }
    public double Tutar { get; set; }
    public string? Not { get; set; }
}
