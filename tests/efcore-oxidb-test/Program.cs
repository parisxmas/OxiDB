// EF Core over OxiDB — ADR-0013 Phase E end-to-end.
using Microsoft.EntityFrameworkCore;
using OxiDb.EntityFrameworkCore;

var port = int.Parse(Environment.GetEnvironmentVariable("OXIDB_PORT") ?? "4444");
var cs = Environment.GetEnvironmentVariable("OXIDB_CS")
    ?? $"Host=127.0.0.1;Port={port};Database=efcore_test";

// A dedicated database so EnsureCreated sees a blank slate.
await using (var boot = await OxiDb.Client.Tcp.OxiDbTcpClient.ConnectAsync("127.0.0.1", port))
{
    await boot.SqlAsync("DROP DATABASE IF EXISTS efcore_test");
    await boot.SqlAsync("CREATE DATABASE efcore_test");
}

using (var db = new ShopContext(cs))
{
    db.Database.EnsureCreated();
    db.Musteriler.RemoveRange(db.Musteriler.ToList());
    db.Siparisler.RemoveRange(db.Siparisler.ToList());
    db.SaveChanges();
}

using (var db = new ShopContext(cs))
{
    // INSERT with generated keys (AUTO_INCREMENT via RETURNING).
    var ali = new Musteri { Ad = "ali", Puan = 10, Kayit = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc) };
    var ayse = new Musteri { Ad = "ayse", Puan = 25, Kayit = new DateTime(2026, 2, 1, 0, 0, 0, DateTimeKind.Utc) };
    db.Musteriler.AddRange(ali, ayse);
    db.SaveChanges();
    Console.WriteLine($"generated keys      : ali={ali.Id} ayse={ayse.Id}");

    db.Siparisler.AddRange(
        new Siparis { MusteriId = ali.Id, Tutar = 12.5, Not = null },
        new Siparis { MusteriId = ali.Id, Tutar = 7.5, Not = "hizli" },
        new Siparis { MusteriId = ayse.Id, Tutar = 100, Not = null });
    db.SaveChanges();
}

using (var db = new ShopContext(cs))
{
    // LINQ: Where + OrderBy + projection.
    var adlar = db.Musteriler.Where(m => m.Puan >= 10).OrderBy(m => m.Ad)
        .Select(m => m.Ad.ToUpper()).ToList();
    Console.WriteLine($"where+order+upper   : {string.Join(",", adlar)}");

    // Join + aggregate (navigationless join).
    var toplamlar = (
        from s in db.Siparisler
        join m in db.Musteriler on s.MusteriId equals m.Id
        group s by m.Ad into g
        orderby g.Key
        select new { Ad = g.Key, Toplam = g.Sum(x => x.Tutar) }).ToList();
    Console.WriteLine($"join+group+sum      : {string.Join(",", toplamlar.Select(t => $"{t.Ad}={t.Toplam}"))}");

    // Contains → LIKE, Skip/Take → LIMIT/OFFSET, FirstOrDefault.
    var likeCount = db.Musteriler.Count(m => m.Ad.Contains("a"));
    var sayfa = db.Siparisler.OrderBy(s => s.Id).Skip(1).Take(1).Single();
    var ilk = db.Musteriler.OrderByDescending(m => m.Puan).FirstOrDefault();
    Console.WriteLine($"contains/skip/first : like={likeCount} sayfa={sayfa.Tutar} ilk={ilk?.Ad}");

    // UPDATE + DELETE via change tracking, inside an explicit transaction.
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
    Console.WriteLine($"update+delete       : ali.puan={db.Musteriler.Single(x => x.Ad == "ali").Puan} siparis={db.Siparisler.Count()}");

    // Rollback path.
    using (var tx = db.Database.BeginTransaction())
    {
        db.Musteriler.RemoveRange(db.Musteriler.ToList());
        db.SaveChanges();
        tx.Rollback();
    }
    db.ChangeTracker.Clear();
    Console.WriteLine($"rollback preserved  : {db.Musteriler.Count()} musteri");
}

using (var db = new ShopContext(cs))
{
    // Date/time surface: date_part members, NOW() + interval arithmetic,
    // date_trunc via .Date.
    var subat = db.Musteriler.Count(m => m.Kayit.Year == 2026 && m.Kayit.Month == 2);
    var eski = db.Musteriler.Count(m => m.Kayit < DateTime.UtcNow.AddDays(-1));
    var gun = db.Musteriler.Count(m => m.Kayit.Date == new DateTime(2026, 2, 1, 0, 0, 0, DateTimeKind.Utc));
    Console.WriteLine($"datetime            : subat={subat} eski={eski} gun={gun}");

    // COUNT(DISTINCT), math scalars, % and the new string scalars.
    var farkli = db.Siparisler.Select(s => s.MusteriId).Distinct().Count();
    var kok = db.Siparisler.Count(s => Math.Sqrt(s.Tutar) > 3);
    var tek = db.Musteriler.Count(m => m.Puan % 2 == 1);
    var idx = db.Musteriler.Count(m => m.Ad.IndexOf("y") == 1);
    var pad = db.Musteriler.OrderBy(m => m.Ad).Select(m => m.Ad.PadLeft(6, '.')).First();
    Console.WriteLine($"distinct/math/string: farkli={farkli} kok={kok} tek={tek} idx={idx} pad={pad}");

    // Correlated collection projection → OUTER/CROSS APPLY → JOIN LATERAL.
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
    Console.WriteLine($"lateral             : {string.Join(",", enBuyuk.Select(x => $"{x.Ad}={x.Max?.ToString() ?? "yok"}"))}");
}

Console.WriteLine("EFCORE OK");

public sealed class ShopContext(string cs) : DbContext
{
    public DbSet<Musteri> Musteriler => Set<Musteri>();
    public DbSet<Siparis> Siparisler => Set<Siparis>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseOxiDb(cs);
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
