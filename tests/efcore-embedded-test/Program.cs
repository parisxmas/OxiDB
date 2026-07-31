// EF Core over the EMBEDDED engine — no server, no socket: the same
// scenarios as tests/efcore-oxidb-test, with a `Path=` connection string.
// The engine runs in-process (OxiDb.Client.Embedded via FFI); interactive
// transactions ride each request as a token, so several DbContexts share
// the one engine handle without sharing transactions.
using Microsoft.EntityFrameworkCore;
using OxiDb.EntityFrameworkCore;

// A dedicated data directory so EnsureCreated sees a blank slate.
var dataDir = Environment.GetEnvironmentVariable("OXIDB_EMBEDDED_DIR")
    ?? Path.Combine(Path.GetTempPath(), "oxidb-efcore-embedded-test");
if (Directory.Exists(dataDir)) Directory.Delete(dataDir, recursive: true);
Directory.CreateDirectory(dataDir);
var cs = $"Path={dataDir}";

using (var db = new ShopContext(cs))
{
    db.Database.EnsureCreated();
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

// Two contexts, two SIMULTANEOUS transactions on the one shared engine
// handle — the per-connection token is what keeps them apart. This is the
// scenario the old per-handle design could not express at all.
using (var a = new ShopContext(cs))
using (var b = new ShopContext(cs))
{
    using var txA = a.Database.BeginTransaction();
    using var txB = b.Database.BeginTransaction();
    a.Musteriler.Single(m => m.Ad == "ali").Puan = 1000;
    b.Musteriler.Single(m => m.Ad == "ayse").Puan = 2000;
    a.SaveChanges();
    b.SaveChanges();
    txA.Commit();
    txB.Rollback();
}
using (var db = new ShopContext(cs))
{
    var ali = db.Musteriler.Single(m => m.Ad == "ali").Puan;
    var ayse = db.Musteriler.Single(m => m.Ad == "ayse").Puan;
    Console.WriteLine($"concurrent tx       : ali={ali} (committed) ayse={ayse} (rolled back)");
    if (ali != 1000 || ayse != 25) throw new Exception("concurrent transactions leaked into each other");
}

using (var db = new ShopContext(cs))
{
    // Date/time surface + scalars, as in the TCP e2e.
    var subat = db.Musteriler.Count(m => m.Kayit.Year == 2026 && m.Kayit.Month == 2);
    var gun = db.Musteriler.Count(m => m.Kayit.Date == new DateTime(2026, 2, 1, 0, 0, 0, DateTimeKind.Utc));
    Console.WriteLine($"datetime            : subat={subat} gun={gun}");

    var farkli = db.Siparisler.Select(s => s.MusteriId).Distinct().Count();
    var pad = db.Musteriler.OrderBy(m => m.Ad).Select(m => m.Ad.PadLeft(6, '.')).First();
    Console.WriteLine($"distinct/string     : farkli={farkli} pad={pad}");

    // Correlated collection projection → JOIN LATERAL.
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

// Durability: everything above must still be there for a fresh "process"
// (a fresh engine open would need a new process; a fresh context at least
// proves the data outlives every context and transaction).
using (var db = new ShopContext(cs))
{
    if (db.Musteriler.Count() != 2 || db.Siparisler.Count() != 2)
        throw new Exception("row counts changed across contexts");
}

Console.WriteLine("EFCORE-EMBEDDED OK");

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
