// EF Core Migrations over OxiDB — Database.Migrate() end-to-end.
//
// Two hand-written migrations (what `dotnet ef migrations add` would emit):
//   1. init   — CreateTable (AUTO_INCREMENT pk) + CreateIndex
//   2. evolve — AddColumn, RenameColumn, DropIndex, CreateIndex
// Verifies: history table creation and rows, incremental application,
// re-Migrate() as a no-op, and that the migrated schema actually works.
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using OxiDb.EntityFrameworkCore;

var port = int.Parse(Environment.GetEnvironmentVariable("OXIDB_PORT") ?? "4444");
var cs = $"Host=127.0.0.1;Port={port};Database=efmig_test";

await using (var boot = await OxiDb.Client.Tcp.OxiDbTcpClient.ConnectAsync("127.0.0.1", port))
{
    await boot.SqlAsync("DROP DATABASE IF EXISTS efmig_test");
    await boot.SqlAsync("CREATE DATABASE efmig_test");
}

using (var db = new MigCtx(cs))
{
    var pending = db.Database.GetPendingMigrations().ToList();
    Console.WriteLine($"pending             : {string.Join(",", pending)}");
    db.Database.Migrate();
    var applied = db.Database.GetAppliedMigrations().ToList();
    Console.WriteLine($"applied             : {string.Join(",", applied)}");
    if (applied.Count != 2) throw new Exception("expected 2 applied migrations");
}

using (var db = new MigCtx(cs))
{
    // Idempotent re-run.
    db.Database.Migrate();
    if (db.Database.GetPendingMigrations().Any()) throw new Exception("still pending after Migrate()");

    // The evolved schema works end-to-end: Isim (renamed) + Puan (added).
    db.Kisiler.Add(new Kisi { Isim = "ali", Puan = 7 });
    db.SaveChanges();
    var k = db.Kisiler.Single(x => x.Puan % 2 == 1);
    Console.WriteLine($"schema works        : id={k.Id} isim={k.Isim} puan={k.Puan}");
}

Console.WriteLine("MIGRATE OK");

public sealed class Kisi
{
    public long Id { get; set; }
    public string? Isim { get; set; }
    public long Puan { get; set; }
}

public sealed class MigCtx(string cs) : DbContext
{
    public DbSet<Kisi> Kisiler => Set<Kisi>();
    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseOxiDb(cs);
        if (Environment.GetEnvironmentVariable("EF_LOG") == "1")
            options.LogTo(Console.WriteLine,
                [Microsoft.EntityFrameworkCore.Diagnostics.RelationalEventId.CommandExecuting]);
    }

    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<Kisi>().ToTable("mig_kisi").HasKey(k => k.Id);
        mb.Entity<Kisi>().Property(k => k.Id).ValueGeneratedOnAdd();
    }
}

[DbContext(typeof(MigCtx))]
[Migration("20260713000001_init")]
public sealed class Init : Migration
{
    protected override void Up(MigrationBuilder mb)
    {
        mb.CreateTable(
            name: "mig_kisi",
            columns: t => new
            {
                Id = t.Column<long>(type: "INT", nullable: false)
                    .Annotation(OxiDbAnnotations.AutoIncrement, true),
                Ad = t.Column<string>(type: "TEXT", nullable: true),
            },
            constraints: t => t.PrimaryKey("PK_mig_kisi", x => x.Id));
        mb.CreateIndex(name: "i_kisi_ad", table: "mig_kisi", column: "Ad");
    }
}

[DbContext(typeof(MigCtx))]
[Migration("20260713000002_evolve")]
public sealed class Evolve : Migration
{
    protected override void Up(MigrationBuilder mb)
    {
        mb.AddColumn<long>(name: "Puan", table: "mig_kisi", type: "INT", nullable: false, defaultValue: 0L);
        mb.RenameColumn(name: "Ad", table: "mig_kisi", newName: "Isim");
        mb.DropIndex(name: "i_kisi_ad", table: "mig_kisi");
        mb.CreateIndex(name: "i_kisi_puan", table: "mig_kisi", column: "Puan");
    }
}
