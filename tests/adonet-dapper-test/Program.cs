// Dapper over the OxiDb.Data ADO.NET provider — the ADR-0013 Phase C
// milestone. Run against a local server with OXIDB_SQL=1.
using System.Data;
using Dapper;
using OxiDb.Data;

var cs = Environment.GetEnvironmentVariable("OXIDB_CS") ?? "Host=127.0.0.1;Port=4444";
using var conn = new OxiDbConnection(cs);
conn.Open();

conn.Execute("DROP TABLE IF EXISTS musteriler");
conn.Execute("""
    CREATE TABLE musteriler (
      id    INT PRIMARY KEY AUTO_INCREMENT,
      ad    TEXT NOT NULL,
      puan  INT,
      kayit TIMESTAMP
    )
    """);

// Named parameters via Dapper.
var n = conn.Execute(
    "INSERT INTO musteriler (ad, puan, kayit) VALUES (@Ad, @Puan, @Kayit)",
    new[]
    {
        new { Ad = "ali",  Puan = (int?)10,  Kayit = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc) },
        new { Ad = "ayse", Puan = (int?)25,  Kayit = new DateTime(2026, 2, 1, 0, 0, 0, DateTimeKind.Utc) },
        new { Ad = "veli", Puan = (int?)null, Kayit = new DateTime(2026, 3, 1, 0, 0, 0, DateTimeKind.Utc) },
    });
Console.WriteLine($"insert affected      : {n}");

// Typed query with named parameter + CASE + COALESCE.
var rows = conn.Query<Musteri>("""
    SELECT id, ad, COALESCE(puan, 0) AS puan, kayit,
           CASE WHEN puan >= @esik THEN true ELSE false END AS sadik
    FROM musteriler ORDER BY id
    """, new { esik = 20 }).ToList();
foreach (var m in rows)
    Console.WriteLine($"row: {m.Id} {m.Ad,-5} puan={m.Puan,-3} sadik={m.Sadik} kayit={m.Kayit:yyyy-MM-dd}");

// Scalar + LIKE.
var count = conn.ExecuteScalar<long>("SELECT COUNT(*) FROM musteriler WHERE ad LIKE 'a%'");
Console.WriteLine($"LIKE 'a%' count      : {count}");

// last_insert_id through a scalar query.
var lastId = conn.QuerySingle<long>("SELECT MAX(id) FROM musteriler");
Console.WriteLine($"max id               : {lastId}");

// ADO.NET transaction (interactive, spans commands) + savepoint + rollback.
using (var tx = conn.BeginTransaction())
{
    conn.Execute("UPDATE musteriler SET puan = 0", transaction: tx);
    tx.Save("s1");
    conn.Execute("DELETE FROM musteriler", transaction: tx);
    tx.Rollback("s1"); // undo the delete, keep the update
    tx.Commit();
}
var after = conn.Query<(long id, long puan)>("SELECT id, COALESCE(puan, -1) FROM musteriler ORDER BY id").ToList();
Console.WriteLine($"after tx             : {string.Join(", ", after.Select(r => $"{r.id}:{r.puan}"))}");

// Rollback path: nothing sticks.
using (var tx = conn.BeginTransaction())
{
    conn.Execute("DELETE FROM musteriler", transaction: tx);
    tx.Rollback();
}
Console.WriteLine($"after rollback count : {conn.ExecuteScalar<long>("SELECT COUNT(*) FROM musteriler")}");

Console.WriteLine("DAPPER OK");

public sealed class Musteri
{
    public long Id { get; set; }
    public string Ad { get; set; } = "";
    public long Puan { get; set; }
    public bool Sadik { get; set; }
    public DateTime Kayit { get; set; }
}
