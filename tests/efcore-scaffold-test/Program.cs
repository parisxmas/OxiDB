// Scaffolded context round-trip: reverse-engineered model queries live data.
using Scaffolded;

using var db = new ScaffoldedCtx();
var kisiler = db.MigKisis.Where(k => k.Puan > 0).OrderBy(k => k.Isim).ToList();
foreach (var k in kisiler)
    Console.WriteLine($"scaffolded read     : id={k.Id} isim={k.Isim} puan={k.Puan}");
Console.WriteLine("SCAFFOLD OK");
