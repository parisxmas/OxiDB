// EF Core on the EMBEDDED OxiDB engine — no server, no socket.
//
// The connection string is the only thing that differs from TCP:
//   Path=./mydata                          -> embedded, in-process (this file)
//   Host=127.0.0.1;Port=4444;Database=app  -> the same code against a server
//
// SQL data lives under ./mydata/sql, created on first use. One process per
// data directory; any number of DbContexts (and simultaneous transactions)
// inside that process are fine.
using Microsoft.EntityFrameworkCore;
using OxiDb.EntityFrameworkCore;

using var db = new TodoContext();
db.Database.EnsureCreated();

db.Todos.Add(new Todo { Title = "ship it", Done = false });
db.SaveChanges();

// A transaction, exactly as over TCP — commit or roll back.
using (var tx = db.Database.BeginTransaction())
{
    db.Todos.Add(new Todo { Title = "temp", Done = false });
    db.SaveChanges();
    tx.Rollback(); // gone
}
db.ChangeTracker.Clear();

foreach (var t in db.Todos.Where(t => !t.Done).OrderBy(t => t.Id))
    Console.WriteLine($"{t.Id}: {t.Title}");

public class Todo
{
    public long Id { get; set; }          // auto-increment, set on SaveChanges
    public string Title { get; set; } = "";
    public bool Done { get; set; }
}

public class TodoContext : DbContext
{
    public DbSet<Todo> Todos => Set<Todo>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseOxiDb("Path=./mydata");   // <- the only embedded-specific line
}
