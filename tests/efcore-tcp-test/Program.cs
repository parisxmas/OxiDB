using Microsoft.EntityFrameworkCore;
using OxiDb.EntityFrameworkCore.Extensions;

// ── Main ────────────────────────────────────────────────────────────────────

var host = args.Length > 0 ? args[0] : "127.0.0.1";
var port = args.Length > 1 ? int.Parse(args[1]) : 4444;
var username = args.Length > 2 ? args[2] : null;
var password = args.Length > 3 ? args[3] : null;

var authLabel = username != null ? $" (auth: {username})" : " (no auth)";
Console.WriteLine($"=== OxiDB TCP EF Core Test ({host}:{port}){authLabel} ===\n");

using var db = new ShopContext(host, port, username, password);

// Clean up from previous runs
try { await db.Database.EnsureDeletedAsync(); } catch { }

// Insert products
Console.WriteLine("Inserting products...");
db.Products.Add(new Product { Name = "Laptop", Price = 1299.99m, Category = "Electronics", Stock = 50 });
db.Products.Add(new Product { Name = "Keyboard", Price = 79.99m, Category = "Electronics", Stock = 200 });
db.Products.Add(new Product { Name = "Desk Chair", Price = 349.00m, Category = "Furniture", Stock = 30 });
db.Products.Add(new Product { Name = "Monitor", Price = 499.99m, Category = "Electronics", Stock = 75 });
db.Products.Add(new Product { Name = "Standing Desk", Price = 599.00m, Category = "Furniture", Stock = 15 });
var insertCount = await db.SaveChangesAsync();
Console.WriteLine($"  Inserted {insertCount} products.\n");

// Insert customers
Console.WriteLine("Inserting customers...");
db.Customers.Add(new Customer { FullName = "Alice Johnson", Email = "alice@example.com" });
db.Customers.Add(new Customer { FullName = "Bob Smith", Email = "bob@example.com" });
db.Customers.Add(new Customer { FullName = "Charlie Brown", Email = "charlie@example.com" });
insertCount = await db.SaveChangesAsync();
Console.WriteLine($"  Inserted {insertCount} customers.\n");

// Query: all electronics
Console.WriteLine("Electronics products:");
var electronics = await db.Products
    .Where(p => p.Category == "Electronics")
    .OrderBy(p => p.Price)
    .ToListAsync();
foreach (var p in electronics)
    Console.WriteLine($"  {p.Name} — ${p.Price} ({p.Stock} in stock)");

// Query: count
var totalProducts = await db.Products.CountAsync();
Console.WriteLine($"\nTotal products: {totalProducts}");

// Query: expensive items
Console.WriteLine("\nProducts over $400:");
var expensive = await db.Products
    .Where(p => p.Price > 400)
    .OrderByDescending(p => p.Price)
    .ToListAsync();
foreach (var p in expensive)
    Console.WriteLine($"  {p.Name} — ${p.Price}");

// Update
Console.WriteLine("\nUpdating Laptop stock...");
var laptop = db.ChangeTracker.Entries<Product>()
    .FirstOrDefault(e => e.Entity.Name == "Laptop");
if (laptop != null)
{
    laptop.Entity.Stock = 45;
    await db.SaveChangesAsync();
    Console.WriteLine($"  Laptop stock updated to {laptop.Entity.Stock}");
}

// Delete
Console.WriteLine("\nDeleting 'Bob Smith'...");
var bob = db.ChangeTracker.Entries<Customer>()
    .FirstOrDefault(e => e.Entity.FullName == "Bob Smith");
if (bob != null)
{
    db.Customers.Remove(bob.Entity);
    await db.SaveChangesAsync();
    Console.WriteLine("  Deleted.");
}

// Final state
var remainingCustomers = await db.Customers.ToListAsync();
Console.WriteLine($"\nRemaining customers ({remainingCustomers.Count}):");
foreach (var c in remainingCustomers)
    Console.WriteLine($"  {c.FullName} <{c.Email}>");

Console.WriteLine("\n=== Test Complete ===");

// ── Models ──────────────────────────────────────────────────────────────────

public class Product
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string Name { get; set; } = "";
    public decimal Price { get; set; }
    public string Category { get; set; } = "";
    public int Stock { get; set; }
}

public class Customer
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string FullName { get; set; } = "";
    public string Email { get; set; } = "";
}

// ── DbContext ───────────────────────────────────────────────────────────────

public class ShopContext : DbContext
{
    private readonly string _host;
    private readonly int _port;
    private readonly string? _username;
    private readonly string? _password;

    public ShopContext(string host, int port, string? username = null, string? password = null)
    {
        _host = host;
        _port = port;
        _username = username;
        _password = password;
    }

    public DbSet<Product> Products => Set<Product>();
    public DbSet<Customer> Customers => Set<Customer>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseOxiDb(_host, _port, username: _username, password: _password);
    }
}
