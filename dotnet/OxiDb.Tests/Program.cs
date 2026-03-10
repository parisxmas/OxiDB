using Microsoft.EntityFrameworkCore;
using OxiDb.EntityFrameworkCore.Extensions;

Console.WriteLine("=== OxiDB EF Core Embedded — One-to-Many Example ===\n");

var dataPath = Path.Combine(Path.GetTempPath(), "oxidb_efcore_" + Guid.NewGuid().ToString("N")[..8]);
Console.WriteLine($"Data path: {dataPath}\n");

try
{
    using var db = new ShopContext(dataPath);
    await db.Database.EnsureCreatedAsync();

    // ── Insert customers ────────────────────────────────────────────
    Console.WriteLine("Inserting customers...");
    var alice = new Customer { FullName = "Alice Johnson", Email = "alice@example.com" };
    var bob = new Customer { FullName = "Bob Smith", Email = "bob@example.com" };
    db.Customers.Add(alice);
    db.Customers.Add(bob);
    await db.SaveChangesAsync();
    Console.WriteLine($"  Created: {alice.FullName} (Id: {alice.Id[..8]}...)");
    Console.WriteLine($"  Created: {bob.FullName} (Id: {bob.Id[..8]}...)\n");

    // ── Insert orders (linked to customers via CustomerId) ──────────
    Console.WriteLine("Inserting orders...");
    db.Orders.Add(new Order { CustomerId = alice.Id, Product = "Laptop", Price = 1299.99m, Quantity = 1 });
    db.Orders.Add(new Order { CustomerId = alice.Id, Product = "Keyboard", Price = 79.99m, Quantity = 2 });
    db.Orders.Add(new Order { CustomerId = alice.Id, Product = "Monitor", Price = 499.99m, Quantity = 1 });
    db.Orders.Add(new Order { CustomerId = bob.Id, Product = "Desk Chair", Price = 349.00m, Quantity = 1 });
    db.Orders.Add(new Order { CustomerId = bob.Id, Product = "Standing Desk", Price = 599.00m, Quantity = 1 });
    await db.SaveChangesAsync();
    Console.WriteLine($"  Inserted 5 orders.\n");

    // ── Query: all orders for Alice ─────────────────────────────────
    Console.WriteLine($"Orders for {alice.FullName}:");
    var aliceOrders = await db.Orders
        .Where(o => o.CustomerId == alice.Id)
        .OrderByDescending(o => o.Price)
        .ToListAsync();
    foreach (var o in aliceOrders)
        Console.WriteLine($"  {o.Product} — ${o.Price} x{o.Quantity}");

    // ── Query: all orders for Bob ───────────────────────────────────
    Console.WriteLine($"\nOrders for {bob.FullName}:");
    var bobOrders = await db.Orders
        .Where(o => o.CustomerId == bob.Id)
        .OrderBy(o => o.Price)
        .ToListAsync();
    foreach (var o in bobOrders)
        Console.WriteLine($"  {o.Product} — ${o.Price} x{o.Quantity}");

    // ── Query: order count per customer ─────────────────────────────
    var aliceCount = await db.Orders.CountAsync(o => o.CustomerId == alice.Id);
    var bobCount = await db.Orders.CountAsync(o => o.CustomerId == bob.Id);
    Console.WriteLine($"\nOrder counts: Alice={aliceCount}, Bob={bobCount}");

    // ── Query: expensive orders (> $400) ────────────────────────────
    Console.WriteLine("\nExpensive orders (> $400):");
    var expensive = await db.Orders
        .Where(o => o.Price > 400)
        .OrderByDescending(o => o.Price)
        .ToListAsync();
    foreach (var o in expensive)
    {
        var customer = await db.Customers.FirstOrDefaultAsync(c => c.Id == o.CustomerId);
        Console.WriteLine($"  {customer?.FullName}: {o.Product} — ${o.Price}");
    }

    // ── Update: change quantity on Alice's keyboard order ────────────
    Console.WriteLine("\nUpdating Alice's Keyboard order quantity to 3...");
    var keyboard = await db.Orders.FirstOrDefaultAsync(o => o.Product == "Keyboard");
    if (keyboard != null)
    {
        keyboard.Quantity = 3;
        await db.SaveChangesAsync();
        Console.WriteLine($"  Updated: {keyboard.Product} x{keyboard.Quantity}");
    }

    // ── Delete: remove Bob's Desk Chair order ───────────────────────
    Console.WriteLine("\nDeleting Bob's 'Desk Chair' order...");
    var chair = db.ChangeTracker.Entries<Order>()
        .FirstOrDefault(e => e.Entity.Product == "Desk Chair");
    if (chair != null)
    {
        db.Orders.Remove(chair.Entity);
        await db.SaveChangesAsync();
        Console.WriteLine("  Deleted.");
    }

    // ── Final state ─────────────────────────────────────────────────
    Console.WriteLine("\n--- Final State ---");
    var customers = await db.Customers.ToListAsync();
    foreach (var c in customers)
    {
        var orders = await db.Orders
            .Where(o => o.CustomerId == c.Id)
            .ToListAsync();
        Console.WriteLine($"\n{c.FullName} <{c.Email}> — {orders.Count} order(s):");
        foreach (var o in orders)
            Console.WriteLine($"  {o.Product} — ${o.Price} x{o.Quantity}");
    }

    var totalOrders = await db.Orders.CountAsync();
    Console.WriteLine($"\nTotal orders: {totalOrders}");

    Console.WriteLine("\n=== All operations completed successfully ===");
}
finally
{
    if (Directory.Exists(dataPath))
        Directory.Delete(dataPath, recursive: true);
}

// ── Models ──────────────────────────────────────────────────────────────────

public class Customer
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string FullName { get; set; } = "";
    public string Email { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

public class Order
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string CustomerId { get; set; } = "";
    public string Product { get; set; } = "";
    public decimal Price { get; set; }
    public int Quantity { get; set; }
    public DateTime OrderedAt { get; set; } = DateTime.UtcNow;
}

// ── DbContext ───────────────────────────────────────────────────────────────

public class ShopContext(string dataPath) : DbContext
{
    public DbSet<Customer> Customers => Set<Customer>();
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseOxiDbEmbedded(dataPath);
    }
}
