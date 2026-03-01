using Microsoft.EntityFrameworkCore;
using OxiDb.EntityFrameworkCore.Extensions;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<ShopContext>(options =>
{
    var config = builder.Configuration.GetSection("OxiDb");
    var username = config["Username"];
    var password = config["Password"];
    options.UseOxiDb(
        config["Host"] ?? "127.0.0.1",
        int.Parse(config["Port"] ?? "4444"),
        username: string.IsNullOrEmpty(username) ? null : username,
        password: string.IsNullOrEmpty(password) ? null : password);
});

var app = builder.Build();

// ── Seed ────────────────────────────────────────────────────────────────────

app.MapPost("/seed", async (ShopContext db) =>
{
    db.Products.Add(new Product { Name = "Laptop", Price = 1299.99m, Category = "Electronics", Stock = 50 });
    db.Products.Add(new Product { Name = "Keyboard", Price = 79.99m, Category = "Electronics", Stock = 200 });
    db.Products.Add(new Product { Name = "Desk Chair", Price = 349.00m, Category = "Furniture", Stock = 30 });
    db.Products.Add(new Product { Name = "Monitor", Price = 499.99m, Category = "Electronics", Stock = 75 });
    db.Products.Add(new Product { Name = "Standing Desk", Price = 599.00m, Category = "Furniture", Stock = 15 });

    db.Customers.Add(new Customer { FullName = "Alice Johnson", Email = "alice@example.com" });
    db.Customers.Add(new Customer { FullName = "Bob Smith", Email = "bob@example.com" });
    db.Customers.Add(new Customer { FullName = "Charlie Brown", Email = "charlie@example.com" });

    var count = await db.SaveChangesAsync();
    return Results.Ok(new { inserted = count });
});

// ── Products ────────────────────────────────────────────────────────────────

app.MapGet("/products", async (ShopContext db, string? category) =>
{
    IQueryable<Product> query = db.Products;
    if (category is not null)
        query = query.Where(p => p.Category == category);
    return await query.ToListAsync();
});

app.MapGet("/products/{id}", async (ShopContext db, string id) =>
{
    var product = await db.Products.FindAsync(id);
    return product is not null ? Results.Ok(product) : Results.NotFound();
});

app.MapPost("/products", async (ShopContext db, Product product) =>
{
    db.Products.Add(product);
    await db.SaveChangesAsync();
    return Results.Created($"/products/{product.Id}", product);
});

app.MapPut("/products/{id}", async (ShopContext db, string id, Product input) =>
{
    var product = await db.Products.FindAsync(id);
    if (product is null) return Results.NotFound();

    product.Name = input.Name;
    product.Price = input.Price;
    product.Category = input.Category;
    product.Stock = input.Stock;
    await db.SaveChangesAsync();
    return Results.Ok(product);
});

app.MapDelete("/products/{id}", async (ShopContext db, string id) =>
{
    var product = await db.Products.FindAsync(id);
    if (product is null) return Results.NotFound();

    db.Products.Remove(product);
    await db.SaveChangesAsync();
    return Results.NoContent();
});

// ── Customers ───────────────────────────────────────────────────────────────

app.MapGet("/customers", async (ShopContext db) =>
    await db.Customers.ToListAsync());

app.MapPost("/customers", async (ShopContext db, Customer customer) =>
{
    db.Customers.Add(customer);
    await db.SaveChangesAsync();
    return Results.Created($"/customers/{customer.Id}", customer);
});

app.MapDelete("/customers/{id}", async (ShopContext db, string id) =>
{
    var customer = await db.Customers.FindAsync(id);
    if (customer is null) return Results.NotFound();

    db.Customers.Remove(customer);
    await db.SaveChangesAsync();
    return Results.NoContent();
});

app.Run();

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
    public ShopContext(DbContextOptions<ShopContext> options) : base(options) { }

    public DbSet<Product> Products => Set<Product>();
    public DbSet<Customer> Customers => Set<Customer>();
}
