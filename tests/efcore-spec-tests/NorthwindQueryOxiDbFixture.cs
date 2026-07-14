using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace OxiDb.EFCore.SpecTests;

public class NorthwindQueryOxiDbFixture<TModelCustomizer> : NorthwindQueryRelationalFixture<TModelCustomizer>
    where TModelCustomizer : ITestModelCustomizer, new()
{
    protected override ITestStoreFactory TestStoreFactory => OxiDbTestStoreFactory.Instance;

    // The relational context carries the keyless ToSqlQuery/ToView mappings
    // (the core context would map them as plain — empty — tables).
    protected override Type ContextType => typeof(NorthwindOxiDbContext);

    public override DbContextOptionsBuilder AddOptions(DbContextOptionsBuilder builder) =>
        // Multiple collection includes run as one query here (no split-query
        // support); silence the advisory warning the base turns into throws.
        base.AddOptions(builder)
            .ConfigureWarnings(w => w.Ignore(RelationalEventId.MultipleCollectionIncludeWarning));

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);
        // The base maps CustomerQuery with SQL-Server-bracket quoting (SQLite
        // happens to accept [brackets]; this engine speaks double quotes).
        modelBuilder.Entity<CustomerQuery>().ToSqlQuery(
            "SELECT \"c\".\"CustomerID\", \"c\".\"Address\", \"c\".\"City\", \"c\".\"CompanyName\", "
            + "\"c\".\"ContactName\", \"c\".\"ContactTitle\", \"c\".\"Country\", \"c\".\"Fax\", "
            + "\"c\".\"Phone\", \"c\".\"PostalCode\", \"c\".\"Region\" FROM \"Customers\" AS \"c\"");
    }

    protected override async Task SeedAsync(NorthwindContext context)
    {
        await base.SeedAsync(context);
        // ProductView maps to this classic Northwind view. The prebuilt
        // SQLite/SqlServer test databases ship it (their Products table has
        // the CategoryID column; the EF model ignores it, so ours doesn't).
        // Materialize it from the exact expected data instead.
        var rows = string.Join(", ", new NorthwindData().ProductViews.Select(v =>
            $"({v.ProductID}, '{v.ProductName.Replace("'", "''")}', '{v.CategoryName.Replace("'", "''")}')"));
        await context.Database.ExecuteSqlRawAsync(
            "CREATE VIEW \"Alphabetical list of products\" AS "
            + "SELECT \"ProductID\", \"ProductName\", \"CategoryName\" "
            + $"FROM (VALUES {rows}) AS \"v\"(\"ProductID\", \"ProductName\", \"CategoryName\")");
    }
}
