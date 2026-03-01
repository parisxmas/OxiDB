using Microsoft.EntityFrameworkCore;
using OxiDb.Tests.EFCore.Models;

namespace OxiDb.Tests.EFCore;

public class BenchmarkDbContext : DbContext
{
    public BenchmarkDbContext(DbContextOptions options) : base(options) { }

    public DbSet<Employee> Employees { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Employee>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).ValueGeneratedOnAdd();
            entity.Property(e => e.Tags).HasConversion(
                v => string.Join(",", v),
                v => v.Split(",", StringSplitOptions.RemoveEmptyEntries).ToList());
        });
    }
}
