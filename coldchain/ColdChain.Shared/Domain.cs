using Microsoft.EntityFrameworkCore;

namespace ColdChain;

// ── The RELATIONAL half: shipments, customers, SLA money. ────────────────────
// This is what the SQL engine is for. It is relational because it genuinely is:
// a shipment belongs to a customer, has a contracted temperature range, and a
// breach costs a specific amount. Foreign keys and joins are the right tool,
// and EF Core is how .NET teams already write this.

public class Customer
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    /// What one breach costs us, per the contract.
    public decimal PenaltyPerBreach { get; set; }
    public List<Shipment> Shipments { get; set; } = [];
}

public class Shipment
{
    public int Id { get; set; }
    public string Reference { get; set; } = "";
    public int CustomerId { get; set; }
    public Customer? Customer { get; set; }
    /// The sensor reporting for this shipment — the join to the time-series data.
    public string DeviceId { get; set; } = "";
    /// The contracted range. Outside it, even briefly, the load is suspect.
    public double MinCelsius { get; set; }
    public double MaxCelsius { get; set; }
    public DateTime DepartedUtc { get; set; }
    public DateTime? DeliveredUtc { get; set; }
    public List<Excursion> Excursions { get; set; } = [];
}

/// A breach of the contracted range. Recorded relationally because it has
/// money attached and an auditor will join it to the shipment and customer.
public class Excursion
{
    public int Id { get; set; }
    public int ShipmentId { get; set; }
    public Shipment? Shipment { get; set; }
    public DateTime AtUtc { get; set; }
    public double Celsius { get; set; }
    public double LimitCelsius { get; set; }
    /// S3 object key of the inspection photo, if one was filed.
    public string? PhotoKey { get; set; }
}

public class ColdChainDb : DbContext
{
    public ColdChainDb(DbContextOptions<ColdChainDb> o) : base(o) { }
    public DbSet<Customer> Customers => Set<Customer>();
    public DbSet<Shipment> Shipments => Set<Shipment>();
    public DbSet<Excursion> Excursions => Set<Excursion>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Customer>().HasMany(c => c.Shipments).WithOne(s => s.Customer)
            .HasForeignKey(s => s.CustomerId);
        b.Entity<Shipment>().HasMany(s => s.Excursions).WithOne(e => e.Shipment)
            .HasForeignKey(e => e.ShipmentId);
        b.Entity<Shipment>().HasIndex(s => s.DeviceId);
        b.Entity<Customer>().Property(c => c.PenaltyPerBreach).HasColumnType("DECIMAL");
    }
}
