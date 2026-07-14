using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestModels.Northwind;

namespace OxiDb.EFCore.SpecTests;

/// <summary>
/// The provider-concrete Northwind context (the relational base is abstract);
/// carries the keyless ToSqlQuery/ToView mappings.
/// </summary>
public class NorthwindOxiDbContext(DbContextOptions options) : NorthwindRelationalContext(options);
