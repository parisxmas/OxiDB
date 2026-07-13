using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;
using OxiDb.EntityFrameworkCore;

namespace Scaffolded;

public partial class ScaffoldedCtx : DbContext
{
    public ScaffoldedCtx()
    {
    }

    public ScaffoldedCtx(DbContextOptions<ScaffoldedCtx> options)
        : base(options)
    {
    }

    public virtual DbSet<MigKisi> MigKisis { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
#warning To protect potentially sensitive information in your connection string, you should move it out of source code. You can avoid scaffolding the connection string by using the Name= syntax to read it from configuration - see https://go.microsoft.com/fwlink/?linkid=2131148. For more guidance on storing connection strings, see https://go.microsoft.com/fwlink/?LinkId=723263.
        => optionsBuilder.UseOxiDb("Host=127.0.0.1;Port=4544;Database=efmig_test");

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MigKisi>(entity =>
        {
            entity.ToTable("mig_kisi");

            entity.HasIndex(e => e.Puan, "i_kisi_puan");
        });

        OnModelCreatingPartial(modelBuilder);
    }

    partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
}
