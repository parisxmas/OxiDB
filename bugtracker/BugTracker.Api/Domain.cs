using Microsoft.EntityFrameworkCore;

namespace BugTracker;

// The bug tracker is relational and small, so it lives in the SQL engine and is
// written the way any .NET team already writes this: EF Core entities. The whole
// point — like ColdChain — is that this is a STANDARD stack (ASP.NET minimal
// API + EF Core + Google's own auth library) pointed at ONE ordinary OxiDB
// process. Nothing here knows it is not talking to PostgreSQL.

/// A reported bug. Anyone signed in with Google may open one; only an admin
/// (email allowlist) may change its status.
public class BugReport
{
    public int Id { get; set; }
    public string Title { get; set; } = "";
    public string Body { get; set; } = "";
    /// "open" | "closed". New reports start open; admins close/reopen.
    public string Status { get; set; } = "open";

    /// Google's stable subject id — the identity we key on, not the email
    /// (an email can be reassigned; the sub cannot).
    public string ReporterSub { get; set; } = "";
    public string ReporterEmail { get; set; } = "";
    public string ReporterName { get; set; } = "";

    public DateTime CreatedUtc { get; set; }
    public DateTime UpdatedUtc { get; set; }

    public List<BugComment> Comments { get; set; } = [];
}

/// A comment on a bug. Same auth rule as opening one: signed-in Google users.
public class BugComment
{
    public int Id { get; set; }
    public int BugReportId { get; set; }
    public BugReport? Bug { get; set; }
    public string Body { get; set; } = "";

    public string AuthorSub { get; set; } = "";
    public string AuthorEmail { get; set; } = "";
    public string AuthorName { get; set; } = "";
    /// Set when the comment was posted by an admin — the frontend badges it.
    public bool IsAdmin { get; set; }

    public DateTime CreatedUtc { get; set; }
}

public class BugDb : DbContext
{
    public BugDb(DbContextOptions<BugDb> o) : base(o) { }

    public DbSet<BugReport> Bugs => Set<BugReport>();
    public DbSet<BugComment> Comments => Set<BugComment>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<BugReport>().HasMany(x => x.Comments).WithOne(c => c.Bug)
            .HasForeignKey(c => c.BugReportId);
        b.Entity<BugReport>().HasIndex(x => x.Status);
        b.Entity<BugReport>().HasIndex(x => x.CreatedUtc);
        b.Entity<BugComment>().HasIndex(x => x.BugReportId);
    }
}
