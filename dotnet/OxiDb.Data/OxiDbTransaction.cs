using System.Data;
using System.Data.Common;

namespace OxiDb.Data;

/// <summary>
/// An interactive SQL transaction (ADR-0013 Phase B): the server parks it on
/// this connection's session between requests, so commands issued through
/// the same <see cref="OxiDbConnection"/> run inside it until
/// <see cref="Commit"/> / <see cref="Rollback"/>.
/// </summary>
public sealed class OxiDbTransaction : DbTransaction
{
    private readonly OxiDbConnection _conn;
    private bool _completed;

    internal OxiDbTransaction(OxiDbConnection conn) => _conn = conn;

    protected override DbConnection DbConnection => _conn;
    public override IsolationLevel IsolationLevel => IsolationLevel.Serializable;

    public override void Commit() => Finish("COMMIT");
    public override void Rollback() => Finish("ROLLBACK");

    private void Finish(string sql)
    {
        if (_completed) throw new InvalidOperationException("transaction already completed");
        _conn.SqlAsync(sql, null, default).GetAwaiter().GetResult();
        _completed = true;
        _conn.ActiveTransaction = null;
    }

    public override void Save(string savepointName) => Point("SAVEPOINT", savepointName);
    public override void Rollback(string savepointName) =>
        Point("ROLLBACK TO SAVEPOINT", savepointName);
    public override void Release(string savepointName) =>
        Point("RELEASE SAVEPOINT", savepointName);

    private void Point(string verb, string name)
    {
        if (!name.All(c => char.IsLetterOrDigit(c) || c == '_'))
            throw new ArgumentException("savepoint names must be alphanumeric/underscore");
        _conn.SqlAsync($"{verb} {name}", null, default).GetAwaiter().GetResult();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_completed)
        {
            try { Rollback(); } catch { /* connection may already be gone */ }
        }
        base.Dispose(disposing);
    }
}
