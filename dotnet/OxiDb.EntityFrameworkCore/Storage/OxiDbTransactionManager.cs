using System.Transactions;
using Microsoft.EntityFrameworkCore.Storage;

namespace OxiDb.EntityFrameworkCore.Storage;

/// <summary>
/// No-op transaction manager for OxiDB. OxiDB transactions are not yet
/// exposed through the EF Core provider — SaveChanges runs without an
/// explicit transaction wrapper.
/// </summary>
public sealed class OxiDbTransactionManager : IDbContextTransactionManager
{
    public IDbContextTransaction? CurrentTransaction => null;

    public IDbContextTransaction BeginTransaction()
        => new OxiDbTransaction();

    public Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
        => Task.FromResult<IDbContextTransaction>(new OxiDbTransaction());

    public void CommitTransaction() { }

    public Task CommitTransactionAsync(CancellationToken cancellationToken = default)
        => Task.CompletedTask;

    public void RollbackTransaction() { }

    public Task RollbackTransactionAsync(CancellationToken cancellationToken = default)
        => Task.CompletedTask;

    public void ResetState() { }

    public Task ResetStateAsync(CancellationToken cancellationToken = default)
        => Task.CompletedTask;

    public TransactionStatus? EnlistedTransaction => null;

    public void EnlistTransaction(Transaction? transaction) { }

    private sealed class OxiDbTransaction : IDbContextTransaction
    {
        public Guid TransactionId { get; } = Guid.NewGuid();

        public void Commit() { }
        public Task CommitAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public void Rollback() { }
        public Task RollbackAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
