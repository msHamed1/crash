using Crash.Domain.Entities;
using Crash.Persistence;

namespace GameEngine.Repository;


public interface ITableRepository
{
    Task CreateTable(Table table, CancellationToken ct);

    Task<Table?> GetById(int tableId, CancellationToken ct);

    Task<List<Table>> GetActiveTables(CancellationToken ct);

    Task<Table?> TryAcquireOwnership(
        int tableId,
        string ownerId,
        TimeSpan leaseDuration,
        CancellationToken ct);

    Task<bool> RenewOwnership(
        int tableId,
        string ownerId,
        long fencingToken,
        TimeSpan leaseDuration,
        CancellationToken ct);

    Task<bool> ReleaseOwnership(
        int tableId,
        string ownerId,
        long fencingToken,
        CancellationToken ct);

    Task<bool> IsValidOwner(
        int tableId,
        string ownerId,
        long fencingToken,
        CancellationToken ct);
}
public class TableRepository:ITableRepository 
{
    
    private readonly DataContext _db;

    public TableRepository(DataContext db)
    {
        _db = db;
    }


    public Task CreateTable(Table table, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task<Table?> GetById(int tableId, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task<List<Table>> GetActiveTables(CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task<Table?> TryAcquireOwnership(int tableId, string ownerId, TimeSpan leaseDuration, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task<bool> RenewOwnership(int tableId, string ownerId, long fencingToken, TimeSpan leaseDuration, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task<bool> ReleaseOwnership(int tableId, string ownerId, long fencingToken, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task<bool> IsValidOwner(int tableId, string ownerId, long fencingToken, CancellationToken ct)
    {
        throw new NotImplementedException();
    }
}
