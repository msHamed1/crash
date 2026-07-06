using Crash.Domain.Entities;
using Crash.Persistence;
using Microsoft.EntityFrameworkCore;

namespace GameEngine.Repository;


public interface ITableRepository
{

 
    Task<Table?> GetById(long tableId, CancellationToken ct);

    Task<List<Table>> GetActiveTables(long ownerId,CancellationToken ct);

    Task<Table?> TryAcquireNewOwnership(
      
        long ownerId,
        CancellationToken ct);

    Task<Table?> RenewOwnership(
        long tableId,
        long ownerId,
        long fencingToken,
        CancellationToken ct);

    Task<bool> ReleaseOwnership(
        long tableId,
        long ownerId,
        long fencingToken,
        CancellationToken ct);

    Task<bool> IsValidOwner(
        long tableId,
        long ownerId,
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

  
    public async Task<Table?> GetById(long tableId, CancellationToken ct)
    {
     var table=  await _db.Tables.FindAsync(tableId, ct);
       
       return table;
    }

  

    public async Task<List<Table>> GetActiveTables(long ownerId, CancellationToken ct)
    {
      return await _db.Tables.Where(r=>r.OwnerId==ownerId).ToListAsync(ct);
    }
    
    

    public async Task<Table?> TryAcquireNewOwnership(long ownerId, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        var inFuture = now.Add(TimeSpan.FromSeconds(60));
        var table = await _db.Tables
            .AsNoTracking()
            .Where(t => t.LeaseExpiresAt == null || t.LeaseExpiresAt < now)
            .OrderBy(t => t.Id)
            .FirstOrDefaultAsync(ct);

        if (table == null)
            return null;

        var affected = await _db.Tables
            .Where(t => t.Id == table.Id &&
                        (t.LeaseExpiresAt == null || t.LeaseExpiresAt < now))
            .ExecuteUpdateAsync(s => s
                    .SetProperty(t => t.OwnerId, ownerId)
                    .SetProperty(t => t.LeaseExpiresAt, inFuture)
                    .SetProperty(t => t.FencingToken, t => t.FencingToken + 1),
                ct);

        if (affected == 0)
            return null; // someone else acquired it first

        return await _db.Tables
            .AsNoTracking()
            .FirstAsync(t => t.Id == table.Id, ct);
     }

    public async Task<Table?> RenewOwnership(long tableId,long ownerId, long fencingToken, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        var inFuture = now.Add(TimeSpan.FromSeconds(40));
        var affected = await _db.Tables
            .Where(t => t.Id == tableId &&
                        t.OwnerId == ownerId &&
                        t.FencingToken == fencingToken &&
                        t.LeaseExpiresAt != null &&
                        t.LeaseExpiresAt > now)
            .ExecuteUpdateAsync(s => s
                    .SetProperty(t => t.LeaseExpiresAt, inFuture),
                ct);

        if (affected == 0)
            return null; // lease expired, owner changed, or fencing token is stale

        return await _db.Tables
            .AsNoTracking()
            .FirstAsync(t => t.Id == tableId, ct);
    }

    public Task<bool> ReleaseOwnership(long tableId, long ownerId, long fencingToken, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public async Task<bool> IsValidOwner(long tableId, long ownerId, long fencingToken, CancellationToken ct)
    {
        
        var table= await _db.Tables.Where(p=>p.OwnerId == ownerId && p.FencingToken == fencingToken).FirstAsync(ct) ;
        return table != null;
    }
}
