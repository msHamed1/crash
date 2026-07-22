using Crash.Domain.Entities;
using Crash.Persistence;
using Microsoft.EntityFrameworkCore;
using System.Data;

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
    
    Task<Table> GetOrCreateTableForPlayer(Player player, CancellationToken ct);
    
    
    
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

    public async Task<Table> GetOrCreateTableForPlayer(Player player, CancellationToken ct)
    {
        // Login/reconnect must be idempotent. Moving an existing player to a new table changes
        // their SignalR group and makes the browser miss lifecycle events from the active round.
        if (player.TableId is { } existingTableId)
        {
            var existingTable = await _db.Tables
                .AsNoTracking()
                .Include(t => t.Players)
                .FirstOrDefaultAsync(t => t.Id == existingTableId, ct);

            if (existingTable is not null)
                return existingTable;

            // Heal a stale foreign key before assigning a replacement table.
            player.TableId = null;
            await _db.SaveChangesAsync(ct);
        }

        // Table allocation must be serialized across every gateway instance.
        // A normal SELECT followed by INSERT allows multiple concurrent requests
        // to all observe "no available table" and each create a one-player table.
        const int maxPlayers = 200;
        var now = DateTimeOffset.UtcNow;
        var connection = _db.Database.GetDbConnection();
        var openedHere = connection.State != ConnectionState.Open;

        if (openedHere)
            await _db.Database.OpenConnectionAsync(ct);

        var allocationLockName = $"{connection.Database}:crash-table-allocation";
        var lockAcquired = false;

        try
        {
            lockAcquired = await AcquireAllocationLockAsync(
                connection,
                allocationLockName,
                ct);

            if (!lockAcquired)
            {
                throw new TimeoutException(
                    "Timed out waiting for the table-allocation lock.");
            }

            // Another request for the same player may have completed while this
            // request was waiting for the allocator lock. Re-read from the DB.
            var assignedTableId = await _db.Players
                .AsNoTracking()
                .Where(candidate => candidate.Id == player.Id)
                .Select(candidate => candidate.TableId)
                .SingleAsync(ct);

            if (assignedTableId is { } currentTableId)
            {
                player.TableId = currentTableId;
                return await LoadTableAsync(currentTableId, ct);
            }

            await using var transaction = await _db.Database.BeginTransactionAsync(ct);

            var tableId = await _db.Tables
                .Where(t => t.PlayersCount < maxPlayers /* && t.Status == TableStatus.Open */)
                .OrderByDescending(t => t.PlayersCount)
                .Select(t => t.Id)
                .FirstOrDefaultAsync(ct);

            if (tableId == 0)
            {
                var newTable = new Table
                {
                    PlayersCount = 1,
                    CreatedAt = now,
                    TableName = "Crash table"
                };

                _db.Tables.Add(newTable);
                await _db.SaveChangesAsync(ct);

                player.TableId = newTable.Id;
                await _db.SaveChangesAsync(ct);
                await transaction.CommitAsync(ct);

                return newTable;
            }

            var updatedRows = await _db.Tables
                .Where(t => t.Id == tableId && t.PlayersCount < maxPlayers /* && t.Status == TableStatus.Open */)
                .ExecuteUpdateAsync(setters => setters
                    .SetProperty(t => t.PlayersCount, t => t.PlayersCount + 1)
                    .SetProperty(t => t.UpdatedAt, now), ct);

            if (updatedRows == 0)
            {
                await transaction.RollbackAsync(ct);
                throw new InvalidOperationException(
                    $"Could not reserve a seat in table {tableId} while holding the allocation lock.");
            }

            player.TableId = tableId;

            await _db.SaveChangesAsync(ct);
            await transaction.CommitAsync(ct);

            return await LoadTableAsync(tableId, ct);
        }
        finally
        {
            if (lockAcquired)
                await ReleaseAllocationLockAsync(connection, allocationLockName);

            if (openedHere)
                await _db.Database.CloseConnectionAsync();
        }
    }

    private async Task<Table> LoadTableAsync(long tableId, CancellationToken ct)
    {
        return await _db.Tables
            .AsNoTracking()
            .Include(table => table.Players)
            .FirstAsync(table => table.Id == tableId, ct);
    }

    private static async Task<bool> AcquireAllocationLockAsync(
        System.Data.Common.DbConnection connection,
        string lockName,
        CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT GET_LOCK(@lockName, 30);";

        var parameter = command.CreateParameter();
        parameter.ParameterName = "@lockName";
        parameter.Value = lockName;
        command.Parameters.Add(parameter);

        var result = await command.ExecuteScalarAsync(ct);
        return result is not null && result != DBNull.Value && Convert.ToInt32(result) == 1;
    }

    private static async Task ReleaseAllocationLockAsync(
        System.Data.Common.DbConnection connection,
        string lockName)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT RELEASE_LOCK(@lockName);";

        var parameter = command.CreateParameter();
        parameter.ParameterName = "@lockName";
        parameter.Value = lockName;
        command.Parameters.Add(parameter);

        await command.ExecuteScalarAsync(CancellationToken.None);
    }
}
