using Crash.Domain.Entities;
using Crash.Persistence;
using Microsoft.EntityFrameworkCore;

namespace GameEngine.Repository;


public interface IRoundRepository
{
    Task SaveRoundAsync(Round round, CancellationToken ct);
    Task<List<Round>> GetActiveRounds(string tableId,CancellationToken ct);

    Task<Round> CreateRoundAsync(long tableId, long fToken, CancellationToken ct);
    
    Task<Round?> UpdateRoundEntropyAsync(long roundId,decimal crashPoints, string rngId,  CancellationToken ct);
    Task<bool> MarkRunningAsync(long roundId, long tableId, long fencingToken, CancellationToken ct);
    Task<bool> MarkCrashedAsync(long roundId, long tableId, long fencingToken, CancellationToken ct);
}
public class RoundRepository:IRoundRepository
{
    private readonly DataContext _db;

    public RoundRepository(DataContext db)
    {
        _db = db;
    }
    public async Task SaveRoundAsync(Round round, CancellationToken ct)
    {
        try
        {
            _db.Rounds.Add(round);
            await _db.SaveChangesAsync(ct);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
       
    }

    public Task<List<Round>> GetActiveRounds(string tableId, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public async Task<Round> CreateRoundAsync(long tableId,long fToken, CancellationToken ct)
    {

        try
        {
            await using var tx = await _db.Database.BeginTransactionAsync(ct);
            var table = await _db.Tables.Where(t => t.Id== tableId && t.FencingToken==fToken).FirstAsync(ct);
        
            var nonce = table.NextNonce;
            table.NextNonce++;

            var round = new Round
            {
                TableId = tableId,
                Nonce = nonce,
                Status = RoundStatus.Pending,
                CreatedAt = DateTimeOffset.UtcNow,
                OwnerId = table.OwnerId,
                Table = table,
                FencingToken = table.FencingToken,
                 StartTime =   DateTimeOffset.UtcNow.AddSeconds(6),
            };
            _db.Rounds.Add(round);
            await _db.SaveChangesAsync(ct);
            await tx.CommitAsync(ct);
            return round;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
 
    }

    public async Task<Round?> UpdateRoundEntropyAsync(long roundId, decimal crashPoints, string rngId,  CancellationToken ct)
    {
        await _db.Rounds.Where(t=>t.Id==roundId) .ExecuteUpdateAsync(setters => setters
                .SetProperty(r => r.CrashPoints, crashPoints)
                .SetProperty(r => r.RngId, rngId)
                .SetProperty(r => r.Status, RoundStatus.Betting)
                 .SetProperty(r => r.UpdatedAt, DateTimeOffset.UtcNow),
            ct);
        
        return await _db.Rounds
            .FirstOrDefaultAsync(r => r.Id == roundId, ct);
        
    }

    public async Task<bool> MarkRunningAsync(
        long roundId,
        long tableId,
        long fencingToken,
        CancellationToken ct)
    {
        var affected = await _db.Rounds
            .Where(r => r.Id == roundId
                        && r.TableId == tableId
                        && r.FencingToken == fencingToken
                        && (r.Status == RoundStatus.Pending || r.Status == RoundStatus.Betting))
            .ExecuteUpdateAsync(setters => setters
                    .SetProperty(r => r.Status, RoundStatus.Running)
                    .SetProperty(r => r.UpdatedAt, DateTimeOffset.UtcNow),
                ct);

        return affected == 1;
    }

    public async Task<bool> MarkCrashedAsync(
        long roundId,
        long tableId,
        long fencingToken,
        CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        var affected = await _db.Rounds
            .Where(r => r.Id == roundId
                        && r.TableId == tableId
                        && r.FencingToken == fencingToken
                        && (r.Status == RoundStatus.Pending
                            || r.Status == RoundStatus.Betting
                            || r.Status == RoundStatus.Running))
            .ExecuteUpdateAsync(setters => setters
                    .SetProperty(r => r.Status, RoundStatus.Crashed)
                    .SetProperty(r => r.EndTime, now)
                    .SetProperty(r => r.UpdatedAt, now),
                ct);

        return affected == 1;
    }
}
