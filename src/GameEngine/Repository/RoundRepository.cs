using Crash.Domain.Entities;
using Crash.Persistence;

namespace GameEngine.Repository;


public interface IRoundRepository
{
    Task SaveRoundAsync(Round round, CancellationToken ct);
    Task<List<Round>> GetActiveRounds(string tableId,CancellationToken ct);
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
    
    
}
