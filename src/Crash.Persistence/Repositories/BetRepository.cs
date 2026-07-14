using Crash.Domain.Entities;
using Microsoft.EntityFrameworkCore;

namespace Crash.Persistence.Repositories;

public interface IBetRepository
{
    Task<Bet> CreateBet(Bet bet, CancellationToken ct);
    Task<Bet> UpdateBet(Bet bet, CancellationToken ct);
    Task<Bet?> GetBet(string betId, CancellationToken ct);
    Task<List<Bet>> GetPlayerBets(long playerId, CancellationToken ct);
    Task<bool> Exists(string betId, CancellationToken ct);

    Task<Bet?> GetPendingBet(string betId, CancellationToken ct);
}
public class BetRepository(DataContext db) :IBetRepository
{
    public async Task<Bet> CreateBet(Bet bet, CancellationToken ct)
    { 
        await db.Bets.AddAsync(bet,ct);
      await db.SaveChangesAsync(ct);
      return bet;

       
        
    }

    public async Task<Bet> UpdateBet(Bet bet, CancellationToken ct)
    {
        db.Bets.Update(bet);
      await db.SaveChangesAsync(ct);
      return bet;
    }

    public async Task<Bet?> GetBet(string betId, CancellationToken ct)
    {
        return await db.Bets.Where(p => p.BetId == betId).FirstOrDefaultAsync(ct)
            ;
    }

    public async Task<List<Bet>> GetPlayerBets(long playerId, CancellationToken ct)
    {
        return await db.Bets
            .Where(b => b.PlayerId == playerId)
            .OrderByDescending(b => b.CreatedAt)
            .ToListAsync(ct);
    }

    public async Task<bool> Exists(string betId, CancellationToken ct)
    {
        return await db.Bets.AnyAsync(b => b.BetId == betId, ct);
    }

    public async Task<Bet?> GetPendingBet(string betId, CancellationToken ct)
    {
        return await db.Bets.FirstOrDefaultAsync(
            b => b.BetId == betId &&
                 b.Status == BetStatus.Placed,
            ct);
    }
}