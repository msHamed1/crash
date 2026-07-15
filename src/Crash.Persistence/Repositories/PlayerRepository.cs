using Crash.Domain.Entities;
using Microsoft.EntityFrameworkCore;

namespace Crash.Persistence.Repositories;


public interface IPlayerRepository
{
  public  Task<Player?> GetPlayerByUsername(string username, CancellationToken ct);
  public  Task<Player?> Create(string username, CancellationToken ct);
  
  public Task<Player?> GetById(long id, CancellationToken ct);

}
public class PlayerRepository(DataContext db) : IPlayerRepository
{
    public async Task<Player?> GetPlayerByUsername(string username, CancellationToken ct)
    {
        return  await db.Players.Where(p=>p.ExternalId==username).FirstOrDefaultAsync(ct);
        
    }

    public async Task<Player?> GetById(long Id, CancellationToken ct)
    {
        return  await db.Players.Where(p=>p.Id==Id).FirstOrDefaultAsync(ct);

    }

    public async Task<Player?> Create(string username, CancellationToken ct)
    {

        var player = new Player
        {
            ExternalId = username,
            Type = "FUN",
            BalanceInUSD = 10000, // Todo Wallet should be a separate table. but for now we support only USD .
         };
        
        await db.Players.AddAsync(player, ct);
        await db.SaveChangesAsync(ct);
        return player;

    }
}
