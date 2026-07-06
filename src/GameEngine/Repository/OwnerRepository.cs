using Crash.Domain.Entities;
using Crash.Persistence;
using Microsoft.EntityFrameworkCore;

namespace GameEngine.Repository;

public interface IOwnerRepository
{
    Task<Owner?> GetById(long ownerId, CancellationToken ct);
    Task<List<Owner>> GetAllOwners(CancellationToken ct);
    Task<Owner?> GetByName(string ownerName, CancellationToken ct);
    
    Task<Owner?> CreateOwner(Owner owner, CancellationToken ct);
}
public class OwnerRepository:IOwnerRepository
{
    private readonly DataContext _db;

    public OwnerRepository(DataContext db)
    {
        _db = db;
    }


    public async Task<Owner?> GetById(long ownerId, CancellationToken ct)
    { 
        return  await _db.Owners.FindAsync([ownerId], ct);
        
    }
    public async Task<List<Owner>> GetAllOwners(CancellationToken ct)
    {
        return await _db.Owners.ToListAsync(ct);
    }

    public async Task<Owner?> GetByName(string ownerName, CancellationToken ct)
    {
        return await _db.Owners
            .FirstOrDefaultAsync(o => o.Name == ownerName, ct);
    }

    public async  Task<Owner?> CreateOwner(Owner owner, CancellationToken ct)
    {
       await _db.Owners.AddAsync(owner,ct);
        await _db.SaveChangesAsync(ct); 
        return owner;
    }
}