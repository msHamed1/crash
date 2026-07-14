 

using Crash.Domain.Entities;
using Crash.Persistence;
using Microsoft.EntityFrameworkCore;

namespace GameEngine.Seeders;


public interface IDatabaseSeeder
{
    Task SeedAsync(CancellationToken ct);
}

public sealed class TablesSeeder : IDatabaseSeeder
{
    private readonly DataContext _db;

    public TablesSeeder(DataContext db)
    {
        _db = db;
    }

    public async Task SeedAsync(CancellationToken ct)
    {
        if (await _db.Tables.AnyAsync(ct))
            return;

        _db.Tables.AddRange(
            new Table
            {
                TableName = "Crash Table 1",
                Rounds= new List<Round>()  ,
                
            }
            // new Table
            // {
            //     TableName = "Crash Table 2",
            //     Rounds= new List<Round>()  ,
            //
            // },
            // new Table
            // {
            //     TableName = "Crash Table 3",
            //     Rounds= new List<Round>()  ,
            //
            // }
        );

        await _db.SaveChangesAsync(ct);
    }
}