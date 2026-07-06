using Crash.Domain.Entities;
using Microsoft.EntityFrameworkCore;

namespace Crash.Persistence;

public class DataContext: DbContext
{
    
    public DataContext(DbContextOptions<DataContext> options) : base(options)
    {}
    
    public DbSet<Bet> Bets => Set<Bet>();
    public DbSet<Player> Players => Set<Player>();
    public DbSet<Round> Rounds  => Set<Round>();
    public DbSet<Table> Tables  => Set<Table>();
    
    
    public DbSet<Owner> Owners  => Set<Owner>();    
    
    
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Bet>();
        modelBuilder.Entity<Player>();
        modelBuilder.Entity<Round>();  
        modelBuilder.Entity<Table>();   
        modelBuilder.Entity<Owner>();

        modelBuilder.Entity<Table>()
            .Property(t => t.CreatedAt)
            .HasDefaultValueSql("CURRENT_TIMESTAMP(6)");
    }
    
    
    public override Task<int> SaveChangesAsync(CancellationToken ct = default)
    {
        var now = DateTimeOffset.UtcNow;

        foreach (var entry in ChangeTracker.Entries<BaseEntity>())
        {
            if (entry.State == EntityState.Added)
            {
                entry.Entity.CreatedAt = now;
                entry.Entity.UpdatedAt = now;
            }

            if (entry.State == EntityState.Modified)
            {
                entry.Entity.UpdatedAt = now;
            }
        }

        return base.SaveChangesAsync(ct);
    }
    
}
