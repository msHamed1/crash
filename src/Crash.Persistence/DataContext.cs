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
    public DbSet<AppLog> AppLogs  => Set<AppLog>();
    
    
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Bet>()
            .HasOne(b => b.Round)
            .WithMany(r => r.Bets)
            .HasForeignKey(b => b.RoundId);

        modelBuilder.Entity<Bet>()
            .Property(b => b.Status)
            .HasConversion<string>();
        
        modelBuilder.Entity<Bet>()
            .Property(b => b.BetId)
            .IsRequired();

        modelBuilder.Entity<Bet>()
            .HasIndex(b => b.BetId)
            .IsUnique();
        
        
        modelBuilder.Entity<Player>();
        modelBuilder.Entity<Round>().HasOne(r=>r.Table).WithMany(t=>t.Rounds).HasForeignKey(r=>r.TableId);
        modelBuilder.Entity<Round>().HasMany(r=>r.Bets).WithOne(b=>b.Round).HasForeignKey(b=>b.RoundId);

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
