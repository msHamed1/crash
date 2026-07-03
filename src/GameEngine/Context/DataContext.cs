using GameEngine.Entities;
using Microsoft.EntityFrameworkCore;
namespace GameEngine.Context;

public class DataContext: DbContext
{
    
    public DataContext(DbContextOptions<DataContext> options) : base(options)
    {}
    
    public DbSet<Bet> Bets => Set<Bet>();
    public DbSet<Player> Players => Set<Player>();
    public DbSet<Round> Rounds  => Set<Round>();
    public DbSet<Table> Tables  => Set<Table>();
    
    
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Bet>();
        modelBuilder.Entity<Player>();
        modelBuilder.Entity<Round>();  
        modelBuilder.Entity<Table>();   
    }
    
}