namespace Crash.Domain.Entities;

public class Player
{
    
    public long Id { get; set; }
    public required string ExternalId { get; set; }
    public required string Type { get; init; } = "FUN";
    public decimal BalanceInUSD { get; set; }
    
    
    public long? TableId { get; set; }
    
}
