namespace Crash.Domain.Entities;

public class Player
{
    
    public int Id { get; set; }
    public required string ExternalId { get; set; }
    public decimal Balance { get; set; }
    
}
