namespace Crash.Domain.Entities;

public class Bet
{
    public int Id { get; set; }
    public int PlayerId { get; set; }
    public required Player Player { get; set; }
    public int Amount { get; set; }
    public required string Currency { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? SettledAt { get; set; }
    public required string Status { get; set; }
    public required string RoundId { get; set; }
    public required Round Round { get; set; }
}
