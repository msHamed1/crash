namespace GameEngine.Entities;

public class Bet
{
    public int Id { get; set; }
    public int PlayerId { get; set; }
    public Player Player { get; set; }
    public int Amount { get; set; }
    public string Currency { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? SettledAt { get; set; }
    public string Status { get; set; }
    public string RoundId { get; set; }
    public Round Round { get; set; }
}