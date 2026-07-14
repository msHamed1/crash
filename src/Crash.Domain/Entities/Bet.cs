using System.Text.Json.Serialization;

namespace Crash.Domain.Entities;

public class Bet
{
    public long Id { get; set; }
    public required string BetId { get; set; }
    public long PlayerId { get; set; }
    public Player Player { get; set; } = null!;

    public long RoundId { get; set; }
    public Round Round { get; set; } = null!;

    public decimal Amount { get; set; }
    public decimal WinAmount { get; set; } = 0;
    public required string Currency { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? SettledAt { get; set; }

    public BetStatus Status { get; set; }
}


[JsonConverter(typeof(JsonStringEnumConverter))]
public enum BetStatus
{
    Placed, // Internal Memory check ;
    Accepted,// Accepted by Our DataBase
    Rejected,
    Canceled
}