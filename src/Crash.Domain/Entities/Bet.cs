using System.Text.Json.Serialization;

namespace Crash.Domain.Entities;

public class Bet
{
    // Database identity.
    public long Id { get; set; }

    // Public/idempotency identity exposed outside the database.
    public required string BetId { get; set; }
 
    public long PlayerId { get; set; }
    public Player Player { get; set; } = null!;

    public long RoundId { get; set; }
    public Round Round { get; set; } = null!;

    public decimal StakeAmount { get; set; }
    public required string Currency { get; init; } = "USD";

    // Optional multiplier selected when the bet is placed.
    public decimal? AutoCashoutMultiplier { get; set; }

    public bool? AutoCashoutEnabled { get; set; }

    // Actual multiplier at which the player successfully cashed out.
    public decimal? CashedOutAtMultiplier { get; set; }

    // Total returned to the player, including the original stake.
    public decimal PayoutAmount { get; set; }

    // Payout minus stake. Negative for a losing bet.
    public decimal Pl { get; set; }

    public BetStatus Status { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? AcceptedAt { get; set; }
    public DateTimeOffset? CashedOutAt { get; set; }
    public DateTimeOffset? SettledAt { get; set; }
}


[JsonConverter(typeof(JsonStringEnumConverter))]
public enum BetStatus
{
    Placed, // Internal Memory check ;
    Accepted,// Accepted by Our DataBase
    Rejected,
    Canceled,
    CashedOut,
    Lost,
    Won,
    Cashback
}