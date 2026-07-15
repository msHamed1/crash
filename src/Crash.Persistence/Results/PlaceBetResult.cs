using Crash.Domain.Entities;

namespace Crash.Persistence.Results;

public enum PlaceBetStatus
{
    Accepted,
    RoundNotBettable,
    PlayerNotFound,
    InsufficientBalance,
    TableOwnershipLost,
    DuplicateBet
}

/// <summary>
/// Carries the durable outcome of the balance reservation and bet insert transaction.
/// </summary>
public sealed record PlaceBetResult
{
    public required PlaceBetStatus Status { get; init; }
    public required string Message { get; init; }
    public decimal UpdatedBalance { get; init; }
    public Bet? Bet { get; init; }

    public bool IsAccepted => Status == PlaceBetStatus.Accepted;
    public string Code => Status switch
    {
        PlaceBetStatus.Accepted => "BET_ACCEPTED",
        PlaceBetStatus.RoundNotBettable => "ROUND_NOT_BETTABLE",
        PlaceBetStatus.PlayerNotFound => "PLAYER_NOT_FOUND",
        PlaceBetStatus.InsufficientBalance => "INSUFFICIENT_BALANCE",
        PlaceBetStatus.TableOwnershipLost => "TABLE_OWNERSHIP_LOST",
        PlaceBetStatus.DuplicateBet => "DUPLICATE_BET",
        _ => "BET_REJECTED"
    };

    public static PlaceBetResult Success(Bet bet, decimal updatedBalance) => new()
    {
        Status = PlaceBetStatus.Accepted,
        Message = "Bet accepted.",
        UpdatedBalance = updatedBalance,
        Bet = bet
    };

    public static PlaceBetResult Rejected(PlaceBetStatus status, string message) => new()
    {
        Status = status,
        Message = message
    };
}
