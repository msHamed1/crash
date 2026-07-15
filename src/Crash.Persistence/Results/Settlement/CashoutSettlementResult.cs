namespace Crash.Persistence.Results.Settlement;

public sealed record CashoutSettlementResult
{
    public bool Succeeded { get; init; }
    public required string BetId { get; init; }
    public long PlayerId { get; init; }
    public long RoundId { get; init; }
    public decimal CashoutMultiplier { get; init; }
    public decimal PayoutAmount { get; init; }
    public decimal NetResultAmount { get; init; }
    public decimal UpdatedBalance { get; init; }
    public DateTimeOffset SettledAt { get; init; }
}
