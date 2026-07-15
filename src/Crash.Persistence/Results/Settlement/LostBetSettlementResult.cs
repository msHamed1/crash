namespace Crash.Persistence.Results.Settlement;

public sealed record LostBetSettlementResult(
    string BetId,
    long PlayerId,
    DateTimeOffset SettledAt);