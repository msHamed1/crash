namespace Crash.Domain.State.Settlement;

/// <summary>
/// Immutable snapshot of an in-memory bet eligible for settlement.
/// </summary>
public sealed record BetSettlementCandidate(
    string BetId,
    long PlayerId,
    long RoundId,
    decimal StakeAmount,
    decimal CashoutMultiplier);
