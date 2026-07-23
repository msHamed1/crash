namespace Crash.Contracts.Messaging.DbWorkers;

public enum DbWorkerResultStatus
{
    Committed,
    AlreadyProcessed,
    Rejected,
    InvestigationRequired
}

public enum DbWorkerResultMessageType
{
    BetAccepted,
    BetSettled,
    BetCancelled
}

public sealed record BetPersistenceResult(
    Guid MessageId,

    // MessageId of the original BetAcceptedForPersistence command.
    Guid CausationMessageId,

    string BetId,
    long PlayerId,
    long TableId,
    long RoundId,
    long Sequence,
    DbWorkerResultMessageType Type,
    DbWorkerResultStatus Status,
    BetSettlementStatus? SettlementStatus,
    decimal UpdatedBalance,
    decimal PayoutAmount,
    decimal ProfitLoss,
    decimal? CashoutMultiplier,
    DateTimeOffset? SettledAt,
    string? ErrorCode,
    string? ErrorMessage,
    DateTimeOffset CompletedAt);
