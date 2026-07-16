namespace Crash.Contracts.Messaging.DbWorkers;

public enum DbWorkerResultStatus
{
    Committed,
    AlreadyProcessed,
    InvestigationRequired
}

public enum DbWorkerResultMessageType
{
    Bet,
    Result,
    Rollback
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
    string? ErrorCode,
    string? ErrorMessage,
    DateTimeOffset CompletedAt);