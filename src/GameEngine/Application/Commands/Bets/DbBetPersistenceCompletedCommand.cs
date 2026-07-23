using Crash.Contracts.Messaging.DbWorkers;

namespace GameEngine.Application.Commands.Bets;

public record DbBetPersistenceCompletedCommand: GameCommand
{
    public required Guid CausationMessageId { get; init; }
    public required string BetId { get; init; }
    public required DbWorkerResultStatus Status { get; init; }
    public required DbWorkerResultMessageType ResultType { get; init; }
    public BetSettlementStatus? SettlementStatus { get; init; }
    public decimal UpdatedBalance { get; init; }
    public decimal PayoutAmount { get; init; }
    public decimal ProfitLoss { get; init; }
    public decimal? CashoutMultiplier { get; init; }
    public DateTimeOffset? SettledAt { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public long PlayerId { get; init; }

    public override string MessageType  => "BetPersistenceCompleted";
}
