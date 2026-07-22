using Crash.Contracts.Messaging.DbWorkers;

namespace GameEngine.Application.Commands.Bets;

public record DbBetPersistenceCompletedCommand: GameCommand
{
    public required Guid CausationMessageId { get; init; }
    public required string BetId { get; init; }
    public required DbWorkerResultStatus Status { get; init; }
    public string? ErrorCode { get; init; }
    public bool IsCreated { get; init; }
    
    public long PlayerId { get; init; }

    public override string MessageType  => "BetPersistenceCompleted";
}