namespace Crash.Contracts.Messaging.EngineToGateway.Rounds;

public sealed record RoundStateSnapshot : GatewayMessage
{
    public override string MessageType => "CurrentState";
    public long PlayerId { get; init; }
    public long RoundId { get; init; }
    public decimal CurrentMultiplier { get; init; } = 1.00m;
    public DateTimeOffset StartsAt { get; init; }
    public bool IsCrashed { get; init; }
    public string? ConnectionId { get; init; }
}
