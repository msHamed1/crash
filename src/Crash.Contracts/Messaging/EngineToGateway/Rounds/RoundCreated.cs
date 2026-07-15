namespace Crash.Contracts.Messaging.EngineToGateway.Rounds;

public sealed record RoundCreated : GatewayMessage
{
    public override string MessageType => "NewRoundInfo";
    public long RoundId { get; init; }
    public decimal CurrentMultiplier { get; init; } = 1.00m;
    public DateTimeOffset StartsAt { get; init; }
    public bool IsCrashed { get; init; }
}
