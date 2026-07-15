namespace Crash.Contracts.Messaging.EngineToGateway.Rounds;

public sealed record RoundCrashed : GatewayMessage
{
    public override string MessageType => "RoundCrashed";
    public long RoundId { get; init; }
    public decimal CurrentMultiplier { get; init; } = 1.00m;
    public bool IsCrashed { get; init; }
    public long TickSequence { get; init; }
}
