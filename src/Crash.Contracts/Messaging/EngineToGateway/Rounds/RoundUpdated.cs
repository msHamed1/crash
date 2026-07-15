namespace Crash.Contracts.Messaging.EngineToGateway.Rounds;

public sealed record RoundUpdated : GatewayMessage
{
    public override string MessageType => "RoundTick";
    public long RoundId { get; init; }
    public decimal CurrentMultiplier { get; init; }
    public long TickSequence { get; init; }
}
