namespace Crash.Contracts.Messaging.EngineToGateway.Bets;

public sealed record BetRejected : GatewayMessage
{
    public override string MessageType => "PlayerBetRejected";
    public long PlayerId { get; init; }
    public decimal UpdatedBalance { get; init; }
    public required string Code { get; init; }
    public required string Reason { get; init; }
}
