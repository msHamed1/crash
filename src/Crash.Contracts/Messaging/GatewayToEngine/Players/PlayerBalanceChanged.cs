using Crash.Contracts.Messaging.Common;

namespace Crash.Contracts.Messaging.GatewayToEngine.Players;

public sealed record PlayerBalanceChanged : MessageEnvelope<PlayerBalanceChangedPayload>
{
    public override string MessageType => "player-balance";
}

public sealed record PlayerBalanceChangedPayload
{
    public required string PlayerId { get; init; }
    public required string PlayerCode { get; init; }
    public decimal Amount { get; init; }
    public required string Currency { get; init; }
}
