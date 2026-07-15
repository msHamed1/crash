using Crash.Contracts.Messaging.Common;

namespace Crash.Contracts.Messaging.GatewayToEngine.Players;

public sealed record PlayerLeft : MessageEnvelope<PlayerLeftPayload>
{
    public override string MessageType => "player-left";
}

public sealed record PlayerLeftPayload
{
    public required string PlayerId { get; init; }
    public required string PlayerCode { get; init; }
}
