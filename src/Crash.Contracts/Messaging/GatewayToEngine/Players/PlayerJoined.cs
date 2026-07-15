using Crash.Contracts.Messaging.Common;

namespace Crash.Contracts.Messaging.GatewayToEngine.Players;

public sealed record PlayerJoined : MessageEnvelope<PlayerJoinedPayload>
{
    public override string MessageType => "player-joined";
}

public sealed record PlayerJoinedPayload
{
    public required string PlayerId { get; init; }
    public required string PlayerCode { get; init; }
}
