using Crash.Domain.Contracts.Common;

namespace Crash.Domain.Contracts.PlayerMessages;

public sealed record PlayerJoinedEvent : MessageEnvelope<PlayerJoinedData>
{
    public override string MessageType => "player-joined";
}

public sealed record PlayerJoinedData
{
    public string PlayerId { get; init; } = default!;
    public string PlayerCode { get; init; } = default!;
}