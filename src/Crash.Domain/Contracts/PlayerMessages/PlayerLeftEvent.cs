using Crash.Domain.Contracts.Common;

namespace Crash.Domain.Contracts.PlayerMessages;

public sealed record PlayerLeftEvent : MessageEnvelope<PlayerLeftData>
{
    public override string MessageType =>"player-left";
}

public sealed record PlayerLeftData
{
    public string PlayerId { get; init; } = default!;
    public string PlayerCode { get; init; } = default!;
}