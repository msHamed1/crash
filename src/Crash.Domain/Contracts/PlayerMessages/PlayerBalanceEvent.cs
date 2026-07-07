using Crash.Domain.Contracts.Common;

namespace Crash.Domain.Contracts.PlayerMessages;

public sealed record PlayerBalanceEvent : MessageEnvelope<PlayerBalanceData>
{
    public override string MessageType => "player-balance";
}

public sealed record PlayerBalanceData
{
    public string PlayerId { get; init; } = default!;
    public string PlayerCode { get; init; } = default!;
    public decimal Amount { get; init; }
    public required string Currency { get; init; }
}