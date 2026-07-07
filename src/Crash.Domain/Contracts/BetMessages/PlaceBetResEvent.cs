using Crash.Domain.Contracts.Common;

namespace Crash.Domain.Contracts.BetMessages;

public sealed record PlaceBetResEvent : MessageEnvelope<PlaceBetResData>
{
    public override string MessageType =>  "place-bet";
}

public sealed record PlaceBetResData
{
    public required string Message { get; init; }
    public bool Accepted { get; init; }

    public string RoundId { get; init; } = default!;
    public string PlayerId { get; init; } = default!;
    public decimal Amount { get; init; }

    public string BetId { get; init; } = default!;
}