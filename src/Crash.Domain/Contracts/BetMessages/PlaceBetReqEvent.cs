using Crash.Domain.Contracts.Common;

namespace Crash.Domain.Contracts.BetMessages;

public sealed record PlaceBetReqEvent : MessageEnvelope<PlaceBetReqData>
{
    public override string MessageType =>  "place-bet";
}

public sealed record PlaceBetReqData
{
    public string RoundId { get; init; } = default!;
    public string PlayerId { get; init; } = default!;
    
    public string CorrelationId { get; init; }= default!;
    public decimal Amount { get; init; }
    public required string Currency { get; init; }
    public decimal? AutoCashoutAt { get; init; }
    public bool? AutoCashoutEnabled { get; init; }
}