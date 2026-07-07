using Crash.Domain.Contracts.Common;

namespace Crash.Domain.Contracts.BetMessages;

public sealed record CashoutBetEvent : MessageEnvelope<CashoutBetData>
{
    public override string MessageType =>  "cash-out";
}

public sealed record CashoutBetData
{
    public string RoundId { get; init; } = default!;
    public string PlayerId { get; init; } = default!;
    public decimal Multiplier { get; init; }
    public required string BetId { get; init; }
}