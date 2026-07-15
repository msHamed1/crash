using Crash.Contracts.Messaging.Common;

namespace Crash.Contracts.Messaging.GatewayToEngine.Bets;

public sealed record CashOutRequested : MessageEnvelope<CashOutRequestPayload>
{
    public override string MessageType => "cash-out";
}

public sealed record CashOutRequestPayload
{
    public required string RoundId { get; init; }
    public required string PlayerId { get; init; }
    public decimal Multiplier { get; init; }
    public required string BetId { get; init; }
}
