using Crash.Contracts.Messaging.Common;

namespace Crash.Contracts.Messaging.GatewayToEngine.Bets;

/// <summary>
/// RabbitMQ fact emitted after the gateway accepts a player's WebSocket bet request.
/// </summary>
public sealed record PlaceBetRequested : MessageEnvelope<PlaceBetRequestPayload>
{
    public override string MessageType => "place-bet";
}

public sealed record PlaceBetRequestPayload
{
    public required string RoundId { get; init; }
    public required string PlayerId { get; init; }
    public decimal Amount { get; init; }
    public required string Currency { get; init; }
    public decimal? AutoCashoutAt { get; init; }
    public bool? AutoCashoutEnabled { get; init; }
}
