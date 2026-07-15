namespace RealtimeGateway.Contracts.WebSockets.Inbound;

/// <summary>
/// Payload accepted directly from the player's SignalR/WebSocket connection.
/// Player identity is intentionally excluded because it comes from the authenticated connection.
/// </summary>
public sealed record PlaceBetRequest
{
    public required string RoundId { get; init; }
    public string? CorrelationId { get; init; }
    public decimal Amount { get; init; }
    public required string Currency { get; init; }
    public decimal? AutoCashoutAt { get; init; }
    public bool? AutoCashoutEnabled { get; init; }
}
