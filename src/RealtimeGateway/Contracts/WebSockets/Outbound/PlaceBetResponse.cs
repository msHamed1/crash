using Crash.Domain.Entities;

namespace RealtimeGateway.Contracts.WebSockets.Outbound;

/// <summary>
/// WebSocket response returned to the player for both accepted and rejected bet requests.
/// </summary>
public sealed record PlaceBetResponse
{
    public required string MessageType { get; init; }
    public long TableId { get; init; }
    public required string MessageId { get; init; }
    public long PlayerId { get; init; }
    public bool Accepted { get; init; }
    public decimal UpdatedBalance { get; init; }
    public Bet? Bet { get; init; }
    public string? Code { get; init; }
    public string? Reason { get; init; }
}
