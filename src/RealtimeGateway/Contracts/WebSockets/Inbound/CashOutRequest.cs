namespace RealtimeGateway.Contracts.WebSockets.Inbound;

public sealed record CashOutRequest
{
    public required string RoundId { get; init; }
    public required string BetId { get; init; }
    public string? CorrelationId { get; init; }
}
