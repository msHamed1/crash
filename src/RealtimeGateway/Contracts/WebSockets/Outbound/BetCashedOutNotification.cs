namespace RealtimeGateway.Contracts.WebSockets.Outbound;

public sealed record BetCashedOutNotification
{
    public required string MessageType { get; init; }
    public required string MessageId { get; init; }
    public required string BetId { get; init; }
    public long RoundId { get; init; }
    public decimal CashoutMultiplier { get; init; }
    public decimal PayoutAmount { get; init; }
    public decimal NetResultAmount { get; init; }
    public decimal UpdatedBalance { get; init; }
    public DateTimeOffset CashedOutAt { get; init; }
}
