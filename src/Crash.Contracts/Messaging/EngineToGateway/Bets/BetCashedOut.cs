namespace Crash.Contracts.Messaging.EngineToGateway.Bets;

/// <summary>
/// Published by GameEngine after the cashout and balance credit commit successfully.
/// </summary>
public sealed record BetCashedOut : GatewayMessage
{
    public override string MessageType => "BetCashedOut";
    public long PlayerId { get; init; }
    public required string BetId { get; init; }
    public long RoundId { get; init; }
    public decimal CashoutMultiplier { get; init; }
    public decimal PayoutAmount { get; init; }
    public decimal NetResultAmount { get; init; }
    public decimal UpdatedBalance { get; init; }
    public DateTimeOffset CashedOutAt { get; init; }
}
