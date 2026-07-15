namespace GameEngine.Application.Commands.Bets;

public sealed record PlaceBetCommand : GameCommand
{
    public required string PlayerId { get; init; }
    public decimal Amount { get; init; }
    public required string Currency { get; init; }
    public required string CorrelationId { get; init; }
    public override string MessageType => "Bet";
    
    public decimal? AutoCashoutMultiplier { get; init; }
    public bool? AutoCashoutEnabled { get; init; }

}
