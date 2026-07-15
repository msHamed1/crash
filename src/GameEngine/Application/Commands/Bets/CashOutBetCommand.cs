namespace GameEngine.Application.Commands.Bets;

public sealed record CashOutBetCommand : GameCommand
{
    public required string PlayerId { get; init; }
    public decimal CurrentMultiplier { get; init; }
    public required string BetId { get; init; }
    public override string MessageType => "Cashout";
}
