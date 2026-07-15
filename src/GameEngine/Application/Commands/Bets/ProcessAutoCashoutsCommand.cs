namespace GameEngine.Application.Commands.Bets;

public sealed record ProcessAutoCashoutsCommand
    : GameCommand
{
    public decimal CurrentMultiplier { get; init; }
    public override string MessageType =>
        "ProcessAutoCashouts";
}