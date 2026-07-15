namespace GameEngine.Application.Commands.Rounds;

public sealed record AdvanceRoundCommand : GameCommand
{
    public decimal CurrentMultiplier { get; init; }
    public long TickSequence { get; init; }
    public override string MessageType => "Tick";
}
