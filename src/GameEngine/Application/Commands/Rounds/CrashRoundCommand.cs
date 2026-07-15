namespace GameEngine.Application.Commands.Rounds;

public sealed record CrashRoundCommand : GameCommand
{
    public decimal CurrentMultiplier { get; init; }
    public long TickSequence { get; init; }
    public override string MessageType => "Crash";
}
